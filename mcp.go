package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// The MCP server gives the agent read-only lookups into the same Square
// account the poller watches. It speaks JSON-RPC 2.0 over stdio, one
// message per line. The access token is a secret setting, so it is not
// in this process's environment: it is fetched from kojo on first use.
// Nothing here writes to Square.

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

const mcpProtocolVersion = "2024-11-05"

// mcpSupportedVersions are the protocol revisions this server speaks.
// Only tools/list and tools/call are implemented, and those have the
// same shape in all of them.
var mcpSupportedVersions = map[string]bool{
	"2024-11-05": true,
	"2025-03-26": true,
	"2025-06-18": true,
}

const (
	mcpCallTimeout   = 2 * time.Minute
	mcpDefaultLimit  = 20
	mcpMaxLimit      = 100
	mcpMaxOrderLimit = 100
)

// squareIDRe is the shape of the Square object IDs a tool accepts. It
// is loose (refund IDs are long and contain '_') but keeps path
// separators and query syntax out.
var squareIDRe = regexp.MustCompile(`^[A-Za-z0-9_-]{1,255}$`)

type mcpTool struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"inputSchema"`
	call        func(ctx context.Context, s *mcpServer, args map[string]any) (any, error)
}

type mcpServer struct {
	cfg Config
	log *slog.Logger
	// secrets fetches the extension's secret settings from kojo.
	secrets func(ctx context.Context) (map[string]string, error)
	// newClient builds the Square client; tests point it elsewhere.
	newClient func(base, token string) *SquareClient

	mu     sync.Mutex
	client *SquareClient
}

func runMCP(ctx context.Context, env Env, in io.Reader, out io.Writer, log *slog.Logger) error {
	srv := &mcpServer{cfg: env.Config, log: log, newClient: newSquareClient}
	if env.APIBase != "" && env.Token != "" {
		srv.secrets = newKojoClient(env.APIBase, env.Token, env.AgentID).Secrets
	} else {
		srv.secrets = func(context.Context) (map[string]string, error) {
			return nil, errors.New("KOJO_API_BASE and the extension token are missing; this server is started by kojo")
		}
	}
	return srv.serve(ctx, in, out)
}

// serve reads requests until stdin closes. Tool calls run concurrently,
// so a notifications/cancelled for one in flight can stop it; everything
// else is answered in order.
func (s *mcpServer) serve(ctx context.Context, in io.Reader, out io.Writer) error {
	sc := bufio.NewScanner(in)
	sc.Buffer(make([]byte, 0, 64<<10), 8<<20)
	enc := json.NewEncoder(out)
	var (
		outMu    sync.Mutex
		writeErr error
		wg       sync.WaitGroup
		calls    sync.Map // request ID (raw JSON) → context.CancelFunc
		// writeFailed is closed once an answer cannot be written, so
		// serve ends without waiting for another input line.
		writeFailed = make(chan struct{})
	)
	// stop cancels every running call; it fires on return and as soon as
	// an answer cannot be written, so no wait hangs on calls whose
	// answers have nowhere to go.
	ctx, stop := context.WithCancel(ctx)
	write := func(resp rpcResponse) {
		outMu.Lock()
		defer outMu.Unlock()
		if writeErr == nil {
			if writeErr = enc.Encode(resp); writeErr != nil {
				close(writeFailed)
				stop()
			}
		}
	}
	defer wg.Wait()
	defer stop()
	// Lines are read on their own goroutine so that a cancelled ctx
	// (SIGTERM) ends serve even while stdin stays open.
	lines := make(chan string)
	go func() {
		defer close(lines)
		for sc.Scan() {
			select {
			case lines <- sc.Text():
			case <-ctx.Done():
				return
			}
		}
	}()
	for {
		var raw string
		select {
		case <-writeFailed:
			outMu.Lock()
			defer outMu.Unlock()
			return writeErr
		case <-ctx.Done():
			outMu.Lock()
			defer outMu.Unlock()
			if writeErr != nil {
				return writeErr
			}
			return ctx.Err()
		case l, ok := <-lines:
			if !ok {
				return s.finish(&wg, &outMu, &writeErr, sc)
			}
			raw = l
		}
		outMu.Lock()
		failed := writeErr
		outMu.Unlock()
		if failed != nil {
			return failed
		}
		line := strings.TrimSpace(raw)
		if line == "" {
			continue
		}
		var req rpcRequest
		if err := json.Unmarshal([]byte(line), &req); err != nil {
			write(rpcResponse{JSONRPC: "2.0", ID: json.RawMessage("null"), Error: &rpcError{Code: -32700, Message: "parse error"}})
			continue
		}
		switch {
		case req.Method == "notifications/cancelled":
			var p struct {
				RequestID json.RawMessage `json:"requestId"`
			}
			if json.Unmarshal(req.Params, &p) == nil {
				if cancel, ok := calls.Load(string(p.RequestID)); ok {
					cancel.(context.CancelFunc)()
				}
			}
		case req.Method == "tools/call" && len(req.ID) > 0:
			key := string(req.ID)
			callCtx, cancel := context.WithCancel(ctx)
			calls.Store(key, cancel)
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer calls.Delete(key)
				defer cancel()
				resp, _ := s.handle(callCtx, req)
				// A cancelled request gets no answer.
				if callCtx.Err() == nil || ctx.Err() != nil {
					write(resp)
				}
			}()
		default:
			if resp, reply := s.handle(ctx, req); reply {
				write(resp)
			}
		}
	}
}

// finish waits for the calls still running once stdin has closed.
func (s *mcpServer) finish(wg *sync.WaitGroup, outMu *sync.Mutex, writeErr *error, sc *bufio.Scanner) error {
	wg.Wait()
	outMu.Lock()
	defer outMu.Unlock()
	if *writeErr != nil {
		return *writeErr
	}
	return sc.Err()
}

func (s *mcpServer) handle(ctx context.Context, req rpcRequest) (rpcResponse, bool) {
	// A request without an id is a notification and gets no answer.
	notification := len(req.ID) == 0
	resp := rpcResponse{JSONRPC: "2.0", ID: req.ID}
	switch req.Method {
	case "initialize":
		var p struct {
			ProtocolVersion string `json:"protocolVersion"`
		}
		_ = json.Unmarshal(req.Params, &p)
		pv := p.ProtocolVersion
		if !mcpSupportedVersions[pv] {
			pv = mcpProtocolVersion
		}
		resp.Result = map[string]any{
			"protocolVersion": pv,
			"capabilities":    map[string]any{"tools": map[string]any{}},
			"serverInfo":      map[string]any{"name": "kojo-ext-square", "version": version},
		}
	case "ping":
		resp.Result = map[string]any{}
	case "tools/list":
		resp.Result = map[string]any{"tools": mcpTools}
	case "tools/call":
		var p struct {
			Name      string         `json:"name"`
			Arguments map[string]any `json:"arguments"`
		}
		if err := json.Unmarshal(req.Params, &p); err != nil {
			resp.Error = &rpcError{Code: -32602, Message: "invalid params"}
			break
		}
		resp.Result = s.callTool(ctx, p.Name, p.Arguments)
	default:
		if notification {
			return resp, false
		}
		resp.Error = &rpcError{Code: -32601, Message: "method not found: " + req.Method}
	}
	return resp, !notification
}

func toolResult(text string, isError bool) map[string]any {
	return map[string]any{
		"content": []map[string]any{{"type": "text", "text": text}},
		"isError": isError,
	}
}

func (s *mcpServer) callTool(ctx context.Context, name string, args map[string]any) map[string]any {
	for _, t := range mcpTools {
		if t.Name != name {
			continue
		}
		ctx, cancel := context.WithTimeout(ctx, mcpCallTimeout)
		defer cancel()
		out, err := t.call(ctx, s, args)
		if err != nil {
			if isAuthError(err) {
				// Forget the client so the next call picks up whatever
				// token is saved by then.
				s.mu.Lock()
				s.client = nil
				s.mu.Unlock()
				return toolResult(err.Error()+" — Square rejected the access token or it lacks a permission this tool needs (ORDERS_READ, PAYMENTS_READ, MERCHANT_PROFILE_READ, CUSTOMERS_READ, ITEMS_READ). Ask the operator to check it in kojo (Settings → Extensions → Square).", true)
			}
			return toolResult(err.Error(), true)
		}
		data, err := json.MarshalIndent(out, "", "  ")
		if err != nil {
			return toolResult(err.Error(), true)
		}
		return toolResult(string(data), false)
	}
	return toolResult("unknown tool: "+name, true)
}

// square returns the Square client, fetching the token on first use.
func (s *mcpServer) square(ctx context.Context) (*SquareClient, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.client != nil {
		return s.client, nil
	}
	secrets, err := s.secrets(ctx)
	if err != nil {
		return nil, fmt.Errorf("reading the Square access token from kojo: %w", err)
	}
	token := strings.TrimSpace(secrets[secretAccessToken])
	if token == "" {
		return nil, errors.New("no Square access token is configured for this agent; ask the operator to set it in kojo (Settings → Extensions → Square)")
	}
	s.client = s.newClient(s.cfg.BaseURL, token)
	return s.client, nil
}

// locationAllowed reports whether a location is within the configured
// ones. With the setting left empty, every location is; a setting that
// holds no usable ID allows none (the poller treats it the same way).
func (s *mcpServer) locationAllowed(id string) bool {
	return !s.cfg.locationsConfigured() || contains(s.cfg.LocationIDs, id)
}

// errNoUsableLocation is returned while the location setting is broken.
var errNoUsableLocation = errors.New("the location IDs setting for this agent holds no usable ID, so no location may be looked at until the operator fixes it in kojo (Settings → Extensions → Square)")

func (s *mcpServer) checkLocation(id string) error {
	if id != "" && !s.locationAllowed(id) {
		return fmt.Errorf("location %s is not one of the locations configured for this agent (%s)", id, strings.Join(s.cfg.LocationIDs, ", "))
	}
	return nil
}

// --- argument helpers ---

func argString(args map[string]any, key string) string {
	if v, ok := args[key].(string); ok {
		return strings.TrimSpace(v)
	}
	return ""
}

func argStrings(args map[string]any, key string) []string {
	var out []string
	switch v := args[key].(type) {
	case []any:
		for _, item := range v {
			if str, ok := item.(string); ok && strings.TrimSpace(str) != "" {
				out = append(out, strings.TrimSpace(str))
			}
		}
	case string:
		for _, part := range strings.Split(v, ",") {
			if p := strings.TrimSpace(part); p != "" {
				out = append(out, p)
			}
		}
	}
	return out
}

func argLimit(args map[string]any, max int) (int, error) {
	switch v := args["limit"].(type) {
	case nil:
		return min(mcpDefaultLimit, max), nil
	case float64:
		if v < 1 || v != float64(int(v)) {
			return 0, errors.New("limit must be a positive integer")
		}
		return min(int(v), max), nil
	case string:
		n, err := strconv.Atoi(strings.TrimSpace(v))
		if err != nil || n < 1 {
			return 0, errors.New("limit must be a positive integer")
		}
		return min(n, max), nil
	}
	return 0, errors.New("limit must be a positive integer")
}

func argID(args map[string]any, key string, required bool) (string, error) {
	id := argString(args, key)
	if id == "" {
		if required {
			return "", fmt.Errorf("%s is required", key)
		}
		return "", nil
	}
	if !squareIDRe.MatchString(id) {
		return "", fmt.Errorf("%s does not look like a Square ID", key)
	}
	return id, nil
}

// argTime reads an RFC 3339 timestamp and returns it normalised.
func argTime(args map[string]any, key string) (string, error) {
	v := argString(args, key)
	if v == "" {
		return "", nil
	}
	t, err := time.Parse(time.RFC3339, v)
	if err != nil {
		return "", fmt.Errorf("%s must be an RFC 3339 timestamp such as 2026-10-18T09:00:00+09:00", key)
	}
	return t.UTC().Format(time.RFC3339Nano), nil
}

func object(required []string, props map[string]any) map[string]any {
	schema := map[string]any{"type": "object", "properties": props}
	if len(required) > 0 {
		schema["required"] = required
	}
	return schema
}

func prop(typ, desc string) map[string]any {
	return map[string]any{"type": typ, "description": desc}
}

func limitProp(max int) map[string]any {
	return map[string]any{"type": "integer", "minimum": 1, "maximum": max, "description": "Rows per page (default " + strconv.Itoa(min(mcpDefaultLimit, max)) + ", max " + strconv.Itoa(max) + ")."}
}

var cursorProp = prop("string", "Pagination cursor from the previous page's `cursor`.")

// --- compact views for list results ---

type moneyView struct {
	Amount   int64  `json:"amount"`
	Currency string `json:"currency"`
	Display  string `json:"display"`
}

func viewMoney(m *Money) *moneyView {
	if m == nil || m.Currency == "" {
		return nil
	}
	return &moneyView{Amount: m.Amount, Currency: m.Currency, Display: formatMoney(m)}
}

type orderListItem struct {
	ID          string     `json:"id"`
	LocationID  string     `json:"location_id"`
	State       string     `json:"state"`
	CreatedAt   string     `json:"created_at"`
	UpdatedAt   string     `json:"updated_at"`
	ClosedAt    string     `json:"closed_at,omitempty"`
	CustomerID  string     `json:"customer_id,omitempty"`
	ReferenceID string     `json:"reference_id,omitempty"`
	Source      string     `json:"source,omitempty"`
	Total       *moneyView `json:"total,omitempty"`
	Items       string     `json:"items,omitempty"`
	Returned    *moneyView `json:"returned,omitempty"`
	ReturnItems string     `json:"returned_items,omitempty"`
	ReturnOf    string     `json:"return_of,omitempty"`
}

// listOrder is the subset of an order the list view reads.
type listOrder struct {
	Order
	CustomerID  string `json:"customer_id"`
	ReferenceID string `json:"reference_id"`
	Source      *struct {
		Name string `json:"name"`
	} `json:"source"`
}

func viewOrder(o listOrder) orderListItem {
	v := orderListItem{
		ID: o.ID, LocationID: o.LocationID, State: o.State,
		CreatedAt: o.CreatedAt, UpdatedAt: o.UpdatedAt, ClosedAt: o.ClosedAt,
		CustomerID: o.CustomerID, ReferenceID: o.ReferenceID,
		Total: viewMoney(o.TotalMoney), Items: itemSummary(o.LineItems),
	}
	if o.Source != nil {
		v.Source = o.Source.Name
	}
	if o.hasReturns() {
		if o.isPureReturn() {
			v.Total = nil
		}
		v.Returned = viewMoney(o.returnMoney())
		v.ReturnItems = itemSummary(o.returnItems())
		v.ReturnOf = o.returnOf()
	}
	return v
}

type paymentListItem struct {
	ID         string     `json:"id"`
	Status     string     `json:"status"`
	LocationID string     `json:"location_id"`
	OrderID    string     `json:"order_id,omitempty"`
	CustomerID string     `json:"customer_id,omitempty"`
	CreatedAt  string     `json:"created_at"`
	UpdatedAt  string     `json:"updated_at"`
	Total      *moneyView `json:"total,omitempty"`
	Refunded   *moneyView `json:"refunded,omitempty"`
	SourceType string     `json:"source_type,omitempty"`
	Card       string     `json:"card,omitempty"`
	Note       string     `json:"note,omitempty"`
}

type listPayment struct {
	Payment
	CustomerID string `json:"customer_id"`
	Note       string `json:"note"`
	Card       *struct {
		Card *struct {
			Brand string `json:"card_brand"`
			Last4 string `json:"last_4"`
		} `json:"card"`
	} `json:"card_details"`
}

func viewPayment(p listPayment) paymentListItem {
	v := paymentListItem{
		ID: p.ID, Status: p.Status, LocationID: p.LocationID, OrderID: p.OrderID,
		CustomerID: p.CustomerID, CreatedAt: p.CreatedAt, UpdatedAt: p.UpdatedAt,
		Total: viewMoney(moneyOf(p.TotalMoney, p.AmountMoney)), SourceType: p.SourceType,
		Note: truncateRunes(p.Note, 200),
	}
	if p.RefundedMoney != nil && p.RefundedMoney.Amount != 0 {
		v.Refunded = viewMoney(p.RefundedMoney)
	}
	if p.Card != nil && p.Card.Card != nil {
		v.Card = strings.TrimSpace(p.Card.Card.Brand + " " + p.Card.Card.Last4)
	}
	return v
}

type refundListItem struct {
	ID         string     `json:"id"`
	Status     string     `json:"status"`
	LocationID string     `json:"location_id,omitempty"`
	PaymentID  string     `json:"payment_id,omitempty"`
	OrderID    string     `json:"order_id,omitempty"`
	CreatedAt  string     `json:"created_at"`
	UpdatedAt  string     `json:"updated_at"`
	Amount     *moneyView `json:"amount,omitempty"`
	Reason     string     `json:"reason,omitempty"`
}

func viewRefund(r Refund) refundListItem {
	return refundListItem{
		ID: r.ID, Status: r.Status, LocationID: r.LocationID, PaymentID: r.PaymentID,
		OrderID: r.OrderID, CreatedAt: r.CreatedAt, UpdatedAt: r.UpdatedAt,
		Amount: viewMoney(r.AmountMoney), Reason: truncateRunes(r.Reason, 200),
	}
}

type customerListItem struct {
	ID          string `json:"id"`
	GivenName   string `json:"given_name,omitempty"`
	FamilyName  string `json:"family_name,omitempty"`
	CompanyName string `json:"company_name,omitempty"`
	Nickname    string `json:"nickname,omitempty"`
	Email       string `json:"email_address,omitempty"`
	Phone       string `json:"phone_number,omitempty"`
	ReferenceID string `json:"reference_id,omitempty"`
	CreatedAt   string `json:"created_at,omitempty"`
}

type catalogVariation struct {
	ID    string     `json:"id"`
	Name  string     `json:"name,omitempty"`
	SKU   string     `json:"sku,omitempty"`
	Price *moneyView `json:"price,omitempty"`
}

type catalogItem struct {
	ID          string             `json:"id"`
	Name        string             `json:"name"`
	Description string             `json:"description,omitempty"`
	Variations  []catalogVariation `json:"variations,omitempty"`
}

type rawCatalogItem struct {
	ID       string `json:"id"`
	ItemData *struct {
		Name        string `json:"name"`
		Description string `json:"description"`
		Variations  []struct {
			ID            string `json:"id"`
			VariationData *struct {
				Name       string `json:"name"`
				SKU        string `json:"sku"`
				PriceMoney *Money `json:"price_money"`
			} `json:"item_variation_data"`
		} `json:"variations"`
	} `json:"item_data"`
}

func viewCatalogItem(it rawCatalogItem) catalogItem {
	v := catalogItem{ID: it.ID}
	if it.ItemData == nil {
		return v
	}
	v.Name, v.Description = it.ItemData.Name, truncateRunes(it.ItemData.Description, 300)
	for _, vr := range it.ItemData.Variations {
		cv := catalogVariation{ID: vr.ID}
		if vr.VariationData != nil {
			cv.Name, cv.SKU, cv.Price = vr.VariationData.Name, vr.VariationData.SKU, viewMoney(vr.VariationData.PriceMoney)
		}
		v.Variations = append(v.Variations, cv)
	}
	return v
}

// page is what every list tool returns.
type page[T any] struct {
	Items  []T    `json:"items"`
	Cursor string `json:"cursor,omitempty"`
}

// --- tools ---

var mcpTools = []*mcpTool{
	{
		Name:        "list_locations",
		Description: "List the Square locations (stores) this agent may look at, with ID, name, status, currency and timezone.",
		InputSchema: object(nil, map[string]any{}),
		call: func(ctx context.Context, s *mcpServer, _ map[string]any) (any, error) {
			c, err := s.square(ctx)
			if err != nil {
				return nil, err
			}
			var resp struct {
				Locations []struct {
					ID       string `json:"id"`
					Name     string `json:"name"`
					Status   string `json:"status"`
					Currency string `json:"currency"`
					Timezone string `json:"timezone"`
					Type     string `json:"type"`
				} `json:"locations"`
			}
			if err := c.do(ctx, http.MethodGet, "/v2/locations", nil, nil, &resp); err != nil {
				return nil, err
			}
			out := resp.Locations[:0]
			for _, l := range resp.Locations {
				if s.locationAllowed(l.ID) {
					out = append(out, l)
				}
			}
			return out, nil
		},
	},
	{
		Name: "search_orders",
		Description: "Search orders, newest first, as compact rows (ID, state, total, items, customer). " +
			"Use get_order for one order's full detail.",
		InputSchema: object(nil, map[string]any{
			"location_ids":   map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "description": "Location IDs (default: every location this agent may look at, max 10)."},
			"states":         map[string]any{"type": "array", "items": map[string]any{"type": "string", "enum": []string{"OPEN", "COMPLETED", "CANCELED", "DRAFT"}}, "description": "Only orders in these states."},
			"created_after":  prop("string", "Only orders created at or after this RFC 3339 time."),
			"created_before": prop("string", "Only orders created before this RFC 3339 time."),
			"customer_ids":   map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "description": "Only orders of these customers."},
			"limit":          limitProp(mcpMaxOrderLimit),
			"cursor":         cursorProp,
		}),
		call: func(ctx context.Context, s *mcpServer, args map[string]any) (any, error) {
			c, err := s.square(ctx)
			if err != nil {
				return nil, err
			}
			locs := argStrings(args, "location_ids")
			for _, l := range locs {
				if !squareIDRe.MatchString(l) {
					return nil, errors.New("location_ids contains something that is not a Square ID")
				}
				if err := s.checkLocation(l); err != nil {
					return nil, err
				}
			}
			if len(locs) == 0 && s.cfg.locationsConfigured() {
				if locs = s.cfg.LocationIDs; len(locs) == 0 {
					return nil, errNoUsableLocation
				}
			}
			if len(locs) == 0 {
				all, err := c.ListLocations(ctx)
				if err != nil {
					return nil, err
				}
				for _, l := range all {
					if l.Status == "ACTIVE" {
						locs = append(locs, l.ID)
					}
				}
			}
			if len(locs) == 0 {
				return nil, errors.New("the account has no active location")
			}
			if len(locs) > ordersLocationChunk {
				return nil, fmt.Errorf("SearchOrders takes at most %d locations at once; pass location_ids", ordersLocationChunk)
			}
			limit, err := argLimit(args, mcpMaxOrderLimit)
			if err != nil {
				return nil, err
			}
			after, err := argTime(args, "created_after")
			if err != nil {
				return nil, err
			}
			before, err := argTime(args, "created_before")
			if err != nil {
				return nil, err
			}
			filter := map[string]any{}
			if after != "" || before != "" {
				rng := map[string]any{}
				if after != "" {
					rng["start_at"] = after
				}
				if before != "" {
					rng["end_at"] = before
				}
				filter["date_time_filter"] = map[string]any{"created_at": rng}
			}
			if states := argStrings(args, "states"); len(states) > 0 {
				for i, st := range states {
					states[i] = strings.ToUpper(st)
					switch states[i] {
					case "OPEN", "COMPLETED", "CANCELED", "DRAFT":
					default:
						return nil, fmt.Errorf("unknown order state %q", st)
					}
				}
				filter["state_filter"] = map[string]any{"states": states}
			}
			if cust := argStrings(args, "customer_ids"); len(cust) > 0 {
				for _, id := range cust {
					if !squareIDRe.MatchString(id) {
						return nil, errors.New("customer_ids contains something that is not a Square ID")
					}
				}
				filter["customer_filter"] = map[string]any{"customer_ids": cust}
			}
			body := map[string]any{
				"location_ids": locs,
				"limit":        limit,
				"query": map[string]any{
					"filter": filter,
					"sort":   map[string]any{"sort_field": "CREATED_AT", "sort_order": "DESC"},
				},
			}
			if cur := argString(args, "cursor"); cur != "" {
				body["cursor"] = cur
			}
			var resp struct {
				Orders []listOrder `json:"orders"`
				Cursor string      `json:"cursor"`
			}
			if err := c.do(ctx, http.MethodPost, "/v2/orders/search", nil, body, &resp); err != nil {
				return nil, err
			}
			out := page[orderListItem]{Items: []orderListItem{}, Cursor: resp.Cursor}
			for _, o := range resp.Orders {
				out.Items = append(out.Items, viewOrder(o))
			}
			return out, nil
		},
	},
	{
		Name:        "get_order",
		Description: "Get one order in full: line items with modifiers and notes, taxes, discounts, fulfillments, tenders, returns, customer ID.",
		InputSchema: object([]string{"order_id"}, map[string]any{"order_id": prop("string", "Order ID.")}),
		call: func(ctx context.Context, s *mcpServer, args map[string]any) (any, error) {
			id, err := argID(args, "order_id", true)
			if err != nil {
				return nil, err
			}
			return s.getObject(ctx, "/v2/orders/"+url.PathEscape(id), "order", true)
		},
	},
	{
		Name: "list_payments",
		Description: "List payments at one location, newest first, as compact rows (status, total, refunded, order, card). " +
			"Use get_payment for one payment's full detail.",
		InputSchema: object(nil, map[string]any{
			"location_id": prop("string", "Location ID. Required when this agent may look at more than one location; otherwise the default is its only location (or the account's main location)."),
			"begin_time":  prop("string", "Only payments created at or after this RFC 3339 time (Square's default is one year ago)."),
			"end_time":    prop("string", "Only payments created before this RFC 3339 time."),
			"limit":       limitProp(mcpMaxLimit),
			"cursor":      cursorProp,
		}),
		call: func(ctx context.Context, s *mcpServer, args map[string]any) (any, error) {
			loc, err := s.singleLocation(args)
			if err != nil {
				return nil, err
			}
			q, err := listQuery(args)
			if err != nil {
				return nil, err
			}
			if loc != "" {
				q.Set("location_id", loc)
			}
			c, err := s.square(ctx)
			if err != nil {
				return nil, err
			}
			var resp struct {
				Payments []listPayment `json:"payments"`
				Cursor   string        `json:"cursor"`
			}
			if err := c.do(ctx, http.MethodGet, "/v2/payments", q, nil, &resp); err != nil {
				return nil, err
			}
			out := page[paymentListItem]{Items: []paymentListItem{}, Cursor: resp.Cursor}
			for _, p := range resp.Payments {
				if s.locationAllowed(p.LocationID) {
					out.Items = append(out.Items, viewPayment(p))
				}
			}
			return out, nil
		},
	},
	{
		Name:        "get_payment",
		Description: "Get one payment in full: card details, amounts, fees, refunded amount, refund IDs, receipt, order and customer IDs.",
		InputSchema: object([]string{"payment_id"}, map[string]any{"payment_id": prop("string", "Payment ID.")}),
		call: func(ctx context.Context, s *mcpServer, args map[string]any) (any, error) {
			id, err := argID(args, "payment_id", true)
			if err != nil {
				return nil, err
			}
			return s.getObject(ctx, "/v2/payments/"+url.PathEscape(id), "payment", true)
		},
	},
	{
		Name:        "list_refunds",
		Description: "List refunds, newest first, as compact rows (status, amount, payment, order, reason). Use get_refund for full detail.",
		InputSchema: object(nil, map[string]any{
			"location_id": prop("string", "Only refunds at this location (default: every location this agent may look at)."),
			"status":      map[string]any{"type": "string", "enum": []string{"PENDING", "COMPLETED", "REJECTED", "FAILED"}, "description": "Only refunds in this status."},
			"begin_time":  prop("string", "Only refunds created at or after this RFC 3339 time (Square's default is one year ago)."),
			"end_time":    prop("string", "Only refunds created before this RFC 3339 time."),
			"limit":       limitProp(mcpMaxLimit),
			"cursor":      cursorProp,
		}),
		call: func(ctx context.Context, s *mcpServer, args map[string]any) (any, error) {
			loc, err := argID(args, "location_id", false)
			if err != nil {
				return nil, err
			}
			if err := s.checkLocation(loc); err != nil {
				return nil, err
			}
			q, err := listQuery(args)
			if err != nil {
				return nil, err
			}
			if loc != "" {
				q.Set("location_id", loc)
			}
			if st := strings.ToUpper(argString(args, "status")); st != "" {
				switch st {
				case "PENDING", "COMPLETED", "REJECTED", "FAILED":
					q.Set("status", st)
				default:
					return nil, fmt.Errorf("unknown refund status %q", st)
				}
			}
			c, err := s.square(ctx)
			if err != nil {
				return nil, err
			}
			var resp struct {
				Refunds []Refund `json:"refunds"`
				Cursor  string   `json:"cursor"`
			}
			if err := c.do(ctx, http.MethodGet, "/v2/refunds", q, nil, &resp); err != nil {
				return nil, err
			}
			out := page[refundListItem]{Items: []refundListItem{}, Cursor: resp.Cursor}
			for _, r := range resp.Refunds {
				if s.locationAllowed(r.LocationID) {
					out.Items = append(out.Items, viewRefund(r))
				}
			}
			return out, nil
		},
	},
	{
		Name:        "get_refund",
		Description: "Get one refund in full.",
		InputSchema: object([]string{"refund_id"}, map[string]any{"refund_id": prop("string", "Refund ID.")}),
		call: func(ctx context.Context, s *mcpServer, args map[string]any) (any, error) {
			id, err := argID(args, "refund_id", true)
			if err != nil {
				return nil, err
			}
			return s.getObject(ctx, "/v2/refunds/"+url.PathEscape(id), "refund", true)
		},
	},
	{
		Name:        "get_customer",
		Description: "Get one customer profile: name, email, phone, address, note, groups. Customers belong to the whole Square account, not to a location. Needs the CUSTOMERS_READ permission.",
		InputSchema: object([]string{"customer_id"}, map[string]any{"customer_id": prop("string", "Customer ID.")}),
		call: func(ctx context.Context, s *mcpServer, args map[string]any) (any, error) {
			id, err := argID(args, "customer_id", true)
			if err != nil {
				return nil, err
			}
			return s.getObject(ctx, "/v2/customers/"+url.PathEscape(id), "customer", false)
		},
	},
	{
		Name:        "search_customers",
		Description: "Find customers by email address or phone number (partial match), newest first; with neither, list customers. Customers belong to the whole Square account, not to a location. Needs the CUSTOMERS_READ permission.",
		InputSchema: object(nil, map[string]any{
			"email":  prop("string", "Part of the email address."),
			"phone":  prop("string", "Part of the phone number."),
			"limit":  limitProp(mcpMaxLimit),
			"cursor": cursorProp,
		}),
		call: func(ctx context.Context, s *mcpServer, args map[string]any) (any, error) {
			limit, err := argLimit(args, mcpMaxLimit)
			if err != nil {
				return nil, err
			}
			filter := map[string]any{}
			if v := argString(args, "email"); v != "" {
				filter["email_address"] = map[string]any{"fuzzy": v}
			}
			if v := argString(args, "phone"); v != "" {
				filter["phone_number"] = map[string]any{"fuzzy": v}
			}
			body := map[string]any{
				"limit": limit,
				"query": map[string]any{
					"filter": filter,
					"sort":   map[string]any{"field": "CREATED_AT", "order": "DESC"},
				},
			}
			if cur := argString(args, "cursor"); cur != "" {
				body["cursor"] = cur
			}
			c, err := s.square(ctx)
			if err != nil {
				return nil, err
			}
			var resp struct {
				Customers []customerListItem `json:"customers"`
				Cursor    string             `json:"cursor"`
			}
			if err := c.do(ctx, http.MethodPost, "/v2/customers/search", nil, body, &resp); err != nil {
				return nil, err
			}
			out := page[customerListItem]{Items: resp.Customers, Cursor: resp.Cursor}
			if out.Items == nil {
				out.Items = []customerListItem{}
			}
			return out, nil
		},
	},
	{
		Name:        "search_catalog_items",
		Description: "Find catalog items (products) by keyword, with their variations and prices. Needs the ITEMS_READ permission.",
		InputSchema: object(nil, map[string]any{
			"text":   prop("string", "Words in the item name, description or SKU. Omit to list items."),
			"limit":  limitProp(mcpMaxLimit),
			"cursor": cursorProp,
		}),
		call: func(ctx context.Context, s *mcpServer, args map[string]any) (any, error) {
			limit, err := argLimit(args, mcpMaxLimit)
			if err != nil {
				return nil, err
			}
			body := map[string]any{"limit": limit}
			if s.cfg.locationsConfigured() {
				if len(s.cfg.LocationIDs) == 0 {
					return nil, errNoUsableLocation
				}
				// Items enabled at any of this agent's locations.
				body["enabled_location_ids"] = s.cfg.LocationIDs
			}
			if v := argString(args, "text"); v != "" {
				body["text_filter"] = v
			}
			if cur := argString(args, "cursor"); cur != "" {
				body["cursor"] = cur
			}
			c, err := s.square(ctx)
			if err != nil {
				return nil, err
			}
			var resp struct {
				Items  []rawCatalogItem `json:"items"`
				Cursor string           `json:"cursor"`
			}
			if err := c.do(ctx, http.MethodPost, "/v2/catalog/search-catalog-items", nil, body, &resp); err != nil {
				return nil, err
			}
			out := page[catalogItem]{Items: []catalogItem{}, Cursor: resp.Cursor}
			for _, it := range resp.Items {
				out.Items = append(out.Items, viewCatalogItem(it))
			}
			return out, nil
		},
	},
}

// singleLocation picks the location for an endpoint that takes one.
func (s *mcpServer) singleLocation(args map[string]any) (string, error) {
	loc, err := argID(args, "location_id", false)
	if err != nil {
		return "", err
	}
	if loc != "" {
		return loc, s.checkLocation(loc)
	}
	if !s.cfg.locationsConfigured() {
		return "", nil
	}
	switch len(s.cfg.LocationIDs) {
	case 0:
		return "", errNoUsableLocation
	case 1:
		return s.cfg.LocationIDs[0], nil
	}
	return "", fmt.Errorf("location_id is required: this agent may look at %s", strings.Join(s.cfg.LocationIDs, ", "))
}

// listQuery is the query string shared by ListPayments and
// ListPaymentRefunds: time range, newest first, limit, cursor.
func listQuery(args map[string]any) (url.Values, error) {
	q := url.Values{"sort_order": {"DESC"}}
	limit, err := argLimit(args, mcpMaxLimit)
	if err != nil {
		return nil, err
	}
	q.Set("limit", strconv.Itoa(limit))
	for _, key := range []string{"begin_time", "end_time"} {
		v, err := argTime(args, key)
		if err != nil {
			return nil, err
		}
		if v != "" {
			q.Set(key, v)
		}
	}
	if cur := argString(args, "cursor"); cur != "" {
		q.Set("cursor", cur)
	}
	return q, nil
}

// getObject fetches one object and returns it as Square sent it, after
// checking it belongs to a location this agent may look at.
// Customers belong to the whole account, not a location, so they are
// fetched unscoped.
func (s *mcpServer) getObject(ctx context.Context, path, key string, scoped bool) (any, error) {
	c, err := s.square(ctx)
	if err != nil {
		return nil, err
	}
	var resp map[string]json.RawMessage
	if err := c.do(ctx, http.MethodGet, path, nil, nil, &resp); err != nil {
		return nil, err
	}
	raw, ok := resp[key]
	if !ok {
		return nil, fmt.Errorf("Square returned no %s", key)
	}
	var loc struct {
		LocationID string `json:"location_id"`
	}
	_ = json.Unmarshal(raw, &loc)
	if scoped && !s.locationAllowed(loc.LocationID) {
		if loc.LocationID == "" {
			return nil, fmt.Errorf("that %s has no location, and this agent is limited to %s", key, strings.Join(s.cfg.LocationIDs, ", "))
		}
		return nil, fmt.Errorf("that %s belongs to location %s, which is not configured for this agent", key, loc.LocationID)
	}
	return raw, nil
}
