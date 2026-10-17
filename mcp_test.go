package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type capturedReq struct {
	Method, Path, Query, Auth, Version string
	Body                               map[string]any
}

// mcpFake answers Square paths from a table and records every request.
type mcpFake struct {
	mu      sync.Mutex
	reqs    []capturedReq
	answers map[string]string // "METHOD /path" → JSON body
	status  int
}

func (f *mcpFake) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c := capturedReq{Method: r.Method, Path: r.URL.Path, Query: r.URL.RawQuery, Auth: r.Header.Get("Authorization"), Version: r.Header.Get("Square-Version")}
	if data, _ := io.ReadAll(r.Body); len(data) > 0 {
		_ = json.Unmarshal(data, &c.Body)
	}
	f.reqs = append(f.reqs, c)
	if f.status != 0 {
		w.WriteHeader(f.status)
		w.Write([]byte(`{"errors":[{"code":"UNAUTHORIZED","detail":"bad token"}]}`))
		return
	}
	body, ok := f.answers[r.Method+" "+r.URL.Path]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"errors":[{"code":"NOT_FOUND"}]}`))
		return
	}
	w.Write([]byte(body))
}

func (f *mcpFake) last() capturedReq {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reqs[len(f.reqs)-1]
}

func newTestMCP(t *testing.T, cfgJSON string, answers map[string]string) (*mcpServer, *mcpFake, *int) {
	t.Helper()
	fake := &mcpFake{answers: answers}
	ts := httptest.NewServer(fake)
	t.Cleanup(ts.Close)
	cfg := parseConfig(cfgJSON)
	secretCalls := 0
	s := &mcpServer{
		cfg: cfg,
		log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		secrets: func(context.Context) (map[string]string, error) {
			secretCalls++
			return map[string]string{secretAccessToken: "tok"}, nil
		},
		newClient: func(_, token string) *SquareClient {
			c := newSquareClient(ts.URL, token)
			c.sleep = func(context.Context, time.Duration) error { return nil }
			return c
		},
	}
	return s, fake, &secretCalls
}

// call runs one tools/call and returns the text and error flag.
func call(t *testing.T, s *mcpServer, name string, args map[string]any) (string, bool) {
	t.Helper()
	res := s.callTool(context.Background(), name, args)
	content := res["content"].([]map[string]any)
	return content[0]["text"].(string), res["isError"].(bool)
}

func TestMCPProtocol(t *testing.T) {
	s, _, _ := newTestMCP(t, `{}`, nil)
	in := strings.Join([]string{
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18"}}`,
		`{"jsonrpc":"2.0","method":"notifications/initialized"}`,
		`{"jsonrpc":"2.0","id":2,"method":"tools/list"}`,
		`{"jsonrpc":"2.0","id":3,"method":"nope"}`,
		`not json`,
	}, "\n")
	var out bytes.Buffer
	if err := s.serve(context.Background(), strings.NewReader(in), &out); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 4 {
		t.Fatalf("want 4 replies (notification gets none), got %d:\n%s", len(lines), out.String())
	}
	if !strings.Contains(lines[0], `"protocolVersion":"2025-06-18"`) {
		t.Errorf("initialize: %s", lines[0])
	}
	var list struct {
		Result struct {
			Tools []struct {
				Name string `json:"name"`
			} `json:"tools"`
		} `json:"result"`
	}
	json.Unmarshal([]byte(lines[1]), &list)
	var names []string
	for _, tl := range list.Result.Tools {
		names = append(names, tl.Name)
	}
	if strings.Join(names, ",") != "list_locations,search_orders,get_order,list_payments,get_payment,list_refunds,get_refund,get_customer,search_customers,search_catalog_items" {
		t.Errorf("tools: %v", names)
	}
	if !strings.Contains(lines[2], `-32601`) || !strings.Contains(lines[3], `-32700`) {
		t.Errorf("errors: %s / %s", lines[2], lines[3])
	}
}

func TestMCPGetOrderAndLocationGuard(t *testing.T) {
	s, fake, secretCalls := newTestMCP(t, `{"locationIds":"L1"}`, map[string]string{
		"GET /v2/orders/O1": `{"order":{"id":"O1","location_id":"L1","state":"OPEN","line_items":[{"name":"Coffee","quantity":"2","note":"hot"}]}}`,
		"GET /v2/orders/O2": `{"order":{"id":"O2","location_id":"L9","state":"OPEN"}}`,
	})
	text, isErr := call(t, s, "get_order", map[string]any{"order_id": "O1"})
	if isErr || !strings.Contains(text, `"note": "hot"`) {
		t.Fatalf("full order expected: %v %s", isErr, text)
	}
	if r := fake.last(); r.Auth != "Bearer tok" || r.Version != squareVersion {
		t.Errorf("headers: %+v", r)
	}
	if text, isErr = call(t, s, "get_order", map[string]any{"order_id": "O2"}); !isErr || !strings.Contains(text, "L9") {
		t.Errorf("other location must be refused: %s", text)
	}
	if _, isErr = call(t, s, "get_order", map[string]any{"order_id": "../v2/payments"}); !isErr {
		t.Error("path-like ID accepted")
	}
	if _, isErr = call(t, s, "get_order", map[string]any{}); !isErr {
		t.Error("missing ID accepted")
	}
	if *secretCalls != 1 {
		t.Errorf("token fetched %d times, want once", *secretCalls)
	}
}

func TestMCPSearchOrders(t *testing.T) {
	s, fake, _ := newTestMCP(t, `{"locationIds":"L1,L2"}`, map[string]string{
		"POST /v2/orders/search": `{"orders":[
			{"id":"O1","location_id":"L1","state":"COMPLETED","total_money":{"amount":1200,"currency":"JPY"},"line_items":[{"name":"Coffee","quantity":"2"}],"customer_id":"C1"},
			{"id":"R1","location_id":"L1","state":"COMPLETED","returns":[{"source_order_id":"O1","return_line_items":[{"quantity":"1","item_type":"CUSTOM_AMOUNT"}]}],"return_amounts":{"total_money":{"amount":300,"currency":"JPY"}}}
		],"cursor":"NEXT"}`,
	})
	text, isErr := call(t, s, "search_orders", map[string]any{
		"states": []any{"completed"}, "created_after": "2026-10-18T09:00:00+09:00", "limit": float64(5), "cursor": "C0",
	})
	if isErr {
		t.Fatal(text)
	}
	var got page[orderListItem]
	if err := json.Unmarshal([]byte(text), &got); err != nil {
		t.Fatal(err)
	}
	if got.Cursor != "NEXT" || len(got.Items) != 2 || got.Items[0].Total.Display != "1,200 JPY" || got.Items[0].Items != "Coffee×2" || got.Items[0].CustomerID != "C1" {
		t.Fatalf("%+v", got)
	}
	if r := got.Items[1]; r.Total != nil || r.Returned.Display != "300 JPY" || r.ReturnOf != "O1" {
		t.Errorf("return row: %+v", r)
	}
	b, _ := json.Marshal(fake.last().Body)
	for _, want := range []string{`"location_ids":["L1","L2"]`, `"limit":5`, `"cursor":"C0"`, `"states":["COMPLETED"]`, `"start_at":"2026-10-18T00:00:00Z"`, `"sort_field":"CREATED_AT"`} {
		if !strings.Contains(string(b), want) {
			t.Errorf("body lacks %s: %s", want, b)
		}
	}
	if _, isErr := call(t, s, "search_orders", map[string]any{"location_ids": []any{"L9"}}); !isErr {
		t.Error("unconfigured location accepted")
	}
	if _, isErr := call(t, s, "search_orders", map[string]any{"created_after": "yesterday"}); !isErr {
		t.Error("bad time accepted")
	}
}

func TestMCPSearchOrdersDiscoversLocations(t *testing.T) {
	s, fake, _ := newTestMCP(t, `{}`, map[string]string{
		"GET /v2/locations":      `{"locations":[{"id":"A","status":"ACTIVE"},{"id":"B","status":"INACTIVE"}]}`,
		"POST /v2/orders/search": `{}`,
	})
	if text, isErr := call(t, s, "search_orders", nil); isErr || !strings.Contains(text, `"items": []`) {
		t.Fatalf("%v %s", isErr, text)
	}
	b, _ := json.Marshal(fake.last().Body)
	if !strings.Contains(string(b), `"location_ids":["A"]`) {
		t.Errorf("%s", b)
	}
}

func TestMCPListPaymentsLocation(t *testing.T) {
	answers := map[string]string{"GET /v2/payments": `{"payments":[
		{"id":"P1","status":"COMPLETED","location_id":"L1","total_money":{"amount":1100,"currency":"JPY"},"refunded_money":{"amount":300,"currency":"JPY"},"card_details":{"card":{"card_brand":"VISA","last_4":"1111"}}}
	]}`}
	s, _, _ := newTestMCP(t, `{"locationIds":"L1,L2"}`, answers)
	if text, isErr := call(t, s, "list_payments", nil); !isErr || !strings.Contains(text, "location_id is required") {
		t.Errorf("want location_id required: %s", text)
	}

	s, fake, _ := newTestMCP(t, `{"locationIds":"L1"}`, answers)
	text, isErr := call(t, s, "list_payments", map[string]any{"begin_time": "2026-10-01T00:00:00Z"})
	if isErr {
		t.Fatal(text)
	}
	q := fake.last().Query
	for _, want := range []string{"location_id=L1", "sort_order=DESC", "limit=20", "begin_time=2026-10-01T00%3A00%3A00Z"} {
		if !strings.Contains(q, want) {
			t.Errorf("query lacks %s: %s", want, q)
		}
	}
	if !strings.Contains(text, `"card": "VISA 1111"`) || !strings.Contains(text, `"display": "300 JPY"`) {
		t.Errorf("%s", text)
	}
}

func TestMCPListRefundsFiltersLocations(t *testing.T) {
	s, fake, _ := newTestMCP(t, `{"locationIds":"L1"}`, map[string]string{"GET /v2/refunds": `{"refunds":[
		{"id":"F1","status":"COMPLETED","location_id":"L1","amount_money":{"amount":300,"currency":"JPY"},"reason":"broken"},
		{"id":"F2","status":"COMPLETED","location_id":"L9","amount_money":{"amount":100,"currency":"JPY"}}
	]}`})
	text, isErr := call(t, s, "list_refunds", map[string]any{"status": "completed"})
	if isErr || !strings.Contains(text, "F1") || strings.Contains(text, "F2") || !strings.Contains(text, "broken") {
		t.Fatalf("%v %s", isErr, text)
	}
	if q := fake.last().Query; !strings.Contains(q, "status=COMPLETED") {
		t.Errorf("%s", q)
	}
}

func TestMCPCustomersAndCatalog(t *testing.T) {
	s, fake, _ := newTestMCP(t, `{}`, map[string]string{
		"POST /v2/customers/search":             `{"customers":[{"id":"C1","given_name":"花子","email_address":"h@example.com"}]}`,
		"POST /v2/catalog/search-catalog-items": `{"items":[{"id":"I1","item_data":{"name":"Coffee","variations":[{"id":"V1","item_variation_data":{"name":"Hot","price_money":{"amount":550,"currency":"JPY"}}}]}}]}`,
	})
	text, isErr := call(t, s, "search_customers", map[string]any{"email": "h@"})
	if isErr || !strings.Contains(text, "花子") {
		t.Fatalf("%v %s", isErr, text)
	}
	b, _ := json.Marshal(fake.last().Body)
	if !strings.Contains(string(b), `"email_address":{"fuzzy":"h@"}`) {
		t.Errorf("%s", b)
	}
	text, isErr = call(t, s, "search_catalog_items", map[string]any{"text": "coffee"})
	if isErr || !strings.Contains(text, `"display": "550 JPY"`) {
		t.Fatalf("%v %s", isErr, text)
	}
	b, _ = json.Marshal(fake.last().Body)
	if !strings.Contains(string(b), `"text_filter":"coffee"`) {
		t.Errorf("%s", b)
	}
}

func TestMCPAuthErrorRefetchesToken(t *testing.T) {
	s, fake, secretCalls := newTestMCP(t, `{}`, map[string]string{"GET /v2/locations": `{"locations":[]}`})
	fake.status = http.StatusUnauthorized
	if text, isErr := call(t, s, "list_locations", nil); !isErr || !strings.Contains(text, "access token") {
		t.Fatalf("%s", text)
	}
	fake.status = 0
	if _, isErr := call(t, s, "list_locations", nil); isErr {
		t.Fatal("recovered call failed")
	}
	if *secretCalls != 2 {
		t.Errorf("token fetched %d times, want 2 (once more after the 401)", *secretCalls)
	}
}

func TestMCPNoToken(t *testing.T) {
	s, _, _ := newTestMCP(t, `{}`, nil)
	s.secrets = func(context.Context) (map[string]string, error) { return map[string]string{}, nil }
	if text, isErr := call(t, s, "list_locations", nil); !isErr || !strings.Contains(text, "no Square access token") {
		t.Fatalf("%s", text)
	}
}

func TestMCPLocationlessObjectsWhenRestricted(t *testing.T) {
	s, _, _ := newTestMCP(t, `{"locationIds":"L1"}`, map[string]string{
		"GET /v2/payments/P1":  `{"payment":{"id":"P1","status":"COMPLETED"}}`,
		"GET /v2/customers/C1": `{"customer":{"id":"C1","given_name":"花子"}}`,
	})
	if text, isErr := call(t, s, "get_payment", map[string]any{"payment_id": "P1"}); !isErr || !strings.Contains(text, "no location") {
		t.Errorf("location-less payment must be refused when restricted: %s", text)
	}
	if text, isErr := call(t, s, "get_customer", map[string]any{"customer_id": "C1"}); isErr || !strings.Contains(text, "花子") {
		t.Errorf("customers are account-wide: %s", text)
	}

	s, _, _ = newTestMCP(t, `{}`, map[string]string{"GET /v2/payments/P1": `{"payment":{"id":"P1","status":"COMPLETED"}}`})
	if text, isErr := call(t, s, "get_payment", map[string]any{"payment_id": "P1"}); isErr {
		t.Errorf("unrestricted agent must see it: %s", text)
	}
}

func TestMCPCatalogScopedToLocations(t *testing.T) {
	s, fake, _ := newTestMCP(t, `{"locationIds":"L1,L2"}`, map[string]string{"POST /v2/catalog/search-catalog-items": `{}`})
	if text, isErr := call(t, s, "search_catalog_items", nil); isErr {
		t.Fatal(text)
	}
	b, _ := json.Marshal(fake.last().Body)
	if !strings.Contains(string(b), `"enabled_location_ids":["L1","L2"]`) {
		t.Errorf("%s", b)
	}
}

// blockingSquare holds every request until the client goes away.
type blockingSquare struct{ started, done chan struct{} }

func (b *blockingSquare) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	close(b.started)
	<-r.Context().Done()
	close(b.done)
}

func TestMCPCancelledCallStopsAndGetsNoAnswer(t *testing.T) {
	bs := &blockingSquare{started: make(chan struct{}), done: make(chan struct{})}
	ts := httptest.NewServer(bs)
	defer ts.Close()
	s, _, _ := newTestMCP(t, `{}`, nil)
	s.newClient = func(_, token string) *SquareClient { return newSquareClient(ts.URL, token) }

	inR, inW := io.Pipe()
	var out bytes.Buffer
	served := make(chan error, 1)
	go func() { served <- s.serve(context.Background(), inR, &out) }()
	io.WriteString(inW, `{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"list_locations"}}`+"\n")
	select {
	case <-bs.started:
	case <-time.After(5 * time.Second):
		t.Fatal("call never reached Square")
	}
	io.WriteString(inW, `{"jsonrpc":"2.0","id":8,"method":"ping"}`+"\n")
	io.WriteString(inW, `{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":7}}`+"\n")
	select {
	case <-bs.done:
	case <-time.After(5 * time.Second):
		t.Fatal("cancel did not stop the Square request")
	}
	inW.Close()
	if err := <-served; err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 1 || !strings.Contains(lines[0], `"id":8`) {
		t.Fatalf("want only the ping answer (ping not blocked by the call), got:\n%s", out.String())
	}
}

func TestMCPBrokenLocationSettingAllowsNothing(t *testing.T) {
	s, fake, _ := newTestMCP(t, `{"locationIds":"a/b, c!d"}`, map[string]string{
		"GET /v2/orders/O1": `{"order":{"id":"O1","location_id":"L1"}}`,
	})
	for _, tc := range []struct {
		tool string
		args map[string]any
	}{
		{"search_orders", nil},
		{"search_catalog_items", nil},
		{"list_payments", nil},
		{"get_order", map[string]any{"order_id": "O1"}},
	} {
		if text, isErr := call(t, s, tc.tool, tc.args); !isErr {
			t.Errorf("%s with a broken location setting: %s", tc.tool, text)
		}
	}
	for _, r := range fake.reqs {
		if r.Path != "/v2/orders/O1" {
			t.Errorf("unexpected Square request %s", r.Path)
		}
	}
}

func TestMCPServeStopsOnContextCancel(t *testing.T) {
	s, _, _ := newTestMCP(t, `{}`, nil)
	inR, inW := io.Pipe()
	defer inW.Close()
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- s.serve(ctx, inR, io.Discard) }()
	cancel()
	select {
	case <-served:
	case <-time.After(5 * time.Second):
		t.Fatal("serve kept running after cancel with stdin open")
	}
}

type failWriter struct{}

func (failWriter) Write([]byte) (int, error) { return 0, errors.New("broken pipe") }

func TestMCPServeStopsOnWriteError(t *testing.T) {
	s, _, _ := newTestMCP(t, `{}`, nil)
	inR, inW := io.Pipe()
	defer inW.Close()
	served := make(chan error, 1)
	go func() { served <- s.serve(context.Background(), inR, failWriter{}) }()
	if _, err := io.WriteString(inW, `{"jsonrpc":"2.0","id":1,"method":"ping"}`+"\n"); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-served:
		if err == nil {
			t.Fatal("serve returned nil after a write error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serve kept waiting for input after a write error")
	}
}

func TestMCPWriteErrorAfterEOFCancelsRunningCalls(t *testing.T) {
	bs := &blockingSquare{started: make(chan struct{}), done: make(chan struct{})}
	ts := httptest.NewServer(bs)
	defer ts.Close()
	s, _, _ := newTestMCP(t, `{}`, nil)
	s.newClient = func(_, token string) *SquareClient { return newSquareClient(ts.URL, token) }
	in := strings.NewReader(`{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"list_locations"}}` + "\n" +
		`{"jsonrpc":"2.0","id":8,"method":"ping"}` + "\n")
	served := make(chan error, 1)
	go func() { served <- s.serve(context.Background(), in, failWriter{}) }()
	select {
	case err := <-served:
		if err == nil {
			t.Fatal("serve returned nil after a write error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serve waited on a running call after its output broke")
	}
}
