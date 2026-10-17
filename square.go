package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// squareVersion pins the Square API version every request asks for.
const squareVersion = "2026-09-16"

const (
	// ordersLocationChunk is SearchOrders' limit on location_ids.
	ordersLocationChunk = 10
	ordersPageLimit     = 500
	listPageLimit       = 100
	// maxPages bounds one sub-query. A query cut short reports how far it
	// got, and the stream cursor stops there, so the rest is fetched on
	// the next poll instead of being skipped.
	maxPages = 40
	// maxPagesStuckFactor × maxPages is the hard limit when the listing
	// has not moved past its start yet (see pageCapReached).
	maxPagesStuckFactor = 10

	retryAttempts  = 5
	retryBase      = time.Second
	retryCap       = 30 * time.Second
	retryAfterCap  = 2 * time.Minute
	requestTimeout = 30 * time.Second
	maxBodyBytes   = 32 << 20
)

// SquareClient is a minimal read-only client for the endpoints the
// poller needs.
type SquareClient struct {
	base  string
	token string
	http  *http.Client
	// sleep waits between retries; tests replace it.
	sleep func(ctx context.Context, d time.Duration) error
	// jitter returns a factor in [0.5, 1).
	jitter func() float64
}

func newSquareClient(base, token string) *SquareClient {
	return &SquareClient{
		base:   strings.TrimRight(base, "/"),
		token:  token,
		http:   &http.Client{Timeout: requestTimeout},
		sleep:  sleepCtx,
		jitter: func() float64 { return 0.5 + rand.Float64()/2 },
	}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// SquareError is a non-2xx answer from Square.
type SquareError struct {
	Status   int
	Code     string
	Category string
	Detail   string
}

func (e *SquareError) Error() string {
	msg := fmt.Sprintf("Square API %d", e.Status)
	if e.Code != "" {
		msg += " " + e.Code
	}
	if e.Detail != "" {
		msg += ": " + e.Detail
	}
	return msg
}

// isAuthError reports a rejected or under-privileged token: retrying
// will not help until the operator fixes it.
func isAuthError(err error) bool {
	var se *SquareError
	return errors.As(err, &se) && (se.Status == http.StatusUnauthorized || se.Status == http.StatusForbidden)
}

func retryable(status int) bool {
	return status == http.StatusTooManyRequests || status >= 500
}

// backoff is the wait before retry attempt n (0-based): exponential,
// capped, with jitter so that several pollers do not retry in step.
func (c *SquareClient) backoff(n int) time.Duration {
	d := retryBase << n
	if d > retryCap || d <= 0 {
		d = retryCap
	}
	return time.Duration(float64(d) * c.jitter())
}

// parseRetryAfter reads a Retry-After header (seconds or an HTTP date).
// Square does not document it, but honours it if it is ever sent.
func parseRetryAfter(v string, now time.Time) (time.Duration, bool) {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0, false
	}
	if secs, err := strconv.Atoi(v); err == nil {
		if secs < 0 {
			return 0, false
		}
		return time.Duration(secs) * time.Second, true
	}
	if t, err := http.ParseTime(v); err == nil {
		d := t.Sub(now)
		if d < 0 {
			d = 0
		}
		return d, true
	}
	return 0, false
}

// do sends one request, retrying 429, 5xx and transport errors.
func (c *SquareClient) do(ctx context.Context, method, path string, query url.Values, body, out any) error {
	var payload []byte
	if body != nil {
		var err error
		if payload, err = json.Marshal(body); err != nil {
			return err
		}
	}
	u := c.base + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	var lastErr error
	for attempt := 0; attempt < retryAttempts; attempt++ {
		if attempt > 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		var rd io.Reader
		if payload != nil {
			rd = bytes.NewReader(payload)
		}
		req, err := http.NewRequestWithContext(ctx, method, u, rd)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+c.token)
		req.Header.Set("Square-Version", squareVersion)
		req.Header.Set("Accept", "application/json")
		if payload != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, err := c.http.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			lastErr = err
			if attempt+1 < retryAttempts {
				if err := c.sleep(ctx, c.backoff(attempt)); err != nil {
					return err
				}
			}
			continue
		}
		data, readErr := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
		resp.Body.Close()
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			if readErr != nil {
				lastErr = readErr
				if attempt+1 < retryAttempts {
					if err := c.sleep(ctx, c.backoff(attempt)); err != nil {
						return err
					}
				}
				continue
			}
			if out == nil {
				return nil
			}
			if err := json.Unmarshal(data, out); err != nil {
				return fmt.Errorf("decode %s %s: %w", method, path, err)
			}
			return nil
		}
		se := decodeSquareError(resp.StatusCode, data)
		if !retryable(resp.StatusCode) {
			return se
		}
		lastErr = se
		if attempt+1 >= retryAttempts {
			break
		}
		wait := c.backoff(attempt)
		if ra, ok := parseRetryAfter(resp.Header.Get("Retry-After"), time.Now()); ok {
			wait = min(ra, retryAfterCap)
		}
		if err := c.sleep(ctx, wait); err != nil {
			return err
		}
	}
	return lastErr
}

func decodeSquareError(status int, body []byte) *SquareError {
	se := &SquareError{Status: status}
	var env struct {
		Errors []struct {
			Category string `json:"category"`
			Code     string `json:"code"`
			Detail   string `json:"detail"`
		} `json:"errors"`
	}
	if json.Unmarshal(body, &env) == nil && len(env.Errors) > 0 {
		se.Code, se.Category, se.Detail = env.Errors[0].Code, env.Errors[0].Category, env.Errors[0].Detail
	} else {
		se.Detail = strings.TrimSpace(string(body))
		if len(se.Detail) > 200 {
			se.Detail = se.Detail[:200]
		}
	}
	return se
}

// Money is an amount in the currency's smallest unit.
type Money struct {
	Amount   int64  `json:"amount"`
	Currency string `json:"currency"`
}

type Location struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Status string `json:"status"`
}

type LineItem struct {
	Name     string `json:"name"`
	Quantity string `json:"quantity"`
	ItemType string `json:"item_type,omitempty"`
}

// OrderReturn is one return inside a return order: the order Square
// creates when a payment is refunded or items are given back.
type OrderReturn struct {
	SourceOrderID   string     `json:"source_order_id"`
	ReturnLineItems []LineItem `json:"return_line_items"`
}

type OrderMoneyAmounts struct {
	TotalMoney *Money `json:"total_money,omitempty"`
}

type Order struct {
	ID         string     `json:"id"`
	LocationID string     `json:"location_id"`
	State      string     `json:"state"`
	Version    *int64     `json:"version,omitempty"`
	CreatedAt  string     `json:"created_at"`
	UpdatedAt  string     `json:"updated_at"`
	ClosedAt   string     `json:"closed_at"`
	TotalMoney *Money     `json:"total_money,omitempty"`
	LineItems  []LineItem `json:"line_items"`
	// Returns and ReturnAmounts are set on return orders, which carry no
	// line items or total of their own.
	Returns       []OrderReturn      `json:"returns,omitempty"`
	ReturnAmounts *OrderMoneyAmounts `json:"return_amounts,omitempty"`
}

// hasReturns reports whether the order records a return: a return order
// Square creates for a refund, or an exchange (sale and return in one).
func (o Order) hasReturns() bool { return len(o.Returns) > 0 }

// isPureReturn is a return-only order: nothing sold, so its own total is
// absent or zero and only the returned side means anything.
func (o Order) isPureReturn() bool {
	return o.hasReturns() && len(o.LineItems) == 0 && (o.TotalMoney == nil || o.TotalMoney.Amount == 0)
}

// returnMoney is the returned total, if Square reports one.
func (o Order) returnMoney() *Money {
	if o.ReturnAmounts == nil {
		return nil
	}
	return o.ReturnAmounts.TotalMoney
}

// returnItems is every returned line item across the order's returns.
func (o Order) returnItems() []LineItem {
	var out []LineItem
	for _, r := range o.Returns {
		out = append(out, r.ReturnLineItems...)
	}
	return out
}

// returnOf is the comma-joined source orders of the returns; unlinked
// returns have none.
func (o Order) returnOf() string {
	var ids []string
	seen := map[string]bool{}
	for _, r := range o.Returns {
		if r.SourceOrderID != "" && !seen[r.SourceOrderID] {
			seen[r.SourceOrderID] = true
			ids = append(ids, r.SourceOrderID)
		}
	}
	return strings.Join(ids, ",")
}

type Payment struct {
	ID            string       `json:"id"`
	Status        string       `json:"status"`
	LocationID    string       `json:"location_id"`
	OrderID       string       `json:"order_id"`
	SourceType    string       `json:"source_type"`
	CreatedAt     string       `json:"created_at"`
	UpdatedAt     string       `json:"updated_at"`
	AmountMoney   *Money       `json:"amount_money,omitempty"`
	TotalMoney    *Money       `json:"total_money,omitempty"`
	RefundedMoney *Money       `json:"refunded_money,omitempty"`
	CardDetails   *CardDetails `json:"card_details,omitempty"`
}

type CardDetails struct {
	Timeline *CardPaymentTimeline `json:"card_payment_timeline,omitempty"`
}

type CardPaymentTimeline struct {
	CapturedAt string `json:"captured_at,omitempty"`
	VoidedAt   string `json:"voided_at,omitempty"`
}

// settledAt is when a card payment reached its current terminal status
// (captured or voided), or "" when Square does not say.
func (p Payment) settledAt() string {
	if p.CardDetails == nil || p.CardDetails.Timeline == nil {
		return ""
	}
	switch p.Status {
	case "COMPLETED":
		return p.CardDetails.Timeline.CapturedAt
	case "CANCELED":
		return p.CardDetails.Timeline.VoidedAt
	}
	return ""
}

type Refund struct {
	ID          string `json:"id"`
	Status      string `json:"status"`
	LocationID  string `json:"location_id"`
	PaymentID   string `json:"payment_id"`
	OrderID     string `json:"order_id"`
	Reason      string `json:"reason"`
	CreatedAt   string `json:"created_at"`
	UpdatedAt   string `json:"updated_at"`
	AmountMoney *Money `json:"amount_money,omitempty"`
}

// ListLocations returns the account's locations (MERCHANT_PROFILE_READ).
func (c *SquareClient) ListLocations(ctx context.Context) ([]Location, error) {
	var out struct {
		Locations []Location `json:"locations"`
	}
	if err := c.do(ctx, http.MethodGet, "/v2/locations", nil, nil, &out); err != nil {
		return nil, err
	}
	return out.Locations, nil
}

// fetchResult is what one stream's queries returned for a poll.
type fetchResult[T any] struct {
	Items []T
	// Truncated is set when a sub-query hit maxPages; Resume is then the
	// updated_at of the last item that sub-query returned (the earliest
	// such point over all truncated sub-queries).
	Truncated bool
	Resume    time.Time
}

func (r *fetchResult[T]) truncateAt(last string) {
	if t, err := parseTime(last); err == nil {
		r.truncateAtTime(t)
	}
}

func (r *fetchResult[T]) truncateAtTime(t time.Time) {
	if !r.Truncated || t.Before(r.Resume) {
		r.Resume = t
	}
	r.Truncated = true
}

// SearchOrders returns orders at locationIDs updated at or after since,
// oldest update first within each chunk of ten locations.
func (c *SquareClient) SearchOrders(ctx context.Context, locationIDs []string, since time.Time) (fetchResult[Order], error) {
	var res fetchResult[Order]
	for start := 0; start < len(locationIDs); start += ordersLocationChunk {
		chunk := locationIDs[start:min(start+ordersLocationChunk, len(locationIDs))]
		body := map[string]any{
			"location_ids": chunk,
			"query": map[string]any{
				"filter": map[string]any{
					"date_time_filter": map[string]any{
						"updated_at": map[string]any{"start_at": formatTime(since)},
					},
				},
				// sort_field has to name the field the date filter uses.
				"sort": map[string]any{"sort_field": "UPDATED_AT", "sort_order": "ASC"},
			},
			"limit": ordersPageLimit,
		}
		var last string
		for page := 0; ; page++ {
			if page >= maxPages && pageCapReached(page, last, since) {
				res.truncateAt(last)
				break
			}
			var out struct {
				Orders []Order `json:"orders"`
				Cursor string  `json:"cursor"`
			}
			if err := c.do(ctx, http.MethodPost, "/v2/orders/search", nil, body, &out); err != nil {
				return res, err
			}
			res.Items = append(res.Items, out.Orders...)
			if n := len(out.Orders); n > 0 {
				last = out.Orders[n-1].UpdatedAt
			}
			if out.Cursor == "" {
				break
			}
			// The cursor goes with the original query.
			body["cursor"] = out.Cursor
		}
	}
	return res, nil
}

// ListPayments returns payments at one location updated at or after
// since. ListPayments without location_id covers only the main
// location, so the caller asks per location.
func (c *SquareClient) ListPayments(ctx context.Context, locationID string, since time.Time) (fetchResult[Payment], error) {
	var res fetchResult[Payment]
	q := url.Values{}
	q.Set("location_id", locationID)
	q.Set("updated_at_begin_time", formatTime(since))
	q.Set("sort_field", "UPDATED_AT")
	q.Set("sort_order", "ASC")
	q.Set("limit", strconv.Itoa(listPageLimit))
	var last string
	for page := 0; ; page++ {
		if page >= maxPages && pageCapReached(page, last, since) {
			res.truncateAt(last)
			break
		}
		var out struct {
			Payments []Payment `json:"payments"`
			Cursor   string    `json:"cursor"`
		}
		if err := c.do(ctx, http.MethodGet, "/v2/payments", q, nil, &out); err != nil {
			return res, err
		}
		res.Items = append(res.Items, out.Payments...)
		if n := len(out.Payments); n > 0 {
			last = out.Payments[n-1].UpdatedAt
		}
		if out.Cursor == "" {
			break
		}
		q.Set("cursor", out.Cursor)
	}
	return res, nil
}

// ListRefunds returns refunds updated at or after since, at one location
// or (locationID "") at every location.
func (c *SquareClient) ListRefunds(ctx context.Context, locationID string, since time.Time) (fetchResult[Refund], error) {
	var res fetchResult[Refund]
	q := url.Values{}
	if locationID != "" {
		q.Set("location_id", locationID)
	}
	q.Set("updated_at_begin_time", formatTime(since))
	q.Set("sort_field", "UPDATED_AT")
	q.Set("sort_order", "ASC")
	q.Set("limit", strconv.Itoa(listPageLimit))
	var last string
	for page := 0; ; page++ {
		if page >= maxPages && pageCapReached(page, last, since) {
			res.truncateAt(last)
			break
		}
		var out struct {
			Refunds []Refund `json:"refunds"`
			Cursor  string   `json:"cursor"`
		}
		if err := c.do(ctx, http.MethodGet, "/v2/refunds", q, nil, &out); err != nil {
			return res, err
		}
		res.Items = append(res.Items, out.Refunds...)
		if n := len(out.Refunds); n > 0 {
			last = out.Refunds[n-1].UpdatedAt
		}
		if out.Cursor == "" {
			break
		}
		q.Set("cursor", out.Cursor)
	}
	return res, nil
}

// pageCapReached decides whether a listing that has read maxPages pages
// may stop and resume from the last item next time. It may only once
// that item is past since: resuming from since itself would read the same
// pages again forever (thousands of changes within one instant). A hard
// limit still stops a server that never ends a listing.
func pageCapReached(page int, last string, since time.Time) bool {
	if page >= maxPages*maxPagesStuckFactor {
		return true
	}
	t, err := parseTime(last)
	return err == nil && t.After(since)
}

func formatTime(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

func parseTime(s string) (time.Time, error) { return time.Parse(time.RFC3339Nano, s) }
