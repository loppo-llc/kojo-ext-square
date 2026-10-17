package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// recordingSleep replaces SquareClient.sleep and remembers the waits.
type recordingSleep struct {
	mu    sync.Mutex
	waits []time.Duration
}

func (r *recordingSleep) sleep(_ context.Context, d time.Duration) error {
	r.mu.Lock()
	r.waits = append(r.waits, d)
	r.mu.Unlock()
	return nil
}

func testSquareClient(url string) (*SquareClient, *recordingSleep) {
	c := newSquareClient(url, "tok")
	rs := &recordingSleep{}
	c.sleep = rs.sleep
	c.jitter = func() float64 { return 1 }
	return c, rs
}

func TestSquareHeaders(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Square-Version"); got != "2026-09-16" {
			t.Errorf("Square-Version = %q", got)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer tok" {
			t.Errorf("Authorization = %q", got)
		}
		fmt.Fprint(w, `{"locations":[{"id":"L1","name":"Main","status":"ACTIVE"}]}`)
	}))
	defer srv.Close()
	c, _ := testSquareClient(srv.URL)
	locs, err := c.ListLocations(context.Background())
	if err != nil || len(locs) != 1 || locs[0].Name != "Main" {
		t.Fatalf("locs=%v err=%v", locs, err)
	}
}

func TestSquareRetryBackoff(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		switch calls {
		case 1:
			w.WriteHeader(http.StatusTooManyRequests)
			fmt.Fprint(w, `{"errors":[{"category":"RATE_LIMIT_ERROR","code":"RATE_LIMITED"}]}`)
		case 2:
			w.Header().Set("Retry-After", "7")
			w.WriteHeader(http.StatusTooManyRequests)
		case 3:
			w.WriteHeader(http.StatusBadGateway)
		default:
			fmt.Fprint(w, `{"locations":[]}`)
		}
	}))
	defer srv.Close()
	c, rs := testSquareClient(srv.URL)
	if _, err := c.ListLocations(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []time.Duration{time.Second, 7 * time.Second, 4 * time.Second}
	if fmt.Sprint(rs.waits) != fmt.Sprint(want) {
		t.Fatalf("waits = %v, want %v", rs.waits, want)
	}
}

func TestSquareRetryAfterIsCapped(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			w.Header().Set("Retry-After", "86400")
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		fmt.Fprint(w, `{}`)
	}))
	defer srv.Close()
	c, rs := testSquareClient(srv.URL)
	if _, err := c.ListLocations(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(rs.waits) != 1 || rs.waits[0] != retryAfterCap {
		t.Fatalf("waits = %v", rs.waits)
	}
}

func TestSquareBackoffCapAndGiveUp(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	c, rs := testSquareClient(srv.URL)
	_, err := c.ListLocations(context.Background())
	se, ok := err.(*SquareError)
	if !ok || se.Status != 500 {
		t.Fatalf("err = %v", err)
	}
	if calls != retryAttempts {
		t.Fatalf("calls = %d", calls)
	}
	if len(rs.waits) != retryAttempts-1 {
		t.Fatalf("waits = %v", rs.waits)
	}
	for _, w := range rs.waits {
		if w > retryCap {
			t.Fatalf("wait %v over cap", w)
		}
	}
	if c.backoff(20) != retryCap {
		t.Fatalf("backoff(20) = %v", c.backoff(20))
	}
}

func TestSquareJitterRange(t *testing.T) {
	c := newSquareClient("http://x", "t")
	for i := 0; i < 100; i++ {
		d := c.backoff(2)
		if d < 2*time.Second || d >= 4*time.Second {
			t.Fatalf("backoff(2) = %v", d)
		}
	}
}

func TestSquareAuthErrorNotRetried(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, `{"errors":[{"category":"AUTHENTICATION_ERROR","code":"UNAUTHORIZED","detail":"bad token"}]}`)
	}))
	defer srv.Close()
	c, _ := testSquareClient(srv.URL)
	_, err := c.ListLocations(context.Background())
	if !isAuthError(err) || calls != 1 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
	if se := err.(*SquareError); se.Code != "UNAUTHORIZED" || se.Detail != "bad token" {
		t.Fatalf("decoded %+v", se)
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC)
	if d, ok := parseRetryAfter("3", now); !ok || d != 3*time.Second {
		t.Fatal(d, ok)
	}
	if d, ok := parseRetryAfter(now.Add(10*time.Second).Format(http.TimeFormat), now); !ok || d != 10*time.Second {
		t.Fatal(d, ok)
	}
	if _, ok := parseRetryAfter("soon", now); ok {
		t.Fatal("parsed garbage")
	}
}

func TestSearchOrdersPaginationAndChunking(t *testing.T) {
	type call struct {
		Locations []string
		Cursor    string
		Query     json.RawMessage
	}
	var mu sync.Mutex
	var calls []call
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v2/orders/search" || r.Method != http.MethodPost {
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
		body, _ := io.ReadAll(r.Body)
		var req struct {
			LocationIDs []string        `json:"location_ids"`
			Cursor      string          `json:"cursor"`
			Query       json.RawMessage `json:"query"`
			Limit       int             `json:"limit"`
		}
		_ = json.Unmarshal(body, &req)
		if len(req.LocationIDs) > 10 {
			t.Errorf("%d location_ids in one request", len(req.LocationIDs))
		}
		if req.Limit != 500 {
			t.Errorf("limit = %d", req.Limit)
		}
		mu.Lock()
		calls = append(calls, call{req.LocationIDs, req.Cursor, req.Query})
		mu.Unlock()
		first := req.LocationIDs[0]
		switch req.Cursor {
		case "":
			fmt.Fprintf(w, `{"orders":[{"id":"%s-a","updated_at":"2026-10-18T00:00:01Z"}],"cursor":"c2"}`, first)
		case "c2":
			fmt.Fprintf(w, `{"orders":[{"id":"%s-b","updated_at":"2026-10-18T00:00:02Z"}]}`, first)
		}
	}))
	defer srv.Close()
	c, _ := testSquareClient(srv.URL)
	var locs []string
	for i := 0; i < 23; i++ {
		locs = append(locs, fmt.Sprintf("L%02d", i))
	}
	since := time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC)
	res, err := c.SearchOrders(context.Background(), locs, since)
	if err != nil {
		t.Fatal(err)
	}
	if len(calls) != 6 || len(res.Items) != 6 {
		t.Fatalf("calls=%d items=%d", len(calls), len(res.Items))
	}
	if len(calls[0].Locations) != 10 || len(calls[2].Locations) != 10 || len(calls[4].Locations) != 3 {
		t.Fatalf("chunks: %d %d %d", len(calls[0].Locations), len(calls[2].Locations), len(calls[4].Locations))
	}
	// The second page re-sends the same query with the cursor.
	if calls[1].Cursor != "c2" || string(calls[1].Query) != string(calls[0].Query) {
		t.Fatalf("page 2 = %+v", calls[1])
	}
	var q struct {
		Filter struct {
			DateTimeFilter struct {
				UpdatedAt struct {
					StartAt string `json:"start_at"`
				} `json:"updated_at"`
			} `json:"date_time_filter"`
		} `json:"filter"`
		Sort struct {
			SortField string `json:"sort_field"`
			SortOrder string `json:"sort_order"`
		} `json:"sort"`
	}
	_ = json.Unmarshal(calls[0].Query, &q)
	if q.Filter.DateTimeFilter.UpdatedAt.StartAt != "2026-10-18T00:00:00Z" || q.Sort.SortField != "UPDATED_AT" || q.Sort.SortOrder != "ASC" {
		t.Fatalf("query = %s", calls[0].Query)
	}
}

func TestListPaymentsPagination(t *testing.T) {
	var queries []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queries = append(queries, r.URL.RawQuery)
		q := r.URL.Query()
		if q.Get("location_id") != "L1" || q.Get("sort_field") != "UPDATED_AT" || q.Get("sort_order") != "ASC" ||
			q.Get("limit") != "100" || q.Get("updated_at_begin_time") != "2026-10-18T00:00:00Z" || q.Get("begin_time") != "" {
			t.Errorf("query = %s", r.URL.RawQuery)
		}
		if q.Get("cursor") == "" {
			fmt.Fprint(w, `{"payments":[{"id":"P1"}],"cursor":"next"}`)
			return
		}
		fmt.Fprint(w, `{"payments":[{"id":"P2"}]}`)
	}))
	defer srv.Close()
	c, _ := testSquareClient(srv.URL)
	res, err := c.ListPayments(context.Background(), "L1", time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC))
	if err != nil || len(res.Items) != 2 || len(queries) != 2 {
		t.Fatalf("res=%v err=%v queries=%v", res, err, queries)
	}
}

func TestListPagesTruncate(t *testing.T) {
	n := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n++
		fmt.Fprintf(w, `{"refunds":[{"id":"R%d","updated_at":"2026-10-18T00:%02d:00Z"}],"cursor":"more"}`, n, n%60)
	}))
	defer srv.Close()
	c, _ := testSquareClient(srv.URL)
	res, err := c.ListRefunds(context.Background(), "", time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if n != maxPages || !res.Truncated {
		t.Fatalf("pages=%d truncated=%v", n, res.Truncated)
	}
	if want := time.Date(2026, 10, 18, 0, maxPages%60, 0, 0, time.UTC); !res.Resume.Equal(want) {
		t.Fatalf("resume = %v, want %v", res.Resume, want)
	}
}

func TestListPagesKeepGoingUntilPastSince(t *testing.T) {
	since := time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC)
	n := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n++
		at := since // thousands of changes stamped with one instant
		if n > maxPages+4 {
			at = since.Add(time.Second)
		}
		fmt.Fprintf(w, `{"refunds":[{"id":"R%d","updated_at":%q}],"cursor":"more"}`, n, formatTime(at))
	}))
	defer srv.Close()
	c, _ := testSquareClient(srv.URL)
	res, err := c.ListRefunds(context.Background(), "", since)
	if err != nil {
		t.Fatal(err)
	}
	if n != maxPages+5 || !res.Truncated || !res.Resume.After(since) {
		t.Fatalf("pages=%d truncated=%v resume=%v", n, res.Truncated, res.Resume)
	}
}

func TestListPagesHardLimit(t *testing.T) {
	since := time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC)
	n := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n++
		fmt.Fprintf(w, `{"refunds":[{"id":"R%d","updated_at":%q}],"cursor":"more"}`, n, formatTime(since))
	}))
	defer srv.Close()
	c, _ := testSquareClient(srv.URL)
	if _, err := c.ListRefunds(context.Background(), "", since); err != nil {
		t.Fatal(err)
	}
	if n != maxPages*maxPagesStuckFactor {
		t.Fatalf("pages=%d", n)
	}
}
