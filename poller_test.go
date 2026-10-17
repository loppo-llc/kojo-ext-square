package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

var t0 = time.Date(2026, 10, 18, 9, 0, 0, 0, time.UTC)

func ts(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

func i64(v int64) *int64 { return &v }

// fakeSquare is an in-memory Square account behind httptest.
type fakeSquare struct {
	mu        sync.Mutex
	locations []Location
	orders    []Order
	payments  []Payment
	refunds   []Refund
	// fail answers every request to a path containing the key with the
	// status.
	fail map[string]int

	orderStarts   []string
	orderLocs     [][]string
	paymentQs     []url.Values
	refundQs      []url.Values
	locationCalls int
}

func newFakeSquare() *fakeSquare {
	return &fakeSquare{
		locations: []Location{{ID: "L1", Name: "渋谷店", Status: "ACTIVE"}, {ID: "L2", Name: "新宿店", Status: "ACTIVE"}},
		fail:      map[string]int{},
	}
}

func (f *fakeSquare) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for key, status := range f.fail {
		if strings.Contains(r.URL.Path, key) {
			w.WriteHeader(status)
			w.Write([]byte(`{"errors":[{"code":"FAIL"}]}`))
			return
		}
	}
	enc := json.NewEncoder(w)
	switch r.URL.Path {
	case "/v2/locations":
		f.locationCalls++
		enc.Encode(map[string]any{"locations": f.locations})
	case "/v2/orders/search":
		body, _ := io.ReadAll(r.Body)
		var req struct {
			LocationIDs []string `json:"location_ids"`
			Query       struct {
				Filter struct {
					DateTimeFilter struct {
						UpdatedAt struct {
							StartAt string `json:"start_at"`
						} `json:"updated_at"`
					} `json:"date_time_filter"`
				} `json:"filter"`
			} `json:"query"`
		}
		json.Unmarshal(body, &req)
		start := req.Query.Filter.DateTimeFilter.UpdatedAt.StartAt
		f.orderStarts = append(f.orderStarts, start)
		f.orderLocs = append(f.orderLocs, req.LocationIDs)
		since, _ := parseTime(start)
		var out []Order
		for _, o := range f.orders {
			u, _ := parseTime(o.UpdatedAt)
			if contains(req.LocationIDs, o.LocationID) && !u.Before(since) {
				out = append(out, o)
			}
		}
		sort.SliceStable(out, func(i, j int) bool { return out[i].UpdatedAt < out[j].UpdatedAt })
		enc.Encode(map[string]any{"orders": out})
	case "/v2/payments":
		q := r.URL.Query()
		f.paymentQs = append(f.paymentQs, q)
		since, _ := parseTime(q.Get("updated_at_begin_time"))
		var out []Payment
		for _, p := range f.payments {
			u, _ := parseTime(p.UpdatedAt)
			if p.LocationID == q.Get("location_id") && !u.Before(since) {
				out = append(out, p)
			}
		}
		enc.Encode(map[string]any{"payments": out})
	case "/v2/refunds":
		q := r.URL.Query()
		f.refundQs = append(f.refundQs, q)
		since, _ := parseTime(q.Get("updated_at_begin_time"))
		var out []Refund
		for _, x := range f.refunds {
			u, _ := parseTime(x.UpdatedAt)
			if (q.Get("location_id") == "" || x.LocationID == q.Get("location_id")) && !u.Before(since) {
				out = append(out, x)
			}
		}
		enc.Encode(map[string]any{"refunds": out})
	default:
		w.WriteHeader(404)
	}
}

func (f *fakeSquare) do(fn func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn()
}

type pollerHarness struct {
	t      *testing.T
	sq     *fakeSquare
	srv    *httptest.Server
	dir    string
	clock  time.Time
	p      *Poller
	store  *Store
	notifs int
}

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newHarness(t *testing.T, cfgJSON string) *pollerHarness {
	t.Helper()
	h := &pollerHarness{t: t, sq: newFakeSquare(), dir: t.TempDir(), clock: t0}
	h.srv = httptest.NewServer(h.sq)
	t.Cleanup(h.srv.Close)
	h.open(cfgJSON)
	return h
}

// open (re)creates the poller over the state on disk, like a restart.
func (h *pollerHarness) open(cfgJSON string) {
	h.t.Helper()
	store, err := openStore(h.dir)
	if err != nil {
		h.t.Fatal(err)
	}
	h.store = store
	sq, _ := testSquareClient(h.srv.URL)
	h.p = newPoller(parseConfig(cfgJSON), sq, store, func() { h.notifs++ }, quietLog())
	h.p.now = func() time.Time { return h.clock }
}

func (h *pollerHarness) cycle(advance time.Duration) []Event {
	h.t.Helper()
	h.clock = h.clock.Add(advance)
	before, _ := h.store.pending()
	h.p.cycle(h.t.Context())
	after, _ := h.store.pending()
	return after[len(before):]
}

func kinds(events []Event) []string {
	out := make([]string, len(events))
	for i, e := range events {
		out[i] = e.Kind + ":" + e.ObjectID
	}
	return out
}

func TestFirstRunNoBackfill(t *testing.T) {
	h := newHarness(t, `{"locationIds":"L1"}`)
	old := t0.Add(-time.Hour)
	h.sq.orders = []Order{{ID: "OLD", LocationID: "L1", State: "OPEN", Version: i64(1), CreatedAt: ts(old), UpdatedAt: ts(t0.Add(-30 * time.Second))}}
	h.sq.payments = []Payment{{ID: "POLD", LocationID: "L1", Status: "COMPLETED", CreatedAt: ts(t0.Add(-time.Minute)), UpdatedAt: ts(t0.Add(-time.Minute))}}

	if ev := h.cycle(0); len(ev) != 0 {
		t.Fatalf("first run reported %v", kinds(ev))
	}
	// The first query starts at the install time, not an overlap before.
	if h.sq.orderStarts[0] != ts(t0) {
		t.Fatalf("first start_at = %s", h.sq.orderStarts[0])
	}
	ss := h.store.stream(streamOrders)
	if ss == nil || !ss.Floor.Equal(t0) || !ss.Cursor.Equal(t0) {
		t.Fatalf("stream = %+v", ss)
	}

	// Something new after the install is reported.
	h.sq.do(func() {
		h.sq.orders = append(h.sq.orders, Order{ID: "NEW", LocationID: "L1", State: "OPEN", Version: i64(1),
			CreatedAt: ts(t0.Add(10 * time.Second)), UpdatedAt: ts(t0.Add(10 * time.Second)),
			TotalMoney: &Money{1200, "JPY"}, LineItems: []LineItem{{"コーヒー", "2"}}})
	})
	ev := h.cycle(30 * time.Second)
	if strings.Join(kinds(ev), ",") != "order.created:NEW" {
		t.Fatalf("events = %v", kinds(ev))
	}
	if ev[0].Items != "コーヒー×2" || ev[0].LocationName != "渋谷店" {
		t.Fatalf("event = %+v", ev[0])
	}
	// Still floored: the overlap does not reach before the install.
	if h.sq.orderStarts[1] != ts(t0) {
		t.Fatalf("second start_at = %s", h.sq.orderStarts[1])
	}
}

func TestCursorPersistenceOverlapAndDedup(t *testing.T) {
	h := newHarness(t, `{"locationIds":"L1"}`)
	h.cycle(0)
	at := t0.Add(5 * time.Minute)
	h.sq.do(func() {
		h.sq.orders = []Order{{ID: "O1", LocationID: "L1", State: "OPEN", Version: i64(1), CreatedAt: ts(at), UpdatedAt: ts(at)}}
	})
	h.clock = at.Add(10 * time.Second)
	if ev := h.cycle(0); len(ev) != 1 {
		t.Fatalf("events = %v", kinds(ev))
	}
	if ss := h.store.stream(streamOrders); !ss.Cursor.Equal(at) {
		t.Fatalf("cursor = %v, want %v", ss.Cursor, at)
	}

	// Restart: the cursor and seen-set come back from disk.
	h.open(`{"locationIds":"L1"}`)
	if ev := h.cycle(30 * time.Second); len(ev) != 0 {
		t.Fatalf("restart re-reported %v", kinds(ev))
	}
	last := h.sq.orderStarts[len(h.sq.orderStarts)-1]
	if last != ts(at.Add(-overlapWindow)) {
		t.Fatalf("start_at = %s, want cursor - overlap", last)
	}

	// A late arrival inside the overlap window is still caught.
	late := at.Add(-time.Minute)
	h.sq.do(func() {
		h.sq.orders = append(h.sq.orders, Order{ID: "LATE", LocationID: "L1", State: "OPEN", Version: i64(1), CreatedAt: ts(late), UpdatedAt: ts(late)})
	})
	if ev := h.cycle(30 * time.Second); strings.Join(kinds(ev), ",") != "order.created:LATE" {
		t.Fatalf("late = %v", kinds(ev))
	}
	// The cursor does not move back for it.
	if ss := h.store.stream(streamOrders); !ss.Cursor.Equal(at) {
		t.Fatalf("cursor = %v", ss.Cursor)
	}
}

func TestOrderStateChanges(t *testing.T) {
	h := newHarness(t, `{"locationIds":"L1"}`)
	h.cycle(0)
	c := t0.Add(time.Minute)
	set := func(o Order) { h.sq.do(func() { h.sq.orders = []Order{o} }) }
	base := Order{ID: "O1", LocationID: "L1", CreatedAt: ts(c), TotalMoney: &Money{500, "USD"}}

	o := base
	o.State, o.Version, o.UpdatedAt = "OPEN", i64(1), ts(c)
	set(o)
	if ev := h.cycle(time.Minute); strings.Join(kinds(ev), ",") != "order.created:O1" {
		t.Fatalf("%v", kinds(ev))
	}
	// Same version again (overlap): nothing.
	if ev := h.cycle(10 * time.Second); len(ev) != 0 {
		t.Fatalf("dup: %v", kinds(ev))
	}
	// New version, same state: nothing.
	o.Version, o.UpdatedAt = i64(2), ts(c.Add(time.Minute))
	set(o)
	if ev := h.cycle(time.Minute); len(ev) != 0 {
		t.Fatalf("same state: %v", kinds(ev))
	}
	o.State, o.Version, o.UpdatedAt, o.ClosedAt = "COMPLETED", i64(3), ts(c.Add(2*time.Minute)), ts(c.Add(2*time.Minute))
	set(o)
	ev := h.cycle(time.Minute)
	if strings.Join(kinds(ev), ",") != "order.state_changed:O1" || ev[0].PrevState != "OPEN" || ev[0].State != "COMPLETED" {
		t.Fatalf("%+v", ev)
	}
}

func TestOrderDraftAndUnknownClosed(t *testing.T) {
	h := newHarness(t, `{"locationIds":"L1"}`)
	h.cycle(0)
	c := t0.Add(time.Minute)
	h.sq.do(func() {
		h.sq.orders = []Order{
			{ID: "D", LocationID: "L1", State: "DRAFT", Version: i64(1), CreatedAt: ts(c), UpdatedAt: ts(c)},
			// Created long before the install, closed after it.
			{ID: "OLDC", LocationID: "L1", State: "CANCELED", Version: i64(9), CreatedAt: ts(t0.Add(-48 * time.Hour)), UpdatedAt: ts(c), ClosedAt: ts(c)},
			// Created long ago, touched now, still open: not news.
			{ID: "OLDO", LocationID: "L1", State: "OPEN", Version: i64(4), CreatedAt: ts(t0.Add(-48 * time.Hour)), UpdatedAt: ts(c)},
		}
	})
	if ev := h.cycle(time.Minute); strings.Join(kinds(ev), ",") != "order.state_changed:OLDC" {
		t.Fatalf("%v", kinds(ev))
	}
	h.sq.do(func() {
		h.sq.orders[0].State, h.sq.orders[0].Version, h.sq.orders[0].UpdatedAt = "OPEN", i64(2), ts(c.Add(time.Minute))
	})
	if ev := h.cycle(time.Minute); strings.Join(kinds(ev), ",") != "order.created:D" {
		t.Fatalf("draft opened: %v", kinds(ev))
	}
}

func TestPaymentsAndRefunds(t *testing.T) {
	h := newHarness(t, `{"locationIds":"L1,L2"}`)
	h.cycle(0)
	c := t0.Add(time.Minute)
	h.sq.do(func() {
		h.sq.payments = []Payment{
			{ID: "P1", LocationID: "L1", Status: "COMPLETED", CreatedAt: ts(c), UpdatedAt: ts(c), TotalMoney: &Money{1200, "JPY"}},
			{ID: "P2", LocationID: "L2", Status: "FAILED", CreatedAt: ts(c), UpdatedAt: ts(c), AmountMoney: &Money{300, "JPY"}},
			{ID: "P3", LocationID: "L2", Status: "APPROVED", CreatedAt: ts(c), UpdatedAt: ts(c), AmountMoney: &Money{900, "JPY"}},
		}
		h.sq.refunds = []Refund{{ID: "R1", LocationID: "L1", PaymentID: "P1", Status: "PENDING", CreatedAt: ts(c), UpdatedAt: ts(c), AmountMoney: &Money{200, "JPY"}}}
	})
	ev := h.cycle(time.Minute)
	got := strings.Join(kinds(ev), ",")
	if got != "refund:R1,payment.completed:P1,payment.failed:P2" {
		t.Fatalf("events = %s", got)
	}
	// One ListPayments per location: it covers only the main one otherwise.
	if len(h.sq.paymentQs) != 4 {
		t.Fatalf("payment queries = %d", len(h.sq.paymentQs))
	}

	// P1's refunded_money rises (R1 is known: not reported twice); R1
	// completes; P3 completes; P2's twin P4 gets refunded with no refund
	// record yet visible (fallback).
	c2 := c.Add(time.Minute)
	h.sq.do(func() {
		h.sq.payments[0].RefundedMoney, h.sq.payments[0].UpdatedAt = &Money{200, "JPY"}, ts(c2)
		h.sq.payments[2].Status, h.sq.payments[2].UpdatedAt = "COMPLETED", ts(c2)
		h.sq.refunds[0].Status, h.sq.refunds[0].UpdatedAt = "COMPLETED", ts(c2)
		h.sq.payments = append(h.sq.payments, Payment{ID: "P4", LocationID: "L1", Status: "COMPLETED", CreatedAt: ts(c), UpdatedAt: ts(c), TotalMoney: &Money{5000, "JPY"}})
	})
	ev = h.cycle(time.Minute)
	if got := strings.Join(kinds(ev), ","); got != "refund:R1,payment.completed:P4,payment.completed:P3" {
		t.Fatalf("events = %s", got)
	}
	if ev[0].PrevState != "PENDING" || ev[0].State != "COMPLETED" {
		t.Fatalf("refund = %+v", ev[0])
	}
	c3 := c2.Add(time.Minute)
	h.sq.do(func() {
		h.sq.payments[3].RefundedMoney, h.sq.payments[3].UpdatedAt = &Money{1500, "JPY"}, ts(c3)
	})
	ev = h.cycle(time.Minute)
	if got := strings.Join(kinds(ev), ","); got != "refund:P4" || formatMoney(ev[0].Amount) != "1,500 JPY" {
		t.Fatalf("fallback = %s %+v", got, ev)
	}
	// The refund record shows up a poll later: already reported.
	c4 := c3.Add(time.Minute)
	h.sq.do(func() {
		h.sq.refunds = append(h.sq.refunds, Refund{ID: "R4", LocationID: "L1", PaymentID: "P4", Status: "PENDING", CreatedAt: ts(c3), UpdatedAt: ts(c4), AmountMoney: &Money{1500, "JPY"}})
	})
	if ev = h.cycle(time.Minute); len(ev) != 0 {
		t.Fatalf("refund reported twice: %v", kinds(ev))
	}
	if _, ok := h.store.stream(streamRefunds).Seen[fallbackKey("P4")]; ok {
		t.Fatal("credit not used up")
	}
	// Its later status change is still news.
	h.sq.do(func() {
		h.sq.refunds[1].Status, h.sq.refunds[1].UpdatedAt = "COMPLETED", ts(c4.Add(time.Minute))
	})
	if ev = h.cycle(time.Minute); strings.Join(kinds(ev), ",") != "refund:R4" || ev[0].PrevState != "PENDING" {
		t.Fatalf("status change = %v", kinds(ev))
	}
}

func TestSettledAfterInstall(t *testing.T) {
	h := newHarness(t, `{"locationIds":"L1"}`)
	h.cycle(0)
	before := ts(t0.Add(-24 * time.Hour))
	c := t0.Add(time.Minute)
	type tl = CardPaymentTimeline
	card := func(timeline tl) *CardDetails { return &CardDetails{Timeline: &timeline} }
	h.sq.do(func() {
		h.sq.payments = []Payment{
			// Authorized before the install, captured after it.
			{ID: "CAP", LocationID: "L1", Status: "COMPLETED", CreatedAt: before, UpdatedAt: ts(c), CardDetails: card(tl{CapturedAt: ts(c)})},
			{ID: "VOID", LocationID: "L1", Status: "CANCELED", CreatedAt: before, UpdatedAt: ts(c), CardDetails: card(tl{VoidedAt: ts(c)})},
			// Captured before the install, touched after it (a refund).
			{ID: "OLD", LocationID: "L1", Status: "COMPLETED", CreatedAt: before, UpdatedAt: ts(c), CardDetails: card(tl{CapturedAt: before})},
		}
		h.sq.refunds = []Refund{
			{ID: "RDONE", LocationID: "L1", Status: "COMPLETED", CreatedAt: before, UpdatedAt: ts(c), AmountMoney: &Money{100, "JPY"}},
			{ID: "RPEND", LocationID: "L1", Status: "PENDING", CreatedAt: before, UpdatedAt: ts(c), AmountMoney: &Money{100, "JPY"}},
		}
	})
	ev := h.cycle(time.Minute)
	if got := strings.Join(kinds(ev), ","); got != "refund:RDONE,payment.completed:CAP,payment.failed:VOID" {
		t.Fatalf("events = %s", got)
	}
	h.sq.do(func() { h.sq.refunds[1].Status, h.sq.refunds[1].UpdatedAt = "COMPLETED", ts(c.Add(time.Minute)) })
	if ev := h.cycle(time.Minute); strings.Join(kinds(ev), ",") != "refund:RPEND" || ev[0].PrevState != "PENDING" {
		t.Fatalf("events = %v", kinds(ev))
	}
}

func TestCatchUpSkipsOverlap(t *testing.T) {
	h := newHarness(t, `{"locationIds":"L1"}`)
	cur := t0.Add(-time.Hour)
	h.store.commit(map[string]*StreamState{streamOrders: {Cursor: cur, Floor: cur.Add(-time.Hour), CatchUp: true, Seen: map[string]Record{}}}, nil)
	if r := h.p.begin(streamOrders, t0); !r.since.Equal(cur) {
		t.Fatalf("catch-up since = %v", r.since)
	}
	h.cycle(0)
	ss := h.store.stream(streamOrders)
	if ss.CatchUp {
		t.Fatal("catch-up not cleared by a complete poll")
	}
	if r := h.p.begin(streamOrders, t0); !r.since.Equal(cur.Add(-overlapWindow)) {
		t.Fatalf("since = %v", r.since)
	}
}

func TestFilters(t *testing.T) {
	h := newHarness(t, `{"locationIds":"L1","minAmount":1000,"notifyPaymentFailed":false,"notifyOrderCreated":false}`)
	h.cycle(0)
	c := t0.Add(time.Minute)
	h.sq.do(func() {
		h.sq.orders = []Order{
			{ID: "OPENBIG", LocationID: "L1", State: "OPEN", Version: i64(1), CreatedAt: ts(c), UpdatedAt: ts(c), TotalMoney: &Money{5000, "JPY"}},
			// A POS sale arrives completed: counts as completed.
			{ID: "POS", LocationID: "L1", State: "COMPLETED", Version: i64(1), CreatedAt: ts(c), UpdatedAt: ts(c), TotalMoney: &Money{1000, "JPY"}},
		}
		h.sq.payments = []Payment{
			{ID: "SMALL", LocationID: "L1", Status: "COMPLETED", CreatedAt: ts(c), UpdatedAt: ts(c), TotalMoney: &Money{999, "JPY"}},
			{ID: "USD", LocationID: "L1", Status: "COMPLETED", CreatedAt: ts(c), UpdatedAt: ts(c), TotalMoney: &Money{100000, "USD"}},
			{ID: "USDSMALL", LocationID: "L1", Status: "COMPLETED", CreatedAt: ts(c), UpdatedAt: ts(c), TotalMoney: &Money{99999, "USD"}},
			{ID: "FAIL", LocationID: "L1", Status: "FAILED", CreatedAt: ts(c), UpdatedAt: ts(c), TotalMoney: &Money{9000, "JPY"}},
		}
		// Refunds are listed at the configured location only; one at L2
		// is not asked for.
		h.sq.refunds = []Refund{
			{ID: "R1", LocationID: "L1", Status: "PENDING", CreatedAt: ts(c), UpdatedAt: ts(c), AmountMoney: &Money{2000, "JPY"}},
			{ID: "R2", LocationID: "L2", Status: "PENDING", CreatedAt: ts(c), UpdatedAt: ts(c), AmountMoney: &Money{2000, "JPY"}},
		}
	})
	ev := h.cycle(time.Minute)
	if got := strings.Join(kinds(ev), ","); got != "order.created:POS,refund:R1,payment.completed:USD" {
		t.Fatalf("events = %s", got)
	}
	// Filtered events are still remembered: turning a filter off later
	// does not re-report them.
	if _, ok := h.store.stream(streamPayments).Seen["SMALL"]; !ok {
		t.Fatal("filtered payment not recorded")
	}
}

func TestLocationFilterOnAllLocationRefunds(t *testing.T) {
	cfg := parseConfig(`{"locationIds":"L1"}`)
	if cfg.passes(Event{Kind: kindRefund, LocationID: "L2"}) {
		t.Fatal("refund at an unwatched location passed")
	}
	if !cfg.passes(Event{Kind: kindRefund, LocationID: "L1"}) {
		t.Fatal("refund at a watched location filtered")
	}
}

func TestDiscoversLocationsWhenNoneConfigured(t *testing.T) {
	h := newHarness(t, `{}`)
	h.sq.locations = append(h.sq.locations, Location{ID: "L3", Status: "INACTIVE"})
	h.cycle(0)
	if h.sq.locationCalls != 1 {
		t.Fatalf("location calls = %d", h.sq.locationCalls)
	}
	if got := h.sq.orderLocs[0]; strings.Join(got, ",") != "L1,L2" {
		t.Fatalf("orders at %v", got)
	}
	if len(h.sq.paymentQs) != 2 {
		t.Fatalf("payment queries = %d", len(h.sq.paymentQs))
	}
	// Refunds default to every location already.
	if len(h.sq.refundQs) != 1 || h.sq.refundQs[0].Get("location_id") != "" {
		t.Fatalf("refund queries = %v", h.sq.refundQs)
	}
	h.cycle(time.Minute)
	if h.sq.locationCalls != 1 {
		t.Fatal("locations not cached")
	}
}

func TestCursorHeldOnFailure(t *testing.T) {
	h := newHarness(t, `{"locationIds":"L1"}`)
	h.cycle(0)
	c := t0.Add(time.Minute)
	h.sq.do(func() {
		h.sq.fail["/v2/payments"] = 500
		h.sq.payments = []Payment{{ID: "P1", LocationID: "L1", Status: "COMPLETED", CreatedAt: ts(c), UpdatedAt: ts(c)}}
		h.sq.orders = []Order{{ID: "O1", LocationID: "L1", State: "OPEN", Version: i64(1), CreatedAt: ts(c), UpdatedAt: ts(c)}}
	})
	ev := h.cycle(time.Minute)
	if got := strings.Join(kinds(ev), ","); got != "order.created:O1" {
		t.Fatalf("events = %s", got)
	}
	if ss := h.store.stream(streamPayments); !ss.Cursor.Equal(t0) {
		t.Fatalf("payments cursor moved to %v", ss.Cursor)
	}
	h.sq.do(func() { delete(h.sq.fail, "/v2/payments") })
	ev = h.cycle(time.Minute)
	if got := strings.Join(kinds(ev), ","); got != "payment.completed:P1" {
		t.Fatalf("after recovery = %s", got)
	}
}

func notes(events []Event) []string {
	var out []string
	for _, e := range events {
		if e.isPluginNote() {
			out = append(out, e.Kind)
		}
	}
	return out
}

func TestWarningOnceAfterConsecutiveFailures(t *testing.T) {
	h := newHarness(t, `{"locationIds":"L1"}`)
	h.sq.fail["/v2/"] = 503
	var all []Event
	for i := 0; i < failureThreshold+3; i++ {
		all = append(all, h.cycle(time.Minute)...)
		if i == failureThreshold-2 && len(notes(all)) != 0 {
			t.Fatal("warned too early")
		}
	}
	if got := notes(all); strings.Join(got, ",") != kindWarning {
		t.Fatalf("notes = %v", got)
	}
	h.sq.do(func() { delete(h.sq.fail, "/v2/") })
	ev := h.cycle(time.Minute)
	if got := notes(ev); strings.Join(got, ",") != kindRecovered {
		t.Fatalf("recovery notes = %v", got)
	}
	if ev := h.cycle(time.Minute); len(notes(ev)) != 0 {
		t.Fatalf("second recovery note")
	}
	// The flag was reset: a new run of failures warns again.
	h.sq.do(func() { h.sq.fail["/v2/"] = 503 })
	all = nil
	for i := 0; i < failureThreshold; i++ {
		all = append(all, h.cycle(time.Minute)...)
	}
	if len(notes(all)) != 1 {
		t.Fatalf("notes after reset = %v", notes(all))
	}
}

func TestAuthFailureWarnsImmediately(t *testing.T) {
	h := newHarness(t, `{"locationIds":"L1"}`)
	h.sq.fail["/v2/"] = 401
	ev := h.cycle(0)
	if got := notes(ev); strings.Join(got, ",") != kindWarning || !strings.Contains(ev[0].Text, "401") {
		t.Fatalf("ev = %+v", ev)
	}
	if !h.p.authFail {
		t.Fatal("authFail not set")
	}
	if ev := h.cycle(time.Minute); len(ev) != 0 {
		t.Fatal("warned twice")
	}
	// The stream floor is remembered even though the first poll failed.
	if ss := h.store.stream(streamOrders); ss == nil || !ss.Floor.Equal(t0) {
		t.Fatalf("floor = %+v", ss)
	}
}

func TestParseConfig(t *testing.T) {
	c := parseConfig(`{"environment":"sandbox","locationIds":"L1, L2;L1 bad/id","pollIntervalSeconds":1,"minAmount":5}`)
	if c.BaseURL != sandboxBaseURL || strings.Join(c.LocationIDs, ",") != "L1,L2" || c.PollInterval != minPollInterval || c.MinAmount != 5 {
		t.Fatalf("%+v", c)
	}
	if len(c.Problems) != 1 {
		t.Fatalf("problems = %v", c.Problems)
	}
	d := parseConfig(`{}`)
	if d.BaseURL != productionBaseURL || d.PollInterval != 30*time.Second || d.locationsConfigured() || !d.Notify.OrderCreated || !d.Notify.Refunds {
		t.Fatalf("%+v", d)
	}
	bad := parseConfig(`{"locationIds":"???"}`)
	if !bad.locationsConfigured() || len(bad.LocationIDs) != 0 {
		t.Fatalf("unusable list widened: %+v", bad)
	}
	if parseConfig(`{"pollIntervalSeconds":999999}`).PollInterval != maxPollInterval {
		t.Fatal("interval not capped")
	}
}

func TestStoreAtomicAndBounded(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]Record{}
	for i := 0; i < maxSeenPerKey+10; i++ {
		seen[string(rune('a'+i%26))+time.Duration(i).String()] = Record{UpdatedAt: t0.Add(-2*time.Hour + time.Duration(i)*time.Second)}
	}
	var evs []Event
	for i := 0; i < maxOutbox+5; i++ {
		evs = append(evs, Event{ID: time.Duration(i).String(), Kind: kindRefund})
	}
	if err := s.commit(map[string]*StreamState{streamOrders: {Cursor: t0, Floor: t0, Seen: seen}}, evs); err != nil {
		t.Fatal(err)
	}
	s2, err := openStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if n := len(s2.stream(streamOrders).Seen); n != maxSeenPerKey {
		t.Fatalf("seen = %d", n)
	}
	out, dropped := s2.pending()
	if len(out) != maxOutbox || dropped != 5 || out[0].ID != time.Duration(5).String() {
		t.Fatalf("outbox = %d dropped = %d first = %s", len(out), dropped, out[0].ID)
	}
	if err := s2.ack(out[:10], dropped); err != nil {
		t.Fatal(err)
	}
	out, dropped = s2.pending()
	if len(out) != maxOutbox-10 || dropped != 0 {
		t.Fatalf("after ack: %d %d", len(out), dropped)
	}
}

func TestPruneKeepsTheQueryWindow(t *testing.T) {
	s, _ := openStore(t.TempDir())
	seen := map[string]Record{"old": {UpdatedAt: t0.Add(-time.Hour)}}
	for i := 0; i < maxSeenPerKey+10; i++ {
		seen[fmt.Sprint("burst", i)] = Record{UpdatedAt: t0.Add(-time.Minute)}
	}
	s.commit(map[string]*StreamState{streamOrders: {Cursor: t0, Floor: t0, Seen: seen}}, nil)
	got := s.stream(streamOrders).Seen
	if _, ok := got["old"]; ok || len(got) != maxSeenPerKey+10 {
		t.Fatalf("seen = %d, old kept = %v", len(got), ok)
	}
}

func TestRefundCreditSurvivesQuietRefundStream(t *testing.T) {
	h := newHarness(t, `{"locationIds":"L1"}`)
	h.cycle(0)
	c := t0.Add(time.Minute)
	h.sq.do(func() {
		h.sq.payments = []Payment{{ID: "P1", LocationID: "L1", Status: "COMPLETED", CreatedAt: ts(c), UpdatedAt: ts(c), TotalMoney: &Money{900, "JPY"}}}
	})
	h.cycle(time.Minute)
	// Much later (refunds list quiet throughout) the payment is refunded.
	c2 := c.Add(30 * time.Minute)
	h.clock = c2
	h.sq.do(func() { h.sq.payments[0].RefundedMoney, h.sq.payments[0].UpdatedAt = &Money{900, "JPY"}, ts(c2) })
	if ev := h.cycle(time.Minute); strings.Join(kinds(ev), ",") != "refund:P1" {
		t.Fatalf("fallback = %v", kinds(ev))
	}
	h.open(`{"locationIds":"L1"}`) // restart: the credit was saved
	h.sq.do(func() {
		h.sq.refunds = []Refund{{ID: "R1", LocationID: "L1", PaymentID: "P1", Status: "COMPLETED", CreatedAt: ts(c2), UpdatedAt: ts(c2.Add(time.Minute)), AmountMoney: &Money{900, "JPY"}}}
	})
	if ev := h.cycle(time.Minute); len(ev) != 0 {
		t.Fatalf("refund reported twice: %v", kinds(ev))
	}
}

func TestStoreCorruptFileStartsFresh(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, stateFile), []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	s, err := openStore(dir)
	if s == nil || err == nil {
		t.Fatalf("store=%v err=%v", s, err)
	}
	if s.stream(streamOrders) != nil {
		t.Fatal("not fresh")
	}
}
