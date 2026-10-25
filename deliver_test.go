package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeKojo mimics kojo's POST /messages for one agent.
type fakeKojo struct {
	t        *testing.T
	mu       sync.Mutex
	posts    []string // accepted messages
	calls    int
	busyNext int // answer this many posts with 409 busy
	failNext int // answer this many posts with 500
}

func newFakeKojo(t *testing.T) (*fakeKojo, *KojoClient) {
	f := &fakeKojo{t: t}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	return f, newKojoClient(srv.URL+"/", "ext-token", "ag_1")
}

func writeKojoError(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	fmt.Fprintf(w, `{"error":{"code":%q,"message":"test"}}`, code)
}

func (f *fakeKojo) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "Bearer ext-token" {
		f.t.Errorf("Authorization = %q", r.Header.Get("Authorization"))
	}
	if r.Method != http.MethodPost || r.URL.Path != "/api/v1/agents/ag_1/messages" {
		f.t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		w.WriteHeader(404)
		return
	}
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var req struct {
		Content string `json:"content"`
	}
	if err := dec.Decode(&req); err != nil || req.Content == "" {
		f.t.Errorf("body: %v %+v", err, req)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	switch {
	case f.busyNext > 0:
		f.busyNext--
		writeKojoError(w, 409, "busy")
	case f.failNext > 0:
		f.failNext--
		writeKojoError(w, 500, "chat_failed")
	default:
		f.posts = append(f.posts, req.Content)
		w.WriteHeader(202)
		fmt.Fprint(w, `{"accepted":true,"queued":false}`)
	}
}

func (f *fakeKojo) snapshot() (posts []string, calls int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.posts...), f.calls
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

type deliverHarness struct {
	t     *testing.T
	kojo  *fakeKojo
	store *Store
	d     *Deliverer
	n     int
}

func startDeliverer(t *testing.T, client *KojoClient, store *Store) *Deliverer {
	d := newDeliverer(client, store, quietLog())
	d.busyDelay, d.retryMin, d.retryMax = 10*time.Millisecond, 10*time.Millisecond, 50*time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		d.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("deliverer did not stop")
		}
	})
	return d
}

func newDeliverHarness(t *testing.T) *deliverHarness {
	k, client := newFakeKojo(t)
	store, err := openStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return &deliverHarness{t: t, kojo: k, store: store, d: startDeliverer(t, client, store)}
}

// add queues one event (as the poller would) and returns its order ID.
func (h *deliverHarness) add() string {
	h.n++
	id := fmt.Sprintf("ORDER%d", h.n)
	if err := h.store.commit(nil, []Event{{ID: "o:" + id, Kind: kindOrderCreated, ObjectID: id}}); err != nil {
		h.t.Fatal(err)
	}
	h.d.Notify()
	return id
}

func (h *deliverHarness) outbox() int {
	ev, _ := h.store.pending()
	return len(ev)
}

func TestDeliverPostsToMainChat(t *testing.T) {
	h := newDeliverHarness(t)
	id := h.add()
	waitFor(t, "post", func() bool { p, _ := h.kojo.snapshot(); return len(p) == 1 })
	waitFor(t, "ack", func() bool { return h.outbox() == 0 })
	posts, _ := h.kojo.snapshot()
	if !strings.Contains(posts[0], "id="+id) {
		t.Fatalf("post = %s", posts[0])
	}
	id2 := h.add()
	waitFor(t, "second post", func() bool { p, _ := h.kojo.snapshot(); return len(p) == 2 })
	if posts, _ = h.kojo.snapshot(); strings.Contains(posts[1], "id="+id) || !strings.Contains(posts[1], "id="+id2) {
		t.Fatalf("second post resent or missing: %s", posts[1])
	}
}

func TestDeliverBusyRetries(t *testing.T) {
	h := newDeliverHarness(t)
	h.kojo.mu.Lock()
	h.kojo.busyNext = 3
	h.kojo.mu.Unlock()
	h.add()
	waitFor(t, "post after busy", func() bool { p, _ := h.kojo.snapshot(); return len(p) == 1 })
	waitFor(t, "ack", func() bool { return h.outbox() == 0 })
	if _, calls := h.kojo.snapshot(); calls != 4 {
		t.Fatalf("calls = %d", calls)
	}
}

func TestDeliverErrorKeepsOutbox(t *testing.T) {
	h := newDeliverHarness(t)
	h.kojo.mu.Lock()
	h.kojo.failNext = 2
	h.kojo.mu.Unlock()
	h.add()
	waitFor(t, "post after errors", func() bool { p, _ := h.kojo.snapshot(); return len(p) == 1 })
	waitFor(t, "ack", func() bool { return h.outbox() == 0 })
}

func TestDeliverFlushesOutboxLeftByPreviousRun(t *testing.T) {
	k, client := newFakeKojo(t)
	dir := t.TempDir()
	store, _ := openStore(dir)
	store.commit(nil, []Event{{ID: "x", Kind: kindPaymentCompleted, ObjectID: "PAY1"}})
	store, _ = openStore(dir) // restart
	startDeliverer(t, client, store)
	waitFor(t, "post", func() bool { p, _ := k.snapshot(); return len(p) == 1 })
	if p, _ := k.snapshot(); !strings.Contains(p[0], "id=PAY1") {
		t.Fatalf("post = %s", p[0])
	}
	waitFor(t, "ack", func() bool { ev, _ := store.pending(); return len(ev) == 0 })
}
