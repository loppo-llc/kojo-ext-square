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

// fakeKojo mimics kojo's ext-chat and steer routes for one agent.
type fakeKojo struct {
	t       *testing.T
	mu      sync.Mutex
	running bool
	turns   []string // messages that started a turn
	steers  []string // contents steered in
	starts  int      // ext-chat calls, accepted or not

	busyNext  int    // answer this many ext-chat calls with 409 busy
	failNext  int    // answer this many ext-chat calls with 500
	steerCode string // answer steer with this error code ("" = OK)
	release   chan struct{}
}

func newFakeKojo(t *testing.T) (*fakeKojo, *KojoClient) {
	f := &fakeKojo{t: t, release: make(chan struct{}, 16)}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(f.release) })
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
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields() // as kojo does
	switch r.URL.Path {
	case "/api/v1/agents/ag_1/ext-chat":
		var req struct {
			SessionKey        string `json:"sessionKey"`
			Message           string `json:"message"`
			SystemPromptExtra string `json:"systemPromptExtra"`
		}
		if err := dec.Decode(&req); err != nil {
			f.t.Errorf("ext-chat body: %v", err)
			writeKojoError(w, 400, "bad_request")
			return
		}
		if req.SessionKey != sessionKey || req.SystemPromptExtra == "" {
			f.t.Errorf("ext-chat req = %+v", req)
		}
		f.mu.Lock()
		f.starts++
		switch {
		case f.busyNext > 0:
			f.busyNext--
			f.mu.Unlock()
			writeKojoError(w, 409, "busy")
			return
		case f.failNext > 0:
			f.failNext--
			f.mu.Unlock()
			writeKojoError(w, 500, "internal")
			return
		case f.running:
			f.mu.Unlock()
			writeKojoError(w, 409, "busy")
			return
		}
		f.running = true
		f.turns = append(f.turns, req.Message)
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		fmt.Fprintln(w, `{"type":"status","status":"thinking"}`)
		w.(http.Flusher).Flush()
		select {
		case <-f.release:
		case <-r.Context().Done():
		}
		fmt.Fprintln(w, `{"type":"done","message":{"content":"ok"}}`)
		f.mu.Lock()
		f.running = false
		f.mu.Unlock()
	case "/api/v1/agents/ag_1/ext-chat/steer":
		var req struct {
			SessionKey string `json:"sessionKey"`
			Content    string `json:"content"`
		}
		if err := dec.Decode(&req); err != nil || req.SessionKey != sessionKey {
			f.t.Errorf("steer body: %v %+v", err, req)
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		f.steers = append(f.steers, req.Content)
		switch {
		case f.steerCode == "steer_uncertain":
			writeKojoError(w, 502, f.steerCode)
		case f.steerCode != "":
			writeKojoError(w, 409, f.steerCode)
		case !f.running:
			writeKojoError(w, 409, "not_busy")
		default:
			fmt.Fprint(w, `{"ok":true}`)
		}
	default:
		f.t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		w.WriteHeader(404)
	}
}

func (f *fakeKojo) snapshot() (turns, steers []string, running bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.turns...), append([]string(nil), f.steers...), f.running
}

func (f *fakeKojo) endTurn() { f.release <- struct{}{} }

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

func newDeliverHarness(t *testing.T) *deliverHarness {
	k, client := newFakeKojo(t)
	store, err := openStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
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
	return &deliverHarness{t: t, kojo: k, store: store, d: d}
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

func TestDeliverTurnThenSteer(t *testing.T) {
	h := newDeliverHarness(t)
	a := h.add()
	waitFor(t, "first turn", func() bool { turns, _, _ := h.kojo.snapshot(); return len(turns) == 1 })
	turns, _, _ := h.kojo.snapshot()
	if !strings.Contains(turns[0], "id="+a) {
		t.Fatalf("turn = %s", turns[0])
	}
	waitFor(t, "ack", func() bool { return h.outbox() == 0 })

	// While our turn runs, the next batch is steered into it.
	b := h.add()
	waitFor(t, "steer", func() bool { _, steers, _ := h.kojo.snapshot(); return len(steers) == 1 })
	_, steers, _ := h.kojo.snapshot()
	if !strings.Contains(steers[0], "id="+b) || strings.Contains(steers[0], "id="+a+" ") {
		t.Fatalf("steer = %s", steers[0])
	}
	waitFor(t, "ack", func() bool { return h.outbox() == 0 })

	// After the turn ends, the next batch is a new turn.
	h.kojo.endTurn()
	waitFor(t, "turn end", func() bool { _, _, running := h.kojo.snapshot(); return !running })
	time.Sleep(20 * time.Millisecond) // let the deliverer see the stream close
	c := h.add()
	waitFor(t, "second turn", func() bool { turns, _, _ := h.kojo.snapshot(); return len(turns) == 2 })
	turns, steers, _ = h.kojo.snapshot()
	if !strings.Contains(turns[1], "id="+c) || len(steers) != 1 {
		t.Fatalf("turns=%v steers=%v", turns, steers)
	}
}

func TestDeliverSteerUnsupportedQueuesUntilTurnEnds(t *testing.T) {
	h := newDeliverHarness(t)
	h.add()
	waitFor(t, "first turn", func() bool { turns, _, _ := h.kojo.snapshot(); return len(turns) == 1 })
	h.kojo.mu.Lock()
	h.kojo.steerCode = "steer_unsupported"
	h.kojo.mu.Unlock()

	b := h.add()
	waitFor(t, "steer attempt", func() bool { _, steers, _ := h.kojo.snapshot(); return len(steers) == 1 })
	c := h.add()
	time.Sleep(50 * time.Millisecond)
	turns, steers, _ := h.kojo.snapshot()
	if len(steers) != 1 || len(turns) != 1 {
		t.Fatalf("kept trying while queued: turns=%d steers=%d", len(turns), len(steers))
	}
	if h.outbox() != 2 {
		t.Fatalf("outbox = %d", h.outbox())
	}

	h.kojo.endTurn()
	waitFor(t, "queued turn", func() bool { turns, _, _ := h.kojo.snapshot(); return len(turns) == 2 })
	turns, _, _ = h.kojo.snapshot()
	if !strings.Contains(turns[1], "id="+b) || !strings.Contains(turns[1], "id="+c) || !strings.Contains(turns[1], "2 件") {
		t.Fatalf("queued turn = %s", turns[1])
	}
	waitFor(t, "ack", func() bool { return h.outbox() == 0 })
}

func TestDeliverBusyRetries(t *testing.T) {
	h := newDeliverHarness(t)
	h.kojo.busyNext = 3
	h.add()
	waitFor(t, "turn", func() bool { turns, _, _ := h.kojo.snapshot(); return len(turns) == 1 })
	waitFor(t, "ack", func() bool { return h.outbox() == 0 })
	h.kojo.mu.Lock()
	defer h.kojo.mu.Unlock()
	if h.kojo.starts != 4 {
		t.Fatalf("starts = %d", h.kojo.starts)
	}
}

func TestDeliverErrorKeepsOutbox(t *testing.T) {
	h := newDeliverHarness(t)
	h.kojo.failNext = 2
	h.add()
	waitFor(t, "two failures", func() bool {
		h.kojo.mu.Lock()
		defer h.kojo.mu.Unlock()
		return h.kojo.starts >= 2
	})
	if turns, _, _ := h.kojo.snapshot(); len(turns) == 0 && h.outbox() != 1 {
		t.Fatal("outbox lost on error")
	}
	waitFor(t, "turn", func() bool { turns, _, _ := h.kojo.snapshot(); return len(turns) == 1 })
	waitFor(t, "ack", func() bool { return h.outbox() == 0 })
}

func TestDeliverSteerUncertainIsNotResent(t *testing.T) {
	h := newDeliverHarness(t)
	h.add()
	waitFor(t, "turn", func() bool { turns, _, _ := h.kojo.snapshot(); return len(turns) == 1 })
	h.kojo.mu.Lock()
	h.kojo.steerCode = "steer_uncertain"
	h.kojo.mu.Unlock()
	h.add()
	waitFor(t, "ack", func() bool { _, steers, _ := h.kojo.snapshot(); return len(steers) == 1 && h.outbox() == 0 })
	time.Sleep(50 * time.Millisecond)
	if _, steers, _ := h.kojo.snapshot(); len(steers) != 1 {
		t.Fatalf("resent: %d", len(steers))
	}
}

func TestDeliverNotBusyStartsNewTurn(t *testing.T) {
	h := newDeliverHarness(t)
	h.add()
	waitFor(t, "turn", func() bool { turns, _, _ := h.kojo.snapshot(); return len(turns) == 1 })
	// kojo says the turn is over before our stream has noticed.
	h.kojo.mu.Lock()
	h.kojo.steerCode = "not_busy"
	h.kojo.mu.Unlock()
	b := h.add()
	waitFor(t, "steer attempt", func() bool { _, steers, _ := h.kojo.snapshot(); return len(steers) >= 1 })
	h.kojo.endTurn()
	waitFor(t, "new turn", func() bool { turns, _, _ := h.kojo.snapshot(); return len(turns) == 2 })
	if turns, _, _ := h.kojo.snapshot(); !strings.Contains(turns[1], "id="+b) {
		t.Fatalf("turn = %s", turns[1])
	}
	waitFor(t, "ack", func() bool { return h.outbox() == 0 })
}

func TestDeliverFlushesOutboxLeftByPreviousRun(t *testing.T) {
	k, client := newFakeKojo(t)
	dir := t.TempDir()
	store, _ := openStore(dir)
	store.commit(nil, []Event{{ID: "x", Kind: kindPaymentCompleted, ObjectID: "PAY1"}})
	store, _ = openStore(dir) // restart
	d := newDeliverer(client, store, quietLog())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		d.Run(ctx)
	}()
	defer func() {
		cancel()
		<-done
	}()
	waitFor(t, "turn", func() bool { turns, _, _ := k.snapshot(); return len(turns) == 1 })
	if turns, _, _ := k.snapshot(); !strings.Contains(turns[0], "id=PAY1") {
		t.Fatalf("turn = %s", turns[0])
	}
	waitFor(t, "ack", func() bool { ev, _ := store.pending(); return len(ev) == 0 })
}

func TestReadTurn(t *testing.T) {
	cases := []struct {
		body     string
		terminal bool
		reply    string
		errText  string
		err      bool
	}{
		{"{\"type\":\"status\"}\n{\"type\":\"done\",\"message\":{\"content\":\"hi\"}}\n", true, "hi", "", false},
		{"{\"type\":\"error\",\"errorMessage\":\"boom\"}\n", true, "", "boom", false},
		{"{\"type\":\"status\"}\nnot json\n", false, "", "", true},
	}
	for i, c := range cases {
		res := readTurn(context.Background(), func() {}, strings.NewReader(c.body))
		if res.Terminal != c.terminal || res.Reply != c.reply || res.Error != c.errText || (res.Err != nil) != c.err {
			t.Errorf("case %d: %+v", i, res)
		}
	}
}
