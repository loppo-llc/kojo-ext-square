package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

const (
	stateFile     = "state.json"
	stateVersion  = 1
	maxSeenPerKey = 5000
	// maxOutbox bounds the notifications waiting for kojo. Past it the
	// oldest are dropped and counted; the next message says how many.
	maxOutbox = 1000
)

// Stream names.
const (
	streamOrders   = "orders"
	streamPayments = "payments"
	streamRefunds  = "refunds"
)

// Record is what the poller remembers about one object.
type Record struct {
	// Fingerprint is the version (orders) or updated_at plus status: an
	// object fetched again with the same fingerprint is a duplicate.
	Fingerprint string    `json:"f"`
	State       string    `json:"s,omitempty"`
	Refunded    int64     `json:"r,omitempty"`
	PaymentID   string    `json:"p,omitempty"`
	UpdatedAt   time.Time `json:"u"`
}

// StreamState is the persisted position of one stream.
type StreamState struct {
	// Cursor is the newest updated_at handled. The next query starts an
	// overlap window before it.
	Cursor time.Time `json:"cursor"`
	// Floor is when the stream was first polled. Nothing older is
	// reported: a fresh install does not backfill.
	Floor time.Time         `json:"floor"`
	Seen  map[string]Record `json:"seen"`
	// CatchUp is set when the last poll was cut short at maxPages. The
	// next query then starts at the cursor itself, not an overlap window
	// before it, so a burst longer than the window cannot make every poll
	// read the same pages. The overlap comes back once it has caught up.
	CatchUp bool `json:"catchUp,omitempty"`
}

// State is everything persisted in the data directory.
type State struct {
	Version int                     `json:"version"`
	Streams map[string]*StreamState `json:"streams"`
	// Outbox holds notifications not yet handed to kojo.
	Outbox []Event `json:"outbox,omitempty"`
	// Dropped counts notifications lost to the outbox bound since the
	// last message that reported it.
	Dropped int `json:"dropped,omitempty"`
}

// Store guards State and writes it to disk atomically.
type Store struct {
	mu    sync.Mutex
	path  string // "" keeps state in memory only
	state State
}

// openStore loads the state file. A file that cannot be parsed is moved
// aside and replaced by a fresh state, which means "start from now".
func openStore(dir string) (*Store, error) {
	s := &Store{state: newState()}
	if dir == "" {
		return s, nil
	}
	s.path = filepath.Join(dir, stateFile)
	data, err := os.ReadFile(s.path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return s, nil
	case err != nil:
		return nil, err
	}
	var st State
	if err := json.Unmarshal(data, &st); err != nil || st.Version != stateVersion {
		bad := s.path + ".corrupt-" + time.Now().UTC().Format("20060102T150405")
		_ = os.Rename(s.path, bad)
		return s, fmt.Errorf("state file unreadable, moved to %s; starting from now", bad)
	}
	if st.Streams == nil {
		st.Streams = map[string]*StreamState{}
	}
	for _, ss := range st.Streams {
		if ss.Seen == nil {
			ss.Seen = map[string]Record{}
		}
	}
	s.state = st
	return s, nil
}

func newState() State {
	return State{Version: stateVersion, Streams: map[string]*StreamState{}}
}

// saveLocked writes the state atomically: temp file, fsync, rename.
func (s *Store) saveLocked() error {
	if s.path == "" {
		return nil
	}
	data, err := json.Marshal(&s.state)
	if err != nil {
		return err
	}
	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, stateFile+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, s.path)
}

// stream returns a copy of a stream's state, or nil if it was never
// polled.
func (s *Store) stream(name string) *StreamState {
	s.mu.Lock()
	defer s.mu.Unlock()
	ss := s.state.Streams[name]
	if ss == nil {
		return nil
	}
	cp := &StreamState{Cursor: ss.Cursor, Floor: ss.Floor, CatchUp: ss.CatchUp, Seen: make(map[string]Record, len(ss.Seen))}
	for k, v := range ss.Seen {
		cp.Seen[k] = v
	}
	return cp
}

// commit replaces the given streams and appends events to the outbox in
// one write, so a cursor never moves past events that are not queued.
func (s *Store) commit(streams map[string]*StreamState, events []Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for name, ss := range streams {
		// Records the next query can return again are kept whatever the
		// bound: forgetting one would report it again as new.
		pruneSeen(ss.Seen, maxSeenPerKey, ss.Cursor.Add(-overlapWindow))
		s.state.Streams[name] = ss
	}
	s.state.Outbox = append(s.state.Outbox, events...)
	if over := len(s.state.Outbox) - maxOutbox; over > 0 {
		s.state.Outbox = append([]Event(nil), s.state.Outbox[over:]...)
		s.state.Dropped += over
	}
	return s.saveLocked()
}

// pending returns the queued notifications and the dropped count.
func (s *Store) pending() ([]Event, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Event(nil), s.state.Outbox...), s.state.Dropped
}

// ack removes delivered notifications. dropped is the dropped count the
// delivered message reported.
func (s *Store) ack(delivered []Event, dropped int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	done := make(map[string]bool, len(delivered))
	for _, e := range delivered {
		done[e.ID] = true
	}
	kept := s.state.Outbox[:0:0]
	for _, e := range s.state.Outbox {
		if !done[e.ID] {
			kept = append(kept, e)
		}
	}
	s.state.Outbox = kept
	s.state.Dropped -= dropped
	if s.state.Dropped < 0 {
		s.state.Dropped = 0
	}
	return s.saveLocked()
}

// pruneSeen keeps the limit most recently updated records, and every
// record updated at or after keepFrom.
func pruneSeen(seen map[string]Record, limit int, keepFrom time.Time) {
	if len(seen) <= limit {
		return
	}
	type kv struct {
		id string
		at time.Time
	}
	all := make([]kv, 0, len(seen))
	for id, r := range seen {
		all = append(all, kv{id, r.UpdatedAt})
	}
	sort.Slice(all, func(i, j int) bool {
		if !all[i].at.Equal(all[j].at) {
			return all[i].at.Before(all[j].at)
		}
		return all[i].id < all[j].id
	})
	for _, e := range all[:len(all)-limit] {
		if !e.at.Before(keepFrom) {
			break // sorted: the rest are newer still
		}
		delete(seen, e.id)
	}
}
