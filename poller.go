package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"
)

const (
	// overlapWindow is how far before the cursor each query starts. It
	// covers Square's eventual consistency (seconds) and small clock
	// differences; duplicates it brings back are dropped by the seen-set.
	overlapWindow = 2 * time.Minute

	// failureThreshold is how many failed polls in a row earn the agent a
	// warning. A rejected token (401/403) warns on the first.
	failureThreshold = 5

	locationRefresh      = time.Hour
	locationRetry        = 5 * time.Minute
	authFailurePollFloor = 5 * time.Minute
)

// Poller asks Square for changes and queues the events they produce.
type Poller struct {
	cfg    Config
	sq     *SquareClient
	store  *Store
	notify func()
	log    *slog.Logger
	now    func() time.Time
	sleep  func(ctx context.Context, d time.Duration) error

	locNames   map[string]string
	locIDs     []string // active locations, when none are configured
	locFetched time.Time
	locErr     error

	failures int
	warned   bool
	authFail bool
}

func newPoller(cfg Config, sq *SquareClient, store *Store, notify func(), log *slog.Logger) *Poller {
	return &Poller{
		cfg:      cfg,
		sq:       sq,
		store:    store,
		notify:   notify,
		log:      log,
		now:      time.Now,
		sleep:    sleepCtx,
		locNames: map[string]string{},
	}
}

// Run polls until ctx ends.
func (p *Poller) Run(ctx context.Context) {
	for {
		p.cycle(ctx)
		if ctx.Err() != nil {
			return
		}
		wait := p.cfg.PollInterval
		if p.authFail && wait < authFailurePollFloor {
			// The token will not start working by itself; kojo restarts
			// the service when the operator saves a new one.
			wait = authFailurePollFloor
		}
		if p.sleep(ctx, wait) != nil {
			return
		}
	}
}

// note queues a plugin message for the agent.
func (p *Poller) note(kind, text string) {
	now := p.now()
	ev := Event{
		ID:   kind + ":" + strconv.FormatInt(now.UnixNano(), 10),
		Kind: kind,
		Text: text,
		At:   now,
	}
	if err := p.store.commit(nil, []Event{ev}); err != nil {
		p.log.Error("saving state failed", "err", err)
	}
	p.notify()
}

// refreshLocations keeps the location names (and, when the operator did
// not list locations, the set to watch) up to date.
func (p *Poller) refreshLocations(ctx context.Context, now time.Time) {
	due := p.locFetched.IsZero() || now.Sub(p.locFetched) >= locationRefresh
	if p.locErr != nil && now.Sub(p.locFetched) >= locationRetry {
		due = true
	}
	if !due {
		return
	}
	locs, err := p.sq.ListLocations(ctx)
	p.locFetched = now
	if err != nil {
		p.locErr = err
		return
	}
	p.locErr = nil
	names := map[string]string{}
	ids := []string{}
	for _, l := range locs {
		names[l.ID] = l.Name
		if l.Status != "INACTIVE" {
			ids = append(ids, l.ID)
		}
	}
	p.locNames, p.locIDs = names, ids
}

// watched returns the locations to poll.
func (p *Poller) watched() ([]string, error) {
	if p.cfg.locationsConfigured() {
		return p.cfg.LocationIDs, nil
	}
	if p.locIDs != nil {
		return p.locIDs, nil
	}
	if p.locErr == nil {
		p.locErr = errors.New("no locations known yet")
	}
	return nil, fmt.Errorf("listing locations (needed when no location IDs are configured): %w", p.locErr)
}

// streamRun is one stream's work in a cycle.
type streamRun struct {
	name  string
	ss    *StreamState
	first bool
	since time.Time
	// cursor is where the stream's cursor moves when the cycle succeeds.
	cursor    time.Time
	truncated bool
	touched   bool
	// dirty marks a seen-set changed even though the stream's own fetch
	// failed (refund credits written by the payments stream).
	dirty bool
	err   error
}

func (p *Poller) begin(name string, now time.Time) *streamRun {
	r := &streamRun{name: name, ss: p.store.stream(name)}
	if r.ss == nil {
		// First poll ever: start from now, no backfill.
		r.first = true
		r.ss = &StreamState{Cursor: now, Floor: now, Seen: map[string]Record{}}
	}
	r.since = r.ss.Cursor.Add(-overlapWindow)
	if r.ss.CatchUp {
		r.since = r.ss.Cursor
	}
	if r.since.Before(r.ss.Floor) {
		r.since = r.ss.Floor
	}
	r.cursor = r.ss.Cursor
	return r
}

// advance moves the pending cursor past what a fetch returned.
func advance[T any](r *streamRun, res fetchResult[T], updatedAt func(T) string) {
	if len(res.Items) > 0 {
		r.touched = true
	}
	for _, it := range res.Items {
		if t, err := parseTime(updatedAt(it)); err == nil && t.After(r.cursor) {
			r.cursor = t
		}
	}
	if res.Truncated {
		r.truncated = true
		if res.Resume.Before(r.cursor) {
			r.cursor = res.Resume
		}
	}
}

// cycle runs one poll. It returns whether every stream succeeded.
func (p *Poller) cycle(ctx context.Context) bool {
	now := p.now()
	p.refreshLocations(ctx, now)
	if ctx.Err() != nil {
		return false
	}
	locs, locErr := p.watched()
	cfg := p.cfg
	var runs []*streamRun

	var (
		orders   fetchResult[Order]
		payments fetchResult[Payment]
		refunds  fetchResult[Refund]
	)
	var oRun, pRun, rRun *streamRun

	if cfg.Notify.anyOrder() {
		oRun = p.begin(streamOrders, now)
		runs = append(runs, oRun)
		switch {
		case locErr != nil:
			oRun.err = locErr
		case len(locs) > 0:
			orders, oRun.err = p.sq.SearchOrders(ctx, locs, oRun.since)
		}
	}
	// Payments are fetched before refunds: a refund that shows up on a
	// payment is then most likely visible in the refunds list too, and
	// is reported once, from there.
	if cfg.Notify.anyPayment() {
		pRun = p.begin(streamPayments, now)
		runs = append(runs, pRun)
		switch {
		case locErr != nil:
			pRun.err = locErr
		default:
			for _, loc := range locs {
				res, err := p.sq.ListPayments(ctx, loc, pRun.since)
				if err != nil {
					pRun.err = err
					break
				}
				payments.Items = append(payments.Items, res.Items...)
				if res.Truncated {
					payments.truncateAtTime(res.Resume)
				}
			}
		}
	}
	if cfg.Notify.Refunds {
		rRun = p.begin(streamRefunds, now)
		runs = append(runs, rRun)
		switch {
		case !cfg.locationsConfigured():
			// ListPaymentRefunds covers every location by default.
			refunds, rRun.err = p.sq.ListRefunds(ctx, "", rRun.since)
		default:
			for _, loc := range cfg.LocationIDs {
				res, err := p.sq.ListRefunds(ctx, loc, rRun.since)
				if err != nil {
					rRun.err = err
					break
				}
				refunds.Items = append(refunds.Items, res.Items...)
				if res.Truncated {
					refunds.truncateAtTime(res.Resume)
				}
			}
		}
	}
	if ctx.Err() != nil {
		return false
	}

	var events []Event
	derive := func(r *streamRun, fn func(d *deriver) []Event) {
		if r == nil || r.err != nil {
			return
		}
		d := &deriver{since: r.since, locationNames: p.locNames}
		events = append(events, fn(d)...)
	}
	derive(oRun, func(d *deriver) []Event {
		advance(oRun, orders, func(o Order) string { return o.UpdatedAt })
		return d.orders(oRun.ss, orders.Items)
	})
	derive(rRun, func(d *deriver) []Event {
		advance(rRun, refunds, func(r Refund) string { return r.UpdatedAt })
		return d.refunds(rRun.ss, refunds.Items)
	})
	derive(pRun, func(d *deriver) []Event {
		advance(pRun, payments, func(p Payment) string { return p.UpdatedAt })
		var refundSS *StreamState
		if rRun != nil {
			refundSS = rRun.ss
		}
		evs := d.payments(pRun.ss, payments.Items, refundSS)
		if rRun != nil && rRun.err != nil && len(evs) > 0 {
			rRun.dirty = true // refund credits may have been added
		}
		return evs
	})

	var kept []Event
	for _, e := range events {
		if cfg.passes(e) {
			kept = append(kept, e)
		}
	}

	commit := map[string]*StreamState{}
	var errs []error
	for _, r := range runs {
		if r.err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", r.name, r.err))
			if r.first || r.dirty {
				// Remember the floor, so a later first success does not
				// skip what happened in between (and any refund credits).
				// The cursor stays where it was.
				commit[r.name] = r.ss
			}
			continue
		}
		if r.first || r.touched || r.truncated != r.ss.CatchUp || !r.cursor.Equal(r.ss.Cursor) {
			r.ss.Cursor = r.cursor
			r.ss.CatchUp = r.truncated
			commit[r.name] = r.ss
		}
	}
	if len(commit) > 0 || len(kept) > 0 {
		if err := p.store.commit(commit, kept); err != nil {
			p.log.Error("saving state failed", "err", err)
		}
	}
	if len(kept) > 0 {
		p.log.Info("queued notifications", "events", len(kept))
		p.notify()
	}
	p.health(errs)
	return len(errs) == 0
}

// health tracks consecutive failures and tells the agent once.
func (p *Poller) health(errs []error) {
	if len(errs) == 0 {
		if p.warned {
			p.log.Info("Square polling recovered")
			p.note(kindRecovered, "Square API へのアクセスが復旧しました。停止中の変更は今回以降の通知に含まれます。")
		}
		p.failures, p.warned, p.authFail = 0, false, false
		return
	}
	p.failures++
	auth := false
	msgs := make([]string, 0, len(errs))
	for _, err := range errs {
		if isAuthError(err) {
			auth = true
		}
		msgs = append(msgs, err.Error())
		p.log.Warn("Square poll failed", "err", err, "consecutive", p.failures)
	}
	p.authFail = auth
	if p.warned || (!auth && p.failures < failureThreshold) {
		return
	}
	p.warned = true
	var text string
	if auth {
		text = "Square がアクセストークンを拒否しました (401/403)。kojo の拡張設定で Square のアクセストークン・環境 (production/sandbox)・権限 (ORDERS_READ, PAYMENTS_READ, MERCHANT_PROFILE_READ) を確認するようオペレーターに伝えてください。直るまで新しい通知は届きません。"
	} else {
		text = "Square API へのアクセスが " + strconv.Itoa(p.failures) + " 回続けて失敗しています。復旧するまで新しい通知は届きません (復旧後にまとめて届きます)。"
	}
	text += " 詳細: " + truncateRunes(strings.Join(msgs, "; "), 300)
	p.note(kindWarning, text)
}
