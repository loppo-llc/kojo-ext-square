package main

import (
	"context"
	"log/slog"
	"time"
)

// kojoAPI is what the deliverer needs from kojo.
type kojoAPI interface {
	PostMessage(ctx context.Context, content string) error
}

// Deliverer hands the outbox to the agent's main chat, one message at a
// time, so the agent's turn and its reply are visible to the user.
//
//   - kojo accepts the message only while the agent is idle; a running
//     turn answers busy and the outbox waits for the next attempt.
//   - Any other refusal (an error, kojo down) is retried with backoff.
//
// Events leave the outbox only once kojo has accepted them.
type Deliverer struct {
	kojo  kojoAPI
	store *Store
	log   *slog.Logger
	wake  chan struct{}

	busyDelay time.Duration
	retryMin  time.Duration
	retryMax  time.Duration
}

func newDeliverer(k kojoAPI, store *Store, log *slog.Logger) *Deliverer {
	return &Deliverer{
		kojo:      k,
		store:     store,
		log:       log,
		wake:      make(chan struct{}, 1),
		busyDelay: 15 * time.Second,
		retryMin:  5 * time.Second,
		retryMax:  5 * time.Minute,
	}
}

// Notify asks for a delivery attempt soon.
func (d *Deliverer) Notify() {
	select {
	case d.wake <- struct{}{}:
	default:
	}
}

// Run delivers until ctx ends.
func (d *Deliverer) Run(ctx context.Context) {
	var (
		retry    *time.Timer
		retryC   <-chan time.Time
		failures int
	)
	schedule := func(after time.Duration) {
		if retry != nil {
			retry.Stop()
		}
		retry = time.NewTimer(after)
		retryC = retry.C
	}
	defer func() {
		if retry != nil {
			retry.Stop()
		}
	}()

	attempt := func() {
		events, dropped := d.store.pending()
		if len(events) == 0 && dropped == 0 {
			return
		}
		err := d.kojo.PostMessage(ctx, formatMessage(events, dropped))
		switch {
		case err == nil:
			d.log.Info("notified the agent", "events", len(events))
			if err := d.store.ack(events, dropped); err != nil {
				d.log.Error("saving state failed", "err", err)
			}
			failures = 0
		case ctx.Err() != nil:
		case apiErrorCode(err) == "busy":
			d.log.Info("agent busy; retrying", "events", len(events))
			schedule(d.busyDelay)
		default:
			failures++
			wait := d.retryMin << min(failures-1, 16)
			if wait > d.retryMax || wait <= 0 {
				wait = d.retryMax
			}
			d.log.Warn("posting to kojo failed; will retry", "err", err, "events", len(events), "retryIn", wait)
			schedule(wait)
		}
	}

	for {
		attempt()
		select {
		case <-ctx.Done():
			return
		case <-d.wake:
		case <-retryC:
			retry, retryC = nil, nil
		}
	}
}
