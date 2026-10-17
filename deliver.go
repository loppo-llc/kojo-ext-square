package main

import (
	"context"
	"log/slog"
	"time"
)

// sessionKey is the one ext-chat session the notifications go to. kojo
// prefixes it with the agent and package IDs.
const sessionKey = "square"

// kojoAPI is what the deliverer needs from kojo.
type kojoAPI interface {
	StartTurn(ctx context.Context, req ChatRequest) (<-chan TurnResult, error)
	Steer(ctx context.Context, sessionKey, content string) error
}

// Deliverer hands the outbox to the agent, one message at a time.
//
//   - With no turn running, the outbox becomes a new turn.
//   - While the turn it started is still running, the outbox is steered
//     into that turn.
//   - When steering is not possible (the backend does not support it)
//     the outbox waits for the turn to end and becomes the next turn.
//   - When kojo refuses (busy, an error, kojo down) the outbox stays
//     and is retried later.
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
	steerWait time.Duration
}

func newDeliverer(k kojoAPI, store *Store, log *slog.Logger) *Deliverer {
	return &Deliverer{
		kojo:      k,
		store:     store,
		log:       log,
		wake:      make(chan struct{}, 1),
		busyDelay: 2 * time.Second,
		retryMin:  5 * time.Second,
		retryMax:  5 * time.Minute,
		steerWait: 30 * time.Second,
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
		turn      <-chan TurnResult // non-nil while our turn runs
		steerable bool
		retry     *time.Timer
		retryC    <-chan time.Time
		failures  int
	)
	schedule := func(after time.Duration) {
		if retry != nil {
			retry.Stop()
		}
		retry = time.NewTimer(after)
		retryC = retry.C
	}
	backoff := func() time.Duration {
		failures++
		dl := d.retryMin << min(failures-1, 16)
		if dl > d.retryMax || dl <= 0 {
			dl = d.retryMax
		}
		return dl
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
		text := formatMessage(events, dropped)
		if turn != nil {
			if !steerable {
				return // waiting for the turn to end
			}
			sctx, cancel := context.WithTimeout(ctx, d.steerWait)
			err := d.kojo.Steer(sctx, sessionKey, text)
			cancel()
			switch code := apiErrorCode(err); {
			case err == nil:
				d.accepted(events, dropped, "steered into the running turn")
				failures = 0
				return
			case code == "steer_uncertain":
				// kojo could not confirm delivery, but the turn may have
				// it. Sending it again could make the agent act twice.
				d.log.Warn("steer delivery unconfirmed; not resending", "err", err)
				d.accepted(events, dropped, "steer unconfirmed")
				failures = 0
				return
			case code == "not_busy":
				// The turn ended before our stream noticed; start a new one.
			case code == "steer_unsupported" || code == "forbidden":
				d.log.Info("cannot steer the running turn; queueing until it ends", "code", code)
				steerable = false
				return
			default:
				if ctx.Err() != nil {
					return
				}
				d.log.Warn("steer failed; will retry", "err", err)
				schedule(backoff())
				return
			}
		}
		ch, err := d.kojo.StartTurn(ctx, ChatRequest{
			SessionKey:        sessionKey,
			Message:           text,
			SystemPromptExtra: systemPromptExtra,
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if apiErrorCode(err) == "busy" {
				// kojo still holds the session for a moment after a
				// turn's stream closes.
				d.log.Info("session busy; retrying", "events", len(events))
				schedule(d.busyDelay)
				return
			}
			wait := backoff()
			d.log.Warn("ext-chat failed; will retry", "err", err, "events", len(events), "retryIn", wait)
			schedule(wait)
			return
		}
		d.accepted(events, dropped, "new turn")
		failures = 0
		turn, steerable = ch, true
	}

	for {
		attempt()
		select {
		case <-ctx.Done():
			return
		case <-d.wake:
		case <-retryC:
			retry, retryC = nil, nil
		case res := <-turn:
			turn, steerable = nil, false
			switch {
			case res.Err != nil:
				d.log.Warn("turn stream ended early", "err", res.Err)
			case res.Error != "":
				d.log.Warn("agent reported an error", "err", res.Error)
			default:
				d.log.Debug("turn finished", "reply", truncateRunes(res.Reply, 200))
			}
		}
	}
}

func (d *Deliverer) accepted(events []Event, dropped int, how string) {
	d.log.Info("notified the agent", "events", len(events), "via", how)
	if err := d.store.ack(events, dropped); err != nil {
		d.log.Error("saving state failed", "err", err)
	}
}
