package main

import (
	"strconv"
	"strings"
	"time"
)

// Event kinds.
const (
	kindOrderCreated      = "order.created"
	kindOrderStateChanged = "order.state_changed"
	kindPaymentCompleted  = "payment.completed"
	kindPaymentFailed     = "payment.failed"
	kindRefund            = "refund"
	kindWarning           = "plugin.warning"
	kindRecovered         = "plugin.recovered"
)

// Event is one notification line. It is persisted in the outbox, so
// everything the message needs is captured when it is derived.
type Event struct {
	// ID is unique per (object, version, kind); acking uses it.
	ID           string    `json:"id"`
	Kind         string    `json:"kind"`
	ObjectID     string    `json:"objectId,omitempty"`
	Amount       *Money    `json:"amount,omitempty"`
	State        string    `json:"state,omitempty"`
	PrevState    string    `json:"prevState,omitempty"`
	Items        string    `json:"items,omitempty"`
	LocationID   string    `json:"locationId,omitempty"`
	LocationName string    `json:"locationName,omitempty"`
	OrderID      string    `json:"orderId,omitempty"`
	PaymentID    string    `json:"paymentId,omitempty"`
	Text         string    `json:"text,omitempty"`
	At           time.Time `json:"at"`
}

func (e Event) isPluginNote() bool { return e.Kind == kindWarning || e.Kind == kindRecovered }

// deriver turns fetched objects into events, updating the stream's
// seen-set as it goes.
type deriver struct {
	// since is the start of this poll's query window: an object created
	// at or after it counts as new.
	since         time.Time
	locationNames map[string]string
	// credited is set when payments left a refund credit in the refunds
	// stream, which then has to be saved.
	credited bool
}

func (d *deriver) locName(id string) string { return d.locationNames[id] }

func moneyOf(ms ...*Money) *Money {
	for _, m := range ms {
		if m != nil && m.Currency != "" {
			cp := *m
			return &cp
		}
	}
	return nil
}

// atOrAfter reports whether ts (RFC 3339) is at or after t. An
// unreadable timestamp is not.
func atOrAfter(ts string, t time.Time) bool {
	v, err := parseTime(ts)
	return err == nil && !v.Before(t)
}

// stale reports an object fetched with an older update than the one
// already recorded (pages of an overlapping query can lag).
func stale(prev Record, known bool, upd time.Time) bool {
	return known && !upd.IsZero() && upd.Before(prev.UpdatedAt)
}

func (d *deriver) orders(ss *StreamState, orders []Order) []Event {
	var out []Event
	for _, o := range orders {
		if o.ID == "" {
			continue
		}
		upd, _ := parseTime(o.UpdatedAt)
		fp := "u" + o.UpdatedAt
		if o.Version != nil {
			fp = "v" + strconv.FormatInt(*o.Version, 10)
		}
		fp += "|" + o.State
		prev, known := ss.Seen[o.ID]
		if known && prev.Fingerprint == fp || stale(prev, known, upd) {
			continue
		}
		ss.Seen[o.ID] = Record{Fingerprint: fp, State: o.State, UpdatedAt: upd}

		ev := Event{
			ObjectID:     o.ID,
			Amount:       moneyOf(o.TotalMoney),
			State:        o.State,
			Items:        itemSummary(o.LineItems),
			LocationID:   o.LocationID,
			LocationName: d.locName(o.LocationID),
			OrderID:      o.ID,
			At:           upd,
		}
		switch {
		case o.State == "DRAFT":
			continue
		case known:
			if prev.State == o.State {
				continue
			}
			ev.Kind, ev.PrevState = kindOrderStateChanged, prev.State
			if prev.State == "DRAFT" {
				// A draft is not reported; leaving the draft state is
				// when the order becomes real.
				ev.Kind = kindOrderCreated
			}
		case atOrAfter(o.CreatedAt, d.since):
			ev.Kind = kindOrderCreated
		case (o.State == "COMPLETED" || o.State == "CANCELED") && atOrAfter(o.ClosedAt, d.since):
			// Never seen before but closed just now: an order older than
			// the seen-set (or the install) that reached its end state.
			ev.Kind = kindOrderStateChanged
		default:
			continue
		}
		ev.ID = "order:" + o.ID + ":" + fp + ":" + ev.Kind
		out = append(out, ev)
	}
	return out
}

func paymentKind(status string) string {
	switch status {
	case "COMPLETED":
		return kindPaymentCompleted
	case "FAILED", "CANCELED":
		return kindPaymentFailed
	}
	return ""
}

// payments derives payment events.
//
// The refunds stream is the authority on refunds. A payment whose
// refunded_money goes up while that stream has seen no refund of it yet
// reports the increase itself, and leaves a credit in refunds (when
// given) so the refund record, once it shows up, is not reported again
// as new.
func (d *deriver) payments(ss *StreamState, payments []Payment, refunds *StreamState) []Event {
	refundKnown := refundIndex(refunds)
	var out []Event
	for _, p := range payments {
		if p.ID == "" {
			continue
		}
		upd, _ := parseTime(p.UpdatedAt)
		var refunded int64
		if p.RefundedMoney != nil {
			refunded = p.RefundedMoney.Amount
		}
		fp := p.UpdatedAt + "|" + p.Status + "|" + strconv.FormatInt(refunded, 10)
		prev, known := ss.Seen[p.ID]
		if known && prev.Fingerprint == fp || stale(prev, known, upd) {
			continue
		}
		ss.Seen[p.ID] = Record{Fingerprint: fp, State: p.Status, Refunded: refunded, UpdatedAt: upd}

		base := Event{
			ObjectID:     p.ID,
			Amount:       moneyOf(p.TotalMoney, p.AmountMoney),
			State:        p.Status,
			LocationID:   p.LocationID,
			LocationName: d.locName(p.LocationID),
			OrderID:      p.OrderID,
			PaymentID:    p.ID,
			At:           upd,
		}
		kind := paymentKind(p.Status)
		switch {
		case kind == "":
		case known && prev.State != p.Status:
			ev := base
			ev.Kind, ev.PrevState = kind, prev.State
			ev.ID = "payment:" + p.ID + ":" + fp + ":" + kind
			out = append(out, ev)
		case !known && (atOrAfter(p.CreatedAt, d.since) || atOrAfter(p.settledAt(), d.since)):
			// New, or older (an authorization from before the install)
			// but captured / voided just now.
			ev := base
			ev.Kind = kind
			ev.ID = "payment:" + p.ID + ":" + fp + ":" + kind
			out = append(out, ev)
		}
		if known && refunded > prev.Refunded && !refundKnown(p.ID) {
			ev := base
			ev.Kind = kindRefund
			ev.State = "refunded_money"
			cur := ""
			if p.RefundedMoney != nil {
				cur = p.RefundedMoney.Currency
			}
			ev.Amount = &Money{Amount: refunded - prev.Refunded, Currency: cur}
			ev.ID = "payment-refund:" + p.ID + ":" + fp
			out = append(out, ev)
			if refunds != nil {
				key := fallbackKey(p.ID)
				c := refunds.Seen[key]
				refunds.Seen[key] = Record{Fingerprint: "credit", State: cur, Refunded: c.Refunded + ev.Amount.Amount, UpdatedAt: upd}
				d.credited = true
			}
		}
	}
	return out
}

func (d *deriver) refunds(ss *StreamState, refunds []Refund) []Event {
	var out []Event
	for _, r := range refunds {
		if r.ID == "" {
			continue
		}
		upd, _ := parseTime(r.UpdatedAt)
		fp := r.UpdatedAt + "|" + r.Status
		prev, known := ss.Seen[r.ID]
		if known && prev.Fingerprint == fp || stale(prev, known, upd) {
			continue
		}
		ss.Seen[r.ID] = Record{Fingerprint: fp, State: r.Status, PaymentID: r.PaymentID, UpdatedAt: upd}
		ev := Event{
			Kind:         kindRefund,
			ObjectID:     r.ID,
			Amount:       moneyOf(r.AmountMoney),
			State:        r.Status,
			LocationID:   r.LocationID,
			LocationName: d.locName(r.LocationID),
			OrderID:      r.OrderID,
			PaymentID:    r.PaymentID,
			At:           upd,
		}
		switch {
		case known:
			if prev.State == r.Status {
				continue
			}
			ev.PrevState = prev.State
		case atOrAfter(r.CreatedAt, d.since):
			if consumeCredit(ss, r) {
				// Already reported from the payment's refunded_money.
				continue
			}
		case r.Status != "PENDING":
			// Older than the window (from before the install) and settled
			// just now: its PENDING state was never seen.
			if consumeCredit(ss, r) {
				continue
			}
		default:
			continue
		}
		ev.ID = "refund:" + r.ID + ":" + fp
		out = append(out, ev)
	}
	return out
}

// fallbackKey is the refunds seen-set entry holding the amount already
// reported from a payment's refunded_money.
func fallbackKey(paymentID string) string { return "credit:" + paymentID }

// consumeCredit reports whether a refund newly seen was already reported
// through its payment's refunded_money, and uses up that credit.
func consumeCredit(ss *StreamState, r Refund) bool {
	key := fallbackKey(r.PaymentID)
	c, ok := ss.Seen[key]
	if r.PaymentID == "" || !ok || r.AmountMoney == nil || r.AmountMoney.Currency != c.State || r.AmountMoney.Amount > c.Refunded {
		return false
	}
	c.Refunded -= r.AmountMoney.Amount
	if c.Refunded <= 0 {
		delete(ss.Seen, key)
	} else {
		ss.Seen[key] = c
	}
	return true
}

// refundIndex reports which payments the refunds stream knows a refund
// of (a refund record, or a credit from an earlier refunded_money
// report).
func refundIndex(ss *StreamState) func(string) bool {
	set := map[string]bool{}
	if ss != nil {
		for key, r := range ss.Seen {
			if r.PaymentID != "" {
				set[r.PaymentID] = true
			}
			if id, ok := strings.CutPrefix(key, "credit:"); ok {
				set[id] = true
			}
		}
	}
	return func(id string) bool { return set[id] }
}

const maxItemsShown = 3

// itemSummary is "name×qty, name×qty, name×qty +N more".
func itemSummary(items []LineItem) string {
	if len(items) == 0 {
		return ""
	}
	parts := make([]string, 0, maxItemsShown)
	for i, it := range items {
		if i >= maxItemsShown {
			break
		}
		name := strings.Join(strings.Fields(it.Name), " ")
		if name == "" {
			name = "(no name)"
		}
		name = truncateRunes(name, 40)
		qty := strings.TrimSpace(it.Quantity)
		if qty == "" {
			qty = "1"
		}
		parts = append(parts, name+"×"+qty)
	}
	s := strings.Join(parts, ", ")
	if extra := len(items) - maxItemsShown; extra > 0 {
		s += " +" + strconv.Itoa(extra) + " more"
	}
	return s
}

func truncateRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}

// passes applies the operator's filters to one event.
func (c Config) passes(e Event) bool {
	if e.isPluginNote() {
		return true
	}
	if c.locationsConfigured() && e.LocationID != "" && !contains(c.LocationIDs, e.LocationID) {
		return false
	}
	n := c.Notify
	switch e.Kind {
	case kindOrderCreated:
		// An order that is already complete when first seen (a POS sale)
		// is one event; it counts under its state's toggle too.
		if !(n.OrderCreated || e.State == "COMPLETED" && n.OrderCompleted || e.State == "CANCELED" && n.OrderCanceled) {
			return false
		}
	case kindOrderStateChanged:
		switch e.State {
		case "COMPLETED":
			if !n.OrderCompleted {
				return false
			}
		case "CANCELED":
			if !n.OrderCanceled {
				return false
			}
		default:
			// Any other transition (back to OPEN) is unusual enough to
			// report whenever orders are watched at all.
			if !n.anyOrder() {
				return false
			}
		}
	case kindPaymentCompleted:
		if !n.PaymentCompleted {
			return false
		}
	case kindPaymentFailed:
		if !n.PaymentFailed {
			return false
		}
	case kindRefund:
		if !n.Refunds {
			return false
		}
	default:
		return false
	}
	if c.MinAmount > 0 && e.Amount != nil {
		if float64(abs64(e.Amount.Amount)) < c.MinAmount*pow10(currencyDecimals(e.Amount.Currency))-1e-9 {
			return false
		}
	}
	return true
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func abs64(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}

func pow10(n int) float64 {
	f := 1.0
	for i := 0; i < n; i++ {
		f *= 10
	}
	return f
}
