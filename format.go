package main

import (
	"strconv"
	"strings"
)

const (
	maxMessageLines = 50
	maxLineRunes    = 400
)

// currencyDecimals is the number of minor-unit digits of an ISO 4217
// currency. Square amounts are always in the smallest unit.
func currencyDecimals(cur string) int {
	switch strings.ToUpper(cur) {
	case "BIF", "CLP", "DJF", "GNF", "ISK", "JPY", "KMF", "KRW", "PYG",
		"RWF", "UGX", "UYI", "VND", "VUV", "XAF", "XOF", "XPF":
		return 0
	case "BHD", "IQD", "JOD", "KWD", "LYD", "OMR", "TND":
		return 3
	}
	return 2
}

// formatMoney renders minor units in the currency's main unit:
// 1200 JPY → "1,200 JPY", 1234 USD → "12.34 USD".
func formatMoney(m *Money) string {
	if m == nil {
		return ""
	}
	dec := currencyDecimals(m.Currency)
	neg := m.Amount < 0
	amt := strconv.FormatInt(abs64(m.Amount), 10)
	if dec > 0 {
		for len(amt) <= dec {
			amt = "0" + amt
		}
	}
	intPart, frac := amt[:len(amt)-dec], amt[len(amt)-dec:]
	var b strings.Builder
	if neg {
		b.WriteByte('-')
	}
	for i, r := range intPart {
		if i > 0 && (len(intPart)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(r)
	}
	if dec > 0 {
		b.WriteByte('.')
		b.WriteString(frac)
	}
	if m.Currency != "" {
		b.WriteByte(' ')
		b.WriteString(m.Currency)
	}
	return b.String()
}

// formatLine is one event: kind, ID, amount, state, items, location.
func formatLine(e Event) string {
	if e.isPluginNote() {
		return "- " + e.Kind + ": " + e.Text
	}
	parts := []string{e.Kind, "id=" + e.ObjectID}
	if e.Amount != nil {
		parts = append(parts, formatMoney(e.Amount))
	}
	if e.State != "" {
		st := e.State
		if e.PrevState != "" {
			st = e.PrevState + "→" + e.State
		}
		parts = append(parts, "state="+st)
	}
	if e.Items != "" {
		parts = append(parts, "items="+e.Items)
	}
	if e.OrderID != "" && e.OrderID != e.ObjectID {
		parts = append(parts, "order="+e.OrderID)
	}
	if e.PaymentID != "" && e.PaymentID != e.ObjectID {
		parts = append(parts, "payment="+e.PaymentID)
	}
	if e.LocationID != "" {
		loc := e.LocationID
		if e.LocationName != "" {
			loc = e.LocationName + " (" + e.LocationID + ")"
		}
		parts = append(parts, "location="+loc)
	}
	return "- " + truncateRunes(strings.Join(parts, " | "), maxLineRunes)
}

// formatMessage builds the one message a delivery sends. Plugin notes
// come first; past maxMessageLines the rest is counted, not listed.
func formatMessage(events []Event, dropped int) string {
	var notes, rest []Event
	for _, e := range events {
		if e.isPluginNote() {
			notes = append(notes, e)
		} else {
			rest = append(rest, e)
		}
	}
	var b strings.Builder
	b.WriteString("[Square プラグイン (kojo-ext-square) からの自動通知]\n")
	if len(rest) > 0 {
		b.WriteString("Square で " + strconv.Itoa(len(rest)) + " 件の動きがありました。詳細 (明細・顧客・決済内容など) は Square MCP で確認してください。\n")
	}
	b.WriteByte('\n')
	lines := 0
	for _, e := range append(notes, rest...) {
		if lines >= maxMessageLines {
			break
		}
		b.WriteString(formatLine(e))
		b.WriteByte('\n')
		lines++
	}
	if more := len(notes) + len(rest) - lines; more > 0 {
		b.WriteString("- ほか " + strconv.Itoa(more) + " 件は省略しました。Square MCP で直近の注文・決済・返金を確認してください。\n")
	}
	if dropped > 0 {
		b.WriteString("- 保留中の通知が上限を超えたため、古い通知 " + strconv.Itoa(dropped) + " 件を破棄しました。Square MCP で取りこぼしがないか確認してください。\n")
	}
	return strings.TrimRight(b.String(), "\n")
}

// systemPromptExtra tells the agent where the turn came from.
const systemPromptExtra = `This turn was started by the kojo Square extension (kojo-ext-square), not by a person. ` +
	`It polls the Square API and reports new or changed orders, payments and refunds as one-line summaries; it never reads more than that. ` +
	`Use the Square MCP server's tools when you need details (line items, customer, tenders, refund reasons). ` +
	`Your text reply in this session is not delivered to anyone: act through your tools, and use your usual channels if a person needs to know. ` +
	`More notifications may arrive while you work; they are added to this turn.`
