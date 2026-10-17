package main

import (
	"fmt"
	"strings"
	"testing"
)

func TestFormatMoney(t *testing.T) {
	cases := []struct {
		m    Money
		want string
	}{
		{Money{1200, "JPY"}, "1,200 JPY"},
		{Money{1234567, "JPY"}, "1,234,567 JPY"},
		{Money{0, "JPY"}, "0 JPY"},
		{Money{1234, "USD"}, "12.34 USD"},
		{Money{5, "USD"}, "0.05 USD"},
		{Money{100000000, "USD"}, "1,000,000.00 USD"},
		{Money{-250, "USD"}, "-2.50 USD"},
		{Money{1500, "KWD"}, "1.500 KWD"},
		{Money{999, "KRW"}, "999 KRW"},
	}
	for _, c := range cases {
		m := c.m
		if got := formatMoney(&m); got != c.want {
			t.Errorf("formatMoney(%v) = %q, want %q", c.m, got, c.want)
		}
	}
}

func TestItemSummary(t *testing.T) {
	items := []LineItem{{"コーヒー", "2"}, {"ケーキ", "1"}, {"", "3"}, {"水", "1"}, {"茶", "1"}}
	if got := itemSummary(items); got != "コーヒー×2, ケーキ×1, (no name)×3 +2 more" {
		t.Fatalf("got %q", got)
	}
	if itemSummary(nil) != "" {
		t.Fatal("empty items")
	}
}

func TestFormatMessageBatch(t *testing.T) {
	events := []Event{
		{Kind: kindOrderCreated, ObjectID: "O1", Amount: &Money{1200, "JPY"}, State: "OPEN", Items: "コーヒー×2", LocationID: "L1", LocationName: "渋谷店"},
		{Kind: kindPaymentCompleted, ObjectID: "P1", Amount: &Money{1234, "USD"}, State: "COMPLETED", OrderID: "O1", LocationID: "L2"},
		{Kind: kindOrderStateChanged, ObjectID: "O2", State: "COMPLETED", PrevState: "OPEN"},
		{Kind: kindWarning, Text: "something broke"},
	}
	msg := formatMessage(events, 0)
	lines := strings.Split(msg, "\n")
	if !strings.Contains(lines[0], "Square プラグイン") || !strings.Contains(msg, "Square MCP") {
		t.Fatalf("header missing:\n%s", msg)
	}
	if !strings.Contains(msg, "3 件") {
		t.Fatalf("count missing:\n%s", msg)
	}
	want := []string{
		"- plugin.warning: something broke",
		"- order.created | id=O1 | 1,200 JPY | state=OPEN | items=コーヒー×2 | location=渋谷店 (L1)",
		"- payment.completed | id=P1 | 12.34 USD | state=COMPLETED | order=O1 | location=L2",
		"- order.state_changed | id=O2 | state=OPEN→COMPLETED",
	}
	got := lines[len(lines)-4:]
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("lines:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func TestFormatMessageCapsLines(t *testing.T) {
	var events []Event
	for i := 0; i < maxMessageLines+7; i++ {
		events = append(events, Event{Kind: kindOrderCreated, ObjectID: fmt.Sprint("O", i)})
	}
	msg := formatMessage(events, 3)
	if n := strings.Count(msg, "- order.created"); n != maxMessageLines {
		t.Fatalf("%d lines listed", n)
	}
	if !strings.Contains(msg, "ほか 7 件") || !strings.Contains(msg, "3 件を破棄") {
		t.Fatalf("summary missing:\n%s", msg[len(msg)-300:])
	}
}
