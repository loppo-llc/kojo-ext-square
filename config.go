package main

import (
	"encoding/json"
	"os"
	"regexp"
	"strings"
	"time"
)

// Square API base URLs. Credentials from one do not work on the other.
const (
	productionBaseURL = "https://connect.squareup.com"
	sandboxBaseURL    = "https://connect.squareupsandbox.com"
)

const (
	defaultPollInterval = 30 * time.Second
	minPollInterval     = 10 * time.Second
	maxPollInterval     = time.Hour
)

// Config is the per-agent settings object (KOJO_EXT_CONFIG). The access
// token is a secret and arrives separately, in KOJO_EXT_SECRETS.
type Config struct {
	Environment  string
	BaseURL      string
	LocationIDs  []string
	PollInterval time.Duration
	// MinAmount is in the currency's main unit (1000 = ¥1,000 or $1,000).
	MinAmount float64
	Notify    NotifyFilter
	// Problems are settings that could not be used as written. They are
	// logged and reported to the agent once at startup.
	Problems []string
}

// NotifyFilter says which event kinds reach the agent.
type NotifyFilter struct {
	OrderCreated     bool
	OrderCompleted   bool
	OrderCanceled    bool
	PaymentCompleted bool
	PaymentFailed    bool
	Refunds          bool
}

func (n NotifyFilter) anyOrder() bool {
	return n.OrderCreated || n.OrderCompleted || n.OrderCanceled
}

func (n NotifyFilter) anyPayment() bool {
	// Payments also feed the refund fallback (refunded_money going up).
	return n.PaymentCompleted || n.PaymentFailed || n.Refunds
}

// locationIDRe is a loose shape check for a Square location ID. It
// keeps a typo such as a pasted URL out of the query string.
var locationIDRe = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

func parseConfig(raw string) Config {
	m := map[string]any{}
	cfg := Config{}
	if strings.TrimSpace(raw) != "" {
		if err := json.Unmarshal([]byte(raw), &m); err != nil {
			cfg.Problems = append(cfg.Problems, "the settings could not be read; using the defaults")
			m = map[string]any{}
		}
	}
	boolOr := func(key string, def bool) bool {
		if v, ok := m[key].(bool); ok {
			return v
		}
		return def
	}

	cfg.Environment = "production"
	cfg.BaseURL = productionBaseURL
	if v, ok := m["environment"].(string); ok {
		switch strings.ToLower(strings.TrimSpace(v)) {
		case "", "production":
		case "sandbox":
			cfg.Environment, cfg.BaseURL = "sandbox", sandboxBaseURL
		default:
			cfg.Problems = append(cfg.Problems, "unknown environment "+quote(v)+"; using production")
		}
	}

	if v, ok := m["locationIds"].(string); ok {
		seen := map[string]bool{}
		for _, part := range strings.FieldsFunc(v, func(r rune) bool {
			return r == ',' || r == ' ' || r == '\n' || r == '\t' || r == ';'
		}) {
			if !locationIDRe.MatchString(part) {
				cfg.Problems = append(cfg.Problems, "ignored location ID "+quote(part)+" (not an ID)")
				continue
			}
			if !seen[part] {
				seen[part] = true
				cfg.LocationIDs = append(cfg.LocationIDs, part)
			}
		}
		if len(cfg.LocationIDs) == 0 && strings.TrimSpace(v) != "" {
			// Falling back to "every location" would widen what the
			// operator asked for, so nothing is watched instead.
			cfg.Problems = append(cfg.Problems, "the location IDs setting holds no usable ID; no orders or payments are watched until it is fixed")
			cfg.LocationIDs = []string{}
		}
	}

	cfg.PollInterval = defaultPollInterval
	if v, ok := m["pollIntervalSeconds"].(float64); ok {
		cfg.PollInterval = time.Duration(v * float64(time.Second))
	}
	if cfg.PollInterval < minPollInterval {
		cfg.PollInterval = minPollInterval
	}
	if cfg.PollInterval > maxPollInterval {
		cfg.PollInterval = maxPollInterval
	}

	if v, ok := m["minAmount"].(float64); ok && v > 0 {
		cfg.MinAmount = v
	}

	cfg.Notify = NotifyFilter{
		OrderCreated:     boolOr("notifyOrderCreated", true),
		OrderCompleted:   boolOr("notifyOrderCompleted", true),
		OrderCanceled:    boolOr("notifyOrderCanceled", true),
		PaymentCompleted: boolOr("notifyPaymentCompleted", true),
		PaymentFailed:    boolOr("notifyPaymentFailed", true),
		Refunds:          boolOr("notifyRefunds", true),
	}
	return cfg
}

// locationsConfigured reports whether the operator named locations
// (possibly none usable) rather than leaving the field empty.
func (c Config) locationsConfigured() bool { return c.LocationIDs != nil }

func quote(s string) string {
	if len(s) > 40 {
		s = s[:40] + "…"
	}
	return `"` + s + `"`
}

// Env is the KOJO_EXT_* contract, read once at startup.
type Env struct {
	APIBase string
	Token   string
	AgentID string
	DataDir string
	Config  Config
	Secrets map[string]string
}

func loadEnv() Env {
	e := Env{
		APIBase: strings.TrimRight(os.Getenv("KOJO_API_BASE"), "/"),
		Token:   os.Getenv("KOJO_EXT_TOKEN"),
		AgentID: os.Getenv("KOJO_EXT_AGENT_ID"),
		DataDir: os.Getenv("KOJO_EXT_DATA_DIR"),
		Config:  parseConfig(os.Getenv("KOJO_EXT_CONFIG")),
		Secrets: map[string]string{},
	}
	if raw := os.Getenv("KOJO_EXT_SECRETS"); raw != "" {
		_ = json.Unmarshal([]byte(raw), &e.Secrets)
	}
	if e.Token == "" {
		if f := os.Getenv("KOJO_EXT_TOKEN_FILE"); f != "" {
			if data, err := os.ReadFile(f); err == nil {
				e.Token = strings.TrimSpace(string(data))
			}
		}
	}
	return e
}

const secretAccessToken = "accessToken"
