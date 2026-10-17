package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// KojoClient talks to the kojo API with the extension's own token.
type KojoClient struct {
	base    string
	token   string
	agentID string
	// stream has no overall timeout: a turn runs as long as the agent
	// needs. Liveness comes from the server's 15s ping lines instead.
	stream *http.Client
	short  *http.Client
}

func newKojoClient(base, token, agentID string) *KojoClient {
	return &KojoClient{
		base:    strings.TrimRight(base, "/"),
		token:   token,
		agentID: agentID,
		stream:  &http.Client{},
		short:   &http.Client{Timeout: 30 * time.Second},
	}
}

// ChatRequest is the ext-chat body. kojo rejects unknown fields.
type ChatRequest struct {
	SessionKey        string `json:"sessionKey"`
	Message           string `json:"message"`
	SystemPromptExtra string `json:"systemPromptExtra,omitempty"`
}

// ChatEvent is the subset of kojo's agent.ChatEvent read here.
type ChatEvent struct {
	Type         string `json:"type"`
	ErrorMessage string `json:"errorMessage,omitempty"`
	Message      *struct {
		Content string `json:"content"`
	} `json:"message,omitempty"`
}

// APIError is a non-2xx answer from kojo.
type APIError struct {
	Status  int
	Code    string
	Message string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("kojo API %d %s: %s", e.Status, e.Code, e.Message)
}

func apiErrorCode(err error) string {
	var ae *APIError
	if errors.As(err, &ae) {
		return ae.Code
	}
	return ""
}

func decodeAPIError(resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	out := &APIError{Status: resp.StatusCode, Message: strings.TrimSpace(string(body))}
	var env struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &env) == nil && env.Error.Code != "" {
		out.Code, out.Message = env.Error.Code, env.Error.Message
	}
	return out
}

func (c *KojoClient) agentPath(suffix string) string {
	return c.base + "/api/v1/agents/" + url.PathEscape(c.agentID) + suffix
}

func (c *KojoClient) post(ctx context.Context, client *http.Client, u string, body any) (*http.Response, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.token)
	return client.Do(req)
}

// streamIdleTimeout is how long the stream may stay silent. kojo
// writes a ping every 15s.
const streamIdleTimeout = 75 * time.Second

// TurnResult is how a turn's stream ended.
type TurnResult struct {
	// Terminal is set when a done or error event arrived.
	Terminal bool
	Error    string
	Reply    string
	Err      error
}

// StartTurn posts a turn. It returns once kojo accepted it (200): from
// then on the message is the agent's. The turn's stream is read in the
// background until it ends; the result arrives on the returned channel.
// The turn lasts as long as the stream is read — kojo stops a turn whose
// caller goes away — so ctx should live as long as the service.
func (c *KojoClient) StartTurn(ctx context.Context, req ChatRequest) (<-chan TurnResult, error) {
	ctx, cancel := context.WithCancel(ctx)
	resp, err := c.post(ctx, c.stream, c.agentPath("/ext-chat"), req)
	if err != nil {
		cancel()
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		defer cancel()
		defer resp.Body.Close()
		return nil, decodeAPIError(resp)
	}
	done := make(chan TurnResult, 1)
	go func() {
		defer cancel()
		defer resp.Body.Close()
		done <- readTurn(ctx, cancel, resp.Body)
	}()
	return done, nil
}

func readTurn(ctx context.Context, cancel context.CancelFunc, body io.Reader) TurnResult {
	idle := time.AfterFunc(streamIdleTimeout, cancel)
	defer idle.Stop()
	var res TurnResult
	sc := bufio.NewScanner(body)
	sc.Buffer(make([]byte, 0, 64<<10), 16<<20)
	for sc.Scan() {
		idle.Reset(streamIdleTimeout)
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var evt ChatEvent
		if json.Unmarshal(line, &evt) != nil {
			continue
		}
		switch evt.Type {
		case "error":
			res.Terminal = true
			if res.Error == "" {
				res.Error = evt.ErrorMessage
			}
		case "done":
			res.Terminal = true
			if evt.Message != nil {
				res.Reply = evt.Message.Content
			}
			if evt.ErrorMessage != "" && res.Error == "" {
				res.Error = evt.ErrorMessage
			}
		}
	}
	if !res.Terminal {
		res.Err = sc.Err()
		if res.Err == nil {
			res.Err = ctx.Err()
		}
		if res.Err == nil {
			res.Err = errors.New("the stream ended before the turn completed")
		}
	}
	return res
}

// Steer adds content to the turn already running for sessionKey.
func (c *KojoClient) Steer(ctx context.Context, sessionKey, content string) error {
	resp, err := c.post(ctx, c.short, c.agentPath("/ext-chat/steer"),
		map[string]string{"sessionKey": sessionKey, "content": content})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return decodeAPIError(resp)
	}
	return nil
}
