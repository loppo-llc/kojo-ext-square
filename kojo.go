package main

import (
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
	short   *http.Client
}

func newKojoClient(base, token, agentID string) *KojoClient {
	return &KojoClient{
		base:    strings.TrimRight(base, "/"),
		token:   token,
		agentID: agentID,
		short:   &http.Client{Timeout: 30 * time.Second},
	}
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

// PostMessage puts content into the agent's main chat. kojo labels it
// as an extension notice and runs it as a turn the user can see. A
// running turn answers 409 busy; the caller retries.
func (c *KojoClient) PostMessage(ctx context.Context, content string) error {
	resp, err := c.post(ctx, c.short, c.agentPath("/messages"), map[string]string{"content": content})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return decodeAPIError(resp)
	}
	return nil
}

// Secrets fetches the extension's secret settings for this agent. The
// MCP server uses it: kojo keeps secrets out of an MCP server's
// environment, which the backend CLI's command line would expose.
func (c *KojoClient) Secrets(ctx context.Context) (map[string]string, error) {
	u := c.base + "/api/v1/extension/secrets"
	if c.agentID != "" {
		u += "?agentId=" + url.QueryEscape(c.agentID)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	resp, err := c.short.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, decodeAPIError(resp)
	}
	var out struct {
		Secrets map[string]string `json:"secrets"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&out); err != nil {
		return nil, err
	}
	return out.Secrets, nil
}
