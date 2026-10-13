// Package oai is a small client for the OpenAI-compatible endpoints this project uses: Chat
// Completions, and Open Responses (plain and streamed). Requests and responses stay raw JSON,
// so items are passed on exactly as the provider produced them.
package oai

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"wetink/internal/ojson"
)

type Client struct {
	BaseURL string // e.g. http://localhost:8080/v1
	APIKey  string
	HTTP    *http.Client
}

// New returns a client; empty baseURL and apiKey fall back to OPENAI_BASE_URL and
// OPENAI_API_KEY.
func New(baseURL, apiKey string, timeout time.Duration) *Client {
	if baseURL == "" {
		baseURL = os.Getenv("OPENAI_BASE_URL")
	}
	if baseURL == "" {
		baseURL = "https://api.openai.com/v1"
	}
	if apiKey == "" {
		apiKey = os.Getenv("OPENAI_API_KEY")
	}
	return &Client{BaseURL: strings.TrimRight(baseURL, "/"), APIKey: apiKey, HTTP: &http.Client{Timeout: timeout}}
}

// Retries of a request the provider turned away for now (429, 502, 503), with a growing
// delay or the one Retry-After asks for (free tiers rate-limit often).
const (
	retries      = 4
	retryDelay   = 2 * time.Second
	maxRetryWait = 30 * time.Second
)

func (c *Client) post(ctx context.Context, path string, body any) (*http.Response, error) {
	data, err := ojson.Marshal(body)
	if err != nil {
		return nil, err
	}
	return c.PostRaw(ctx, path, data)
}

// PostRaw posts a JSON body as is and returns the 200 response, retrying as post does. Any
// other status is an *HTTPError.
func (c *Client) PostRaw(ctx context.Context, path string, data []byte) (*http.Response, error) {
	for attempt := 0; ; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+path, bytes.NewReader(data))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		if c.APIKey != "" {
			req.Header.Set("Authorization", "Bearer "+c.APIKey)
		}
		resp, err := c.HTTP.Do(req)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode == http.StatusOK {
			return resp, nil
		}
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
		resp.Body.Close()
		err = &HTTPError{Path: path, Status: resp.StatusCode, Header: resp.Header, Body: msg}
		retryable := resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusBadGateway ||
			resp.StatusCode == http.StatusServiceUnavailable
		if !retryable || attempt == retries {
			return nil, err
		}
		wait := retryDelay << attempt
		if s, perr := strconv.Atoi(resp.Header.Get("Retry-After")); perr == nil && s > 0 {
			wait = time.Duration(s) * time.Second
		}
		wait = min(wait, maxRetryWait)
		slog.Warn("model request turned away; retrying", "path", path, "status", resp.StatusCode, "in", wait)
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// HTTPError is a provider's answer other than 200.
type HTTPError struct {
	Path   string
	Status int
	Header http.Header
	Body   []byte
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("%s: HTTP %d: %s", e.Path, e.Status, e.Body[:min(len(e.Body), 500)])
}

func (c *Client) postJSON(ctx context.Context, path string, body any) (map[string]any, error) {
	resp, err := c.post(ctx, path, body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return out, nil
}

// ChatCompletion posts to /chat/completions and returns the first choice's message content.
func (c *Client) ChatCompletion(ctx context.Context, body any) (string, error) {
	out, err := c.postJSON(ctx, "/chat/completions", body)
	if err != nil {
		return "", err
	}
	choices, _ := out["choices"].([]any)
	if len(choices) == 0 {
		return "", fmt.Errorf("chat completion without choices")
	}
	choice, _ := choices[0].(map[string]any)
	msg, _ := choice["message"].(map[string]any)
	content, _ := msg["content"].(string)
	return content, nil
}

// Response posts a non-streaming /responses request and returns the response object.
func (c *Client) Response(ctx context.Context, body any) (map[string]any, error) {
	return c.postJSON(ctx, "/responses", body)
}

// OutputText joins the output_text parts of a response's message items.
func OutputText(response map[string]any) string {
	var b strings.Builder
	output, _ := response["output"].([]any)
	for _, it := range output {
		m, _ := it.(map[string]any)
		if m["type"] != "message" {
			continue
		}
		content, _ := m["content"].([]any)
		for _, p := range content {
			part, _ := p.(map[string]any)
			if part["type"] == "output_text" {
				s, _ := part["text"].(string)
				b.WriteString(s)
			}
		}
	}
	return b.String()
}

// Event is one server-sent event of a streamed response, decoded from its data line.
type Event map[string]any

func (e Event) Type() string { s, _ := e["type"].(string); return s }

// Item returns the "item" object of output_item events.
func (e Event) Item() map[string]any { m, _ := e["item"].(map[string]any); return m }

// StreamResponse posts a /responses request with stream=true and yields its events.
func (c *Client) StreamResponse(ctx context.Context, body *ojson.Object) iter.Seq2[Event, error] {
	return func(yield func(Event, error) bool) {
		body.Set("stream", true)
		resp, err := c.post(ctx, "/responses", body)
		if err != nil {
			yield(nil, err)
			return
		}
		defer resp.Body.Close()
		sc := bufio.NewScanner(resp.Body)
		sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
		var data strings.Builder
		for sc.Scan() {
			line := sc.Text()
			switch {
			case strings.HasPrefix(line, "data:"):
				if data.Len() > 0 {
					data.WriteByte('\n')
				}
				data.WriteString(strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
			case line == "" && data.Len() > 0:
				payload := data.String()
				data.Reset()
				if payload == "[DONE]" {
					return
				}
				var ev Event
				if err := json.Unmarshal([]byte(payload), &ev); err != nil {
					yield(nil, fmt.Errorf("bad event: %w", err))
					return
				}
				if !yield(ev, nil) {
					return
				}
			}
		}
		if err := sc.Err(); err != nil {
			yield(nil, err)
		}
	}
}
