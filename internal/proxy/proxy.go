// Package proxy is a model endpoint that taps whatever agent calls it: it passes the agent's
// model calls on to the provider and publishes what comes back, as the master's tap does, so
// any agent (Claude Code, Codex, ...) writes to shared context while its model runs.
//
// An agent's base URL names its session and master: http://host:port/s/<session>/<master>.
// Two APIs are tapped, streamed or not:
//
//   - Anthropic Messages, POST .../v1/messages (Claude Code: ANTHROPIC_BASE_URL=<that URL>).
//     The agent's own headers and credentials go on to the Anthropic upstream. Calls that
//     offer no tools are the agent's side calls (titles, summaries) and are not tapped.
//   - Open Responses, POST .../v1/responses (Codex and the like: base URL <that URL>/v1). The
//     proxy's provider key is used, so agents need none.
//
// Anything else is passed on untouched: to the Anthropic upstream if it carries an
// anthropic-version header, else to the Responses provider.
//
// Every exchange is also logged, one JSON line per model call, under LogDir/<session>/<master>.jsonl:
// the output items, the input new since the last call (tool results), and usage.
package proxy

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"wetink/internal/envelope"
	"wetink/internal/item"
	"wetink/internal/oai"
	"wetink/internal/tap"
)

// DefaultAnthropicURL is the Anthropic upstream unless Options.AnthropicURL says otherwise.
const DefaultAnthropicURL = "https://api.anthropic.com"

type Options struct {
	NATSURL      string
	AnthropicURL string // default DefaultAnthropicURL
	LogDir       string // "" for no log
	// ToolOutputs also publishes the results of the agent's own tools (file reads, test runs),
	// found in the input of its next call. Off by default: they are long and mostly noise.
	ToolOutputs bool
	// Tapped says which sessions are published; nil publishes all. The others are only logged
	// (agents without a board).
	Tapped func(session string) bool
	Tap    tap.Options
}

type Proxy struct {
	client *oai.Client
	opts   Options

	mu     sync.Mutex
	agents map[[2]string]*agent
}

// agent is one session/master's tap and what the proxy has seen of it.
type agent struct {
	tap *tap.Tap

	mu       sync.Mutex
	inputLen int             // input items (or messages) in its last tapped call
	tools    string          // tool names offered in its last call
	calls    map[string]bool // call ids of its published function calls
	log      *os.File
}

// New returns a proxy whose Responses calls go to the provider client points at, with its key.
func New(client *oai.Client, opts Options) *Proxy {
	if opts.AnthropicURL == "" {
		opts.AnthropicURL = DefaultAnthropicURL
	}
	return &Proxy{client: client, opts: opts, agents: map[[2]string]*agent{}}
}

func (p *Proxy) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /s/{session}/{master}/v1/responses", p.responses)
	mux.HandleFunc("POST /s/{session}/{master}/v1/messages", p.messages)
	mux.HandleFunc("/s/{session}/{master}/{rest...}", p.passOn)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, "ok") })
	return mux
}

// Close flushes every tap (up to timeout each) and closes the logs.
func (p *Proxy) Close(timeout time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, a := range p.agents {
		a.tap.Close(timeout)
		if a.log != nil {
			a.log.Close()
		}
	}
	p.agents = map[[2]string]*agent{}
}

func (p *Proxy) agent(session, master string) (*agent, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	key := [2]string{session, master}
	if a := p.agents[key]; a != nil {
		return a, nil
	}
	topts := p.opts.Tap
	if p.opts.Tapped != nil && !p.opts.Tapped(session) {
		topts.NoStreamMessages = true
		topts.JetStream = func(context.Context) (tap.Publisher, error) { return discard{}, nil }
	}
	t, err := tap.New(session, master, p.opts.NATSURL, topts)
	if err != nil {
		return nil, err
	}
	a := &agent{tap: t, calls: map[string]bool{}}
	if p.opts.LogDir != "" {
		dir := filepath.Join(p.opts.LogDir, session)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
		if a.log, err = os.OpenFile(filepath.Join(dir, master+".jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644); err != nil {
			return nil, err
		}
	}
	t.Start()
	p.agents[key] = a
	slog.Info("agent connected", "session", session, "master", master)
	return a, nil
}

type discard struct{}

func (discard) Publish(context.Context, string, []byte, ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	return &jetstream.PubAck{}, nil
}

// exchange is one model call, as logged.
type exchange struct {
	Time     time.Time        `json:"time"`
	Millis   int64            `json:"ms"`
	API      string           `json:"api"` // "messages" or "responses"
	Model    string           `json:"model,omitempty"`
	Status   int              `json:"status"`
	Side     bool             `json:"side,omitempty"`  // a side call, not tapped
	Tools    []string         `json:"tools,omitempty"` // when they change
	Error    string           `json:"error,omitempty"`
	NewInput []any            `json:"new_input,omitempty"`
	Output   []map[string]any `json:"output,omitempty"`
	Usage    map[string]any   `json:"usage,omitempty"`
}

// added returns the input added since the agent's last call: everything if the input shrank,
// as after a compaction.
func (a *agent) added(input []any) []any {
	a.mu.Lock()
	defer a.mu.Unlock()
	start := a.inputLen
	if start > len(input) {
		start = 0
	}
	a.inputLen = len(input)
	return input[start:]
}

func (a *agent) toolsChanged(tools []string) bool {
	joined := strings.Join(tools, ",")
	a.mu.Lock()
	defer a.mu.Unlock()
	changed := joined != a.tools
	a.tools = joined
	return changed
}

// publish publishes an output item and remembers its call id.
func (a *agent) publish(ex *exchange, it map[string]any) {
	if item.String(it, "type") == "function_call" {
		a.mu.Lock()
		a.calls[item.String(it, "call_id")] = true
		a.mu.Unlock()
	}
	ex.Output = append(ex.Output, it)
	a.tap.Publish(it, 0)
}

// publishToolOutput publishes a function_call_output item if Options.ToolOutputs is set and
// it answers one of the agent's published calls.
func (p *Proxy) publishToolOutput(a *agent, it map[string]any) {
	if !p.opts.ToolOutputs {
		return
	}
	a.mu.Lock()
	known := a.calls[item.String(it, "call_id")]
	a.mu.Unlock()
	if known {
		a.tap.Publish(it, 0)
	}
}

func (a *agent) write(ex *exchange) {
	ex.Millis = time.Since(ex.Time).Milliseconds()
	if a.log == nil {
		return
	}
	data, err := json.Marshal(ex)
	if err != nil {
		slog.Warn("proxy log", "err", err)
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, err := a.log.Write(append(data, '\n')); err != nil {
		slog.Warn("proxy log", "err", err)
	}
}

func mergeUsage(ex *exchange, usage any) {
	m, _ := usage.(map[string]any)
	if len(m) == 0 {
		return
	}
	if ex.Usage == nil {
		ex.Usage = map[string]any{}
	}
	for k, v := range m {
		if v != nil {
			ex.Usage[k] = v
		}
	}
}

type toolDecl struct {
	Type string `json:"type"`
	Name string `json:"name"`
}

func toolNames(tools []toolDecl) []string {
	var names []string
	for _, t := range tools {
		names = append(names, cmp.Or(t.Name, t.Type))
	}
	return names
}

func (p *Proxy) responses(w http.ResponseWriter, r *http.Request) {
	session, master := r.PathValue("session"), r.PathValue("master")
	a, err := p.agent(session, master)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var req struct {
		Model  string     `json:"model"`
		Stream bool       `json:"stream"`
		Input  any        `json:"input"`
		Tools  []toolDecl `json:"tools"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "request is not JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	ex := &exchange{Time: time.Now(), API: "responses", Model: req.Model}
	defer a.write(ex)
	if tools := toolNames(req.Tools); a.toolsChanged(tools) {
		ex.Tools = tools
	}
	input, _ := req.Input.([]any)
	ex.NewInput = a.added(input)
	for _, v := range ex.NewInput {
		if it, _ := v.(map[string]any); item.String(it, "type") == "function_call_output" {
			p.publishToolOutput(a, it)
		}
	}

	resp, err := p.client.PostRaw(r.Context(), "/responses", body)
	if err != nil {
		ex.Status, ex.Error = writeError(w, err)
		slog.Warn("model call failed", "session", session, "master", master, "err", err)
		return
	}
	defer resp.Body.Close()
	ex.Status = resp.StatusCode
	for _, h := range []string{"Content-Type", "Cache-Control"} {
		if v := resp.Header.Get(h); v != "" {
			w.Header().Set(h, v)
		}
	}
	w.WriteHeader(resp.StatusCode)

	if !req.Stream {
		data, err := io.ReadAll(resp.Body)
		w.Write(data)
		if err != nil {
			ex.Error = err.Error()
			return
		}
		var out map[string]any
		if json.Unmarshal(data, &out) == nil {
			mergeUsage(ex, out["usage"])
			output, _ := out["output"].([]any)
			for _, v := range output {
				if it, ok := v.(map[string]any); ok {
					a.publish(ex, it)
				}
			}
		}
		return
	}

	// Stream: pass every chunk on as it arrives, and watch the events go by.
	observer := a.tap.NewObserver()
	events := &sseParser{on: func(payload string) {
		var ev oai.Event
		if payload == "[DONE]" || json.Unmarshal([]byte(payload), &ev) != nil {
			return
		}
		switch ev.Type() {
		case "response.output_item.done":
			if it := ev.Item(); it != nil {
				a.mu.Lock()
				if item.String(it, "type") == "function_call" {
					a.calls[item.String(it, "call_id")] = true
				}
				a.mu.Unlock()
				ex.Output = append(ex.Output, it)
			}
		case "response.completed", "response.incomplete", "response.failed":
			if resp, ok := ev["response"].(map[string]any); ok {
				mergeUsage(ex, resp["usage"])
				if e, ok := resp["error"].(map[string]any); ok {
					ex.Error, _ = e["message"].(string)
				}
			}
		case "error":
			ex.Error, _ = ev["message"].(string)
		}
		observer.Observe(ev) // publishes finished items, and messages while written
	}}
	if err := copyFlushing(w, resp.Body, events); err != nil {
		ex.Error = err.Error()
	}
	events.end()
}

// copyFlushing copies a stream to the agent chunk by chunk, flushing each, and feeds it to sse.
func copyFlushing(w http.ResponseWriter, body io.Reader, sse *sseParser) error {
	flusher, _ := w.(http.Flusher)
	buf := make([]byte, 32*1024)
	for {
		n, err := body.Read(buf)
		if n > 0 {
			w.Write(buf[:n])
			if flusher != nil {
				flusher.Flush()
			}
			sse.write(buf[:n])
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// writeError answers with the provider's error as is, or 502 if it gave none.
func writeError(w http.ResponseWriter, err error) (int, string) {
	var he *oai.HTTPError
	if errors.As(err, &he) {
		if ct := he.Header.Get("Content-Type"); ct != "" {
			w.Header().Set("Content-Type", ct)
		}
		w.WriteHeader(he.Status)
		w.Write(he.Body)
		return he.Status, string(he.Body)
	}
	if errors.Is(err, context.Canceled) {
		return 499, err.Error()
	}
	http.Error(w, err.Error(), http.StatusBadGateway)
	return http.StatusBadGateway, err.Error()
}

// reverseProxy sends a request to base + "/" + rest; key, if set, replaces the agent's.
func reverseProxy(base, rest, key string, modify func(*http.Response) error) (*httputil.ReverseProxy, error) {
	target, err := url.Parse(base)
	if err != nil {
		return nil, err
	}
	return &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(target)
			pr.Out.URL.Path = strings.TrimRight(target.Path, "/") + "/" + rest
			pr.Out.URL.RawPath = ""
			pr.Out.Host = target.Host
			pr.Out.Header.Del("Accept-Encoding") // so the reply can be read (Go decompresses its own)
			if key != "" {
				pr.Out.Header.Set("Authorization", "Bearer "+key)
			}
		},
		ModifyResponse: modify,
		FlushInterval:  -1,
	}, nil
}

// passOn sends any other request on untouched.
func (p *Proxy) passOn(w http.ResponseWriter, r *http.Request) {
	if err := envelope.CheckName(r.PathValue("session"), "session"); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	rest := r.PathValue("rest")
	base, key := p.opts.AnthropicURL, ""
	if r.Header.Get("Anthropic-Version") == "" {
		if strings.HasSuffix(rest, "/chat/completions") {
			slog.Warn("chat completions are passed on but not tapped; use the Responses API",
				"session", r.PathValue("session"), "master", r.PathValue("master"))
		}
		// The provider's base URL ends in its /v1 (or /api/v1); the agent's path repeats it.
		base, key = p.client.BaseURL, p.client.APIKey
		rest = strings.TrimPrefix(rest, "v1/")
	}
	rp, err := reverseProxy(base, rest, key, nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	rp.ServeHTTP(w, r)
}
