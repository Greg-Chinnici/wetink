package proxy_test

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"wetink/internal/envelope"
	"wetink/internal/mockgw"
	"wetink/internal/oai"
	"wetink/internal/proxy"
	"wetink/internal/tap"
)

type fakeJS struct {
	mu   sync.Mutex
	envs []envelope.Envelope
}

func (f *fakeJS) Publish(_ context.Context, _ string, data []byte, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	env, err := envelope.Unmarshal(data)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.envs = append(f.envs, env)
	return &jetstream.PubAck{Sequence: uint64(len(f.envs))}, nil
}

func (f *fakeJS) published() []envelope.Envelope {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]envelope.Envelope(nil), f.envs...)
}

// start serves a proxy in front of upstream and returns its base URL for session/master.
func start(t *testing.T, upstream http.Handler, opts proxy.Options) (string, *fakeJS, *proxy.Proxy) {
	t.Helper()
	up := httptest.NewServer(upstream)
	t.Cleanup(up.Close)
	js := &fakeJS{}
	opts.Tap.JetStream = func(context.Context) (tap.Publisher, error) { return js, nil }
	p := proxy.New(oai.New(up.URL+"/v1", "provider-key", 10*time.Second), opts)
	srv := httptest.NewServer(p.Handler())
	t.Cleanup(srv.Close)
	return srv.URL + "/s/s-proxy/master-a/v1", js, p
}

func post(t *testing.T, url string, body map[string]any) (*http.Response, string) {
	t.Helper()
	data, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, url, strings.NewReader(string(data)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer agent-has-no-key")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	return resp, string(out)
}

var lookup = map[string]any{"type": "function", "name": "lookup",
	"parameters": map[string]any{"type": "object", "properties": map[string]any{"query": map[string]any{"type": "string"}}}}

func TestStreamedCallsAreTappedAndLogged(t *testing.T) {
	logDir := t.TempDir()
	base, js, p := start(t, mockgw.Handler(), proxy.Options{LogDir: logDir, ToolOutputs: true})

	user := map[string]any{"role": "user", "content": "Find the default port.\n\nThen report it."}
	resp, body := post(t, base+"/responses", map[string]any{"model": "mock", "stream": true,
		"input": []any{user}, "tools": []any{lookup}})
	if resp.StatusCode != 200 || !strings.Contains(resp.Header.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("status %d, content type %q", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	// The agent gets the provider's stream as is.
	var call map[string]any
	sc := bufio.NewScanner(strings.NewReader(body))
	for sc.Scan() {
		var ev oai.Event
		if json.Unmarshal([]byte(strings.TrimPrefix(sc.Text(), "data: ")), &ev) == nil &&
			ev.Type() == "response.output_item.done" {
			call = ev.Item()
		}
	}
	if call["type"] != "function_call" {
		t.Fatalf("expected a function call, got %v", call)
	}

	// The next call carries the tool output; the reply streams in paragraphs.
	output := map[string]any{"type": "function_call_output", "call_id": call["call_id"], "output": "4222"}
	post(t, base+"/responses", map[string]any{"model": "mock", "stream": true,
		"input": []any{user, call, output}, "tools": []any{lookup}})
	p.Close(2 * time.Second)

	var types []string
	snapshots := 0
	for _, e := range js.published() {
		if e.Session != "s-proxy" || e.Master != "master-a" {
			t.Fatalf("envelope for %s/%s", e.Session, e.Master)
		}
		if e.Rev > 0 {
			snapshots++
			continue
		}
		types = append(types, e.ItemType())
	}
	if got := strings.Join(types, ","); got != "function_call,function_call_output,message" {
		t.Fatalf("published %s", got)
	}
	if snapshots == 0 {
		t.Fatal("no snapshots of the message while it was written")
	}

	data, err := os.ReadFile(filepath.Join(logDir, "s-proxy", "master-a.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 2 {
		t.Fatalf("%d log lines", len(lines))
	}
	var second struct {
		Status   int              `json:"status"`
		NewInput []map[string]any `json:"new_input"`
		Output   []map[string]any `json:"output"`
	}
	if err := json.Unmarshal([]byte(lines[1]), &second); err != nil {
		t.Fatal(err)
	}
	if second.Status != 200 || len(second.NewInput) != 2 || second.NewInput[1]["type"] != "function_call_output" ||
		len(second.Output) != 1 || second.Output[0]["type"] != "message" {
		t.Fatalf("second exchange logged as %s", lines[1])
	}
}

func TestPlainCallsAreTappedAndToolOutputsOffByDefault(t *testing.T) {
	base, js, p := start(t, mockgw.Handler(), proxy.Options{})
	user := map[string]any{"role": "user", "content": "Hello there."}
	resp, body := post(t, base+"/responses", map[string]any{"model": "mock", "input": []any{user}, "tools": []any{lookup}})
	var out map[string]any
	if resp.StatusCode != 200 || json.Unmarshal([]byte(body), &out) != nil {
		t.Fatalf("status %d: %s", resp.StatusCode, body)
	}
	call := out["output"].([]any)[0]
	post(t, base+"/responses", map[string]any{"model": "mock", "input": []any{user, call,
		map[string]any{"type": "function_call_output", "call_id": call.(map[string]any)["call_id"], "output": "x"}}})
	p.Close(2 * time.Second)
	var types []string
	for _, e := range js.published() {
		types = append(types, e.ItemType())
	}
	if got := strings.Join(types, ","); got != "function_call,message" {
		t.Fatalf("published %s", got)
	}
}

func TestProviderErrorsAndOtherPathsPassThrough(t *testing.T) {
	var gotAuth, gotPath string
	upstream := http.NewServeMux()
	upstream.HandleFunc("POST /v1/responses", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, `{"error":{"message":"unknown model"}}`)
	})
	upstream.HandleFunc("GET /v1/models", func(w http.ResponseWriter, r *http.Request) {
		gotAuth, gotPath = r.Header.Get("Authorization"), r.URL.String()
		io.WriteString(w, `{"data":[]}`)
	})
	base, js, p := start(t, upstream, proxy.Options{})
	resp, body := post(t, base+"/responses", map[string]any{"model": "nope", "stream": true, "input": "hi"})
	if resp.StatusCode != http.StatusBadRequest || !strings.Contains(body, "unknown model") {
		t.Fatalf("status %d: %s", resp.StatusCode, body)
	}
	r, err := http.Get(base + "/models?limit=1")
	if err != nil {
		t.Fatal(err)
	}
	r.Body.Close()
	if r.StatusCode != 200 || gotAuth != "Bearer provider-key" || gotPath != "/v1/models?limit=1" {
		t.Fatalf("passed on as %d %q %q", r.StatusCode, gotAuth, gotPath)
	}
	p.Close(time.Second)
	if n := len(js.published()); n != 0 {
		t.Fatalf("published %d items", n)
	}
}

// anthropicSSE is a streamed Messages reply: a text block in two paragraphs, then a tool call.
const anthropicSSE = `event: message_start
data: {"type":"message_start","message":{"id":"msg_01abc","usage":{"input_tokens":120,"cache_read_input_tokens":80,"output_tokens":1}}}

event: content_block_start
data: {"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}

event: content_block_stop
data: {"type":"content_block_stop","index":0}

event: content_block_start
data: {"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}

event: content_block_delta
data: {"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"The parser keeps state in a struct.\n\n"}}

event: content_block_delta
data: {"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"I will add a Reset method."}}

event: content_block_stop
data: {"type":"content_block_stop","index":1}

event: content_block_start
data: {"type":"content_block_start","index":2,"content_block":{"type":"tool_use","id":"toolu_1","name":"mcp__wetink__share_context","input":{}}}

event: content_block_delta
data: {"type":"content_block_delta","index":2,"delta":{"type":"input_json_delta","partial_json":"{\"kind\": \"decision\", "}}

event: content_block_delta
data: {"type":"content_block_delta","index":2,"delta":{"type":"input_json_delta","partial_json":"\"text\": \"Add Reset\"}"}}

event: content_block_stop
data: {"type":"content_block_stop","index":2}

event: message_delta
data: {"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":42}}

event: message_stop
data: {"type":"message_stop"}

`

func TestMessagesAreTappedWithTheAgentsCredentials(t *testing.T) {
	var gotKey, gotPath string
	upstream := http.NewServeMux()
	upstream.HandleFunc("POST /v1/messages", func(w http.ResponseWriter, r *http.Request) {
		gotKey, gotPath = r.Header.Get("X-Api-Key"), r.URL.String()
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
		}
		if req["tools"] == nil { // a side call
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, `{"id":"msg_side","content":[{"type":"text","text":"Fix parser"}],"usage":{"input_tokens":5,"output_tokens":2}}`)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, anthropicSSE)
	})
	up := httptest.NewServer(upstream)
	defer up.Close()
	logDir := t.TempDir()
	js := &fakeJS{}
	p := proxy.New(oai.New("http://unused/v1", "", time.Second), proxy.Options{AnthropicURL: up.URL, LogDir: logDir, ToolOutputs: true,
		Tap: tap.Options{JetStream: func(context.Context) (tap.Publisher, error) { return js, nil }}})
	srv := httptest.NewServer(p.Handler())
	defer srv.Close()
	base := srv.URL + "/s/s-claude/master-a"

	call := func(body map[string]any) string {
		data, _ := json.Marshal(body)
		req, _ := http.NewRequest(http.MethodPost, base+"/v1/messages?beta=true", strings.NewReader(string(data)))
		req.Header.Set("X-Api-Key", "agent-key")
		req.Header.Set("Anthropic-Version", "2023-06-01")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		out, _ := io.ReadAll(resp.Body)
		return string(out)
	}
	tools := []any{map[string]any{"name": "Bash"}, map[string]any{"name": "mcp__wetink__share_context"}}
	user := map[string]any{"role": "user", "content": "Fix the parser."}
	if got := call(map[string]any{"model": "claude-haiku-4-5", "stream": true, "messages": []any{user}, "tools": tools}); got != anthropicSSE {
		t.Fatalf("stream changed on the way:\n%s", got)
	}
	if gotKey != "agent-key" || gotPath != "/v1/messages?beta=true" {
		t.Fatalf("passed on with key %q to %q", gotKey, gotPath)
	}
	call(map[string]any{"model": "claude-haiku-4-5", "messages": []any{map[string]any{"role": "user", "content": "Title?"}}})
	result := map[string]any{"role": "user", "content": []any{map[string]any{"type": "tool_result", "tool_use_id": "toolu_1", "content": "Shared as a decision: entry #7."}}}
	call(map[string]any{"model": "claude-haiku-4-5", "stream": true, "tools": tools,
		"messages": []any{user, map[string]any{"role": "assistant", "content": []any{}}, result}})
	p.Close(2 * time.Second)

	var final []string
	snapshots := 0
	for _, e := range js.published() {
		if e.Rev > 0 {
			snapshots++
			continue
		}
		final = append(final, e.ItemType()+":"+e.ItemID())
	}
	// The fake replies the same both times; the side call in between is not tapped.
	want := "message:msg_01abc_1,function_call:fc_toolu_1,function_call_output:fco_toolu_1,message:msg_01abc_1,function_call:fc_toolu_1"
	if got := strings.Join(final, ","); got != want {
		t.Fatalf("published %s\nwant      %s", got, want)
	}
	if snapshots == 0 {
		t.Fatal("no snapshots while the text was written")
	}
	for _, e := range js.published() {
		if e.ItemType() == "function_call" && e.Item["arguments"] != `{"kind": "decision", "text": "Add Reset"}` {
			t.Fatalf("arguments %v", e.Item["arguments"])
		}
	}

	data, _ := os.ReadFile(filepath.Join(logDir, "s-claude", "master-a.jsonl"))
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	var first, side struct {
		Side  bool           `json:"side"`
		Usage map[string]any `json:"usage"`
		Tools []string       `json:"tools"`
	}
	if len(lines) != 3 {
		t.Fatalf("log:\n%s", data)
	}
	if json.Unmarshal([]byte(lines[0]), &first) != nil || json.Unmarshal([]byte(lines[1]), &side) != nil || first.Usage["input_tokens"] != 120.0 || first.Usage["output_tokens"] != 42.0 ||
		len(first.Tools) != 2 || !side.Side {
		t.Fatalf("log:\n%s", data)
	}
}
