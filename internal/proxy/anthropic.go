package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"wetink/internal/item"
	"wetink/internal/tap"
)

// messages taps POST .../v1/messages, Anthropic's API as Claude Code calls it. The request goes
// on with the agent's own headers and credentials; the reply is watched as it streams back,
// and its text and tool_use blocks are published as message and function_call items.
func (p *Proxy) messages(w http.ResponseWriter, r *http.Request) {
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
	r.Body = io.NopCloser(bytes.NewReader(body))
	var req struct {
		Model    string     `json:"model"`
		Stream   bool       `json:"stream"`
		Messages []any      `json:"messages"`
		Tools    []toolDecl `json:"tools"`
	}
	_ = json.Unmarshal(body, &req) // a body it cannot read still goes on, untapped

	ex := &exchange{Time: time.Now(), API: "messages", Model: req.Model, Side: len(req.Tools) == 0}
	c := &messageConv{p: p, a: a, ex: ex, tapped: !ex.Side, blocks: map[int]*block{}}
	if c.tapped {
		if tools := toolNames(req.Tools); a.toolsChanged(tools) {
			ex.Tools = tools
		}
		ex.NewInput = a.added(req.Messages)
		for _, m := range ex.NewInput {
			for _, out := range toolResults(m) {
				p.publishToolOutput(a, out)
			}
		}
	}

	rp, err := reverseProxy(p.opts.AnthropicURL, "v1/messages", "", func(resp *http.Response) error {
		ex.Status = resp.StatusCode
		var watch func([]byte)
		var end func()
		if resp.StatusCode != http.StatusOK {
			var errBody bytes.Buffer
			watch = func(b []byte) { errBody.Write(b) }
			end = func() { ex.Error = errBody.String()[:min(errBody.Len(), 2000)] }
		} else if strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
			sse := &sseParser{on: c.event}
			watch, end = sse.write, sse.end
		} else {
			var whole bytes.Buffer
			watch = func(b []byte) { whole.Write(b) }
			end = func() { c.whole(whole.Bytes()) }
		}
		resp.Body = &tee{rc: resp.Body, watch: watch, end: func() { end(); a.write(ex) }}
		return nil
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	rp.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, err error) {
		ex.Status, ex.Error = http.StatusBadGateway, err.Error()
		a.write(ex)
		slog.Warn("model call failed", "session", session, "master", master, "err", err)
		http.Error(w, err.Error(), http.StatusBadGateway)
	}
	rp.ServeHTTP(w, r)
}

// tee passes a body through while watching it; end runs once, at EOF or close.
type tee struct {
	rc    io.ReadCloser
	watch func([]byte)
	end   func()
	once  sync.Once
}

func (t *tee) Read(b []byte) (int, error) {
	n, err := t.rc.Read(b)
	if n > 0 {
		t.watch(b[:n])
	}
	if err != nil {
		t.once.Do(t.end)
	}
	return n, err
}

func (t *tee) Close() error {
	t.once.Do(t.end)
	return t.rc.Close()
}

// toolResults are a message's tool_result blocks, as function_call_output items.
func toolResults(m any) []map[string]any {
	msg, _ := m.(map[string]any)
	content, _ := msg["content"].([]any)
	var out []map[string]any
	for _, b := range content {
		blk, _ := b.(map[string]any)
		if item.String(blk, "type") != "tool_result" {
			continue
		}
		out = append(out, map[string]any{
			"type": "function_call_output", "id": "fco_" + item.String(blk, "tool_use_id"),
			"call_id": item.String(blk, "tool_use_id"), "output": item.TextOf(blk["content"]),
		})
	}
	return out
}

// messageConv turns one Messages reply into items as it streams.
type messageConv struct {
	p      *Proxy
	a      *agent
	ex     *exchange
	tapped bool
	msgID  string
	blocks map[int]*block
}

type block struct {
	typ, id, name string
	text, input   strings.Builder
	stream        *tap.MessageStream
}

func (c *messageConv) itemID(index int) string { return fmt.Sprintf("%s_%d", c.msgID, index) }

func (c *messageConv) event(payload string) {
	var ev map[string]any
	if json.Unmarshal([]byte(payload), &ev) != nil {
		return
	}
	index := -1
	if f, ok := ev["index"].(float64); ok {
		index = int(f)
	}
	switch item.String(ev, "type") {
	case "message_start":
		msg, _ := ev["message"].(map[string]any)
		c.msgID = item.String(msg, "id")
		mergeUsage(c.ex, msg["usage"])
	case "content_block_start":
		cb, _ := ev["content_block"].(map[string]any)
		b := &block{typ: item.String(cb, "type"), id: item.String(cb, "id"), name: item.String(cb, "name")}
		b.text.WriteString(item.String(cb, "text"))
		if b.typ == "text" && c.tapped && !c.p.opts.Tap.NoStreamMessages {
			b.stream = c.a.tap.NewMessageStream(c.itemID(index))
		}
		c.blocks[index] = b
	case "content_block_delta":
		b := c.blocks[index]
		if b == nil {
			return
		}
		d, _ := ev["delta"].(map[string]any)
		switch item.String(d, "type") {
		case "text_delta":
			s := item.String(d, "text")
			b.text.WriteString(s)
			if b.stream != nil {
				b.stream.Feed(s)
			}
		case "input_json_delta":
			b.input.WriteString(item.String(d, "partial_json"))
		}
	case "content_block_stop":
		if b := c.blocks[index]; b != nil {
			delete(c.blocks, index)
			c.finish(index, b)
		}
	case "message_delta":
		mergeUsage(c.ex, ev["usage"])
	case "error":
		e, _ := ev["error"].(map[string]any)
		c.ex.Error = item.String(e, "message")
	}
}

// whole handles a reply that was not streamed.
func (c *messageConv) whole(data []byte) {
	var msg map[string]any
	if json.Unmarshal(data, &msg) != nil {
		return
	}
	c.msgID = item.String(msg, "id")
	mergeUsage(c.ex, msg["usage"])
	content, _ := msg["content"].([]any)
	for i, v := range content {
		cb, _ := v.(map[string]any)
		b := &block{typ: item.String(cb, "type"), id: item.String(cb, "id"), name: item.String(cb, "name")}
		b.text.WriteString(item.String(cb, "text"))
		if in, ok := cb["input"]; ok {
			raw, _ := json.Marshal(in)
			b.input.Write(raw)
		}
		c.finish(i, b)
	}
}

func (c *messageConv) finish(index int, b *block) {
	var it map[string]any
	switch b.typ {
	case "text":
		if strings.TrimSpace(b.text.String()) == "" {
			return
		}
		it = map[string]any{"type": "message", "id": c.itemID(index), "role": "assistant", "status": "completed",
			"content": []any{map[string]any{"type": "output_text", "text": b.text.String(), "annotations": []any{}}}}
	case "tool_use":
		args := b.input.String()
		if strings.TrimSpace(args) == "" {
			args = "{}"
		}
		it = map[string]any{"type": "function_call", "id": "fc_" + b.id, "call_id": b.id, "name": b.name,
			"arguments": args, "status": "completed"}
	default: // thinking and the like stay private
		return
	}
	if !c.tapped {
		c.ex.Output = append(c.ex.Output, it)
		return
	}
	c.a.publish(c.ex, it)
}
