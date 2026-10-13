// Package master is a minimal master: a tool loop against an Open Responses endpoint, with
// every item published through a tap.
//
// The master keeps its own full input array and never uses previous_response_id, so every
// turn is portable across providers. With an MCP URL it also gets the shared-context tools
// (mcpserver): an MCP client lists them, offers them to the model as plain function tools, and
// runs the calls, so this works with any provider behind the gateway.
package master

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"wetink/internal/oai"
	"wetink/internal/ojson"
	"wetink/internal/tap"
)

// LookupTool is a stand-in tool so the loop always has something to call.
var LookupTool = ojson.New().
	Set("type", "function").
	Set("name", "lookup").
	Set("description", "Look up a fact relevant to the task.").
	Set("parameters", ojson.New().
		Set("type", "object").
		Set("properties", ojson.New().Set("query", ojson.New().Set("type", "string"))).
		Set("required", []string{"query"}))

// pyRepr quotes a string the way Python's repr does (for the lookup tool's canned output).
func pyRepr(s string) string {
	if strings.Contains(s, "'") && !strings.Contains(s, `"`) {
		return `"` + s + `"`
	}
	return "'" + strings.ReplaceAll(strings.ReplaceAll(s, `\`, `\\`), "'", `\'`) + "'"
}

func runTool(name, arguments string) string {
	var args map[string]any
	_ = json.Unmarshal([]byte(cmpOr(arguments, "{}")), &args) // bad arguments: an empty query
	q, _ := args["query"].(string)
	return fmt.Sprintf("%s result for %s", name, pyRepr(q))
}

func cmpOr(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

// SharedTools are the MCP server's tools as Responses function tools, called through an MCP
// client.
type SharedTools struct {
	Tools        []*ojson.Object
	Instructions string
	names        map[string]bool
	session      *mcp.ClientSession
}

type headerTransport struct {
	headers http.Header
	base    http.RoundTripper
}

func (t headerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	for k, v := range t.headers {
		r.Header[k] = v
	}
	return t.base.RoundTrip(r)
}

// ConnectShared connects to the MCP server as session/master and lists its tools.
func ConnectShared(ctx context.Context, url, session, master, token string) (*SharedTools, error) {
	headers := http.Header{}
	headers.Set("X-Wetink-Session", session)
	headers.Set("X-Wetink-Master", master)
	if token != "" {
		headers.Set("Authorization", "Bearer "+token)
	}
	hc := &http.Client{Transport: headerTransport{headers, http.DefaultTransport}, Timeout: 5 * time.Minute}
	client := mcp.NewClient(&mcp.Implementation{Name: "wetink-master", Version: "1.0.0"}, nil)
	cs, err := client.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: url, HTTPClient: hc}, nil)
	if err != nil {
		return nil, err
	}
	s := &SharedTools{session: cs, names: map[string]bool{}}
	if init := cs.InitializeResult(); init != nil {
		s.Instructions = init.Instructions
	}
	list, err := cs.ListTools(ctx, nil)
	if err != nil {
		cs.Close()
		return nil, err
	}
	for _, t := range list.Tools {
		s.names[t.Name] = true
		s.Tools = append(s.Tools, ojson.New().
			Set("type", "function").
			Set("name", t.Name).
			Set("description", t.Description).
			Set("parameters", t.InputSchema))
	}
	return s, nil
}

func (s *SharedTools) Has(name string) bool { return s != nil && s.names[name] }

// Call runs a tool with the model's JSON arguments and returns its text ("Error: ..." if the
// tool failed).
func (s *SharedTools) Call(ctx context.Context, name, arguments string) (string, error) {
	var args map[string]any
	if err := json.Unmarshal([]byte(cmpOr(arguments, "{}")), &args); err != nil {
		return "Error: arguments are not a JSON object", nil
	}
	res, err := s.session.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		return "", err
	}
	var parts []string
	for _, c := range res.Content {
		if t, ok := c.(*mcp.TextContent); ok {
			parts = append(parts, t.Text)
		}
	}
	text := strings.Join(parts, "\n")
	if res.IsError && !strings.HasPrefix(text, "Error") {
		text = "Error: " + text
	}
	return text, nil
}

func (s *SharedTools) Close() error { return s.session.Close() }

// Options of one master run.
type Options struct {
	Model    string
	MaxTurns int   // default 5
	Context  []any // a handoff's input, placed before the prompt
	Shared   *SharedTools
}

// Run runs the tool loop until the model answers with a message, and returns its text.
func Run(ctx context.Context, t *tap.Tap, client *oai.Client, prompt string, o Options) (string, error) {
	if o.MaxTurns == 0 {
		o.MaxTurns = 5
	}
	input := append(append([]any{}, o.Context...), ojson.New().Set("role", "user").Set("content", prompt))
	tools := []any{LookupTool}
	if o.Shared != nil {
		for _, st := range o.Shared.Tools {
			tools = append(tools, st)
		}
	}
	for range o.MaxTurns {
		body := ojson.New().Set("model", o.Model).Set("input", input).Set("tools", tools)
		if o.Shared != nil && o.Shared.Instructions != "" {
			body.Set("instructions", o.Shared.Instructions)
		}
		var produced []map[string]any
		for ev, err := range t.Stream(ctx, client, body) {
			if err != nil {
				return "", err
			}
			if ev.Type() == "response.output_item.done" {
				produced = append(produced, ev.Item())
			}
		}
		var calls []map[string]any
		for _, it := range produced {
			input = append(input, it)
			if it["type"] == "function_call" {
				calls = append(calls, it)
			}
		}
		if len(calls) == 0 {
			var b strings.Builder
			for _, it := range produced {
				if it["type"] != "message" {
					continue
				}
				content, _ := it["content"].([]any)
				for _, p := range content {
					part, _ := p.(map[string]any)
					s, _ := part["text"].(string)
					b.WriteString(s)
				}
			}
			return b.String(), nil
		}
		for _, call := range calls {
			name, _ := call["name"].(string)
			args, _ := call["arguments"].(string)
			var output string
			if o.Shared.Has(name) {
				var err error
				if output, err = o.Shared.Call(ctx, name, args); err != nil {
					return "", err
				}
			} else {
				output = runTool(name, args)
			}
			result := map[string]any{"type": "function_call_output", "call_id": call["call_id"], "output": output}
			t.Publish(result, 0)
			input = append(input, result)
		}
	}
	return "", fmt.Errorf("no final message after %d turns", o.MaxTurns)
}

// FetchHandoff gets a handoff (GET .../handoff) to start a fresh master from.
func FetchHandoff(ctx context.Context, url, token string) (map[string]any, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("handoff: HTTP %d", resp.StatusCode)
	}
	var out map[string]any
	return out, json.NewDecoder(resp.Body).Decode(&out)
}
