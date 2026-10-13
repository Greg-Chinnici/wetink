// Package mockgw is a deterministic mock of the model endpoints, for running and testing
// without a provider: Open Responses (POST /v1/responses, plain or SSE), Chat Completions
// and the TypeSafe System One API.
//
//   - If the user text has `[tool: name {"json": "args"}]` markers, it calls those tools in
//     order, one per turn, then replies.
//   - Otherwise, if tools are offered and the input has no function_call_output yet, it calls
//     the first tool with {"query": <last user text>}.
//   - The reply is a text message echoing the user text and the tool outputs, streamed word by
//     word.
//   - Classification (the JSON-schema request, chat with a schema, /v1/systemone): topic from
//     a `[topic: X]` marker (else "general"); kind from keywords (``` -> artifact,
//     decided/decision/we will -> decision, maybe/might/probably/hypothesis -> hypothesis,
//     "?" -> open_question, else fact); final unless the kind is hypothesis or open_question;
//     score from a `[score: N]` marker, else by kind.
//   - Relations (System One, or the wetink_relation schema): from a `[rel: replaces]` or
//     `[rel: replaces: some text]` marker in the new entry, else "new". The target is the first
//     (most recent) existing entry, or the first one containing the given text.
package mockgw

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"wetink/internal/classifier"
	"wetink/internal/ojson"
)

var (
	topicMark = regexp.MustCompile(`(?i)\[topic:\s*([^\]]+)\]`)
	scoreMark = regexp.MustCompile(`(?i)\[score:\s*([1-5])\]`)
	toolMark  = regexp.MustCompile(`(?s)\[tool:\s*([A-Za-z0-9_-]+)\s*(\{.*?\})?\s*\]`)
	relMark   = regexp.MustCompile(`\[rel:\s*(\w+)(?::\s*([^\]]+))?\]`)
	itemRe    = regexp.MustCompile(`(?s)<item[^>]*>\n?(.*?)\n?</item>`)
	decided   = regexp.MustCompile(`\b(decided|decision|we will)\b`)
	tentative = regexp.MustCompile(`\b(maybe|might|probably|hypothesis)\b`)
)

var kindScore = map[string]int{"decision": 5, "fact": 4, "artifact": 4, "open_question": 3, "hypothesis": 2}

func id(prefix string) string {
	b := make([]byte, 12)
	rand.Read(b)
	return prefix + "_" + hex.EncodeToString(b)
}

func textOf(content any) string {
	switch c := content.(type) {
	case string:
		return c
	case []any:
		var parts []string
		for _, p := range c {
			if o, ok := p.(*ojson.Object); ok {
				t, _ := o.Get("text")
				s, _ := t.(string)
				parts = append(parts, s)
			}
		}
		return strings.Join(parts, " ")
	}
	return ""
}

func str(o *ojson.Object, key string) string {
	v, _ := o.Get(key)
	s, _ := v.(string)
	return s
}

// Guess is the keyword heuristic classification of a prompt (or the <item> inside it).
type Guess struct {
	Topic string `json:"topic"`
	Kind  string `json:"kind"`
	Final bool   `json:"final"`
	Score int    `json:"score"`
}

func Classify(prompt string) Guess {
	text := prompt
	if m := itemRe.FindStringSubmatch(prompt); m != nil {
		text = m[1]
	}
	lower := strings.ToLower(text)
	kind := "fact"
	switch {
	case strings.Contains(text, "```"):
		kind = "artifact"
	case decided.MatchString(lower):
		kind = "decision"
	case tentative.MatchString(lower):
		kind = "hypothesis"
	case strings.Contains(text, "?"):
		kind = "open_question"
	}
	g := Guess{Topic: "general", Kind: kind, Final: kind != "hypothesis" && kind != "open_question", Score: kindScore[kind]}
	if m := topicMark.FindStringSubmatch(text); m != nil {
		g.Topic = strings.TrimSpace(m[1])
	}
	if m := scoreMark.FindStringSubmatch(text); m != nil {
		g.Score = int(m[1][0] - '0')
	}
	return g
}

func guessJSON(g Guess) string {
	return fmt.Sprintf(`{"topic": %s, "kind": %q, "final": %v, "score": %d}`, quote(g.Topic), g.Kind, g.Final, g.Score)
}

func quote(s string) string { b, _ := json.Marshal(s); return string(b) }

func message(text string) *ojson.Object {
	return ojson.New().
		Set("type", "message").
		Set("id", id("msg")).
		Set("status", "completed").
		Set("role", "assistant").
		Set("content", []any{ojson.New().Set("type", "output_text").Set("text", text).Set("annotations", []any{})})
}

func planOutput(body *ojson.Object) ([]*ojson.Object, error) {
	rawInput, _ := body.Get("input")
	var items []*ojson.Object
	switch in := rawInput.(type) {
	case string:
		items = []*ojson.Object{ojson.New().Set("role", "user").Set("content", in)}
	case []any:
		for _, x := range in {
			if o, ok := x.(*ojson.Object); ok {
				items = append(items, o)
			}
		}
	}
	userText := ""
	for i := len(items) - 1; i >= 0; i-- {
		if str(items[i], "role") == "user" {
			c, _ := items[i].Get("content")
			userText = textOf(c)
			break
		}
	}
	var toolOutputs []string
	for _, it := range items {
		if str(it, "type") == "function_call_output" {
			o, _ := it.Get("output")
			toolOutputs = append(toolOutputs, fmt.Sprint(o))
		}
	}
	var tools []*ojson.Object
	if ts, ok := body.Get("tools"); ok {
		list, _ := ts.([]any)
		for _, t := range list {
			if o, ok := t.(*ojson.Object); ok && str(o, "type") == "function" {
				tools = append(tools, o)
			}
		}
	}
	if f, ok := ojson.Path(body, "text", "format"); ok {
		if fo, ok := f.(*ojson.Object); ok && str(fo, "type") == "json_schema" {
			switch str(fo, "name") {
			case classifier.SchemaName:
				return []*ojson.Object{message(guessJSON(Classify(userText)))}, nil
			case classifier.RelationSchemaName:
				return []*ojson.Object{message(relationJSON(userText))}, nil
			}
			return nil, fmt.Errorf("mock only supports the %s and %s schemas", classifier.SchemaName, classifier.RelationSchemaName)
		}
	}
	call := func(name, args string) []*ojson.Object {
		return []*ojson.Object{ojson.New().
			Set("type", "function_call").
			Set("id", id("fc")).
			Set("call_id", id("call")).
			Set("name", name).
			Set("arguments", args).
			Set("status", "completed")}
	}
	scripted := toolMark.FindAllStringSubmatch(userText, -1)
	if len(scripted) > 0 && len(toolOutputs) < len(scripted) {
		m := scripted[len(toolOutputs)]
		args := m[2]
		if args == "" {
			args = "{}"
		}
		return call(m[1], args), nil
	}
	if len(tools) > 0 && len(toolOutputs) == 0 && len(scripted) == 0 {
		return call(str(tools[0], "name"), `{"query": `+quote(userText)+`}`), nil
	}
	text := "Mock reply to: " + userText
	if len(toolOutputs) > 0 {
		text += " (tool said: " + strings.Join(toolOutputs, " | ") + ")"
	}
	return []*ojson.Object{message(text)}, nil
}

func response(body *ojson.Object, respID string, created int64, output []*ojson.Object, status string) *ojson.Object {
	model := str(body, "model")
	if model == "" {
		model = "mock"
	}
	tools, ok := body.Get("tools")
	if !ok || tools == nil {
		tools = []any{}
	}
	out := make([]any, len(output))
	for i, o := range output {
		out[i] = o
	}
	return ojson.New().
		Set("id", respID).
		Set("object", "response").
		Set("created_at", created).
		Set("status", status).
		Set("model", model).
		Set("output", out).
		Set("parallel_tool_calls", true).
		Set("tool_choice", "auto").
		Set("tools", tools)
}

func clone(o *ojson.Object) *ojson.Object {
	c := ojson.New()
	for _, k := range o.Keys() {
		v, _ := o.Get(k)
		c.Set(k, v)
	}
	return c
}

func events(body *ojson.Object, respID string, created int64, output []*ojson.Object) []*ojson.Object {
	evs := []*ojson.Object{
		ojson.New().Set("type", "response.created").Set("response", response(body, respID, created, nil, "in_progress")),
		ojson.New().Set("type", "response.in_progress").Set("response", response(body, respID, created, nil, "in_progress")),
	}
	for idx, it := range output {
		pending := clone(it).Set("status", "in_progress")
		typ := str(it, "type")
		if typ == "message" {
			pending.Set("content", []any{})
		}
		if typ == "function_call" {
			pending.Set("arguments", "")
		}
		evs = append(evs, ojson.New().Set("type", "response.output_item.added").Set("output_index", idx).Set("item", pending))
		itemID := str(it, "id")
		switch typ {
		case "message":
			c, _ := it.Get("content")
			part := c.([]any)[0].(*ojson.Object)
			text := str(part, "text")
			loc := func(o *ojson.Object) *ojson.Object {
				return o.Set("item_id", itemID).Set("output_index", idx).Set("content_index", 0)
			}
			empty := ojson.New().Set("type", "output_text").Set("text", "").Set("annotations", []any{})
			evs = append(evs, loc(ojson.New().Set("type", "response.content_part.added")).Set("part", empty))
			for _, word := range strings.Split(text, " ") {
				evs = append(evs, loc(ojson.New().Set("type", "response.output_text.delta")).Set("delta", word+" "))
			}
			evs = append(evs, loc(ojson.New().Set("type", "response.output_text.done")).Set("text", text))
			evs = append(evs, loc(ojson.New().Set("type", "response.content_part.done")).Set("part", part))
		case "function_call":
			args := str(it, "arguments")
			evs = append(evs,
				ojson.New().Set("type", "response.function_call_arguments.delta").Set("item_id", itemID).Set("output_index", idx).Set("delta", args),
				ojson.New().Set("type", "response.function_call_arguments.done").Set("item_id", itemID).Set("output_index", idx).Set("arguments", args))
		}
		evs = append(evs, ojson.New().Set("type", "response.output_item.done").Set("output_index", idx).Set("item", it))
	}
	evs = append(evs, ojson.New().Set("type", "response.completed").Set("response", response(body, respID, created, output, "completed")))
	for n, ev := range evs {
		ev.Set("sequence_number", n)
	}
	return evs
}

func readBody(r *http.Request) (*ojson.Object, error) {
	data, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	v, err := ojson.Decode(data)
	if err != nil {
		return nil, err
	}
	o, ok := v.(*ojson.Object)
	if !ok {
		return nil, fmt.Errorf("body must be an object")
	}
	return o, nil
}

func writeJSON(w http.ResponseWriter, v any) {
	data, _ := ojson.Marshal(v)
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func createResponse(w http.ResponseWriter, r *http.Request) {
	body, err := readBody(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	output, err := planOutput(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	respID, created := id("resp"), time.Now().Unix()
	if stream, _ := body.Get("stream"); stream != true {
		writeJSON(w, response(body, respID, created, output, "completed"))
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	flusher, _ := w.(http.Flusher)
	for _, ev := range events(body, respID, created, output) {
		data, _ := ojson.Marshal(ev)
		fmt.Fprintf(w, "event: %s\ndata: %s\n\n", str(ev, "type"), data)
		if flusher != nil {
			flusher.Flush()
		}
	}
}

func chatCompletions(w http.ResponseWriter, r *http.Request) {
	body, err := readBody(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	userText := ""
	if ms, ok := body.Get("messages"); ok {
		list, _ := ms.([]any)
		for i := len(list) - 1; i >= 0; i-- {
			if m, ok := list[i].(*ojson.Object); ok && str(m, "role") == "user" {
				c, _ := m.Get("content")
				userText = textOf(c)
				break
			}
		}
	}
	content := "Mock reply to: " + userText
	if f, ok := body.Get("response_format"); ok {
		if fo, ok := f.(*ojson.Object); ok && str(fo, "type") == "json_schema" {
			name, _ := ojson.Path(fo, "json_schema", "name")
			switch name {
			case classifier.SchemaName:
				content = guessJSON(Classify(userText))
			case classifier.RelationSchemaName:
				content = relationJSON(userText)
			default:
				http.Error(w, "mock only supports the "+classifier.SchemaName+" and "+classifier.RelationSchemaName+" schemas",
					http.StatusBadRequest)
				return
			}
		}
	}
	model := str(body, "model")
	if model == "" {
		model = "mock"
	}
	writeJSON(w, ojson.New().
		Set("id", id("chatcmpl")).
		Set("object", "chat.completion").
		Set("created", time.Now().Unix()).
		Set("model", model).
		Set("choices", []any{ojson.New().
			Set("index", 0).
			Set("message", ojson.New().Set("role", "assistant").Set("content", content)).
			Set("finish_reason", "stop")}))
}

// relationJSON answers a chat relation question (classifier.RelateInput) by the same markers
// as relation: the new entry's [rel: ...], and the first existing entry (or the first that
// contains the marker's text) as the target.
func relationJSON(prompt string) string {
	existing, newEntry, _ := strings.Cut(prompt, "\nNew entry: ")
	rel, target := "new", "none"
	if m := relMark.FindStringSubmatch(newEntry); m != nil {
		for _, line := range strings.Split(existing, "\n") {
			ref, text, ok := strings.Cut(line, ": ")
			if !ok || !strings.HasPrefix(ref, "#") || (m[2] != "" && !strings.Contains(text, strings.TrimSpace(m[2]))) {
				continue
			}
			rel, target = m[1], ref
			break
		}
	}
	return `{"relation": ` + quote(rel) + `, "target": ` + quote(target) + `}`
}

func relation(body *ojson.Object) *ojson.Object {
	newEntry, _ := ojson.Path(body, "state", "new entry")
	m := relMark.FindStringSubmatch(fmt.Sprint(newEntry))
	rel := "new"
	if m != nil {
		rel = m[1]
	}
	existingV, _ := ojson.Path(body, "state", "existing entries")
	existing, _ := existingV.(*ojson.Object)
	criteriaV, _ := ojson.Path(body, "questions", "target", "criteria")
	criteria, _ := criteriaV.(*ojson.Object)
	var refs []string
	for _, ref := range criteria.Keys() {
		if ref == "none" {
			continue
		}
		if m != nil && m[2] != "" {
			text, _ := existing.Get(ref)
			s, _ := text.(string)
			if !strings.Contains(s, strings.TrimSpace(m[2])) {
				continue
			}
		}
		refs = append(refs, ref)
	}
	target := "none"
	if rel != "new" && len(refs) > 0 {
		target = refs[0]
	}
	return ojson.New().
		Set("relation", ojson.New().Set("type", "choice").Set("choice", rel).
			Set("probabilities", ojson.New().Set(rel, 1.0)).Set("confidence", 1.0)).
		Set("target", ojson.New().Set("type", "choice").Set("choice", target).
			Set("probabilities", ojson.New().Set(target, 1.0)))
}

func systemOne(w http.ResponseWriter, r *http.Request) {
	body, err := readBody(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	qv, _ := body.Get("questions")
	questions, _ := qv.(*ojson.Object)
	if _, ok := questions.Get("relation"); ok {
		writeJSON(w, ojson.New().Set("model", "mock").Set("answers", relation(body)))
		return
	}
	stateV, _ := body.Get("state")
	state, ok := stateV.(string)
	if !ok {
		b, _ := ojson.Marshal(stateV)
		state = string(b)
	}
	g := Classify(state)
	answers := ojson.New()
	for _, qid := range questions.Keys() {
		qv, _ := questions.Get(qid)
		q := qv.(*ojson.Object)
		switch str(q, "type") {
		case "noul":
			v := 0.0
			if g.Final {
				v = 1.0
			}
			answers.Set(qid, ojson.New().Set("type", "noul").Set("noul", v))
		case "score":
			answers.Set(qid, ojson.New().Set("type", "score").Set("score", float64(g.Score-1)).Set("confidence", 1.0))
		default:
			cv, _ := q.Get("criteria")
			var options []string
			if co, ok := cv.(*ojson.Object); ok {
				options = co.Keys()
			}
			wanted := g.Topic
			if qid == "kind" {
				wanted = g.Kind
			}
			choice := options[len(options)-1]
			for _, o := range options {
				if o == wanted {
					choice = wanted
				}
			}
			probs := ojson.New()
			for _, o := range options {
				p := 0.0
				if o == choice {
					p = 1.0
				}
				probs.Set(o, p)
			}
			answers.Set(qid, ojson.New().Set("type", "choice").Set("choice", choice).Set("probabilities", probs).Set("confidence", 1.0))
		}
	}
	writeJSON(w, ojson.New().Set("model", "mock").Set("answers", answers))
}

// Options shape the mock's /v1/systemone timing like a real decision model (benchmarks).
type Options struct {
	Delay time.Duration // per request
	Slots int           // requests served at once (0: unlimited), like llama-server slots
}

// Handler serves the mock under /v1, answering at once.
func Handler() http.Handler { return NewHandler(Options{}) }

func NewHandler(o Options) http.Handler {
	systemone := http.HandlerFunc(systemOne)
	if o.Delay > 0 || o.Slots > 0 {
		var slots chan struct{}
		if o.Slots > 0 {
			slots = make(chan struct{}, o.Slots)
		}
		systemone = func(w http.ResponseWriter, r *http.Request) {
			if slots != nil {
				select {
				case slots <- struct{}{}:
					defer func() { <-slots }()
				case <-r.Context().Done():
					return
				}
			}
			time.Sleep(o.Delay)
			systemOne(w, r)
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/responses", createResponse)
	mux.HandleFunc("POST /v1/chat/completions", chatCompletions)
	mux.Handle("POST /v1/systemone", systemone)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, "ok") })
	return mux
}
