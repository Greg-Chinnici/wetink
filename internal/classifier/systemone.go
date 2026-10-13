package classifier

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"wetink/internal/ojson"
)

// TopicNamer names a new topic from an entry's text.
type TopicNamer func(ctx context.Context, text string) (string, error)

// SystemOne classifies with a decision model; new topics are named by an optional chat model.
//
// The model can be served by several endpoints (e.g. one llama-server per GPU or machine):
// each call goes to the endpoint with the fewest calls in flight, at most `concurrency` per
// endpoint. A single local server is compute-bound (its slots share the GPU), so more
// endpoints, not more concurrency, is what raises throughput. Hosted models (Jev, Clef on
// Workers AI) are not, so they take more calls in flight.
type SystemOne struct {
	BaseURLs []string // e.g. http://localhost:9931/v1, or a full Workers AI /ai/run/<model> URL
	Model    string   // empty: the server's default
	APIKey   string
	Namer    TopicNamer
	HTTP     *http.Client

	sem      chan struct{} // total calls in flight
	mu       sync.Mutex
	inflight []int
}

// NewSystemOne returns a client for one or more endpoints (comma-separated base URLs).
func NewSystemOne(baseURLs, model, apiKey string, namer TopicNamer, concurrency int, timeout time.Duration) *SystemOne {
	if concurrency < 1 {
		concurrency = 4
	}
	var urls []string
	for _, u := range strings.Split(baseURLs, ",") {
		if u = strings.TrimRight(strings.TrimSpace(u), "/"); u != "" {
			urls = append(urls, u)
		}
	}
	return &SystemOne{
		BaseURLs: urls,
		Model:    model,
		APIKey:   apiKey,
		Namer:    namer,
		HTTP:     &http.Client{Timeout: timeout},
		sem:      make(chan struct{}, concurrency*max(len(urls), 1)),
		inflight: make([]int, len(urls)),
	}
}

// acquire picks the endpoint with the fewest calls in flight once a call may start.
func (c *SystemOne) acquire(ctx context.Context) (int, error) {
	select {
	case c.sem <- struct{}{}:
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	best := 0
	for i, n := range c.inflight {
		if n < c.inflight[best] {
			best = i
		}
	}
	c.inflight[best]++
	return best, nil
}

func (c *SystemOne) release(i int) {
	c.mu.Lock()
	c.inflight[i]--
	c.mu.Unlock()
	<-c.sem
}

// ask posts one /systemone request; the response gets the round trip in "ms".
func (c *SystemOne) ask(ctx context.Context, state any, questions *ojson.Object) (*ojson.Object, error) {
	if len(c.BaseURLs) == 0 {
		return nil, errorf("systemone: no endpoint configured")
	}
	body := ojson.New().Set("state", state).Set("questions", questions)
	if c.Model != "" {
		body.Set("model", c.Model)
	}
	data, err := ojson.Marshal(body)
	if err != nil {
		return nil, err
	}
	endpoint, err := c.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer c.release(endpoint)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, systemOneURL(c.BaseURLs[endpoint]), bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.APIKey)
	}
	start := time.Now()
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, errorf("systemone HTTP %d: %s", resp.StatusCode, truncate(string(raw), 200))
	}
	v, err := ojson.Decode(raw)
	if err != nil {
		return nil, errorf("systemone: %v", err)
	}
	obj, ok := v.(*ojson.Object)
	if !ok {
		return nil, errorf("systemone: response is not an object")
	}
	if obj, err = unwrapEnvelope(obj); err != nil {
		return nil, err
	}
	obj.Set("ms", float64(time.Since(start).Milliseconds()))
	return obj, nil
}

// systemOneURL is the request URL for a base URL: Workers AI's /ai/run/<model> URLs (Clef)
// and URLs already ending in /systemone are used as they are; others get /systemone.
func systemOneURL(base string) string {
	if strings.Contains(base, "/ai/run/") || strings.HasSuffix(base, "/systemone") {
		return base
	}
	return base + "/systemone"
}

// unwrapEnvelope returns the System One response inside a Cloudflare REST envelope
// ({"success": ..., "errors": [...], "result": {...}}), and other responses unchanged.
func unwrapEnvelope(obj *ojson.Object) (*ojson.Object, error) {
	success, ok := obj.Get("success")
	if !ok {
		return obj, nil
	}
	if success != true {
		errs, _ := obj.Get("errors")
		data, _ := ojson.Marshal(errs)
		return nil, errorf("systemone: request failed: %s", truncate(string(data), 200))
	}
	result, _ := obj.Get("result")
	inner, ok := result.(*ojson.Object)
	if !ok {
		return nil, errorf("systemone: envelope has no result object")
	}
	return inner, nil
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

func (c *SystemOne) Classify(ctx context.Context, text, master string, topics Topics) (Classification, error) {
	artifact := IsArtifact(text)
	response, err := c.ask(ctx, text, SystemOneQuestions(topics, !artifact))
	if err != nil {
		return Classification{}, err
	}
	kind := ""
	var rule any
	if artifact {
		kind, rule = "artifact", "artifact: contains a code block"
	} else if HasQuestion(text) {
		p, _ := ojson.Path(response, "answers", "kind", "probabilities", "open_question")
		if f, ok := num(p); ok && f >= QuestionMinProb {
			kind = "open_question"
			rule = "open question: has a '?' and p >= " + strconv.FormatFloat(QuestionMinProb, 'f', -1, 64)
		}
	}
	if choice, _ := ojson.Path(response, "answers", "kind", "choice"); kind == "" && choice == "narration" {
		probs, _ := ojson.Path(response, "answers", "kind", "probabilities")
		p, _ := ojson.Path(probs, "narration")
		pn, _ := num(p)
		if o, ok := probs.(*ojson.Object); ok && (pn < NarrationMinProb || !NarrationShaped(text)) {
			if best, ok := argmax(o, "narration"); ok {
				kind, rule = best, "not narration: p below the bar, longer than a short paragraph, or markup"
			}
		}
	}
	result, needsName, err := ParseSystemOne(response, kind)
	if err != nil {
		return Classification{}, err
	}
	model, _ := response.Get("model")
	ms, _ := response.Get("ms")
	answers, _ := response.Get("answers")
	trace := map[string]any{"model": model, "ms": ms, "answers": answers, "kind_rule": rule}
	if needsName && c.Namer != nil {
		start := time.Now()
		name, err := c.Namer(ctx, text)
		if err != nil {
			return Classification{}, fmt.Errorf("topic namer: %w", err)
		}
		result.Topic = NormalizeTopic(name)
		trace["named"] = map[string]any{"topic": result.Topic, "ms": time.Since(start).Milliseconds()}
	}
	result.Trace = trace
	return result, nil
}

// Relate asks how entry relates to the existing entries (labelled texts, in the order given).
func (c *SystemOne) Relate(ctx context.Context, entry string, existing []Candidate) (Relation, error) {
	if len(existing) == 0 {
		return NewRelation(1.0), nil
	}
	refs := make(map[string]int64, len(existing))
	order := make([]string, len(existing))
	texts := ojson.New()
	for i, e := range existing {
		ref := "#" + strconv.FormatInt(e.ID, 10)
		refs[ref], order[i] = e.ID, ref
		texts.Set(ref, e.Text)
	}
	state := ojson.New().Set("existing entries", texts).Set("new entry", entry)
	response, err := c.ask(ctx, state, RelationQuestions(order))
	if err != nil {
		return Relation{}, err
	}
	rel, err := ParseRelation(response, refs)
	if err != nil {
		return Relation{}, err
	}
	ms, _ := response.Get("ms")
	answers, _ := response.Get("answers")
	rel.Trace = map[string]any{"ms": ms, "answers": answers, "state": state}
	return rel, nil
}
