package classifier

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"wetink/internal/oai"
	"wetink/internal/ojson"
)

const SchemaName = "wetink_classification"

// ChatKinds are the kinds a chat classifier chooses from (Instructions describes them); the
// decision model also has "narration".
var ChatKinds = []string{"fact", "decision", "hypothesis", "open_question", "artifact"}

// TextFormat is the Responses `text` parameter; the topic enum makes reuse of known topics
// grammar-enforced.
func TextFormat(knownTopics []string) *ojson.Object {
	enum := append(append([]string{}, knownTopics...), NewTopic)
	props := ojson.New().
		Set("topic", ojson.New().Set("type", "string").Set("enum", enum)).
		Set("new_topic", ojson.New().Set("type", "string")).
		Set("kind", ojson.New().Set("type", "string").Set("enum", ChatKinds)).
		Set("final", ojson.New().Set("type", "boolean")).
		Set("score", ojson.New().Set("type", "integer").Set("minimum", 1).Set("maximum", 5))
	schema := ojson.New().
		Set("type", "object").
		Set("properties", props).
		Set("required", []string{"topic", "new_topic", "kind", "final", "score"}).
		Set("additionalProperties", false)
	return ojson.New().Set("format", ojson.New().
		Set("type", "json_schema").
		Set("name", SchemaName).
		Set("strict", true).
		Set("schema", schema))
}

const Instructions = `You classify one item produced by an AI agent (a "master") that works alongside other masters.
Return JSON with:
- topic: the known topic the item is about. Only if none of them fits, use "(new)".
- new_topic: when topic is "(new)", a short snake_case label (1-3 words); otherwise "".
- kind: one of
  fact: verified information or an observed result
  decision: a choice the master has committed to
  hypothesis: a guess, assumption or unverified idea
  open_question: something unresolved that needs an answer
  artifact: produced content such as code, a document or a config
- final: true only if the item is stated as settled or committed; false if it is tentative, exploratory or still in progress.
- score: how relevant and reliable the item is for the other masters, 1-5:
  5: a committed decision or verified result the others must know
  4: specific, reliable and useful
  3: somewhat useful, or useful but uncertain
  2: a minor detail or weakly supported
  1: noise: chit-chat, narration of what the master is about to do, no new information

Examples:
Known topics: storage
<item>Benchmarked both: SQLite handles 2k writes/s, Postgres 15k writes/s on the same box.</item>
{"topic": "storage", "new_topic": "", "kind": "fact", "final": true, "score": 4}

Known topics: storage, deployment
<item>Going with Fly.io for hosting; the Heroku option is dropped.</item>
{"topic": "deployment", "new_topic": "", "kind": "decision", "final": true, "score": 5}

Known topics: none yet
<item>Perhaps the timeout is caused by DNS resolution, need to check.</item>
{"topic": "(new)", "new_topic": "timeouts", "kind": "hypothesis", "final": false, "score": 3}

Known topics: deployment
<item>Let me read the config files next.</item>
{"topic": "deployment", "new_topic": "", "kind": "fact", "final": false, "score": 1}`

func BuildInput(text, master string, knownTopics []string) string {
	topics := "none yet"
	if len(knownTopics) > 0 {
		topics = strings.Join(knownTopics, ", ")
	}
	return "Known topics: " + topics + "\n\n<item master=\"" + master + "\">\n" + text + "\n</item>"
}

// Parse reads a chat model's JSON answer; small models sometimes wrap it in prose or fences.
func Parse(raw string) (Classification, error) {
	s := raw
	if start := strings.Index(raw, "{"); start != -1 {
		end := strings.LastIndex(raw, "}")
		if end+1 >= start {
			s = raw[start : end+1]
		} else {
			s = ""
		}
	}
	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()
	var data map[string]any
	if err := dec.Decode(&data); err != nil || data == nil || dec.More() {
		return Classification{}, errorf("unparseable classification %q", raw)
	}
	topicV, ok1 := data["topic"]
	kindV, ok2 := data["kind"]
	finalV, ok3 := data["final"]
	scoreV, ok4 := data["score"]
	if !ok1 || !ok2 || !ok3 || !ok4 {
		return Classification{}, errorf("unparseable classification %q", raw)
	}
	if topicV == NewTopic {
		topicV = "general"
		if nt, ok := data["new_topic"]; ok && nt != nil && nt != "" && nt != false {
			topicV = nt
		}
	}
	topic, okT := topicV.(string)
	kind, _ := kindV.(string)
	final, okF := finalV.(bool)
	// An integer literal (JSON 3.0 or 3e0 is a float, and true is not a number).
	n, okN := scoreV.(json.Number)
	score, err := n.Int64()
	if !okT || !IsKind(kind) || !okF || !okN || err != nil || strings.ContainsAny(string(n), ".eE") {
		return Classification{}, errorf("invalid classification %q", raw)
	}
	return Classification{
		Topic: NormalizeTopic(topic),
		Kind:  kind,
		Final: final,
		Score: float64(min(max(score, 1), 5)),
	}, nil
}

// ChatAPIs are the chat-model backends: an Open Responses provider or gateway ("responses"),
// or Chat Completions ("chat", for servers like LM Studio that enforce a JSON schema only
// there).
var ChatAPIs = []string{"responses", "chat"}

type Chat struct {
	Client *oai.Client
	Model  string
	API    string
	sem    chan struct{}
}

func NewChat(client *oai.Client, model, api string, concurrency int) (*Chat, error) {
	if api != "responses" && api != "chat" {
		return nil, fmt.Errorf("api must be responses or chat, got %q", api)
	}
	return &Chat{Client: client, Model: model, API: api, sem: make(chan struct{}, max(concurrency, 1))}, nil
}

func (c *Chat) Classify(ctx context.Context, text, master string, topics Topics) (Classification, error) {
	names := topics.Names()
	start := time.Now()
	raw, err := c.complete(ctx, Instructions, BuildInput(text, master, names), TextFormat(names))
	if err != nil {
		return Classification{}, err
	}
	result, err := Parse(raw)
	if err != nil {
		return Classification{}, err
	}
	result.Trace = map[string]any{"model": c.Model, "ms": time.Since(start).Milliseconds(), "raw": raw}
	return result, nil
}

// complete asks for a JSON answer in the given text format ({"format": {json_schema ...}}),
// over Chat Completions or Responses.
func (c *Chat) complete(ctx context.Context, instructions, input string, textFormat *ojson.Object) (string, error) {
	select {
	case c.sem <- struct{}{}:
	case <-ctx.Done():
		return "", ctx.Err()
	}
	defer func() { <-c.sem }()
	if c.API == "chat" {
		f, _ := textFormat.Get("format")
		format := f.(*ojson.Object)
		schema := ojson.New()
		for _, k := range []string{"name", "strict", "schema"} {
			v, _ := format.Get(k)
			schema.Set(k, v)
		}
		body := ojson.New().
			Set("model", c.Model).
			Set("messages", []any{
				ojson.New().Set("role", "system").Set("content", instructions),
				ojson.New().Set("role", "user").Set("content", input),
			}).
			Set("response_format", ojson.New().Set("type", "json_schema").Set("json_schema", schema))
		return c.Client.ChatCompletion(ctx, body)
	}
	body := ojson.New().
		Set("model", c.Model).
		Set("instructions", instructions).
		Set("input", input).
		Set("text", textFormat).
		Set("store", false)
	resp, err := c.Client.Response(ctx, body)
	if err != nil {
		return "", err
	}
	return oai.OutputText(resp), nil
}

// NamerPrompt and NamerExamples: small models copy examples from the system prompt and echo
// jargon like "snake_case"; plain wording plus examples as prior turns works.
const NamerPrompt = "What is the subject of the text? Answer with one to three lowercase words and nothing else."

var NamerExamples = [][2]string{
	{"We switched the logger to structured JSON output.", "logging"},
	{"The login form rejects passwords longer than 64 characters.", "login form"},
	{"Kubernetes pods restart every hour because of OOM kills.", "pod memory"},
}

// ChatTopicNamer names new topics with a small chat model (only when no known topic fits).
func ChatTopicNamer(client *oai.Client, model string) TopicNamer {
	return func(ctx context.Context, text string) (string, error) {
		messages := []any{ojson.New().Set("role", "system").Set("content", NamerPrompt)}
		for _, ex := range NamerExamples {
			messages = append(messages,
				ojson.New().Set("role", "user").Set("content", ex[0]),
				ojson.New().Set("role", "assistant").Set("content", ex[1]))
		}
		messages = append(messages, ojson.New().Set("role", "user").Set("content", text))
		body := ojson.New().
			Set("model", model).
			Set("messages", messages).
			Set("max_tokens", 12).
			Set("temperature", 0)
		content, err := client.ChatCompletion(ctx, body)
		if err != nil {
			return "", err
		}
		content = strings.TrimSpace(content)
		var words []string
		if content != "" {
			words = strings.Fields(strings.SplitN(content, "\n", 2)[0])
		}
		if len(words) > 3 {
			words = words[:3]
		}
		if len(words) == 0 {
			return "general", nil
		}
		return strings.Join(words, " "), nil
	}
}

// APIs are the classifier backends.
var APIs = []string{"responses", "chat", "systemone"}

type Options struct {
	API, Model, BaseURL, APIKey           string
	NamerModel, NamerBaseURL, NamerAPIKey string
	Timeout                               time.Duration
	Concurrency                           int // systemone: calls in flight per endpoint (default 4)
}

// Build returns the classifier for opts.API; for "systemone", the Namer* options configure
// the topic namer.
func Build(o Options) (Classifier, error) {
	if o.Timeout == 0 {
		o.Timeout = 60 * time.Second
	}
	if o.API == "systemone" {
		if o.BaseURL == "" {
			return nil, fmt.Errorf("systemone needs a base URL, e.g. http://localhost:9931/v1 (several: comma-separated)")
		}
		var namer TopicNamer
		if o.NamerModel != "" {
			namer = ChatTopicNamer(oai.New(o.NamerBaseURL, o.NamerAPIKey, o.Timeout), o.NamerModel)
		}
		model := o.Model
		if model == "default" {
			model = ""
		}
		return NewSystemOne(o.BaseURL, model, o.APIKey, namer, o.Concurrency, o.Timeout), nil
	}
	return NewChat(oai.New(o.BaseURL, o.APIKey, o.Timeout), o.Model, o.API, 8)
}

// BuildFromEnv is Build with keys and the topic namer taken from WETINK_* environment variables.
func BuildFromEnv(api, model, baseURL string, timeout time.Duration) (Classifier, error) {
	env := func(keys ...string) string {
		for _, k := range keys {
			if v := os.Getenv(k); v != "" {
				return v
			}
		}
		return ""
	}
	return Build(Options{
		API:          api,
		Model:        model,
		BaseURL:      baseURL,
		APIKey:       env("WETINK_CLASSIFIER_API_KEY", "OPENAI_API_KEY"),
		NamerModel:   env("WETINK_TOPIC_NAMER_MODEL"),
		NamerBaseURL: env("WETINK_TOPIC_NAMER_BASE_URL"),
		NamerAPIKey:  env("WETINK_TOPIC_NAMER_API_KEY", "OPENAI_API_KEY"),
		Timeout:      timeout,
		Concurrency:  envInt("WETINK_CLASSIFIER_CONCURRENCY"),
	})
}

// envInt is an integer environment variable, 0 when unset or not a number.
func envInt(key string) int {
	n, _ := strconv.Atoi(os.Getenv(key))
	return n
}

// WithRelateFromEnv relates with a separate backend when WETINK_RELATE_API is set
// (WETINK_RELATE_MODEL, WETINK_RELATE_BASE_URL, WETINK_RELATE_API_KEY,
// WETINK_RELATE_CONCURRENCY), and with c otherwise.
func WithRelateFromEnv(c Classifier, timeout time.Duration) (Classifier, error) {
	api := os.Getenv("WETINK_RELATE_API")
	if api == "" {
		return c, nil
	}
	key := os.Getenv("WETINK_RELATE_API_KEY")
	if key == "" {
		key = os.Getenv("OPENAI_API_KEY")
	}
	r, err := Build(Options{API: api, Model: os.Getenv("WETINK_RELATE_MODEL"), BaseURL: os.Getenv("WETINK_RELATE_BASE_URL"),
		APIKey: key, Timeout: timeout, Concurrency: envInt("WETINK_RELATE_CONCURRENCY")})
	if err != nil {
		return nil, err
	}
	return Split{Classifier: c, Relater: r}, nil
}
