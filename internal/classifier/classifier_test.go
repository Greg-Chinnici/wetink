package classifier

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"wetink/internal/golden"
	"wetink/internal/ojson"
)

type goldenData struct {
	Questions []struct {
		Topics  [][2]*string `json:"topics"`
		AskKind bool         `json:"ask_kind"`
		JSON    string       `json:"json"`
	} `json:"questions"`
	RelationQuestions []struct {
		Refs []string `json:"refs"`
		JSON string   `json:"json"`
	} `json:"relation_questions"`
	RelateState    string `json:"relate_state"`
	ParseSystemOne []struct {
		Response json.RawMessage `json:"response"`
		Kind     *string         `json:"kind"`
		Error    bool            `json:"error"`
		Result   struct {
			Topic      string  `json:"topic"`
			Kind       string  `json:"kind"`
			Final      bool    `json:"final"`
			Score      float64 `json:"score"`
			Confidence float64 `json:"confidence"`
			NeedsName  bool    `json:"needs_name"`
		} `json:"result"`
	} `json:"parse_systemone"`
	ParseRelation []struct {
		Response json.RawMessage `json:"response"`
		Error    bool            `json:"error"`
		Result   []any           `json:"result"`
	} `json:"parse_relation"`
	ParseChat []struct {
		Raw    string `json:"raw"`
		Error  bool   `json:"error"`
		Result []any  `json:"result"`
	} `json:"parse_chat"`
	TextFormat     []string          `json:"text_format"`
	Instructions   string            `json:"instructions"`
	BuildInput     []string          `json:"build_input"`
	NormalizeTopic map[string]string `json:"normalize_topic"`
	Visible        [][3]any          `json:"visible"`
	QuestionRegex  map[string]bool   `json:"question_regex"`
}

func load(t *testing.T) goldenData {
	var g goldenData
	golden.Load(t, "classifier", &g)
	return g
}

func marshal(t *testing.T, v any) string {
	b, err := ojson.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func decode(t *testing.T, raw []byte) any {
	v, err := ojson.Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestQuestionsGolden(t *testing.T) {
	g := load(t)
	for _, q := range g.Questions {
		var topics Topics
		for _, kv := range q.Topics {
			topics = append(topics, Topic{Name: *kv[0], Desc: kv[1]})
		}
		if got := marshal(t, SystemOneQuestions(topics, q.AskKind)); got != q.JSON {
			t.Errorf("questions(%v, %v)\n got %s\nwant %s", q.Topics, q.AskKind, got, q.JSON)
		}
	}
	for _, q := range g.RelationQuestions {
		if got := marshal(t, RelationQuestions(q.Refs)); got != q.JSON {
			t.Errorf("relation questions\n got %s\nwant %s", got, q.JSON)
		}
	}
	for i, names := range [][]string{{}, {"storage", "ids"}} {
		if got := marshal(t, TextFormat(names)); got != g.TextFormat[i] {
			t.Errorf("text format\n got %s\nwant %s", got, g.TextFormat[i])
		}
	}
	if Instructions != g.Instructions {
		t.Errorf("instructions differ")
	}
	if got := BuildInput("Use SQLite.", "master-a", nil); got != g.BuildInput[0] {
		t.Errorf("build input %q", got)
	}
	if got := BuildInput("Use SQLite.", "master-b", []string{"storage", "ids"}); got != g.BuildInput[1] {
		t.Errorf("build input %q", got)
	}
}

func TestParseGolden(t *testing.T) {
	g := load(t)
	for _, c := range g.ParseSystemOne {
		kind := ""
		if c.Kind != nil {
			kind = *c.Kind
		}
		got, needs, err := ParseSystemOne(decode(t, c.Response), kind)
		if c.Error {
			if err == nil {
				t.Errorf("%s: want error, got %+v", c.Response, got)
			}
			continue
		}
		want := c.Result
		if err != nil || got.Topic != want.Topic || got.Kind != want.Kind || got.Final != want.Final ||
			got.Score != want.Score || *got.Confidence != want.Confidence || needs != want.NeedsName {
			t.Errorf("%s:\n got %+v needs=%v err=%v\nwant %+v", c.Response, got, needs, err, want)
		}
	}
	refs := map[string]int64{"#12": 12, "#3": 3}
	for _, c := range g.ParseRelation {
		got, err := ParseRelation(decode(t, c.Response), refs)
		if c.Error {
			if err == nil {
				t.Errorf("%s: want error", c.Response)
			}
			continue
		}
		var target any
		if got.Target != nil {
			target = float64(*got.Target)
		}
		if err != nil || got.Relation != c.Result[0] || target != c.Result[1] || got.Probability != c.Result[2] {
			t.Errorf("%s: got %+v err=%v want %v", c.Response, got, err, c.Result)
		}
	}
	for _, c := range g.ParseChat {
		got, err := Parse(c.Raw)
		if c.Error {
			if err == nil {
				t.Errorf("%q: want error, got %+v", c.Raw, got)
			}
			continue
		}
		if err != nil || got.Topic != c.Result[0] || got.Kind != c.Result[1] || got.Final != c.Result[2] || got.Score != c.Result[3] {
			t.Errorf("%q: got %+v err=%v want %v", c.Raw, got, err, c.Result)
		}
	}
}

func TestHelpersGolden(t *testing.T) {
	g := load(t)
	for in, want := range g.NormalizeTopic {
		if got := NormalizeTopic(in); got != want {
			t.Errorf("NormalizeTopic(%q) = %q, want %q", in, got, want)
		}
	}
	for _, v := range g.Visible {
		if got := Visible(v[0].(string), v[1].(float64)); got != v[2] {
			t.Errorf("Visible(%v, %v) = %v", v[0], v[1], got)
		}
	}
	for in, want := range g.QuestionRegex {
		if got := HasQuestion(in); got != want {
			t.Errorf("HasQuestion(%q) = %v", in, got)
		}
	}
}

// fakeServer answers /systemone with the given response and records request bodies.
func fakeServer(t *testing.T, response string) (*httptest.Server, *[]string) {
	var bodies []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		bodies = append(bodies, string(b))
		w.Write([]byte(response))
	}))
	t.Cleanup(srv.Close)
	return srv, &bodies
}

func TestWorkersAIEnvelope(t *testing.T) {
	// Clef on Workers AI REST: the URL names the model, the answers sit under "result".
	var paths []string
	answers := `{"answers": {"kind": {"choice": "decision", "probabilities": {"decision": 0.9}}, "relevance": {"score": 3.0, "confidence": 0.8}}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		if r.URL.Path == "/ai/run/@cf/cloudflare/clef-flash" {
			w.Write([]byte(`{"success": true, "errors": [], "messages": [], "result": ` + answers + `}`))
			return
		}
		w.Write([]byte(`{"success": false, "errors": [{"code": 5006, "message": "bad model"}], "result": null}`))
	}))
	t.Cleanup(srv.Close)
	c := NewSystemOne(srv.URL+"/ai/run/@cf/cloudflare/clef-flash", "clef-flash", "", nil, 1, 0)
	got, err := c.Classify(context.Background(), "We will use Postgres.", "m", nil)
	if err != nil || got.Kind != "decision" {
		t.Fatalf("envelope: %+v %v", got, err)
	}
	c = NewSystemOne(srv.URL+"/v1", "jev-latest", "", nil, 1, 0)
	if _, err := c.Classify(context.Background(), "We will use Postgres.", "m", nil); err == nil {
		t.Errorf("failed envelope: no error")
	}
	if want := []string{"/ai/run/@cf/cloudflare/clef-flash", "/v1/systemone"}; paths[0] != want[0] || paths[1] != want[1] {
		t.Errorf("paths %v, want %v", paths, want)
	}
}

func TestRelateState(t *testing.T) {
	g := load(t)
	srv, bodies := fakeServer(t, `{"answers": {"relation": {"choice": "replaces", "probabilities": {"replaces": 0.8}}, "target": {"choice": "#12"}}}`)
	c := NewSystemOne(srv.URL, "", "", nil, 1, 0)
	rel, err := c.Relate(context.Background(), "[decision from master-a] z", []Candidate{
		{12, "[fact from master-a] x"}, {3, "[decision from master-b] y"},
	})
	if err != nil || rel.Relation != "replaces" || *rel.Target != 12 {
		t.Fatalf("relate: %+v %v", rel, err)
	}
	body := ojson.New()
	if err := json.Unmarshal([]byte((*bodies)[0]), body); err != nil {
		t.Fatal(err)
	}
	state, _ := body.Get("state")
	if got := marshal(t, state); got != g.RelateState {
		t.Errorf("state\n got %s\nwant %s", got, g.RelateState)
	}
}

func TestQuestionRuleAndArtifacts(t *testing.T) {
	// The model leans fact, but a question with p(open_question) >= 0.25 is an open question.
	resp := `{"answers": {"kind": {"choice": "fact", "probabilities": {"fact": 0.49, "open_question": 0.36}}, "relevance": {"score": 2.0, "confidence": 0.5}}}`
	srv, bodies := fakeServer(t, resp)
	c := NewSystemOne(srv.URL, "", "", nil, 1, 0)
	got, err := c.Classify(context.Background(), "What format should the codes have?", "m", nil)
	if err != nil || got.Kind != "open_question" {
		t.Fatalf("question rule: %+v %v", got, err)
	}
	got, _ = c.Classify(context.Background(), "No question here.", "m", nil)
	if got.Kind != "fact" {
		t.Errorf("without '?': %s", got.Kind)
	}
	// An entry with a code block is an artifact, and kind is not asked.
	got, _ = c.Classify(context.Background(), "Here:\n```go\nx := 1\n```", "m", nil)
	if got.Kind != "artifact" || got.Trace["kind_rule"] == nil {
		t.Errorf("artifact: %+v", got)
	}
	var last map[string]any
	if err := json.Unmarshal([]byte((*bodies)[len(*bodies)-1]), &last); err != nil {
		t.Fatal(err)
	}
	if _, asked := last["questions"].(map[string]any)["kind"]; asked {
		t.Errorf("kind asked for an artifact")
	}
}

func TestNamer(t *testing.T) {
	resp := `{"answers": {"kind": {"choice": "fact", "probabilities": {"fact": 1}}, "relevance": {"score": 3.0, "confidence": 1}}}`
	srv, _ := fakeServer(t, resp)
	named := ""
	namer := func(_ context.Context, text string) (string, error) { named = text; return "CI Pipeline", nil }
	c := NewSystemOne(srv.URL, "", "", namer, 1, 0)
	got, err := c.Classify(context.Background(), "Builds take 3 minutes.", "m", nil)
	if err != nil || got.Topic != "ci_pipeline" || named == "" {
		t.Fatalf("namer: %+v %v", got, err)
	}
	c.Namer = nil
	if got, _ := c.Classify(context.Background(), "Builds take 3 minutes.", "m", nil); got.Topic != "general" {
		t.Errorf("without namer: %s", got.Topic)
	}
}
