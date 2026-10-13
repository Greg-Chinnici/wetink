// Package classifier tags an entry with topic, kind and a relevance score, and relates a new
// entry to existing ones.
//
// Two backends:
//   - SystemOne: a decision model behind a TypeSafe System One compatible /v1/systemone
//     endpoint (llama-server with LiquidAI d1-3B). One forward pass returns a probability per
//     option, so the score comes with a confidence. The default.
//   - Chat: a small chat model with JSON-schema structured output (Responses or Chat
//     Completions), with an anchored 1-5 rubric and tolerant parsing. No relations.
package classifier

import (
	"context"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strings"

	"wetink/internal/ojson"
	"wetink/internal/pystr"
)

var Kinds = []string{"fact", "decision", "hypothesis", "open_question", "artifact", "narration"}

// NewTopic is the topic choice for "none of the known topics fits".
const NewTopic = "(new)"

// FinalKinds: decision models pay one pass per question, so "final" is derived from kind
// instead of asked (on the eval it was also more accurate: 17/17 against 14/17).
var FinalKinds = map[string]bool{"fact": true, "decision": true, "artifact": true}

func IsKind(k string) bool {
	for _, x := range Kinds {
		if x == k {
			return true
		}
	}
	return false
}

// Thresholds. Keep the visibility ones in sync with the context_entries view (schema.sql).
const (
	// Entries below this relevance, and hypotheses, are not shown in shared context. Open
	// questions score lower by nature ("useful but uncertain"), so they have their own.
	MinVisibleScore         = 3.0
	MinVisibleQuestionScore = 2.5
	// A new topic only when the model clearly prefers it over every offered topic; otherwise
	// entries drift into near-duplicate topics and per-topic ranking and relations break.
	NewTopicMinProb = 0.6
	// A question mark plus at least this probability for open_question makes an open
	// question: the model alone put "What format should the short codes have? ..." at fact
	// 0.49 against open_question 0.36, and questions drive coordination.
	QuestionMinProb = 0.25
	// Below this probability a relation other than "new" is not acted on.
	RelationMinProb = 0.5
)

func Visible(kind string, score float64) bool {
	if kind == "open_question" {
		return score >= MinVisibleQuestionScore
	}
	return kind != "hypothesis" && kind != "narration" && score >= MinVisibleScore
}

// NarrationMaxChars: narration is a sentence or two of process talk. Longer text, or text
// with markup or code, is content the model misread (a README draft's "Run `nix develop`").
const NarrationMaxChars = 300

// NarrationMinProb: on the sales-pipeline scenario short decisions ("We'll load everything
// into DuckDB") came back as narration at 0.41-0.55, real narration at 0.58-0.64.
const NarrationMinProb = 0.6

func NarrationShaped(text string) bool {
	t := pystr.Strip(text)
	return pystr.Len(t) <= NarrationMaxChars && !strings.ContainsAny(t, "\n`#|")
}

// Error is a malformed or unusable classifier answer.
type Error struct{ msg string }

func (e *Error) Error() string { return e.msg }

func errorf(format string, args ...any) error { return &Error{fmt.Sprintf(format, args...)} }

// IsError reports whether err is a classifier Error.
func IsError(err error) bool {
	var e *Error
	return errors.As(err, &e)
}

type Classification struct {
	Topic      string
	Kind       string
	Final      bool
	Score      float64  // relevance 1-5; fractional when it is an expected value
	Confidence *float64 // 0-1, from decision models only
	// What the model was asked and answered, for inspection (stored with the entry).
	Trace map[string]any
}

type Relation struct {
	Relation    string // one of Relations
	Target      *int64 // entry id; nil for "new"
	Probability float64
	// ChangeProbability is P(replaces) + P(contradicts) from a decision model: for an entry by
	// the same author both mean it changed its mind, and the model often splits between them.
	// 0 when unknown (chat models).
	ChangeProbability float64
	Trace             map[string]any
}

// NewRelation is "adds information": nothing else changes.
func NewRelation(p float64) Relation { return Relation{Relation: "new", Probability: p} }

// Topic is a topic offered to the classifier, with an optional description.
type Topic struct {
	Name string
	Desc *string
}

// Topics in the order they are offered: the session's declared ones, then recent ones.
type Topics []Topic

func (t Topics) Names() []string {
	out := make([]string, len(t))
	for i, x := range t {
		out[i] = x.Name
	}
	return out
}

func (t Topics) Has(name string) bool {
	for _, x := range t {
		if x.Name == name {
			return true
		}
	}
	return false
}

// Add appends a topic without a description unless it is already there.
func (t *Topics) Add(name string) {
	if !t.Has(name) {
		*t = append(*t, Topic{Name: name})
	}
}

// Candidate is an existing entry a new one is related to, labelled with kind and author.
type Candidate struct {
	ID   int64
	Text string
}

type Classifier interface {
	Classify(ctx context.Context, text, master string, topics Topics) (Classification, error)
	Relate(ctx context.Context, entry string, existing []Candidate) (Relation, error)
}

var topicChars = regexp.MustCompile(`[^a-z0-9]+`)

func NormalizeTopic(topic string) string {
	t := strings.Trim(topicChars.ReplaceAllString(strings.ToLower(topic), "_"), "_")
	t = pystr.Prefix(t, 64)
	if t == "" {
		return "general"
	}
	return t
}

func round3(x float64) float64 { return math.Round(x*1000) / 1000 }

// IsArtifact: the decision model labelled plain decisions ("we'll load everything into
// DuckDB") as artifacts whatever the wording, so an artifact is structural: a code block.
func IsArtifact(text string) bool { return strings.Contains(text, "```") }

var questionRe = regexp.MustCompile(`\?([` + pystr.SpaceClass + `]|$)`)

// HasQuestion reports a question mark that ends a sentence.
func HasQuestion(text string) bool { return questionRe.MatchString(text) }

// Decision-model criteria. Keep in sync with the rubric in Instructions.
var KindCriteria = ojson.New().
	Set("fact", "verified information or an observed result").
	Set("decision", "a choice the agent has committed to").
	// "suggestion" matters: "perhaps we should ..." came back as a decision without it.
	Set("hypothesis", "a guess, assumption, suggestion or unverified idea").
	// "a question" matters: without it, plain questions came back as facts (p=0.08).
	Set("open_question", "a question, or something unresolved that needs an answer").
	// Real masters open their messages with "I'll start by sharing my design ..."; scored as
	// relevant (3-3.7) they filled the board with process talk.
	Set("narration", "only the agent's own process or next steps (checking, reading, sharing, writing up), with no choice, result or question about the task")

var RelevanceLevels = []string{
	"noise: chit-chat, narration of what the agent is about to do, no new information",
	"a minor detail or weakly supported",
	"somewhat useful, or useful but uncertain",
	"specific, reliable and useful",
	"a committed decision or verified result the other agents must know",
}

var Relations = []string{"new", "restates", "replaces", "answers", "contradicts"}

var RelationCriteria = ojson.New().
	Set("new", "adds information that is not in the existing entries").
	Set("restates", "repeats, confirms or acknowledges an existing entry without adding information").
	Set("replaces", "revises, reverses or replaces an existing entry, which no longer holds").
	Set("answers", "answers an open question among the existing entries").
	Set("contradicts", "disagrees with or conflicts with an existing entry")

// SystemOneQuestions are the classification questions: kind (unless the entry is an
// artifact), relevance, and topic when there are topics to choose from.
func SystemOneQuestions(topics Topics, askKind bool) *ojson.Object {
	q := ojson.New()
	if askKind {
		q.Set("kind", ojson.New().
			Set("type", "choice").
			Set("instructions", "What kind of contribution is this item from an AI agent?").
			Set("criteria", KindCriteria))
	}
	q.Set("relevance", ojson.New().
		Set("type", "score").
		Set("instructions", "How relevant and reliable is this item for other agents working on the same task?").
		Set("criteria", RelevanceLevels))
	if len(topics) > 0 {
		criteria := ojson.New()
		for _, t := range topics {
			if t.Desc != nil {
				criteria.Set(t.Name, *t.Desc)
			} else {
				criteria.Set(t.Name, nil)
			}
		}
		criteria.Set(NewTopic, "none of the other topics fits")
		q.Set("topic", ojson.New().
			Set("type", "choice").
			Set("instructions", "Which topic is the item about?").
			Set("criteria", criteria))
	}
	return q
}

// RelationQuestions ask how a new entry relates to existing ones (refs "#id"), and which.
func RelationQuestions(refs []string) *ojson.Object {
	targets := ojson.New()
	for _, r := range refs {
		targets.Set(r, nil)
	}
	targets.Set("none", "none of them")
	return ojson.New().
		Set("relation", ojson.New().
			Set("type", "choice").
			Set("instructions", "How does the new entry relate to the existing entries?").
			Set("criteria", RelationCriteria)).
		Set("target", ojson.New().
			Set("type", "choice").
			Set("instructions", "Which existing entry does the new entry restate, replace, answer or contradict?").
			Set("criteria", targets))
}

func num(v any) (float64, bool) {
	f, ok := v.(float64)
	return f, ok
}

// argmax returns the key with the highest value; the first one on ties (key order).
func argmax(o *ojson.Object, skip string) (string, bool) {
	best, found := "", false
	bestP := math.Inf(-1)
	for _, k := range o.Keys() {
		if k == skip {
			continue
		}
		v, _ := o.Get(k)
		p, ok := num(v)
		if !ok {
			continue
		}
		if !found || p > bestP {
			best, bestP, found = k, p, true
		}
	}
	return best, found
}

// ParseSystemOne reads a /v1/systemone classification response. kind, if set, overrides the
// model's (a rule decided it). It also reports whether a new topic needs a name.
func ParseSystemOne(response any, kind string) (Classification, bool, error) {
	answers, ok := ojson.Path(response, "answers")
	if _, isObj := answers.(*ojson.Object); !ok || !isObj {
		return Classification{}, false, errorf("malformed systemone response: no answers")
	}
	if kind == "" {
		k, ok := ojson.Path(answers, "kind", "choice")
		if kind, _ = k.(string); !ok || kind == "" {
			return Classification{}, false, errorf("malformed systemone response: no kind")
		}
	}
	s, ok1 := ojson.Path(answers, "relevance", "score")
	c, ok2 := ojson.Path(answers, "relevance", "confidence")
	score, ok3 := num(s)
	confidence, ok4 := num(c)
	if !ok1 || !ok2 || !ok3 || !ok4 {
		return Classification{}, false, errorf("malformed systemone response: relevance")
	}
	score++ // level index 0-4 -> 1-5
	topic := NewTopic
	probs := ojson.New()
	if ta, ok := ojson.Path(answers, "topic"); ok {
		tobj, isObj := ta.(*ojson.Object)
		if !isObj {
			return Classification{}, false, errorf("malformed systemone response: topic")
		}
		if v, ok := tobj.Get("choice"); ok {
			if topic, ok = v.(string); !ok {
				return Classification{}, false, errorf("malformed systemone response: topic choice")
			}
		}
		if v, ok := tobj.Get("probabilities"); ok {
			if probs, ok = v.(*ojson.Object); !ok {
				return Classification{}, false, errorf("malformed systemone response: probabilities")
			}
		}
	}
	if !IsKind(kind) {
		return Classification{}, false, errorf("unknown kind %q", kind)
	}
	score = math.Min(math.Max(score, 1), 5)
	if topic == NewTopic {
		pNew := 1.0
		if v, ok := probs.Get(NewTopic); ok {
			pNew, _ = num(v)
		}
		// Hidden entries (hypotheses, noise) never create topics; they would attract later
		// entries away from the declared ones.
		if best, ok := argmax(probs, NewTopic); ok && (pNew < NewTopicMinProb || !Visible(kind, score)) {
			topic = best
		}
	}
	needsName := topic == NewTopic && Visible(kind, score)
	if needsName {
		topic = "general"
	} else {
		topic = NormalizeTopic(topic)
	}
	conf := round3(confidence)
	return Classification{
		Topic:      topic,
		Kind:       kind,
		Final:      FinalKinds[kind],
		Score:      round3(score),
		Confidence: &conf,
	}, needsName, nil
}

// ParseRelation reads a relation response; refs maps "#id" to entry ids.
func ParseRelation(response any, refs map[string]int64) (Relation, error) {
	r, ok1 := ojson.Path(response, "answers", "relation", "choice")
	t, ok2 := ojson.Path(response, "answers", "target", "choice")
	relation, ok3 := r.(string)
	if !ok1 || !ok2 || !ok3 {
		return Relation{}, errorf("malformed relation response")
	}
	p, ok := ojson.Path(response, "answers", "relation", "probabilities", relation)
	probability, isNum := num(p)
	if !ok || !isNum {
		return Relation{}, errorf("malformed relation response: probability")
	}
	known := false
	for _, x := range Relations {
		known = known || x == relation
	}
	if !known {
		return Relation{}, errorf("unknown relation %q", relation)
	}
	ref, _ := t.(string)
	target, found := refs[ref]
	rel := Relation{Relation: relation, Target: &target, Probability: probability}
	gate := probability
	if relation == "replaces" || relation == "contradicts" {
		for _, r := range []string{"replaces", "contradicts"} {
			p, _ := ojson.Path(response, "answers", "relation", "probabilities", r)
			if x, ok := num(p); ok {
				rel.ChangeProbability += x
			}
		}
		gate = rel.ChangeProbability
	}
	if relation == "new" || !found || gate < RelationMinProb {
		return NewRelation(probability), nil
	}
	return rel, nil
}
