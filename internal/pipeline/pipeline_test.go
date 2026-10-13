package pipeline

import (
	"reflect"
	"slices"
	"testing"

	"wetink/internal/classifier"
	"wetink/internal/golden"
)

var candidates = map[int64]CandidateInfo{
	1: {"decision", "master-a"},
	2: {"open_question", "master-b"},
	3: {"fact", "master-a"},
}

func rel(relation string, target int64, p float64) classifier.Relation {
	r := classifier.Relation{Relation: relation, Probability: p}
	if target != 0 {
		r.Target = &target
	}
	return r
}

func outcome(status, relation string, related int64, close *Close) Outcome {
	return Outcome{Status: status, Relation: relation, Related: &related, Close: close}
}

func TestDecide(t *testing.T) {
	for i, c := range []struct {
		kind, master string
		rel          classifier.Relation
		want         Outcome
	}{
		// Own revision of the same kind supersedes...
		{"decision", "master-a", rel("replaces", 1, 0.9), outcome("active", "replaces", 1, &Close{"superseded", 1})},
		// ...but not when unsure, or across kinds.
		{"decision", "master-a", rel("replaces", 1, 0.6), NewOutcome},
		{"fact", "master-a", rel("replaces", 1, 0.9), NewOutcome},
		// Another master's "replacement" is a conflict, nothing is hidden.
		{"decision", "master-b", rel("replaces", 1, 0.55), outcome("active", "contradicts", 1, nil)},
		{"fact", "master-b", rel("contradicts", 3, 0.6), outcome("active", "contradicts", 3, nil)},
		// Answers resolve open questions only.
		{"fact", "master-a", rel("answers", 2, 0.8), outcome("active", "answers", 2, &Close{"resolved", 2})},
		{"fact", "master-b", rel("answers", 3, 0.9), NewOutcome},
		{"fact", "master-a", rel("answers", 2, 0.6), NewOutcome},
		// Restatements are hidden, except a decision adopting a fact.
		{"decision", "master-b", rel("restates", 1, 0.9), outcome("duplicate", "restates", 1, nil)},
		{"decision", "master-b", rel("restates", 3, 0.9), NewOutcome},
		{"fact", "master-b", rel("restates", 3, 0.65), NewOutcome},
		// Unknown target or "new".
		{"fact", "master-a", rel("replaces", 99, 0.9), NewOutcome},
		{"fact", "master-a", rel("new", 0, 1.0), NewOutcome},
	} {
		if got := Decide(c.kind, c.master, c.rel, candidates); !reflect.DeepEqual(got, c.want) {
			t.Errorf("case %d: got %+v want %+v", i, got, c.want)
		}
	}
}

func TestItemsGolden(t *testing.T) {
	var g struct {
		ItemParts []struct {
			Item    map[string]any `json:"item"`
			Call    map[string]any `json:"call"`
			Via     string         `json:"via"`
			Partial bool           `json:"partial"`
			Parts   []string       `json:"parts"`
		} `json:"item_parts"`
		Labels [][4]string `json:"labels"`
	}
	golden.Load(t, "items", &g)
	for _, c := range g.ItemParts {
		got := ItemParts(c.Item, c.Call, c.Via, c.Partial)
		if !slices.Equal(got, c.Parts) && (len(got) > 0 || len(c.Parts) > 0) {
			t.Errorf("ItemParts(%v, %v, %s, %v) = %q, want %q", c.Item, c.Call, c.Via, c.Partial, got, c.Parts)
		}
	}
	for _, l := range g.Labels {
		if got := Label(l[0], l[1], l[2]); got != l[3] {
			t.Errorf("Label(%q, %q, ...) = %q, want %q", l[0], l[1], got, l[3])
		}
	}
}
