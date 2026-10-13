package classifier

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"wetink/internal/ojson"
)

const RelationSchemaName = "wetink_relation"

// RelateInstructions: the distinctions small models get wrong are spelled out (a further
// detail is new, not a replacement; a changed choice replaces even when worded as an addition).
const RelateInstructions = `Several AI agents work on one task and post entries to a shared board. You compare a new entry with existing entries and say how it relates to them.
Return JSON with:
- relation: one of
  new: adds information that no existing entry has: another detail, component or result. It changes none of them.
  restates: says the same as an existing entry again (reworded, confirmed or acknowledged) without adding information.
  replaces: changes something an existing entry stated: a different choice, value or result now holds on the same point (switching a database, changing a code length, a newer progress figure). The existing entry no longer holds.
  answers: answers an open question among the existing entries.
  contradicts: cannot both be true with an existing entry, typically a different choice by another agent on the same point.
- target: the id ("#12") of the existing entry it restates, replaces, answers or contradicts; "none" when the relation is new.
Entries are labelled with their kind and the agent that wrote them. When several relations apply, prefer replaces, answers or contradicts over restates.`

func relationFormat(refs []string) *ojson.Object {
	targets := append(append([]string{}, refs...), "none")
	props := ojson.New().
		Set("relation", ojson.New().Set("type", "string").Set("enum", Relations)).
		Set("target", ojson.New().Set("type", "string").Set("enum", targets))
	schema := ojson.New().
		Set("type", "object").
		Set("properties", props).
		Set("required", []string{"relation", "target"}).
		Set("additionalProperties", false)
	return ojson.New().Set("format", ojson.New().
		Set("type", "json_schema").
		Set("name", RelationSchemaName).
		Set("strict", true).
		Set("schema", schema))
}

// RelateInput lists the existing entries, then the new one.
func RelateInput(entry string, existing []Candidate) string {
	var b strings.Builder
	b.WriteString("Existing entries:\n")
	for _, e := range existing {
		b.WriteString("#" + strconv.FormatInt(e.ID, 10) + ": " + e.Text + "\n")
	}
	b.WriteString("\nNew entry: " + entry)
	return b.String()
}

// ParseRelationJSON reads a chat model's relation answer. A chat model gives no
// probabilities: its answer counts as certain (1.0), so the pipeline applies it.
func ParseRelationJSON(raw string, refs map[string]int64) (Relation, error) {
	s := raw
	if start := strings.Index(raw, "{"); start != -1 {
		if end := strings.LastIndex(raw, "}"); end > start {
			s = raw[start : end+1]
		}
	}
	var data struct {
		Relation string `json:"relation"`
		Target   string `json:"target"`
	}
	if err := json.Unmarshal([]byte(s), &data); err != nil {
		return Relation{}, errorf("unparseable relation %q", raw)
	}
	known := false
	for _, x := range Relations {
		known = known || x == data.Relation
	}
	if !known {
		return Relation{}, errorf("unknown relation %q", data.Relation)
	}
	target, found := refs[data.Target]
	if data.Relation == "new" || !found {
		return NewRelation(1.0), nil
	}
	return Relation{Relation: data.Relation, Target: &target, Probability: 1.0}, nil
}

func (c *Chat) Relate(ctx context.Context, entry string, existing []Candidate) (Relation, error) {
	if len(existing) == 0 {
		return NewRelation(1.0), nil
	}
	refs := make(map[string]int64, len(existing))
	order := make([]string, len(existing))
	for i, e := range existing {
		ref := "#" + strconv.FormatInt(e.ID, 10)
		refs[ref], order[i] = e.ID, ref
	}
	start := time.Now()
	input := RelateInput(entry, existing)
	raw, err := c.complete(ctx, RelateInstructions, input, relationFormat(order))
	if err != nil {
		return Relation{}, err
	}
	rel, err := ParseRelationJSON(raw, refs)
	if err != nil {
		return Relation{}, err
	}
	rel.Trace = map[string]any{"model": c.Model, "ms": time.Since(start).Milliseconds(), "raw": raw, "input": input}
	return rel, nil
}

// Split classifies with one classifier and relates with another (e.g. a fast decision model
// for every entry, a stronger chat model for the fewer relation judgments).
type Split struct {
	Classifier
	Relater Classifier
}

func (s Split) Relate(ctx context.Context, entry string, existing []Candidate) (Relation, error) {
	return s.Relater.Relate(ctx, entry, existing)
}
