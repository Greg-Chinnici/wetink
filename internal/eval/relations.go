package eval

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"time"

	"wetink/internal/classifier"
	"wetink/internal/ojson"
	"wetink/internal/pipeline"
)

// RelationCase is one labelled relation (evals/relations.jsonl): a new entry, the existing
// entries it is compared with, and the expected outcome (target unset for "new").
type RelationCase struct {
	ID       string          `json:"id"`
	Entry    RelationEntry   `json:"entry"`
	Existing []RelationEntry `json:"existing"`
	Expect   struct {
		Relation string `json:"relation"`
		Target   *int64 `json:"target"`
	} `json:"expect"`
}

type RelationEntry struct {
	ID     int64  `json:"id"`
	Kind   string `json:"kind"`
	Master string `json:"master"`
	Text   string `json:"text"`
}

func loadJSONL[T any](path string) ([]T, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []T
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		if len(sc.Bytes()) == 0 {
			continue
		}
		var v T
		if err := json.Unmarshal(sc.Bytes(), &v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, sc.Err()
}

func sameTarget(want, got *int64) bool {
	return want == nil || (got != nil && *got == *want)
}

// EvalRelations scores a classifier's Relate against labelled cases: the model's own answer
// ("model") and the outcome the pipeline applies to it (pipeline.Decide: thresholds, same
// author, compatible kinds), which is what the board shows.
func EvalRelations(ctx context.Context, c classifier.Classifier, path string, verbose bool, out io.Writer) error {
	cases, err := loadJSONL[RelationCase](path)
	if err != nil {
		return err
	}
	var modelHits, decidedHits, errs int
	perRelation := map[string][2]int{} // expected relation -> decided hits, cases
	var latencies []float64
	for _, rc := range cases {
		existing := make([]classifier.Candidate, len(rc.Existing))
		infos := map[int64]pipeline.CandidateInfo{}
		for i, e := range rc.Existing {
			existing[i] = classifier.Candidate{ID: e.ID, Text: pipeline.Label(e.Kind, e.Master, e.Text)}
			infos[e.ID] = pipeline.CandidateInfo{Kind: e.Kind, Master: e.Master}
		}
		start := time.Now()
		rel, err := c.Relate(ctx, pipeline.Label(rc.Entry.Kind, rc.Entry.Master, rc.Entry.Text), existing)
		if err != nil {
			if !classifier.IsError(err) {
				return err
			}
			errs++
			fmt.Fprintf(out, "✗ %s: %v\n", rc.ID, err)
			continue
		}
		latencies = append(latencies, time.Since(start).Seconds())
		outcome := pipeline.Decide(rc.Entry.Kind, rc.Entry.Master, rel, infos)
		modelOK := rel.Relation == rc.Expect.Relation && sameTarget(rc.Expect.Target, rel.Target)
		decidedOK := outcome.Relation == rc.Expect.Relation && sameTarget(rc.Expect.Target, outcome.Related)
		if rc.Expect.Relation == "new" {
			decidedOK = outcome.Relation == "new"
		}
		if modelOK {
			modelHits++
		}
		h := perRelation[rc.Expect.Relation]
		if decidedOK {
			decidedHits++
			h[0]++
		}
		h[1]++
		perRelation[rc.Expect.Relation] = h
		if !decidedOK || verbose {
			mark := "✓"
			if !decidedOK {
				mark = "✗"
			}
			target := "-"
			if rel.Target != nil {
				target = fmt.Sprint(*rel.Target)
			}
			want := rc.Expect.Relation
			if rc.Expect.Target != nil {
				want += fmt.Sprintf(" #%d", *rc.Expect.Target)
			}
			fmt.Fprintf(out, "%s %s: model %s #%s p=%.2f -> %s, expected %s%s\n",
				mark, rc.ID, rel.Relation, target, rel.Probability, outcome.Relation, want, distribution(rel))
		}
	}
	fmt.Fprintf(out, "\ncases=%d errors=%d\n  model   %d/%d\n  decided %d/%d\n", len(cases), errs,
		modelHits, len(cases)-errs, decidedHits, len(cases)-errs)
	for _, r := range classifier.Relations {
		if h, ok := perRelation[r]; ok {
			fmt.Fprintf(out, "    %-11s %d/%d\n", r, h[0], h[1])
		}
	}
	if len(latencies) > 0 {
		slices.Sort(latencies)
		fmt.Fprintf(out, "  latency p50=%.2fs max=%.2fs\n", median(latencies), latencies[len(latencies)-1])
	}
	return nil
}

// distribution is a decision model's relation probabilities (" [new .43 restates .31 ...]").
func distribution(rel classifier.Relation) string {
	p, ok := ojson.Path(rel.Trace["answers"], "relation", "probabilities")
	o, isObj := p.(*ojson.Object)
	if !ok || !isObj {
		return ""
	}
	var b strings.Builder
	for _, k := range o.Keys() {
		v, _ := o.Get(k)
		if x, ok := v.(float64); ok {
			fmt.Fprintf(&b, " %s %.2f", k, x)
		}
	}
	return " [" + strings.TrimSpace(b.String()) + "]"
}
