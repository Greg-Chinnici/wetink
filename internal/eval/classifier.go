package eval

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"slices"
	"time"

	"wetink/internal/classifier"
)

// ClassifierCase is one labelled item (evals/classifier.jsonl). A null expectation is not
// checked; a list means any listed value is accepted; score is an inclusive [lo, hi] band.
// new_topic: true means the model should not reuse a known topic.
type ClassifierCase struct {
	ID     string   `json:"id"`
	Topics []string `json:"topics"`
	Text   string   `json:"text"`
	Expect struct {
		Topic    any      `json:"topic,omitempty"`
		NewTopic bool     `json:"new_topic,omitempty"`
		Kind     any      `json:"kind,omitempty"`
		Final    *bool    `json:"final,omitempty"`
		Score    [2]int64 `json:"score"`
	} `json:"expect"`
}

var fields = []string{"topic", "kind", "final", "score"}

func anyOf(v any) []string {
	switch t := v.(type) {
	case string:
		return []string{t}
	case []any:
		var out []string
		for _, x := range t {
			if s, ok := x.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}

// pyRound rounds half to even, like Python's round().
func pyRound(x float64) int64 { return int64(math.RoundToEven(x)) }

func checkCase(c ClassifierCase, got classifier.Classification) map[string]*bool {
	ok := map[string]*bool{}
	set := func(f string, v bool) { ok[f] = &v }
	normalized := func(names []string) []string {
		out := make([]string, len(names))
		for i, n := range names {
			out[i] = classifier.NormalizeTopic(n)
		}
		return out
	}
	switch {
	case c.Expect.NewTopic:
		set("topic", !slices.Contains(normalized(c.Topics), got.Topic))
	case c.Expect.Topic != nil:
		set("topic", slices.Contains(normalized(anyOf(c.Expect.Topic)), got.Topic))
	}
	if c.Expect.Kind != nil {
		set("kind", slices.Contains(anyOf(c.Expect.Kind), got.Kind))
	}
	if c.Expect.Final != nil {
		set("final", got.Final == *c.Expect.Final)
	}
	r := pyRound(got.Score)
	set("score", c.Expect.Score[0] <= r && r <= c.Expect.Score[1])
	return ok
}

// EvalClassifier scores a classifier against labelled cases and prints accuracy per field,
// latency and mean confidence.
func EvalClassifier(ctx context.Context, c classifier.Classifier, path string, verbose bool, out io.Writer) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	var cases []ClassifierCase
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if len(sc.Bytes()) == 0 {
			continue
		}
		var cc ClassifierCase
		if err := json.Unmarshal(sc.Bytes(), &cc); err != nil {
			return err
		}
		cases = append(cases, cc)
	}
	hits := map[string][2]int{}
	var latencies, confidences []float64
	errs := 0
	for _, cc := range cases {
		var topics classifier.Topics
		for _, t := range cc.Topics {
			topics.Add(t)
		}
		start := time.Now()
		got, err := c.Classify(ctx, cc.Text, "master-a", topics)
		if err != nil {
			if !classifier.IsError(err) {
				return err
			}
			errs++
			fmt.Fprintf(out, "✗ %s: %v\n", cc.ID, err)
			continue
		}
		latencies = append(latencies, time.Since(start).Seconds())
		if got.Confidence != nil {
			confidences = append(confidences, *got.Confidence)
		}
		ok := checkCase(cc, got)
		var bad []string
		for _, f := range fields {
			if v := ok[f]; v != nil {
				h := hits[f]
				if *v {
					h[0]++
				} else {
					bad = append(bad, f)
				}
				h[1]++
				hits[f] = h
			}
		}
		if len(bad) > 0 || verbose {
			mark := "✓"
			if len(bad) > 0 {
				mark = "✗"
			}
			want, _ := json.Marshal(cc.Expect)
			fmt.Fprintf(out, "%s %s: got %s/%s final=%v score=%.2f expected %s (wrong: %v)\n",
				mark, cc.ID, got.Topic, got.Kind, got.Final, got.Score, want, bad)
		}
	}
	fmt.Fprintf(out, "\ncases=%d parse_errors=%d\n", len(cases), errs)
	for _, f := range fields {
		fmt.Fprintf(out, "  %-6s %d/%d\n", f, hits[f][0], hits[f][1])
	}
	if len(latencies) > 0 {
		slices.Sort(latencies)
		p95 := latencies[min(len(latencies)-1, int(0.95*float64(len(latencies))))]
		fmt.Fprintf(out, "  latency p50=%.2fs p95=%.2fs\n", median(latencies), p95)
	}
	if len(confidences) > 0 {
		sum := 0.0
		for _, x := range confidences {
			sum += x
		}
		fmt.Fprintf(out, "  relevance confidence mean=%.2f\n", sum/float64(len(confidences)))
	}
	return nil
}

func median(sorted []float64) float64 {
	n := len(sorted)
	if n%2 == 1 {
		return sorted[n/2]
	}
	return (sorted[n/2-1] + sorted[n/2]) / 2
}
