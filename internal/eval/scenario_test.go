package eval

import (
	"testing"

	"wetink/internal/golden"
)

func TestChecksGolden(t *testing.T) {
	var cases []struct {
		Scenario string `json:"scenario"`
		Text     string `json:"text"`
		Results  []struct {
			Check  Check  `json:"check"`
			OK     bool   `json:"ok"`
			Detail string `json:"detail"`
		} `json:"results"`
	}
	golden.Load(t, "checks", &cases)
	n := 0
	for _, c := range cases {
		for _, r := range c.Results {
			ok, detail, err := r.Check.Run(c.Text)
			if err != nil || ok != r.OK || detail != r.Detail {
				t.Errorf("%s/%s: got %v %q %v, want %v %q", c.Scenario, r.Check.ID, ok, detail, err, r.OK, r.Detail)
			}
			n++
		}
	}
	if n < 50 {
		t.Errorf("only %d checks", n)
	}
}

func TestScenariosLoad(t *testing.T) {
	scenarios := LoadScenarios(golden.Root() + "/evals/context")
	if len(scenarios) != 4 {
		t.Fatalf("%d scenarios", len(scenarios))
	}
	s := scenarios["url-shortener"]
	if s.Topics.Keys()[0] != "storage" || len(s.Steps) != 16 || !s.Steps[0].Stream {
		t.Errorf("url-shortener: %v %d", s.Topics.Keys(), len(s.Steps))
	}
}
