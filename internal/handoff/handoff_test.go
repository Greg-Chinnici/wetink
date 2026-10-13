package handoff

import (
	"encoding/json"
	"slices"
	"testing"

	"wetink/internal/golden"
)

type goldenOutput struct {
	Session  string  `json:"session"`
	UptoSeq  int64   `json:"upto_seq"`
	Pending  int64   `json:"pending"`
	Items    int     `json:"items"`
	EntryIDs []int64 `json:"entry_ids"`
	Input    []struct {
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
	} `json:"input"`
}

func pairs(raw [][]int64) [][2]int64 {
	out := make([][2]int64, len(raw))
	for i, c := range raw {
		out[i] = [2]int64{c[0], c[1]}
	}
	return out
}

func TestBuildGolden(t *testing.T) {
	var cases []struct {
		Session   string       `json:"session"`
		Task      *string      `json:"task"`
		Entries   []Entry      `json:"entries"`
		Conflicts [][]int64    `json:"conflicts"`
		MaxChars  int          `json:"max_chars"`
		Output    goldenOutput `json:"output"`
	}
	golden.Load(t, "handoff", &cases)
	for _, c := range cases {
		got := Build(c.Session, c.Entries, pairs(c.Conflicts), c.Output.UptoSeq, c.Output.Pending, c.Task, c.MaxChars)
		want := ""
		if len(c.Output.Input) > 0 {
			want = c.Output.Input[0].Content[0].Text
		}
		if got.Text() != want {
			t.Errorf("%s max_chars=%d:\n--- got\n%s\n--- want\n%s", c.Session, c.MaxChars, got.Text(), want)
		}
		if got.Items != c.Output.Items || !slices.Equal(got.EntryIDs, c.Output.EntryIDs) || len(got.Input) != len(c.Output.Input) {
			t.Errorf("%s max_chars=%d: items %d ids %v, want %d %v", c.Session, c.MaxChars, got.Items, got.EntryIDs, c.Output.Items, c.Output.EntryIDs)
		}
	}
}

func TestPullGolden(t *testing.T) {
	var cases []struct {
		Since, Upto, Pending int64
		New                  []Entry   `json:"new"`
		Closed               []Closed  `json:"closed"`
		Conflicts            [][]int64 `json:"conflicts"`
		Topic                *string   `json:"topic"`
		MaxChars             int       `json:"max_chars"`
		Output               Changes   `json:"output"`
	}
	golden.Load(t, "pull", &cases)
	for _, c := range cases {
		topic := ""
		if c.Topic != nil {
			topic = *c.Topic
		}
		got := BuildChanges("syn", c.Since, c.Upto, c.Pending, c.New, c.Closed, pairs(c.Conflicts), topic, c.MaxChars)
		g, _ := json.Marshal(got)
		w, _ := json.Marshal(c.Output)
		if string(g) != string(w) {
			t.Errorf("since=%d:\n got %s\nwant %s", c.Since, g, w)
		}
	}
}

func TestParseTopics(t *testing.T) {
	got, err := ParseTopics([]any{"Data Quality", "db"})
	if err != nil || !slices.Equal(got.Keys(), []string{"data_quality", "db"}) {
		t.Errorf("list: %v %v", got.Keys(), err)
	}
	if _, err := ParseTopics([]any{1}); err == nil {
		t.Error("non-string name accepted")
	}
	if _, err := ParseTopics("x"); err == nil {
		t.Error("string accepted")
	}
}
