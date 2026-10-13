// Package eval runs the evaluations: context scenarios (a scripted session through the whole
// pipeline, checking what the handoff and pulls contain) and the classifier eval.
//
// Scenario files (evals/context/*.json) list steps per master, in stream order:
//   - {"text": ...}: an assistant message; with "stream": true it is written word by word at
//     StreamWPS, as a model streams it, and the tap publishes it while it is written;
//   - {"tool": {name, arguments, output}}: a tool exchange;
//   - {"share": {text, kind, topic?}}: the master calls share_context on the MCP server;
//   - {"pull": {topic?, since?: "last"}, "id": ...}: the master calls get_shared_context once
//     everything before it is processed ("last": since its previous pull's upto_seq).
//
// Shares and pulls run through a real MCP server (in-process), and the master's tap publishes
// the tool calls and outputs as a real master would.
//
// Checks run on the final handoff, or on a pull's output with "in": <pull id>:
//   - include: the pattern must appear;
//   - exclude: it must not appear;
//   - exclude_as_fact: it may appear only in an entry labelled hypothesis or open question.
package eval

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"wetink/internal/ojson"
)

type Step struct {
	Master string         `json:"master"`
	Text   string         `json:"text,omitempty"`
	Stream bool           `json:"stream,omitempty"`
	Tool   *ToolStep      `json:"tool,omitempty"`
	Share  map[string]any `json:"share,omitempty"`
	Pull   map[string]any `json:"pull,omitempty"`
	ID     string         `json:"id,omitempty"`
}

type ToolStep struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
	Output    string `json:"output"`
}

type Check struct {
	ID      string `json:"id"`
	In      string `json:"in,omitempty"`
	Expect  string `json:"expect"`
	Pattern string `json:"pattern"`
	Why     string `json:"why"`
}

type Scenario struct {
	Name   string        `json:"name"`
	Task   string        `json:"task"`
	Topics *ojson.Object `json:"topics"` // name -> description, in order
	Steps  []Step        `json:"steps"`
	Checks []Check       `json:"checks"`
	Path   string        `json:"-"`
}

func LoadScenario(path string) (*Scenario, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var s Scenario
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	s.Path = path
	return &s, nil
}

// LoadScenarios reads every *.json scenario in dir, keyed by name (unreadable ones skipped).
func LoadScenarios(dir string) map[string]*Scenario {
	out := map[string]*Scenario{}
	paths, _ := filepath.Glob(filepath.Join(dir, "*.json"))
	sort.Strings(paths)
	for _, p := range paths {
		if s, err := LoadScenario(p); err == nil {
			out[s.Name] = s
		}
	}
	return out
}

// Entries splits a handoff into its rendered entries ("- [label from master, ...] ..."), with
// continuation lines.
func Entries(text string) []string {
	pieces := strings.Split(text, "\n- [")
	var out []string
	for i, p := range pieces {
		if i > 0 {
			p = "- [" + p
		}
		if strings.HasPrefix(p, "- [") {
			out = append(out, p)
		}
	}
	return out
}

var labelRe = regexp.MustCompile(`^- \[([a-z ]+?) from `)

func label(entry string) string {
	if m := labelRe.FindStringSubmatch(entry); m != nil {
		return m[1]
	}
	return ""
}

var tentative = map[string]bool{"hypothesis": true, "open question": true}

// Run evaluates a check against a handoff or pull text: whether it passed and a short detail.
func (c Check) Run(text string) (bool, string, error) {
	re, err := regexp.Compile(c.Pattern)
	if err != nil {
		return false, "", fmt.Errorf("check %s: %w", c.ID, err)
	}
	anywhere := re.MatchString(text)
	switch c.Expect {
	case "include":
		if anywhere {
			return true, "present", nil
		}
		return false, "missing", nil
	case "exclude":
		if anywhere {
			return false, "present", nil
		}
		return true, "absent", nil
	case "exclude_as_fact":
		var hits, bad []string
		for _, e := range Entries(text) {
			if re.MatchString(e) {
				hits = append(hits, label(e))
				if !tentative[label(e)] {
					bad = append(bad, label(e))
				}
			}
		}
		if len(bad) > 0 {
			return false, "shown as " + strings.Join(bad, ", "), nil
		}
		if len(hits) > 0 {
			return true, "shown as " + strings.Join(hits, ", "), nil
		}
		return true, "absent", nil
	}
	return false, "", fmt.Errorf("unknown expectation %q", c.Expect)
}
