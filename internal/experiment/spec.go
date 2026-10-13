// Package experiment runs coding agents on related issues of one repository, each in its own
// checkout, with and without a shared board, and scores what they produce: each agent's
// hidden tests, whether their branches merge, and the tests of the merged result.
//
// An experiment is a directory with experiment.json (Spec) and the files it names. A run
// starts every agent from the same base commit, in a clone of a repository that holds only the
// history up to that commit, so no agent can find the answer, or another agent's branch, in git.
package experiment

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/template"
	"time"

	"wetink/internal/envelope"
)

// Spec is experiment.json. Paths are relative to its directory.
type Spec struct {
	Name string `json:"name"`
	// Repo is a git URL, a git repository, or a plain directory (committed as it is).
	Repo string `json:"repo"`
	Base string `json:"base"` // the commit every agent starts from (default HEAD)
	// Setup runs in each checkout before its agent starts, outside any sandbox (downloads,
	// warm builds).
	Setup []string `json:"setup"`
	// Env is set for setup, agents and tests; $CACHE in a value is the agent's own writable
	// cache directory (e.g. "GOCACHE": "$CACHE/go-build").
	Env    map[string]string `json:"env"`
	Task   string            `json:"task"`   // the session's task, for the board
	Topics json.RawMessage   `json:"topics"` // the session's topics, for the board
	Agents []AgentTask       `json:"agents"`
	// CombinedTests are hidden tests that need every agent's change (a file, or a directory
	// whose files are laid over the checkout), run with CombinedTest after the merge.
	CombinedTests string `json:"combined_tests"`
	// CombinedTest runs on the merged result with every hidden test in place (default: each
	// agent's test command, in turn).
	CombinedTest string `json:"combined_test"`
	// TestGlob matches the agents' own test files (e.g. "*_test.go", against the path or the
	// file name). Agents adding tests to the same file conflict for no reason that matters
	// here, so the combined tests run on a merge without them; the full merge is reported too.
	TestGlob string `json:"test_glob"`
	// CombinedGold is a reference for the merged result, for Check (default: each agent's gold).
	CombinedGold string               `json:"combined_gold"`
	Conditions   map[string]Condition `json:"conditions"` // default DefaultConditions
	Timeout      string               `json:"timeout"`    // per agent (default 30m)
	TestTimeout  string               `json:"test_timeout"`

	dir string
}

// AgentTask is one agent's issue and how its work is judged.
type AgentTask struct {
	Name  string `json:"name"`  // the master name, e.g. "master-a"
	Issue string `json:"issue"` // a Markdown file with the issue
	// Tests are hidden tests: a patch, or a directory whose files are laid over the checkout.
	// They replace any file of the same name the agent wrote.
	Tests string `json:"tests"`
	Test  string `json:"test"` // the command that must pass with them
	Gold  string `json:"gold"` // a reference fix (patch or directory), for Check

	issue string
}

// Condition is how agents are set up: with a board or not, and the prompt they get.
type Condition struct {
	Board bool `json:"board"`
	// Prompt is a text/template over PromptData.
	Prompt string `json:"prompt"`
}

type PromptData struct {
	Name   string
	Issue  string
	Others []Other
}

type Other struct{ Name, Issue string }

const isolatedPrompt = `You are working in a git repository (the current directory). Resolve this issue:

{{.Issue}}

Change the code, add or update tests as you see fit, and run the tests to check your work. When you are done, leave your changes in the working tree; you may commit them. Do not ask questions: nobody will answer them.`

const sharedPrompt = isolatedPrompt + `

Another engineer is working on a related issue at the same time, in their own copy of the repository. Afterwards both changes will be merged, so they must fit together.{{range .Others}}

Their issue ({{.Name}}):

{{.Issue}}{{end}}

You share a board with them through the shared-context tools. Call get_shared_context before you start, after each step of your work, and before you finish. Share what the other engineer needs as you go (share_context): decisions on names, interfaces, formats and files; what you found; and questions for them.`

// DefaultConditions: agents that know nothing of each other, and agents that know each
// other's issue and share a board.
var DefaultConditions = map[string]Condition{
	"isolated": {Board: false, Prompt: isolatedPrompt},
	"shared":   {Board: true, Prompt: sharedPrompt},
}

// Load reads dir/experiment.json and the issues it names.
func Load(dir string) (*Spec, error) {
	data, err := os.ReadFile(filepath.Join(dir, "experiment.json"))
	if err != nil {
		return nil, err
	}
	var s Spec
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("experiment.json: %w", err)
	}
	s.dir, err = filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	if err := envelope.CheckName(s.Name, "experiment name"); err != nil {
		return nil, err
	}
	if s.Repo == "" || len(s.Agents) == 0 {
		return nil, fmt.Errorf("experiment.json needs a repo and agents")
	}
	if s.Base == "" {
		s.Base = "HEAD"
	}
	if s.Conditions == nil {
		s.Conditions = DefaultConditions
	}
	for name, c := range s.Conditions {
		if err := envelope.CheckName(name, "condition"); err != nil {
			return nil, err
		}
		if _, err := template.New(name).Parse(c.Prompt); err != nil {
			return nil, fmt.Errorf("condition %s: %w", name, err)
		}
	}
	seen := map[string]bool{}
	for i := range s.Agents {
		a := &s.Agents[i]
		if err := envelope.CheckName(a.Name, "agent name"); err != nil {
			return nil, err
		}
		if seen[a.Name] {
			return nil, fmt.Errorf("agent %s appears twice", a.Name)
		}
		seen[a.Name] = true
		if a.Issue == "" || a.Test == "" {
			return nil, fmt.Errorf("agent %s needs an issue and a test command", a.Name)
		}
		issue, err := os.ReadFile(s.Path(a.Issue))
		if err != nil {
			return nil, err
		}
		a.issue = strings.TrimSpace(string(issue))
	}
	for _, d := range []string{s.Timeout, s.TestTimeout} {
		if d != "" {
			if _, err := time.ParseDuration(d); err != nil {
				return nil, err
			}
		}
	}
	return &s, nil
}

// Path resolves a path of the spec ("" stays "").
func (s *Spec) Path(p string) string {
	if p == "" || filepath.IsAbs(p) {
		return p
	}
	return filepath.Join(s.dir, p)
}

func (s *Spec) Dir() string { return s.dir }

func duration(s string, def time.Duration) time.Duration {
	if d, err := time.ParseDuration(s); err == nil && s != "" {
		return d
	}
	return def
}

func (s *Spec) AgentTimeout() time.Duration { return duration(s.Timeout, 30*time.Minute) }
func (s *Spec) testTimeout() time.Duration  { return duration(s.TestTimeout, 15*time.Minute) }

// Prompt is agent a's prompt in condition c.
func (s *Spec) Prompt(cond string, a AgentTask) (string, error) {
	c, ok := s.Conditions[cond]
	if !ok {
		return "", fmt.Errorf("no condition %q", cond)
	}
	data := PromptData{Name: a.Name, Issue: a.issue}
	for _, o := range s.Agents {
		if o.Name != a.Name {
			data.Others = append(data.Others, Other{Name: o.Name, Issue: o.issue})
		}
	}
	t, err := template.New(cond).Parse(c.Prompt)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	if err := t.Execute(&b, data); err != nil {
		return "", err
	}
	return b.String(), nil
}

func (s *Spec) isTestFile(path string) bool {
	full, _ := filepath.Match(s.TestGlob, path)
	base, _ := filepath.Match(s.TestGlob, filepath.Base(path))
	return full || base
}

// combinedTest is the command run on the merged result.
func (s *Spec) combinedTest() string {
	if s.CombinedTest != "" {
		return s.CombinedTest
	}
	var cmds []string
	for _, a := range s.Agents {
		cmds = append(cmds, "("+a.Test+")")
	}
	return strings.Join(cmds, " && ")
}

// env is the environment for setup, agents and tests in a checkout with the given cache dir.
func (s *Spec) env(cache string) []string {
	var out []string
	for k, v := range s.Env {
		out = append(out, k+"="+os.Expand(v, func(name string) string {
			if name == "CACHE" {
				return cache
			}
			return os.Getenv(name)
		}))
	}
	return out
}
