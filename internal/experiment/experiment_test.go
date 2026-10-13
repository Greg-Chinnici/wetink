package experiment

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"wetink/internal/oai"
)

// goldAgent "solves" its issue by laying a fix from the toy experiment over its checkout.
type goldAgent struct{ fixes map[string]string } // master -> fix directory ("" for none)

func (goldAgent) Name() string { return "gold" }

func (g goldAgent) Run(ctx context.Context, r AgentRun) error {
	master := filepath.Base(r.Dir)
	if !strings.Contains(r.ProxyURL, "/s/") || r.MCPURL != "" {
		return nil
	}
	if fix := g.fixes[master]; fix != "" {
		return copyDir(fix, r.Dir)
	}
	return nil
}

func toy(t *testing.T) *Spec {
	t.Helper()
	s, err := Load("../../experiments/toy")
	if err != nil {
		t.Fatal(err)
	}
	s.Conditions = map[string]Condition{"isolated": {Prompt: isolatedPrompt}}
	return s
}

func runToy(t *testing.T, fixes map[string]string) *Result {
	t.Helper()
	s := toy(t)
	for k, v := range fixes {
		if v != "" {
			fixes[k] = s.Path(v)
		}
	}
	out := t.TempDir()
	r, err := NewRunner(context.Background(), Config{Out: out, Agent: goldAgent{fixes}, Model: "none",
		Provider: oai.New("http://127.0.0.1:1/v1", "", 0)}, s, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	res, err := r.Run(context.Background(), "isolated", 1)
	if err != nil {
		t.Fatal(err)
	}
	res.runDir = filepath.Join(out, "toy", res.Run)
	if _, err := os.Stat(filepath.Join(res.runDir, "result.json")); err != nil {
		t.Fatal(err)
	}
	return res
}

func TestFixesMadeApartMergeButFailTheCombinedTests(t *testing.T) {
	// The reference fixes, each made without the other: both pass their own tests, git merges
	// them, and the merged store loses expiry on Save/Load.
	res := runToy(t, map[string]string{"master-a": "gold/ttl", "master-b": "gold/save"})
	for name, a := range res.Agents {
		if a.Exit != "ok" || !a.Tests.Pass || a.Diff == "" {
			t.Fatalf("%s: exit %s, tests %v, diff %q\n%s", name, a.Exit, a.Tests.Pass, a.Diff, a.Tests.Output)
		}
	}
	if !res.Merge.Clean || res.Combined == nil || res.Combined.Pass || !strings.Contains(res.Combined.Output, "lost its expiry") {
		t.Fatalf("merge %+v, combined %+v", res.Merge, res.Combined)
	}
}

func TestOwnTestFilesDoNotBlockTheCombinedTests(t *testing.T) {
	// Both agents add a test to kv_test.go (a conflict) and one of them makes the full fix.
	base, err := os.ReadFile("../../experiments/toy/repo/kv_test.go")
	if err != nil {
		t.Fatal(err)
	}
	fixes := map[string]string{}
	for _, m := range []string{"master-a", "master-b"} {
		dir := t.TempDir()
		test := string(base) + "\nfunc TestBy" + strings.ReplaceAll(m, "-", "") + "(t *testing.T) {}\n"
		if err := os.WriteFile(filepath.Join(dir, "kv_test.go"), []byte(test), 0o644); err != nil {
			t.Fatal(err)
		}
		fixes[m] = dir
	}
	if err := copyDir("../../experiments/toy/gold/combined", fixes["master-a"]); err != nil {
		t.Fatal(err)
	}
	res := runToy(t, fixes)
	m := res.Merge
	if m.Clean || len(m.Conflicts) != 1 || m.Conflicts[0] != "kv_test.go" || m.SourceClean == nil || !*m.SourceClean {
		t.Fatalf("merge %s", m)
	}
	if res.Combined == nil || !res.Combined.Pass || !strings.Contains(res.Summary(), "merged without them pass") {
		t.Fatalf("combined %+v; %s", res.Combined, res.Summary())
	}

	// Rescoring gives the same result.
	s := toy(t)
	sc, err := NewScorer(s, filepath.Dir(filepath.Dir(res.runDir)), nil)
	if err != nil {
		t.Fatal(err)
	}
	again, err := sc.Rescore(context.Background(), res.Run)
	if err != nil {
		t.Fatal(err)
	}
	if again.Merge.String() != m.String() || again.Combined == nil || !again.Combined.Pass {
		t.Fatalf("rescored: %s", again.Summary())
	}
}

func TestConflictingEditsAreReported(t *testing.T) {
	base, err := os.ReadFile("../../experiments/toy/repo/kv.go")
	if err != nil {
		t.Fatal(err)
	}
	fixes := map[string]string{}
	for _, m := range []string{"master-a", "master-b"} {
		dir := t.TempDir()
		edited := strings.Replace(string(base), "small in-memory", "small in-memory ("+m+")", 1)
		if err := os.WriteFile(filepath.Join(dir, "kv.go"), []byte(edited), 0o644); err != nil {
			t.Fatal(err)
		}
		fixes[m] = dir
	}
	res := runToy(t, fixes)
	if res.Merge.Clean || len(res.Merge.Conflicts) != 1 || res.Merge.Conflicts[0] != "kv.go" || res.Combined != nil {
		t.Fatalf("merge %+v, combined %+v", res.Merge, res.Combined)
	}
	if !strings.Contains(res.Summary(), "merge conflicts") {
		t.Fatal(res.Summary())
	}
}

func TestOneFixForBothPassesTheCombinedTests(t *testing.T) {
	res := runToy(t, map[string]string{"master-a": "gold/combined", "master-b": ""})
	if !res.Agents["master-a"].Tests.Pass || res.Agents["master-b"].Tests.Pass {
		t.Fatalf("own tests: a %v, b %v", res.Agents["master-a"].Tests.Pass, res.Agents["master-b"].Tests.Pass)
	}
	if !res.Merge.Clean || res.Combined == nil || !res.Combined.Pass {
		t.Fatalf("merge %+v, combined %+v", res.Merge, res.Combined)
	}
	if !strings.Contains(res.Summary(), "merged pass") {
		t.Fatal(res.Summary())
	}
}

func TestPromptsNameTheOtherIssueOnlyWithABoard(t *testing.T) {
	s, err := Load("../../experiments/toy")
	if err != nil {
		t.Fatal(err)
	}
	iso, _ := s.Prompt("isolated", s.Agents[0])
	shared, _ := s.Prompt("shared", s.Agents[0])
	if strings.Contains(iso, "Save and load") || !strings.Contains(iso, "Entries that expire") {
		t.Fatalf("isolated prompt:\n%s", iso)
	}
	if !strings.Contains(shared, "Save and load") || !strings.Contains(shared, "get_shared_context") {
		t.Fatalf("shared prompt:\n%s", shared)
	}
}
