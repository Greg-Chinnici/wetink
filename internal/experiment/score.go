package experiment

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Usage is what an agent spent, from the proxy's log.
type Usage struct {
	Calls        int `json:"calls"`      // model calls of its main loop
	SideCalls    int `json:"side_calls"` // titles, summaries and the like
	InputTokens  int `json:"input_tokens"`
	CachedTokens int `json:"cached_tokens"` // of InputTokens, read from or written to the cache
	OutputTokens int `json:"output_tokens"`
	ToolCalls    int `json:"tool_calls"`
	Pulls        int `json:"pulls"`  // get_shared_context calls
	Shares       int `json:"shares"` // share_context calls
	Errors       int `json:"errors"` // failed model calls
}

func readUsage(path string) (Usage, error) {
	var u Usage
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return u, nil
	}
	if err != nil {
		return u, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1024*1024), 64*1024*1024)
	for sc.Scan() {
		var ex struct {
			Status int              `json:"status"`
			Side   bool             `json:"side"`
			Output []map[string]any `json:"output"`
			Usage  map[string]any   `json:"usage"`
		}
		if json.Unmarshal(sc.Bytes(), &ex) != nil {
			continue
		}
		if ex.Status != 200 {
			u.Errors++
		}
		n := func(k string) int { f, _ := ex.Usage[k].(float64); return int(f) }
		// Messages counts cached input apart from input_tokens; Responses counts it within.
		cached := n("cache_read_input_tokens") + n("cache_creation_input_tokens")
		u.InputTokens += n("input_tokens") + cached
		if d, ok := ex.Usage["input_tokens_details"].(map[string]any); ok {
			f, _ := d["cached_tokens"].(float64)
			cached += int(f)
		}
		u.CachedTokens += cached
		u.OutputTokens += n("output_tokens")
		if ex.Side {
			u.SideCalls++
			continue
		}
		u.Calls++
		for _, it := range ex.Output {
			if it["type"] != "function_call" {
				continue
			}
			u.ToolCalls++
			name, _ := it["name"].(string)
			switch {
			case strings.HasSuffix(name, "get_shared_context"):
				u.Pulls++
			case strings.HasSuffix(name, "share_context"):
				u.Shares++
			}
		}
	}
	return u, sc.Err()
}

// score judges a run in a scratch clone: each agent's branch with its hidden tests, then the
// branches merged, with every hidden test.
func (r *Runner) score(ctx context.Context, runDir, base string, res *Result) error {
	dir := filepath.Join(runDir, "score")
	if err := checkout(ctx, r.dir("base.git"), dir); err != nil {
		return err
	}
	cache := filepath.Join(runDir, "score.cache")
	env := append(os.Environ(), r.spec.env(cache)...)
	for _, s := range r.spec.Setup {
		if code, out, err := shell(ctx, dir, env, r.spec.testTimeout(), s); err != nil || code != 0 {
			return fmt.Errorf("setup %q failed (exit %d): %v\n%s", s, code, err, out)
		}
	}
	// The hidden tests, each on its own branch from the base.
	hidden := map[string][]string{} // branch -> the files it holds
	for _, a := range r.spec.Agents {
		files, err := hiddenBranch(ctx, dir, "tests-"+a.Name, r.spec.Path(a.Tests))
		if err != nil {
			return fmt.Errorf("%s's hidden tests: %w", a.Name, err)
		}
		hidden["tests-"+a.Name] = files
		if _, err := git(ctx, dir, "fetch", "-q", filepath.Join(runDir, a.Name), "HEAD:refs/heads/"+a.Name); err != nil {
			return err
		}
	}
	combined, err := hiddenBranch(ctx, dir, "tests-combined", r.spec.Path(r.spec.CombinedTests))
	if err != nil {
		return fmt.Errorf("combined hidden tests: %w", err)
	}
	hidden["tests-combined"] = combined

	for _, a := range r.spec.Agents {
		ar := res.Agents[a.Name]
		ar.Tests = r.test(ctx, dir, env, a.Name, a.Test, map[string][]string{"tests-" + a.Name: hidden["tests-"+a.Name]})
		r.logf("  %s: hidden tests %s", a.Name, passFail(ar.Tests.Pass))
	}

	// Merge every branch into the first; with a test glob, also without the agents' own tests,
	// which is what the combined tests then run on.
	var branches, sources []string
	for _, a := range r.spec.Agents {
		branches = append(branches, a.Name)
		if r.spec.TestGlob != "" {
			src, err := r.withoutOwnTests(ctx, dir, a.Name)
			if err != nil {
				return err
			}
			sources = append(sources, src)
		}
	}
	res.Merge.Clean, res.Merge.Conflicts, res.Merge.Error = merge(ctx, dir, branches)
	clean := res.Merge.Clean
	if sources != nil {
		var conflicts []string
		clean, conflicts, _ = merge(ctx, dir, sources)
		res.Merge.SourceClean, res.Merge.SourceConflicts = &clean, conflicts
	}
	r.logf("  merge: %s", res.Merge)
	if !clean {
		return nil
	}
	t := r.test(ctx, dir, env, "merged", r.spec.combinedTest(), hidden)
	res.Combined = &t
	r.logf("  merged: tests %s", passFail(t.Pass))
	return nil
}

// merge merges every branch into the first, on branch "merged"; it returns whether that was
// clean, and the conflicted files if not.
func merge(ctx context.Context, dir string, branches []string) (bool, []string, string) {
	if _, err := git(ctx, dir, "checkout", "-q", "-f", "-B", "merged", branches[0]); err != nil {
		return false, nil, err.Error()
	}
	for _, b := range branches[1:] {
		if _, err := git(ctx, dir, "merge", "-q", "--no-edit", b); err != nil {
			var conflicts []string
			if out, _ := git(ctx, dir, "diff", "--name-only", "--diff-filter=U"); out != "" {
				conflicts = strings.Split(out, "\n")
			}
			_, _ = git(ctx, dir, "merge", "--abort")
			return false, conflicts, err.Error()
		}
	}
	return true, nil, ""
}

// withoutOwnTests makes branch src-<agent>: the agent's branch with its changes to files
// matching the spec's test glob undone.
func (r *Runner) withoutOwnTests(ctx context.Context, dir, agent string) (string, error) {
	src := "src-" + agent
	if _, err := git(ctx, dir, "checkout", "-q", "-f", "-B", src, agent); err != nil {
		return "", err
	}
	out, err := git(ctx, dir, "diff", "--name-only", "base", src)
	if err != nil {
		return "", err
	}
	for _, f := range strings.Split(out, "\n") {
		if f == "" || !r.spec.isTestFile(f) {
			continue
		}
		undo := []string{"rm", "-q", "--", f} // a file the agent added
		if _, inBase := git(ctx, dir, "cat-file", "-e", "base:"+f); inBase == nil {
			undo = []string{"checkout", "-q", "base", "--", f}
		}
		if _, err := git(ctx, dir, undo...); err != nil {
			return "", err
		}
	}
	_, err = commitAll(ctx, dir, "without "+agent+"'s own tests")
	return src, err
}

func (m Merge) String() string {
	s := "clean"
	if !m.Clean {
		s = fmt.Sprintf("conflicts in %v", m.Conflicts)
	}
	if m.SourceClean != nil {
		if *m.SourceClean {
			s += "; without the agents' own tests: clean"
		} else {
			s += fmt.Sprintf("; without the agents' own tests: conflicts in %v", m.SourceConflicts)
		}
	}
	return s
}

func passFail(ok bool) string {
	if ok {
		return "pass"
	}
	return "FAIL"
}

// hiddenBranch commits src (a patch or a directory; "" for none) on a new branch from the
// base, and returns the files it touches.
func hiddenBranch(ctx context.Context, dir, branch, src string) ([]string, error) {
	if _, err := git(ctx, dir, "checkout", "-q", "-f", "-B", branch, "base"); err != nil {
		return nil, err
	}
	if src == "" {
		return nil, nil
	}
	if err := lay(ctx, src, dir); err != nil {
		return nil, err
	}
	if _, err := commitAll(ctx, dir, "hidden tests"); err != nil {
		return nil, err
	}
	return filesOf(ctx, dir, branch), nil
}

// filesOf are the files a branch changes from the base (that still exist on it).
func filesOf(ctx context.Context, dir, branch string) []string {
	out, err := git(ctx, dir, "diff", "--name-only", "--diff-filter=d", "base", branch)
	if err != nil || out == "" {
		return nil
	}
	return strings.Split(out, "\n")
}

// test checks out branch (the current one if it is "merged"), lays the files of the given
// hidden-test branches over it, and runs cmd.
func (r *Runner) test(ctx context.Context, dir string, env []string, branch, cmd string, overlays map[string][]string) TestResult {
	var t TestResult
	if branch != "merged" {
		if _, err := git(ctx, dir, "checkout", "-q", "-f", branch); err != nil {
			t.Error = err.Error()
			return t
		}
	}
	_, _ = git(ctx, dir, "clean", "-qfdx") // build output of an earlier test
	for from, files := range overlays {
		if len(files) == 0 {
			continue
		}
		if _, err := git(ctx, dir, append([]string{"checkout", from, "--"}, files...)...); err != nil {
			t.Error = err.Error()
			return t
		}
	}
	start := time.Now()
	code, out, err := shell(ctx, dir, env, r.spec.testTimeout(), cmd)
	t.Seconds = time.Since(start).Seconds()
	t.Exit, t.Output, t.Pass = code, out, err == nil && code == 0
	if err != nil {
		t.Error = err.Error()
	}
	// Leave the branch as it was (the overlay is staged by checkout).
	_, _ = git(ctx, dir, "reset", "-q", "--hard")
	return t
}
