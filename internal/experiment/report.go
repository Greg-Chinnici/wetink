package experiment

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// Summary is a one-line account of a run.
func (res *Result) Summary() string {
	var parts []string
	for _, name := range slices.Sorted(maps.Keys(res.Agents)) {
		a := res.Agents[name]
		parts = append(parts, fmt.Sprintf("%s %s (%s, %d calls, %d shares)", name, passFail(a.Tests.Pass), a.Exit, a.Calls, a.Shares))
	}
	merged := "merge conflicts"
	switch {
	case res.Merge.Clean:
		merged = "merged " + passFail(res.Combined != nil && res.Combined.Pass)
	case res.Merge.SourceClean != nil && *res.Merge.SourceClean:
		merged = "conflicts only in their own tests; merged without them " + passFail(res.Combined != nil && res.Combined.Pass)
	}
	return strings.Join(parts, "; ") + "; " + merged
}

// LoadResults reads <dir>/*/result.json.
func LoadResults(dir string) ([]*Result, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*", "result.json"))
	if err != nil {
		return nil, err
	}
	var out []*Result
	for _, p := range paths {
		data, err := os.ReadFile(p)
		if err != nil {
			return nil, err
		}
		var res Result
		if err := json.Unmarshal(data, &res); err != nil {
			return nil, fmt.Errorf("%s: %w", p, err)
		}
		out = append(out, &res)
	}
	slices.SortFunc(out, func(a, b *Result) int { return a.Started.Compare(b.Started) })
	return out, nil
}

// Report compares conditions (per agent kind and model) over the runs given.
func Report(results []*Result, w io.Writer) {
	type key struct{ cond, agent, model string }
	groups := map[key][]*Result{}
	for _, r := range results {
		k := key{r.Condition, r.Agent, r.Model}
		groups[k] = append(groups[k], r)
	}
	keys := slices.SortedFunc(func(yield func(key) bool) {
		for k := range groups {
			if !yield(k) {
				return
			}
		}
	}, func(a, b key) int {
		return cmp.Or(cmp.Compare(a.agent, b.agent), cmp.Compare(a.model, b.model), cmp.Compare(a.cond, b.cond))
	})
	fmt.Fprintf(w, "%-10s %-24s %4s  %-9s %-7s %-8s %-8s %6s %6s %8s %7s %6s\n",
		"condition", "agent/model", "runs", "own tests", "merges", "src only", "combined", "min", "calls", "tokens", "shares", "pulls")
	for _, k := range keys {
		rs := groups[k]
		var own, ownN, clean, srcClean, srcN, combined int
		var minutes, calls, tokens, shares, pulls float64
		for _, r := range rs {
			if r.Merge.Clean {
				clean++
			}
			if r.Merge.SourceClean != nil {
				srcN++
				if *r.Merge.SourceClean {
					srcClean++
				}
			}
			if r.Combined != nil && r.Combined.Pass {
				combined++
			}
			var longest float64
			for _, a := range r.Agents {
				ownN++
				if a.Tests.Pass {
					own++
				}
				longest = max(longest, a.Seconds)
				calls += float64(a.Calls)
				tokens += float64(a.InputTokens + a.OutputTokens)
				shares += float64(a.Shares)
				pulls += float64(a.Pulls)
			}
			minutes += longest / 60
		}
		n := float64(len(rs))
		src := "-"
		if srcN > 0 {
			src = fmt.Sprintf("%d/%d", srcClean, srcN)
		}
		fmt.Fprintf(w, "%-10s %-24s %4d  %-9s %-7s %-8s %-8s %6.1f %6.0f %7.0fk %7.1f %6.1f\n",
			k.cond, k.agent+"/"+k.model, len(rs), fmt.Sprintf("%d/%d", own, ownN),
			fmt.Sprintf("%d/%d", clean, len(rs)), src, fmt.Sprintf("%d/%d", combined, len(rs)),
			minutes/n, calls/n, tokens/n/1000, shares/n, pulls/n)
	}
	fmt.Fprintln(w, "\nown tests: agents passing their hidden tests; merges: runs whose branches merged cleanly;")
	fmt.Fprintln(w, "src only: merged cleanly without the agents' own test files (test_glob); combined: the")
	fmt.Fprintln(w, "merged result passing every hidden test; min: the slowest agent; calls, tokens, shares and")
	fmt.Fprintln(w, "pulls: per run, all agents together.")
}

// Check validates an experiment without any model: on the base, each agent's hidden tests
// (and the combined ones) must fail; with its gold fix they must pass; and the gold fixes
// together (or CombinedGold) must pass every hidden test. It returns false if any of that does not hold.
func Check(ctx context.Context, s *Spec, out string, w io.Writer) (bool, error) {
	out, err := filepath.Abs(out)
	if err != nil {
		return false, err
	}
	work := filepath.Join(out, s.Name, "check")
	os.RemoveAll(work)
	defer os.RemoveAll(work)
	base, err := baseRepo(ctx, s, filepath.Join(out, s.Name, "base.git"))
	if err != nil {
		return false, err
	}
	fmt.Fprintf(w, "base %s\n", base[:min(12, len(base))])
	cache := filepath.Join(out, s.Name, "check.cache")
	env := append(os.Environ(), s.env(cache)...)
	r := &Runner{spec: s, cfg: Config{Out: out, Log: io.Discard}}
	ok := true
	try := func(label string, wantPass bool, fixes []string, tests []string, cmd string) error {
		dir := filepath.Join(work, strings.NewReplacer(" ", "-", "+", "").Replace(label))
		if err := checkout(ctx, filepath.Join(out, s.Name, "base.git"), dir); err != nil {
			return err
		}
		for _, setup := range s.Setup {
			if code, o, err := shell(ctx, dir, env, s.testTimeout(), setup); err != nil || code != 0 {
				return fmt.Errorf("setup %q failed (exit %d): %v\n%s", setup, code, err, o)
			}
		}
		for _, f := range append(fixes, tests...) {
			if f == "" {
				continue
			}
			if err := lay(ctx, s.Path(f), dir); err != nil {
				return fmt.Errorf("%s: %w", f, err)
			}
		}
		if _, err := commitAll(ctx, dir, label); err != nil {
			return err
		}
		t := r.test(ctx, dir, env, "merged", cmd, nil)
		good := t.Pass == wantPass
		ok = ok && good
		mark := "✓"
		if !good {
			mark = "✗"
		}
		fmt.Fprintf(w, "%s %-28s %s (want %s)\n", mark, label, passFail(t.Pass), passFail(wantPass))
		if !good {
			fmt.Fprintf(w, "%s\n", indent(t.Output))
		}
		return nil
	}
	var golds, tests []string
	for _, a := range s.Agents {
		tests = append(tests, a.Tests)
		if err := try(a.Name+" base", false, nil, []string{a.Tests}, a.Test); err != nil {
			return false, err
		}
		if a.Gold == "" {
			fmt.Fprintf(w, "- %-28s no gold fix\n", a.Name+" gold")
			continue
		}
		golds = append(golds, a.Gold)
		if err := try(a.Name+" gold", true, []string{a.Gold}, []string{a.Tests}, a.Test); err != nil {
			return false, err
		}
	}
	if s.CombinedGold != "" {
		golds = []string{s.CombinedGold}
	} else if len(golds) < len(s.Agents) {
		return ok, nil
	}
	tests = append(tests, s.CombinedTests)
	if s.CombinedTests != "" {
		if err := try("combined tests on base", false, nil, []string{s.CombinedTests}, s.combinedTest()); err != nil {
			return false, err
		}
	}
	if err := try("combined gold", true, golds, tests, s.combinedTest()); err != nil {
		return false, err
	}
	return ok, nil
}

func indent(s string) string {
	return "    " + strings.ReplaceAll(strings.TrimSpace(s), "\n", "\n    ")
}
