package experiment

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"wetink/internal/classifier"
	"wetink/internal/consumer"
	"wetink/internal/envelope"
	"wetink/internal/handoff"
	"wetink/internal/mcpserver"
	"wetink/internal/oai"
	"wetink/internal/ojson"
	"wetink/internal/proxy"
	"wetink/internal/tap"
)

// Config is what a series of runs shares.
type Config struct {
	Out         string // runs go under Out/<experiment>/<run>
	Agent       Agent
	Model       string
	NatsURL     string
	DatabaseURL string
	Classifier  classifier.Classifier // classifies the board
	// Provider is where Responses agents' calls go (with its key); Claude Code's go to the
	// Anthropic API with its own credentials.
	Provider *oai.Client
	Log      io.Writer // progress
}

// Runner runs an experiment: its own proxy and MCP server for every run, and a consumer
// for each run with a board.
type Runner struct {
	cfg  Config
	spec *Spec
	db   *pgxpool.Pool

	proxy    *proxy.Proxy
	proxyURL string
	mcpURL   string
	stop     []func()

	mu     sync.Mutex
	boards map[string]bool // sessions with a board, which the proxy taps
}

func NewRunner(ctx context.Context, cfg Config, spec *Spec, db *pgxpool.Pool) (*Runner, error) {
	out, err := filepath.Abs(cfg.Out)
	if err != nil {
		return nil, err
	}
	cfg.Out = out
	r := &Runner{cfg: cfg, spec: spec, db: db, boards: map[string]bool{}}
	if cfg.Log == nil {
		r.cfg.Log = io.Discard
	}
	r.proxy = proxy.New(cfg.Provider, proxy.Options{NATSURL: cfg.NatsURL, LogDir: r.dir("proxy"),
		Tapped: func(session string) bool { r.mu.Lock(); defer r.mu.Unlock(); return r.boards[session] }})
	url, stop, err := serveLocal(r.proxy.Handler())
	if err != nil {
		return nil, err
	}
	r.proxyURL = url
	r.stop = append(r.stop, stop, func() { r.proxy.Close(10 * time.Second) })
	mcp := mcpserver.New(db, cfg.NatsURL, "")
	url, stop, err = serveLocal(mcp.Handler())
	if err != nil {
		r.Close()
		return nil, err
	}
	r.mcpURL = url + "/mcp"
	r.stop = append(r.stop, stop, mcp.Close)
	return r, nil
}

// NewScorer returns a runner that can only score runs already made (Rescore).
func NewScorer(spec *Spec, out string, log io.Writer) (*Runner, error) {
	abs, err := filepath.Abs(out)
	if err != nil {
		return nil, err
	}
	if log == nil {
		log = io.Discard
	}
	return &Runner{cfg: Config{Out: abs, Log: log}, spec: spec}, nil
}

// Rescore judges a finished run again (after the scoring or the hidden tests changed) and
// rewrites its result.json.
func (r *Runner) Rescore(ctx context.Context, run string) (*Result, error) {
	runDir := r.dir(run)
	var res Result
	data, err := os.ReadFile(filepath.Join(runDir, "result.json"))
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(data, &res); err != nil {
		return nil, err
	}
	if err := os.RemoveAll(filepath.Join(runDir, "score")); err != nil {
		return nil, err
	}
	res.Merge, res.Combined = Merge{}, nil
	r.logf("%s: rescoring", run)
	if err := r.score(ctx, runDir, res.Base, &res); err != nil {
		return nil, err
	}
	r.logf("%s: %s", run, res.Summary())
	return &res, writeJSON(filepath.Join(runDir, "result.json"), &res)
}

func (r *Runner) Close() {
	for i := len(r.stop) - 1; i >= 0; i-- {
		r.stop[i]()
	}
	r.stop = nil
}

func serveLocal(h http.Handler) (string, func(), error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", nil, err
	}
	srv := &http.Server{Handler: h}
	go func() { _ = srv.Serve(ln) }()
	return "http://" + ln.Addr().String(), func() { srv.Close() }, nil
}

func (r *Runner) dir(parts ...string) string {
	return filepath.Join(append([]string{r.cfg.Out, r.spec.Name}, parts...)...)
}

func (r *Runner) logf(format string, args ...any) {
	fmt.Fprintf(r.cfg.Log, "%s  %s\n", time.Now().Format("15:04:05"), fmt.Sprintf(format, args...))
}

// Result is one run, as written to <run>/result.json.
type Result struct {
	Run        string                  `json:"run"`
	Experiment string                  `json:"experiment"`
	Condition  string                  `json:"condition"`
	Board      bool                    `json:"board"`
	Agent      string                  `json:"agent"`
	Model      string                  `json:"model"`
	Base       string                  `json:"base"`
	Started    time.Time               `json:"started"`
	Seconds    float64                 `json:"seconds"`
	Agents     map[string]*AgentResult `json:"agents"`
	Merge      Merge                   `json:"merge"`
	Combined   *TestResult             `json:"combined,omitempty"` // nil if the merge it runs on failed

	runDir string // set by tests
}

type AgentResult struct {
	Session string  `json:"session"`
	Seconds float64 `json:"seconds"`
	Exit    string  `json:"exit"` // "ok", "timeout" or the error
	Commit  string  `json:"commit"`
	Diff    string  `json:"diff"` // git diff --shortstat against the base
	Usage
	Tests TestResult `json:"tests"`
}

type TestResult struct {
	Pass    bool    `json:"pass"`
	Exit    int     `json:"exit"`
	Seconds float64 `json:"seconds"`
	Output  string  `json:"output"` // its tail
	Error   string  `json:"error,omitempty"`
}

type Merge struct {
	Clean     bool     `json:"clean"`
	Conflicts []string `json:"conflicts,omitempty"`
	Error     string   `json:"error,omitempty"`
	// The merge without the agents' changes to their own test files (Spec.TestGlob), which
	// the combined tests run on; nil without a test glob.
	SourceClean     *bool    `json:"source_clean,omitempty"`
	SourceConflicts []string `json:"source_conflicts,omitempty"`
}

// Run runs every agent once in condition cond and scores the result.
func (r *Runner) Run(ctx context.Context, cond string, n int) (*Result, error) {
	c, ok := r.spec.Conditions[cond]
	if !ok {
		return nil, fmt.Errorf("no condition %q", cond)
	}
	base, err := baseRepo(ctx, r.spec, r.dir("base.git"))
	if err != nil {
		return nil, fmt.Errorf("base repository: %w", err)
	}
	runID := fmt.Sprintf("%s-%s-%d-%s", r.spec.Name, cond, n, tap.NewID("r")[2:8])
	runDir := r.dir(runID)
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		return nil, err
	}
	res := &Result{Run: runID, Experiment: r.spec.Name, Condition: cond, Board: c.Board, Agent: r.cfg.Agent.Name(),
		Model: r.cfg.Model, Base: base, Started: time.Now(), Agents: map[string]*AgentResult{}}
	r.logf("%s: %d agents, %s, board %v", runID, len(r.spec.Agents), r.cfg.Agent.Name(), c.Board)

	// One session for agents that share a board; a session each (logged, not tapped) otherwise.
	sessions := map[string]string{}
	for _, a := range r.spec.Agents {
		sessions[a.Name] = runID
		if !c.Board {
			sessions[a.Name] = runID + "-" + a.Name
		}
	}
	if c.Board {
		stop, err := r.startBoard(ctx, runID)
		if err != nil {
			return nil, err
		}
		defer stop()
	}

	var wg sync.WaitGroup
	errs := make([]error, len(r.spec.Agents))
	for i, a := range r.spec.Agents {
		ar := &AgentResult{Session: sessions[a.Name]}
		res.Agents[a.Name] = ar
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = r.runAgent(ctx, cond, c, runDir, base, a, ar)
		}()
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	r.logf("%s: agents done; scoring", runID)
	if err := r.score(ctx, runDir, base, res); err != nil {
		return nil, fmt.Errorf("scoring: %w", err)
	}
	res.Seconds = time.Since(res.Started).Seconds()
	if err := writeJSON(filepath.Join(runDir, "result.json"), res); err != nil {
		return nil, err
	}
	r.logf("%s: %s", runID, res.Summary())
	return res, nil
}

// startBoard declares the session and starts a consumer for it.
func (r *Runner) startBoard(ctx context.Context, session string) (func(), error) {
	topics := ojson.New()
	if len(r.spec.Topics) > 0 {
		raw, err := ojson.Decode(r.spec.Topics)
		if err != nil {
			return nil, err
		}
		if topics, err = handoff.ParseTopics(raw); err != nil {
			return nil, err
		}
	}
	var task *string
	if r.spec.Task != "" {
		task = &r.spec.Task
	}
	if err := handoff.PutSession(ctx, r.db, session, task, topics); err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.boards[session] = true
	r.mu.Unlock()
	durable := "exp-" + session
	cctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	done := make(chan error, 1)
	go func() {
		done <- consumer.Run(cctx, consumer.Config{NatsURL: r.cfg.NatsURL, DatabaseURL: r.cfg.DatabaseURL,
			Durable: durable, Subject: envelope.Subject(session)}, r.cfg.Classifier)
	}()
	return func() {
		settle(r.db, session, 2*time.Minute) // let the board catch up before it is read later
		cancel()
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			slog.Warn("board consumer", "session", session, "err", err)
		}
		if nc, err := nats.Connect(r.cfg.NatsURL); err == nil {
			if js, err := jetstream.New(nc); err == nil {
				_ = js.DeleteConsumer(context.Background(), envelope.StreamName, durable)
			}
			nc.Close()
		}
	}, nil
}

// settle waits until every stored event of the session has been processed, or timeout.
func settle(db *pgxpool.Pool, session string, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	time.Sleep(3 * time.Second) // the last items may still be on their way through NATS
	for {
		var pending int
		err := db.QueryRow(ctx, "SELECT count(*) FROM entries WHERE session = $1 AND status = 'pending'", session).Scan(&pending)
		if err != nil || pending == 0 {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func (r *Runner) runAgent(ctx context.Context, cond string, c Condition, runDir, base string, a AgentTask, ar *AgentResult) error {
	dir := filepath.Join(runDir, a.Name)
	home, cache := filepath.Join(runDir, a.Name+".home"), filepath.Join(runDir, a.Name+".cache")
	for _, d := range []string{home, cache} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return err
		}
	}
	if err := checkout(ctx, r.dir("base.git"), dir); err != nil {
		return err
	}
	env := append(os.Environ(), r.spec.env(cache)...)
	for _, s := range r.spec.Setup {
		code, out, err := shell(ctx, dir, env, r.spec.testTimeout(), s)
		if err != nil || code != 0 {
			return fmt.Errorf("%s setup %q failed (exit %d): %v\n%s", a.Name, s, code, err, out)
		}
	}
	prompt, err := r.spec.Prompt(cond, a)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(runDir, a.Name+".prompt.md"), []byte(prompt), 0o644); err != nil {
		return err
	}
	stdout, err := os.Create(filepath.Join(runDir, a.Name+".out.jsonl"))
	if err != nil {
		return err
	}
	defer stdout.Close()
	stderr, err := os.Create(filepath.Join(runDir, a.Name+".err.log"))
	if err != nil {
		return err
	}
	defer stderr.Close()
	run := AgentRun{Dir: dir, Home: home, Cache: cache, Prompt: prompt, Model: r.cfg.Model,
		ProxyURL: fmt.Sprintf("%s/s/%s/%s", r.proxyURL, ar.Session, a.Name),
		Env:      r.spec.env(cache), Stdout: stdout, Stderr: stderr}
	if c.Board {
		run.MCPURL = mcpURL(r.mcpURL, ar.Session, a.Name)
	}
	r.logf("  %s started (session %s)", a.Name, ar.Session)
	actx, cancel := context.WithTimeout(ctx, r.spec.AgentTimeout())
	defer cancel()
	start := time.Now()
	err = r.cfg.Agent.Run(actx, run)
	ar.Seconds = time.Since(start).Seconds()
	switch {
	case actx.Err() == context.DeadlineExceeded:
		ar.Exit = "timeout"
	case err != nil:
		ar.Exit = err.Error()
	default:
		ar.Exit = "ok"
	}
	r.logf("  %s finished in %.0fs: %s", a.Name, ar.Seconds, ar.Exit)
	if ctx.Err() != nil {
		return nil
	}
	if _, err := commitAll(ctx, dir, "work of "+a.Name); err != nil {
		return err
	}
	if ar.Commit, err = git(ctx, dir, "rev-parse", "HEAD"); err != nil {
		return err
	}
	ar.Diff, _ = git(ctx, dir, "diff", "--shortstat", "base", "HEAD")
	ar.Usage, err = readUsage(filepath.Join(r.dir("proxy"), ar.Session, a.Name+".jsonl"))
	return err
}
