// Command wetink runs the shared-context services and tools.
//
//	wetink consumer          NATS -> events -> entries (classify, relate)
//	wetink api               sessions, handoffs and pulls over HTTP (:8090)
//	wetink mcp               MCP server with get_shared_context and share_context (:8091/mcp)
//	wetink dashboard         live view (:8095)
//	wetink master PROMPT     a demo master (tool loop through a tap)
//	wetink mockgw            mock model endpoints (:8080)
//	wetink eval [SCENARIO]   play a context scenario through the pipeline and check the handoff
//	wetink eval-classifier   score a classifier against labelled items
//	wetink eval-relations    score relation judgments against labelled cases
//	wetink seed [SCENARIOS]  play scenarios into sessions named after them (--reset first)
//	wetink stats             publish -> classified latency
//	wetink bench             throughput and latency under load (mock or real classifier)
//
// Defaults come from the environment the Nix dev shell sets (NATS_URL, DATABASE_URL,
// WETINK_TOKEN, WETINK_CLASSIFIER_*, WETINK_TOPIC_NAMER_*, OPENAI_BASE_URL, OPENAI_API_KEY).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"wetink/internal/bench"
	"wetink/internal/classifier"
	"wetink/internal/consumer"
	"wetink/internal/dashboard"
	"wetink/internal/eval"
	"wetink/internal/experiment"
	"wetink/internal/handoff"
	"wetink/internal/master"
	"wetink/internal/mcpserver"
	"wetink/internal/mockgw"
	"wetink/internal/oai"
	"wetink/internal/proxy"
	"wetink/internal/store"
	"wetink/internal/tap"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

type command struct {
	summary string
	run     func(ctx context.Context, args []string) error
}

var commands = map[string]command{
	"consumer":        {"store and classify items from NATS", runConsumer},
	"api":             {"serve sessions, handoffs and pulls over HTTP", runAPI},
	"mcp":             {"serve the shared-context MCP tools", runMCP},
	"dashboard":       {"serve the live dashboard", runDashboard},
	"master":          {"run one demo master", runMaster},
	"mockgw":          {"serve the mock model endpoints", runMockGW},
	"proxy":           {"serve a model endpoint that taps any agent calling it", runProxy},
	"eval":            {"play a context scenario and check the handoff", runEval},
	"eval-classifier": {"score a classifier against labelled items", runEvalClassifier},
	"eval-relations":  {"score relation judgments against labelled cases", runEvalRelations},
	"experiment":      {"run coding agents on related issues, with and without a board", runExperiment},
	"seed":            {"play scenarios into sessions named after them", runSeed},
	"stats":           {"publish -> classified latency", runStats},
	"bench":           {"measure throughput and latency under load", runBench},
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: wetink <command> [flags]\n\ncommands:")
	var names []string
	for n := range commands {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		fmt.Fprintf(os.Stderr, "  %-16s %s\n", n, commands[n].summary)
	}
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd, ok := commands[os.Args[1]]
	if !ok {
		usage()
		os.Exit(2)
	}
	level := slog.LevelInfo
	if os.Args[1] == "eval" || os.Args[1] == "seed" || os.Args[1] == "eval-classifier" || os.Args[1] == "bench" {
		level = slog.LevelWarn // their output is the report
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := cmd.run(ctx, os.Args[2:]); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, "wetink "+os.Args[1]+":", err)
		os.Exit(1)
	}
}

// parse parses flags that may come before or after positional arguments.
func parse(fs *flag.FlagSet, args []string) []string {
	var positional []string
	for len(args) > 0 {
		if err := fs.Parse(args); err != nil {
			os.Exit(2)
		}
		args = fs.Args()
		if len(args) > 0 {
			positional = append(positional, args[0])
			args = args[1:]
		}
	}
	return positional
}

type classifierFlags struct{ api, model, baseURL *string }

func addClassifierFlags(fs *flag.FlagSet, defaultAPI, defaultModel string) classifierFlags {
	return classifierFlags{
		api:     fs.String("classifier-api", env("WETINK_CLASSIFIER_API", defaultAPI), "responses, chat or systemone"),
		model:   fs.String("classifier-model", env("WETINK_CLASSIFIER_MODEL", defaultModel), "model name"),
		baseURL: fs.String("classifier-base-url", env("WETINK_CLASSIFIER_BASE_URL", ""), "classifier endpoint"),
	}
}

func (c classifierFlags) build(timeout time.Duration) (classifier.Classifier, error) {
	cl, err := classifier.BuildFromEnv(*c.api, *c.model, *c.baseURL, timeout)
	if err != nil {
		return nil, err
	}
	return classifier.WithRelateFromEnv(cl, timeout)
}

func databaseFlag(fs *flag.FlagSet) *string {
	return fs.String("database-url", env("DATABASE_URL", ""), "Postgres URL")
}

func natsFlag(fs *flag.FlagSet) *string {
	return fs.String("nats-url", env("NATS_URL", "nats://localhost:4222"), "NATS URL")
}

func openDB(ctx context.Context, url string) (*pgxpool.Pool, error) {
	if url == "" {
		return nil, errors.New("--database-url or DATABASE_URL is required")
	}
	return store.Open(ctx, url)
}

func serve(ctx context.Context, addr string, h http.Handler) error {
	srv := &http.Server{Addr: addr, Handler: h}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	slog.Info("listening", "url", "http://"+ln.Addr().String())
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
	}()
	if err := srv.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func runConsumer(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("consumer", flag.ExitOnError)
	natsURL, dbURL := natsFlag(fs), databaseFlag(fs)
	durable := fs.String("durable", "store", "durable consumer name")
	maxSessions := fs.Int("max-sessions", consumer.DefaultMaxSessions, "sessions processed at once")
	ahead := fs.Int("ahead", consumer.DefaultAhead, "entries classified ahead within a session")
	cf := addClassifierFlags(fs, "responses", "mock")
	parse(fs, args)
	if *dbURL == "" {
		return errors.New("--database-url or DATABASE_URL is required")
	}
	c, err := cf.build(60 * time.Second)
	if err != nil {
		return err
	}
	return consumer.Run(ctx, consumer.Config{NatsURL: *natsURL, DatabaseURL: *dbURL, Durable: *durable,
		MaxSessions: *maxSessions, Ahead: *ahead}, c)
}

func hostPort(fs *flag.FlagSet, port int) (*string, *int) {
	return fs.String("host", "127.0.0.1", "listen host"), fs.Int("port", port, "listen port")
}

func runAPI(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("api", flag.ExitOnError)
	host, port := hostPort(fs, 8090)
	dbURL := databaseFlag(fs)
	parse(fs, args)
	db, err := openDB(ctx, *dbURL)
	if err != nil {
		return err
	}
	defer db.Close()
	api := &handoff.API{DB: db, Token: os.Getenv("WETINK_TOKEN")}
	return serve(ctx, fmt.Sprintf("%s:%d", *host, *port), api.Handler())
}

func runMCP(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("mcp", flag.ExitOnError)
	host, port := hostPort(fs, 8091)
	natsURL, dbURL := natsFlag(fs), databaseFlag(fs)
	parse(fs, args)
	db, err := openDB(ctx, *dbURL)
	if err != nil {
		return err
	}
	defer db.Close()
	srv := mcpserver.New(db, *natsURL, os.Getenv("WETINK_TOKEN"))
	defer srv.Close()
	return serve(ctx, fmt.Sprintf("%s:%d", *host, *port), srv.Handler())
}

func runDashboard(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("dashboard", flag.ExitOnError)
	host, port := hostPort(fs, 8095)
	natsURL, dbURL := natsFlag(fs), databaseFlag(fs)
	scenarios := fs.String("scenarios", "evals/context", "directory of scenario files")
	parse(fs, args)
	db, err := openDB(ctx, *dbURL)
	if err != nil {
		return err
	}
	defer db.Close()
	abs, _ := filepath.Abs(*scenarios)
	s := &dashboard.Server{DB: db, DatabaseURL: *dbURL, NatsURL: *natsURL, Scenarios: abs}
	s.Start(ctx)
	defer s.Close()
	return serve(ctx, fmt.Sprintf("%s:%d", *host, *port), s.Handler())
}

func runMockGW(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("mockgw", flag.ExitOnError)
	host, port := hostPort(fs, 8080)
	parse(fs, args)
	return serve(ctx, fmt.Sprintf("%s:%d", *host, *port), mockgw.Handler())
}

func runProxy(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("proxy", flag.ExitOnError)
	host, port := hostPort(fs, 8092)
	natsURL := natsFlag(fs)
	upstream := fs.String("upstream", env("WETINK_PROXY_UPSTREAM", ""), "provider base URL (default OPENAI_BASE_URL)")
	key := fs.String("upstream-key", env("WETINK_PROXY_API_KEY", ""), "provider key (default OPENAI_API_KEY)")
	logDir := fs.String("log-dir", "runs/proxy", "log every model call under DIR/<session>/<master>.jsonl (\"\" for none)")
	toolOutputs := fs.Bool("tool-outputs", false, "also publish the outputs of the agents' own tools")
	parse(fs, args)
	p := proxy.New(oai.New(*upstream, *key, 30*time.Minute),
		proxy.Options{NATSURL: *natsURL, LogDir: *logDir, ToolOutputs: *toolOutputs})
	defer p.Close(5 * time.Second)
	slog.Info("agents use base URL http://HOST:PORT/s/<session>/<master>/v1")
	return serve(ctx, fmt.Sprintf("%s:%d", *host, *port), p.Handler())
}

func runMaster(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("master", flag.ExitOnError)
	session := fs.String("session", env("WETINK_SESSION", "s-dev"), "session")
	name := fs.String("master", "master-a", "master name")
	model := fs.String("model", env("WETINK_MODEL", "mock"), "model")
	natsURL := natsFlag(fs)
	handoffURL := fs.String("handoff-url", "", "start from this handoff (GET .../handoff)")
	mcpURL := fs.String("mcp-url", env("WETINK_MCP_URL", ""), "shared-context MCP server, e.g. http://127.0.0.1:8091/mcp")
	maxTurns := fs.Int("max-turns", 12, "model calls before giving up without a final message")
	prompt := strings.Join(parse(fs, args), " ")
	if prompt == "" {
		return errors.New("usage: wetink master [flags] PROMPT")
	}
	token := os.Getenv("WETINK_TOKEN")
	o := master.Options{Model: *model, MaxTurns: *maxTurns}
	if *handoffURL != "" {
		h, err := master.FetchHandoff(ctx, *handoffURL, token)
		if err != nil {
			return err
		}
		o.Context, _ = h["input"].([]any)
		slog.Info("starting from handoff", "items", h["items"], "upto_seq", h["upto_seq"])
	}
	if *mcpURL != "" {
		shared, err := master.ConnectShared(ctx, *mcpURL, *session, *name, token)
		if err != nil {
			return err
		}
		defer shared.Close()
		o.Shared = shared
	}
	t, err := tap.New(*session, *name, *natsURL, tap.Options{})
	if err != nil {
		return err
	}
	t.Start()
	defer t.Close(5 * time.Second)
	answer, err := master.Run(ctx, t, oai.New("", "", 5*time.Minute), prompt, o)
	if err != nil {
		return err
	}
	fmt.Println(answer)
	return nil
}

type evalFlags struct {
	natsURL, dbURL *string
	cf             classifierFlags
	perTopic       *int
	pace, wps      *float64
}

func addEvalFlags(fs *flag.FlagSet) evalFlags {
	return evalFlags{
		natsURL:  natsFlag(fs),
		dbURL:    databaseFlag(fs),
		cf:       addClassifierFlags(fs, "systemone", "default"),
		perTopic: fs.Int("per-topic", 8, "entries per topic in the handoff"),
		pace:     fs.Float64("pace", 0, "seconds between steps, to watch a run live"),
		wps:      fs.Float64("stream-wps", 40, "words per second for stream steps"),
	}
}

func (f evalFlags) config() (eval.Config, error) {
	c, err := f.cf.build(60 * time.Second)
	if err != nil {
		return eval.Config{}, err
	}
	return eval.Config{NatsURL: *f.natsURL, DatabaseURL: *f.dbURL, Classifier: c, PerTopic: *f.perTopic,
		Pace: *f.pace, StreamWPS: *f.wps, Out: os.Stdout}, nil
}

func runEval(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("eval", flag.ExitOnError)
	ef := addEvalFlags(fs)
	show := fs.Bool("show", false, "print the pulls and the handoff")
	noTopics := fs.Bool("no-topics", false, "do not declare session topics")
	runs := fs.Int("runs", 1, "repeat to measure run-to-run spread")
	session := fs.String("session", "", "session name (default: a random ctx-... name)")
	paths := parse(fs, args)
	path := "evals/context/url-shortener.json"
	if len(paths) > 0 {
		path = paths[0]
	}
	sc, err := eval.LoadScenario(path)
	if err != nil {
		return err
	}
	cfg, err := ef.config()
	if err != nil {
		return err
	}
	cfg.Show, cfg.NoTopics, cfg.Session = *show, *noTopics, *session
	db, err := openDB(ctx, *ef.dbURL)
	if err != nil {
		return err
	}
	defer db.Close()
	ok, err := eval.Run(ctx, cfg, db, sc, *runs)
	if err != nil {
		return err
	}
	if !ok {
		os.Exit(1)
	}
	return nil
}

func runSeed(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("seed", flag.ExitOnError)
	ef := addEvalFlags(fs)
	reset := fs.Bool("reset", false, "delete all data first")
	paths := parse(fs, args)
	if len(paths) == 0 {
		paths, _ = filepath.Glob("evals/context/*.json")
		sort.Strings(paths)
	}
	cfg, err := ef.config()
	if err != nil {
		return err
	}
	db, err := openDB(ctx, *ef.dbURL)
	if err != nil {
		return err
	}
	defer db.Close()
	if *reset {
		if err := eval.Reset(ctx, db, *ef.natsURL); err != nil {
			return err
		}
		fmt.Print("reset: dropped events, entries and sessions; recreated the WETINK stream\n\n")
	}
	failed := false
	for _, p := range paths {
		sc, err := eval.LoadScenario(p)
		if err != nil {
			return err
		}
		if !*reset {
			// Seeding a scenario again replaces its session.
			if err := eval.Forget(ctx, db, *ef.natsURL, sc.Name); err != nil {
				return err
			}
		}
		cfg.Session = sc.Name
		results, err := eval.RunOnce(ctx, cfg, db, sc)
		if err != nil {
			return err
		}
		for _, r := range results {
			failed = failed || !r
		}
	}
	if failed {
		os.Exit(1)
	}
	return nil
}

func runEvalClassifier(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("eval-classifier", flag.ExitOnError)
	model := fs.String("model", env("WETINK_CLASSIFIER_MODEL", "mock"), "model")
	baseURL := fs.String("base-url", env("WETINK_CLASSIFIER_BASE_URL", ""), "classifier endpoint")
	api := fs.String("api", env("WETINK_CLASSIFIER_API", "responses"), "responses, chat or systemone")
	data := fs.String("data", "evals/classifier.jsonl", "labelled items")
	verbose := fs.Bool("v", false, "print every case")
	parse(fs, args)
	c, err := classifier.BuildFromEnv(*api, *model, *baseURL, 120*time.Second)
	if err != nil {
		return err
	}
	fmt.Printf("model=%s ", *model)
	return eval.EvalClassifier(ctx, c, *data, *verbose, os.Stdout)
}

func runEvalRelations(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("eval-relations", flag.ExitOnError)
	model := fs.String("model", env("WETINK_CLASSIFIER_MODEL", "mock"), "model")
	baseURL := fs.String("base-url", env("WETINK_CLASSIFIER_BASE_URL", ""), "classifier endpoint")
	api := fs.String("api", env("WETINK_CLASSIFIER_API", "responses"), "responses, chat or systemone")
	data := fs.String("data", "evals/relations.jsonl", "labelled relations")
	verbose := fs.Bool("v", false, "print every case")
	parse(fs, args)
	c, err := classifier.BuildFromEnv(*api, *model, *baseURL, 120*time.Second)
	if err != nil {
		return err
	}
	fmt.Printf("model=%s ", *model)
	return eval.EvalRelations(ctx, c, *data, *verbose, os.Stdout)
}

const statsQuery = `
SELECT count(*),
       coalesce(percentile_cont(0.5)  WITHIN GROUP (ORDER BY ms), 0),
       coalesce(percentile_cont(0.95) WITHIN GROUP (ORDER BY ms), 0),
       coalesce(max(ms), 0)
FROM (
    SELECT extract(epoch FROM classified_at - published_at)::float8 * 1000 AS ms
    FROM events
    WHERE classified_at IS NOT NULL AND published_at IS NOT NULL
      AND ($1::text IS NULL OR session = $1)
) t`

func runStats(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("stats", flag.ExitOnError)
	session := fs.String("session", "", "only this session")
	dbURL := databaseFlag(fs)
	parse(fs, args)
	db, err := openDB(ctx, *dbURL)
	if err != nil {
		return err
	}
	defer db.Close()
	var s any
	if *session != "" {
		s = *session
	}
	var n int64
	var p50, p95, worst float64
	if err := db.QueryRow(ctx, statsQuery, s).Scan(&n, &p50, &p95, &worst); err != nil {
		return err
	}
	if n == 0 {
		fmt.Println("no classified events")
		return nil
	}
	fmt.Printf("events=%d p50=%.0fms p95=%.0fms max=%.0fms\n", n, p50, p95, worst)
	return nil
}

func runBench(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	natsURL, dbURL := natsFlag(fs), databaseFlag(fs)
	sessions := fs.Int("sessions", 8, "sessions in parallel")
	events := fs.Int("events", 20, "messages per session")
	parts := fs.Int("parts", 3, "paragraphs per message")
	masters := fs.Int("masters", 2, "masters per session")
	interval := fs.Duration("interval", 0, "time between a session's messages (0: all at once)")
	delay := fs.Duration("mock-delay", 250*time.Millisecond, "mock model latency per call")
	slots := fs.Int("mock-slots", 4, "mock model calls served at once, per server")
	servers := fs.Int("mock-servers", 1, "mock model servers (the classifier spreads calls over them)")
	real := fs.Bool("real", false, "use the configured classifier instead of the mock")
	keep := fs.Bool("keep", false, "keep the sessions")
	cf := addClassifierFlags(fs, "systemone", "default")
	parse(fs, args)
	db, err := openDB(ctx, *dbURL)
	if err != nil {
		return err
	}
	defer db.Close()
	cfg := bench.Config{NatsURL: *natsURL, DatabaseURL: *dbURL, Sessions: *sessions, Events: *events,
		Parts: *parts, Masters: *masters, Interval: *interval, MockDelay: *delay, MockSlots: *slots, MockServers: *servers, Keep: *keep}
	what := fmt.Sprintf("mock model %s/call, %d server(s) x %d slots", *delay, *servers, *slots)
	if *real {
		if cfg.Classifier, err = cf.build(60 * time.Second); err != nil {
			return err
		}
		what = *cf.api + " " + *cf.baseURL
	}
	fmt.Printf("bench: %d sessions x %d messages x %d parts, %d masters each, %s\n", *sessions, *events, *parts, *masters, what)
	res, err := bench.Run(ctx, cfg, db)
	if err != nil {
		return err
	}
	fmt.Println(res)
	return nil
}

func runExperiment(ctx context.Context, args []string) error {
	sub := ""
	if len(args) > 0 {
		sub, args = args[0], args[1:]
	}
	switch sub {
	case "run":
		return runExperimentRun(ctx, args)
	case "check":
		return runExperimentCheck(ctx, args)
	case "report":
		return runExperimentReport(args)
	case "rescore":
		return runExperimentRescore(ctx, args)
	}
	fmt.Fprintln(os.Stderr, `usage: wetink experiment <command> [flags] <experiment dir>

  run     run the agents in each condition and score them (results under --out)
  check   check the experiment's hidden tests and gold fixes, without any model
  report  compare the conditions over the runs so far
  rescore score finished runs again (all, or --run NAME)`)
	os.Exit(2)
	return nil
}

func experimentDir(fs *flag.FlagSet, positional []string) (*experiment.Spec, error) {
	if len(positional) != 1 {
		fs.Usage()
		os.Exit(2)
	}
	return experiment.Load(positional[0])
}

func runExperimentRun(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("experiment run", flag.ExitOnError)
	out := fs.String("out", "runs", "results directory")
	agentName := fs.String("agent", "claude", "coding agent: claude or codex")
	model := fs.String("model", "", "the agents' model (default: haiku for claude)")
	conditions := fs.String("conditions", "", "comma-separated conditions (default: all, in name order)")
	runs := fs.Int("runs", 1, "runs of each condition")
	upstream := fs.String("upstream", env("WETINK_PROXY_UPSTREAM", ""), "Responses provider for codex (default OPENAI_BASE_URL)")
	key := fs.String("upstream-key", env("WETINK_PROXY_API_KEY", ""), "its key (default OPENAI_API_KEY)")
	natsURL, dbURL := natsFlag(fs), databaseFlag(fs)
	cf := addClassifierFlags(fs, "responses", "mock")
	spec, err := experimentDir(fs, parse(fs, args))
	if err != nil {
		return err
	}
	agent, ok := experiment.Agents[*agentName]
	if !ok {
		return fmt.Errorf("unknown agent %q", *agentName)
	}
	if *model == "" {
		if *agentName != "claude" {
			return errors.New("--model is required for " + *agentName)
		}
		*model = "haiku"
	}
	conds := slices.Sorted(maps.Keys(spec.Conditions))
	if *conditions != "" {
		conds = strings.Split(*conditions, ",")
	}
	c, err := cf.build(60 * time.Second)
	if err != nil {
		return err
	}
	db, err := openDB(ctx, *dbURL)
	if err != nil {
		return err
	}
	defer db.Close()
	r, err := experiment.NewRunner(ctx, experiment.Config{Out: *out, Agent: agent, Model: *model, NatsURL: *natsURL,
		DatabaseURL: *dbURL, Classifier: c, Provider: oai.New(*upstream, *key, 30*time.Minute), Log: os.Stdout}, spec, db)
	if err != nil {
		return err
	}
	defer r.Close()
	for i := 1; i <= *runs; i++ {
		for _, cond := range conds {
			if _, err := r.Run(ctx, cond, i); err != nil {
				return err
			}
		}
	}
	results, err := experiment.LoadResults(filepath.Join(*out, spec.Name))
	if err != nil {
		return err
	}
	fmt.Println()
	experiment.Report(results, os.Stdout)
	return nil
}

func runExperimentCheck(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("experiment check", flag.ExitOnError)
	out := fs.String("out", "runs", "results directory (the base repository is kept there)")
	spec, err := experimentDir(fs, parse(fs, args))
	if err != nil {
		return err
	}
	ok, err := experiment.Check(ctx, spec, *out, os.Stdout)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("the experiment's tests do not behave as they should")
	}
	return nil
}

func runExperimentRescore(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("experiment rescore", flag.ExitOnError)
	out := fs.String("out", "runs", "results directory")
	only := fs.String("run", "", "only this run")
	spec, err := experimentDir(fs, parse(fs, args))
	if err != nil {
		return err
	}
	results, err := experiment.LoadResults(filepath.Join(*out, spec.Name))
	if err != nil {
		return err
	}
	r, err := experiment.NewScorer(spec, *out, os.Stdout)
	if err != nil {
		return err
	}
	for _, res := range results {
		if *only != "" && res.Run != *only {
			continue
		}
		if _, err := r.Rescore(ctx, res.Run); err != nil {
			return fmt.Errorf("%s: %w", res.Run, err)
		}
	}
	results, err = experiment.LoadResults(filepath.Join(*out, spec.Name))
	if err != nil {
		return err
	}
	fmt.Println()
	experiment.Report(results, os.Stdout)
	return nil
}

func runExperimentReport(args []string) error {
	fs := flag.NewFlagSet("experiment report", flag.ExitOnError)
	out := fs.String("out", "runs", "results directory")
	spec, err := experimentDir(fs, parse(fs, args))
	if err != nil {
		return err
	}
	results, err := experiment.LoadResults(filepath.Join(*out, spec.Name))
	if err != nil {
		return err
	}
	if len(results) == 0 {
		return fmt.Errorf("no runs under %s", filepath.Join(*out, spec.Name))
	}
	experiment.Report(results, os.Stdout)
	return nil
}
