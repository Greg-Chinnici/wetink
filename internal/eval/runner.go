package eval

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"wetink/internal/classifier"
	"wetink/internal/consumer"
	"wetink/internal/envelope"
	"wetink/internal/handoff"
	"wetink/internal/master"
	"wetink/internal/mcpserver"
	"wetink/internal/pipeline"
	"wetink/internal/pystr"
	"wetink/internal/tap"
)

// Config of scenario runs.
type Config struct {
	NatsURL, DatabaseURL string
	Classifier           classifier.Classifier
	PerTopic             int
	Show                 bool    // print pulls and the handoff
	NoTopics             bool    // do not declare the session's topics
	Session              string  // default a random ctx-... name
	Pace                 float64 // seconds between steps
	StreamWPS            float64 // words per second for stream steps (default 40)
	Out                  io.Writer
}

// player plays a scenario's steps in order, one tap (and MCP client) per master.
type player struct {
	cfg       Config
	session   string
	mcpURL    string
	taps      map[string]*tap.Tap
	tools     map[string]*master.SharedTools
	published int
	pulls     map[string]string
	lastUpto  map[string]int64
	db        *pgxpool.Pool
}

func (p *player) close() {
	for _, t := range p.taps {
		t.Close(5 * time.Second)
	}
	for _, t := range p.tools {
		t.Close()
	}
}

func (p *player) tap(m string) (*tap.Tap, error) {
	if t, ok := p.taps[m]; ok {
		return t, nil
	}
	t, err := tap.New(p.session, m, p.cfg.NatsURL, tap.Options{})
	if err != nil {
		return nil, err
	}
	t.Start()
	p.taps[m] = t
	return t, nil
}

func (p *player) publish(m string, items ...map[string]any) error {
	t, err := p.tap(m)
	if err != nil {
		return err
	}
	for _, it := range items {
		t.Publish(it, 0)
		p.published++
	}
	if !t.Flush(10 * time.Second) { // keep global stream order equal to script order
		return errors.New("publish timed out")
	}
	return nil
}

func stepItems(s Step) []map[string]any {
	if s.Tool == nil {
		return []map[string]any{{
			"type": "message", "id": tap.NewID("msg"), "role": "assistant",
			"content": []any{map[string]any{"type": "output_text", "text": s.Text, "annotations": []any{}}},
		}}
	}
	callID := tap.NewID("call")
	return []map[string]any{
		{"type": "function_call", "id": tap.NewID("fc"), "call_id": callID, "name": s.Tool.Name, "arguments": s.Tool.Arguments},
		{"type": "function_call_output", "call_id": callID, "output": s.Tool.Output},
	}
}

func (p *player) callTool(ctx context.Context, m, name string, args map[string]any) (string, error) {
	if p.mcpURL == "" {
		return "", errors.New("share and pull steps need the MCP server")
	}
	tools, ok := p.tools[m]
	if !ok {
		var err error
		if tools, err = master.ConnectShared(ctx, p.mcpURL, p.session, m, ""); err != nil {
			return "", err
		}
		p.tools[m] = tools
	}
	argBytes, _ := json.Marshal(args)
	call := map[string]any{"type": "function_call", "id": tap.NewID("fc"), "call_id": tap.NewID("call"),
		"name": name, "arguments": string(argBytes)}
	if err := p.publish(m, call); err != nil {
		return "", err
	}
	output, err := tools.Call(ctx, name, string(argBytes))
	if err != nil {
		return "", err
	}
	if name == pipeline.ShareTool && !strings.HasPrefix(output, "Error") {
		p.published++ // the MCP server published the share
	}
	return output, p.publish(m, map[string]any{"type": "function_call_output", "call_id": call["call_id"], "output": output})
}

var words = regexp.MustCompile(`\S+\s*`)
var sinceRe = regexp.MustCompile(`since_seq=(\d+)`)

func (p *player) step(ctx context.Context, s Step) error {
	switch {
	case s.Share != nil:
		_, err := p.callTool(ctx, s.Master, pipeline.ShareTool, s.Share)
		return err
	case s.Pull != nil:
		if err := settle(ctx, p.db, p.session, p.published); err != nil {
			return err
		}
		args := map[string]any{}
		since := int64(0)
		for k, v := range s.Pull {
			if k == "since" {
				if v == "last" {
					since = p.lastUpto[s.Master]
				}
				continue
			}
			args[k] = v
		}
		args["since_seq"] = since
		output, err := p.callTool(ctx, s.Master, pipeline.PullTool, args)
		if err != nil {
			return err
		}
		if m := sinceRe.FindStringSubmatch(output); m != nil {
			p.lastUpto[s.Master], _ = strconv.ParseInt(m[1], 10, 64)
		}
		id := s.ID
		if id == "" {
			id = fmt.Sprintf("pull-%d", len(p.pulls)+1)
		}
		p.pulls[id] = output
		return nil
	case s.Stream && s.Tool == nil:
		// Write the message word by word through the tap, like a streaming model.
		it := stepItems(s)[0]
		t, err := p.tap(s.Master)
		if err != nil {
			return err
		}
		ms := t.NewMessageStream(it["id"].(string))
		wps := p.cfg.StreamWPS
		if wps <= 0 {
			wps = 40
		}
		for _, w := range words.FindAllString(s.Text, -1) {
			ms.Feed(w)
			time.Sleep(time.Duration(float64(time.Second) / wps))
		}
		p.published += ms.Revs
		return p.publish(s.Master, it)
	default:
		return p.publish(s.Master, stepItems(s)...)
	}
}

// settle waits until n items of the session are stored and all of them are processed.
func settle(ctx context.Context, db *pgxpool.Pool, session string, n int) error {
	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) {
		var stored, pending int
		err := db.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE classified_at IS NULL)
			FROM events WHERE session = $1`, session).Scan(&stored, &pending)
		if err != nil {
			return err
		}
		if stored >= n && pending == 0 {
			return nil
		}
		select {
		case <-time.After(250 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// For each part of a streamed message: how much earlier it was published (in a snapshot) than
// the finished message, i.e. its head start over publishing whole items.
const selectStreamLead = `
SELECT extract(epoch FROM f.published_at - e.published_at)::float8
FROM entries x
JOIN events e ON e.id = x.event_id
JOIN events f ON f.master = x.master AND f.item_id = x.item_id AND f.rev IS NULL
WHERE x.session = $1 AND x.item_id IN (
    SELECT item_id FROM events WHERE session = $1 AND rev IS NOT NULL)`

func streamReport(ctx context.Context, db *pgxpool.Pool, session string) (string, error) {
	rows, err := db.Query(ctx, selectStreamLead, session)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var leads, early []float64
	for rows.Next() {
		var t *float64
		if err := rows.Scan(&t); err != nil {
			return "", err
		}
		if t != nil {
			leads = append(leads, *t)
			if *t > 0 {
				early = append(early, *t)
			}
		}
	}
	if len(leads) == 0 {
		return "", rows.Err()
	}
	s := fmt.Sprintf("streamed messages: %d of %d parts were published before their message finished", len(early), len(leads))
	if len(early) > 0 {
		sum := 0.0
		for _, t := range early {
			sum += t
		}
		s += fmt.Sprintf(", %.1f s earlier on average", sum/float64(len(early)))
	}
	return s, nil
}

// serveMCP runs the MCP server on a free localhost port; it returns the /mcp URL.
func serveMCP(db *pgxpool.Pool, natsURL string) (string, func(), error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", nil, err
	}
	srv := mcpserver.New(db, natsURL, "")
	hs := &http.Server{Handler: srv.Handler()}
	go func() { _ = hs.Serve(ln) }()
	return "http://" + ln.Addr().String() + "/mcp", func() { hs.Close(); srv.Close() }, nil
}

func randomSession() string { return "ctx-" + tap.NewID("x")[2:10] }

// RunOnce plays the scenario in a new session and evaluates its checks.
func RunOnce(ctx context.Context, cfg Config, db *pgxpool.Pool, sc *Scenario) ([]bool, error) {
	out := cfg.Out
	session := cfg.Session
	if session == "" {
		session = randomSession()
	}
	var existing int
	if err := db.QueryRow(ctx, "SELECT count(*) FROM events WHERE session = $1", session).Scan(&existing); err != nil {
		return nil, err
	}
	if existing > 0 {
		return nil, fmt.Errorf("session %s already has events; reset or pick another name", session)
	}
	durable := "eval-" + session
	cctx, stopConsumer := context.WithCancel(ctx)
	consumerDone := make(chan error, 1)
	go func() {
		consumerDone <- consumer.Run(cctx, consumer.Config{
			NatsURL: cfg.NatsURL, DatabaseURL: cfg.DatabaseURL, Durable: durable, Subject: envelope.Subject(session),
		}, cfg.Classifier)
	}()
	defer func() {
		stopConsumer()
		<-consumerDone
		if nc, err := nats.Connect(cfg.NatsURL); err == nil {
			if js, err := jetstream.New(nc); err == nil {
				_ = js.DeleteConsumer(context.Background(), envelope.StreamName, durable) // best effort
			}
			nc.Close()
		}
	}()
	if !cfg.NoTopics && sc.Topics != nil && sc.Topics.Len() > 0 {
		topics, err := handoff.ParseTopics(sc.Topics)
		if err != nil {
			return nil, err
		}
		task := sc.Task
		if err := handoff.PutSession(ctx, db, session, &task, topics); err != nil {
			return nil, err
		}
	}

	p := &player{cfg: cfg, session: session, taps: map[string]*tap.Tap{}, tools: map[string]*master.SharedTools{},
		pulls: map[string]string{}, lastUpto: map[string]int64{}, db: db}
	for _, s := range sc.Steps {
		if s.Share != nil || s.Pull != nil {
			url, stop, err := serveMCP(db, cfg.NatsURL)
			if err != nil {
				return nil, err
			}
			defer stop()
			p.mcpURL = url
			break
		}
	}
	err := func() error {
		defer p.close()
		for _, s := range sc.Steps {
			if err := p.step(ctx, s); err != nil {
				return err
			}
			if cfg.Pace > 0 {
				time.Sleep(time.Duration(cfg.Pace * float64(time.Second)))
			}
		}
		return nil
	}()
	if err != nil {
		return nil, err
	}
	if err := settle(ctx, db, session, p.published); err != nil {
		return nil, err
	}
	h, err := handoff.Snapshot(ctx, db, session, handoff.Options{PerTopic: cfg.PerTopic})
	if err != nil {
		return nil, err
	}
	text := h.Text()
	if cfg.Show {
		for _, s := range sc.Steps {
			if s.Pull != nil {
				fmt.Fprintf(out, "--- %s\n%s\n\n", s.ID, p.pulls[s.ID])
			}
		}
		fmt.Fprintf(out, "%s \n\n", text)
	}
	var results []bool
	for _, c := range sc.Checks {
		target := text
		if c.In != "" {
			target = p.pulls[c.In]
		}
		ok, detail, err := c.Run(target)
		if err != nil {
			return nil, err
		}
		results = append(results, ok)
		if !ok || runsOf(ctx) == 1 {
			mark := "✓"
			if !ok {
				mark = "✗"
			}
			fmt.Fprintf(out, "%s %-20s %-16s %-24s %s\n", mark, c.ID, c.Expect, detail, c.Why)
		}
	}
	for _, s := range sc.Steps {
		if s.Stream {
			if report, err := streamReport(ctx, db, session); err == nil && report != "" {
				fmt.Fprintln(out, report)
			}
			break
		}
	}
	passed := 0
	for _, r := range results {
		if r {
			passed++
		}
	}
	fmt.Fprintf(out, "%s: %d/%d checks, session=%s, items=%d, chars=%d, upto_seq=%d\n\n",
		sc.Name, passed, len(results), session, h.Items, pystr.Len(text), h.UptoSeq)
	return results, nil
}

type runsKey struct{}

func runsOf(ctx context.Context) int {
	if n, ok := ctx.Value(runsKey{}).(int); ok {
		return n
	}
	return 1
}

// Run plays the scenario runs times and prints the spread; it reports whether every run
// passed every check.
func Run(ctx context.Context, cfg Config, db *pgxpool.Pool, sc *Scenario, runs int) (bool, error) {
	if cfg.Session != "" && runs > 1 {
		return false, errors.New("a session name names one run; drop it or use one run")
	}
	ctx = context.WithValue(ctx, runsKey{}, runs)
	var all [][]bool
	for range runs {
		r, err := RunOnce(ctx, cfg, db, sc)
		if err != nil {
			return false, err
		}
		all = append(all, r)
	}
	ok := true
	for _, r := range all {
		for _, x := range r {
			ok = ok && x
		}
	}
	if runs > 1 {
		fmt.Fprintf(cfg.Out, "%s over %d runs:\n", sc.Name, runs)
		for i, c := range sc.Checks {
			n := 0
			for _, r := range all {
				if r[i] {
					n++
				}
			}
			if n < runs {
				fmt.Fprintf(cfg.Out, "  %-20s passed %d/%d  %s\n", c.ID, n, runs, c.Why)
			}
		}
		var totals []string
		for _, r := range all {
			n := 0
			for _, x := range r {
				if x {
					n++
				}
			}
			totals = append(totals, strconv.Itoa(n))
		}
		fmt.Fprintf(cfg.Out, "  checks per run: [%s] of %d\n", strings.Join(totals, ", "), len(sc.Checks))
	}
	return ok, nil
}
