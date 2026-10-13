// Package dashboard is a live view of shared context being built (wetink dashboard).
//
// Read-only views of the session state in Postgres (events, entries with the models' answers,
// the handoff, scenario checks), the NATS stream and its consumers, and the model servers.
// Live updates come over one server-sent-events stream:
//   - change: Postgres rows changed (LISTEN wetink_changes, see schema.sql), per session;
//   - wire:   items as they are published on NATS (a plain subscription to wetink.*.items);
//   - system: stream, consumer and model-server status, every 2 s;
//   - log:    output of scenario runs started from the page (POST /api/run);
//   - run:    a run started or finished.
//
// It binds to localhost and has no auth: it can start scenario runs, nothing else writes.
package dashboard

import (
	"bufio"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"wetink/internal/classifier"
	"wetink/internal/envelope"
	"wetink/internal/eval"
	"wetink/internal/handoff"
	"wetink/internal/item"
	"wetink/internal/pipeline"
	"wetink/internal/pystr"
)

//go:embed static/dashboard.html
var page []byte

const (
	systemEvery    = 2 * time.Second
	changeCoalesce = 150 * time.Millisecond
)

const selectSessions = `
WITH names AS (SELECT session FROM events UNION SELECT session FROM sessions)
SELECT n.session, s.task, s.created_at,
       (SELECT count(*) FROM events e WHERE e.session = n.session) AS events,
       (SELECT count(*) FROM events e WHERE e.session = n.session AND e.classified_at IS NULL) AS pending,
       (SELECT count(*) FROM entries x WHERE x.session = n.session) AS entries,
       (SELECT max(received_at) FROM events e WHERE e.session = n.session) AS last_event
FROM names n LEFT JOIN sessions s USING (session)
ORDER BY greatest((SELECT max(received_at) FROM events e WHERE e.session = n.session),
                  s.created_at) DESC NULLS LAST`

const selectEvents = `
SELECT id, master, seq, stream_seq, item, via, rev, received_at, published_at, classified_at,
       classify_error
FROM events WHERE session = $1 ORDER BY stream_seq`

const selectEntries = `
SELECT x.id, x.event_id, x.part, x.master, x.seq, x.stream_seq, x.text, x.topic, x.kind,
       x.score, x.confidence, x.status, x.relation, x.related_id, x.relation_prob, x.closed_by,
       x.classified_at, x.trace, x.declared, c.topic_rank, c.id IS NOT NULL AS shown
FROM entries x LEFT JOIN context_ranked c USING (id)
WHERE x.session = $1 ORDER BY x.stream_seq, x.part`

const selectTotals = `
SELECT (SELECT count(*) FROM events), (SELECT count(*) FROM events WHERE classified_at IS NULL),
       (SELECT count(*) FROM entries), (SELECT count(DISTINCT session) FROM events)`

// Server is the dashboard.
type Server struct {
	DB          *pgxpool.Pool
	DatabaseURL string
	NatsURL     string
	Scenarios   string // directory of scenario files

	clients map[chan []byte]bool
	mu      sync.Mutex
	system  map[string]any
	nc      *nats.Conn
	js      jetstream.JetStream
	http    *http.Client
	run     *runState
}

type runState struct {
	ID       int     `json:"id"`
	Scenario string  `json:"scenario"`
	Session  string  `json:"session"`
	Started  float64 `json:"started"`
	Status   string  `json:"status"`
	cmd      *exec.Cmd
}

func (s *Server) send(kind string, data any) {
	b, err := json.Marshal(data)
	if err != nil {
		return
	}
	line := []byte("event: " + kind + "\ndata: " + string(b) + "\n\n")
	s.mu.Lock()
	defer s.mu.Unlock()
	for c := range s.clients {
		select {
		case c <- line:
		default: // a stalled browser drops messages, not the server
		}
	}
}

// Start connects to NATS and starts the background listeners; they stop with ctx.
func (s *Server) Start(ctx context.Context) {
	s.clients = map[chan []byte]bool{}
	s.http = &http.Client{Timeout: time.Second}
	nc, err := nats.Connect(s.NatsURL, nats.Name("dashboard"), nats.MaxReconnects(-1))
	if err != nil {
		slog.Warn("NATS unavailable", "err", err)
	} else {
		s.nc = nc
		s.js, _ = jetstream.New(nc)
		if _, err := nc.Subscribe(envelope.StreamSubjects, s.onWire); err != nil {
			slog.Warn("no wire view", "err", err)
		}
	}
	go s.listenChanges(ctx)
	go s.pushSystem(ctx)
}

func (s *Server) Close() {
	s.mu.Lock()
	run := s.run
	s.mu.Unlock()
	if run != nil && run.cmd.ProcessState == nil && run.cmd.Process != nil {
		_ = run.cmd.Process.Kill()
	}
	if s.nc != nil {
		s.nc.Close()
	}
}

func (s *Server) onWire(msg *nats.Msg) {
	env, err := envelope.Unmarshal(msg.Data)
	if err != nil {
		return
	}
	var preview string
	switch env.ItemType() {
	case "function_call_output":
		preview, _ = item.Render(env.Item, nil)
	case "message":
		preview = item.TextOf(env.Item["content"])
	case "function_call":
		preview = item.Str(env.Item, "name") + "(" + item.String(env.Item, "arguments") + ")"
	}
	var rev any
	if env.Rev > 0 {
		rev = env.Rev
	}
	s.send("wire", map[string]any{
		"time": float64(time.Now().UnixNano()) / 1e9, "subject": msg.Subject, "session": env.Session,
		"master": env.Master, "seq": env.Seq, "type": env.ItemType(), "msg_id": msg.Header.Get("Nats-Msg-Id"),
		"rev": rev, "bytes": len(msg.Data), "preview": pystr.Prefix(preview, 300),
	})
}

// listenChanges turns Postgres notifications into coalesced `change` messages per session.
func (s *Server) listenChanges(ctx context.Context) {
	notes := make(chan map[string]string, 1000)
	go func() {
		for ctx.Err() == nil {
			conn, err := pgx.Connect(ctx, s.DatabaseURL)
			if err == nil {
				_, err = conn.Exec(ctx, "LISTEN wetink_changes")
			}
			for err == nil {
				var n *pgconn.Notification
				n, err = conn.WaitForNotification(ctx)
				if err == nil {
					var payload map[string]string
					if json.Unmarshal([]byte(n.Payload), &payload) == nil {
						select {
						case notes <- payload:
						default:
						}
					}
				}
			}
			if conn != nil {
				conn.Close(context.Background())
			}
			if ctx.Err() == nil {
				slog.Warn("change listener reconnecting", "err", err)
				time.Sleep(2 * time.Second)
			}
		}
	}()
	changed := map[string]map[string]bool{}
	ticker := time.NewTicker(changeCoalesce)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case p := <-notes:
			if changed[p["session"]] == nil {
				changed[p["session"]] = map[string]bool{}
			}
			changed[p["session"]][p["table"]] = true
		case <-ticker.C:
			for session, tables := range changed {
				var list []string
				for t := range tables {
					list = append(list, t)
				}
				s.send("change", map[string]any{"session": session, "tables": list})
			}
			changed = map[string]map[string]bool{}
		}
	}
}

func (s *Server) pushSystem(ctx context.Context) {
	t := time.NewTicker(systemEvery)
	defer t.Stop()
	for {
		s.mu.Lock()
		n := len(s.clients)
		s.mu.Unlock()
		if n > 0 {
			sys := s.systemStatus(ctx)
			s.mu.Lock()
			s.system = sys
			s.mu.Unlock()
			s.send("system", sys)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (s *Server) probe(url string) string {
	resp, err := s.http.Get(url)
	if err != nil {
		return "down"
	}
	resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return "ok"
	}
	return fmt.Sprintf("HTTP %d", resp.StatusCode)
}

func (s *Server) systemStatus(ctx context.Context) map[string]any {
	out := map[string]any{"time": float64(time.Now().UnixNano()) / 1e9}
	if s.js != nil && s.nc.IsConnected() {
		if st, err := s.js.Stream(ctx, envelope.StreamName); err != nil {
			out["nats"] = "error: " + err.Error()
		} else {
			info, err := st.Info(ctx)
			if err != nil {
				out["nats"] = "error: " + err.Error()
			} else {
				out["stream"] = map[string]any{"messages": info.State.Msgs, "bytes": info.State.Bytes, "last_seq": info.State.LastSeq}
				consumers := []map[string]any{}
				lister := st.ListConsumers(ctx)
				for c := range lister.Info() {
					consumers = append(consumers, map[string]any{
						"name": c.Name, "pending": c.NumPending, "ack_pending": c.NumAckPending,
						"redelivered": c.NumRedelivered, "delivered_seq": c.Delivered.Stream,
					})
				}
				out["consumers"] = consumers
				out["nats"] = "ok"
			}
		}
	} else {
		out["nats"] = "disconnected"
	}
	var ev, pending, en, sessions int64
	if err := s.DB.QueryRow(ctx, selectTotals).Scan(&ev, &pending, &en, &sessions); err != nil {
		out["postgres"] = "error: " + err.Error()
	} else {
		out["postgres"] = "ok"
		out["totals"] = map[string]any{"events": ev, "pending": pending, "entries": en, "sessions": sessions}
	}
	if u := os.Getenv("WETINK_CLASSIFIER_BASE_URL"); u != "" {
		out["classifier"] = s.probe(strings.TrimSuffix(strings.TrimSuffix(u, "/"), "/v1") + "/health")
	}
	if u := os.Getenv("WETINK_TOPIC_NAMER_BASE_URL"); u != "" {
		out["namer"] = s.probe(strings.TrimRight(u, "/") + "/models")
	}
	s.mu.Lock()
	if s.run != nil {
		r := *s.run
		out["run"] = r
	}
	s.mu.Unlock()
	return out
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v) // fails only if the client went away
}

func queryMaps(ctx context.Context, db *pgxpool.Pool, sql string, args ...any) ([]map[string]any, error) {
	rows, err := db.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToMap)
}

// pullOutputs returns the output of each pull step of a scenario, matched by order per master.
func pullOutputs(sc *eval.Scenario, events []map[string]any) map[string]string {
	pulls := map[[2]string]bool{}
	outputs := map[string][]string{}
	for _, e := range events {
		it, _ := e["item"].(map[string]any)
		if item.String(it, "type") == "function_call" && item.String(it, "name") == pipeline.PullTool {
			pulls[[2]string{e["master"].(string), item.String(it, "call_id")}] = true
		}
	}
	for _, e := range events {
		it, _ := e["item"].(map[string]any)
		m := e["master"].(string)
		if item.String(it, "type") == "function_call_output" && pulls[[2]string{m, item.String(it, "call_id")}] {
			outputs[m] = append(outputs[m], item.TextOf(it["output"]))
		}
	}
	out := map[string]string{}
	seen := map[string]int{}
	for i, step := range sc.Steps {
		if step.Pull == nil {
			continue
		}
		n := seen[step.Master]
		seen[step.Master]++
		if n < len(outputs[step.Master]) {
			id := step.ID
			if id == "" {
				id = "pull-" + strconv.Itoa(i)
			}
			out[id] = outputs[step.Master][n]
		}
	}
	return out
}

func matchScenario(scenarios map[string]*eval.Scenario, session string, task *string) *eval.Scenario {
	for name, sc := range scenarios {
		if session == name || strings.HasPrefix(session, name+"-") {
			return sc
		}
	}
	if task != nil {
		for _, sc := range scenarios {
			if sc.Task == *task {
				return sc
			}
		}
	}
	return nil
}

func (s *Server) sessionState(ctx context.Context, session string, r *http.Request) (map[string]any, error) {
	q := r.URL.Query()
	o := handoff.Options{ExcludeMaster: q.Get("exclude_master")}
	if v, err := strconv.Atoi(q.Get("per_topic")); err == nil {
		o.PerTopic = v
	}
	if v, err := strconv.Atoi(q.Get("max_chars")); err == nil {
		o.MaxChars = v
	}
	var task *string
	var topics []byte
	if err := s.DB.QueryRow(ctx, "SELECT task, topics FROM sessions WHERE session = $1", session).Scan(&task, &topics); err != nil && err != pgx.ErrNoRows {
		return nil, err
	}
	events, err := queryMaps(ctx, s.DB, selectEvents, session)
	if err != nil {
		return nil, err
	}
	entries, err := queryMaps(ctx, s.DB, selectEntries, session)
	if err != nil {
		return nil, err
	}
	h, err := handoff.Snapshot(ctx, s.DB, session, o)
	if err != nil {
		return nil, err
	}
	var checks any
	if sc := matchScenario(eval.LoadScenarios(s.Scenarios), session, task); sc != nil {
		full := h
		if o != (handoff.Options{}) {
			if full, err = handoff.Snapshot(ctx, s.DB, session, handoff.Options{}); err != nil {
				return nil, err
			}
		}
		text := full.Text()
		pulls := pullOutputs(sc, events)
		var results []map[string]any
		for _, c := range sc.Checks {
			res := map[string]any{"id": c.ID, "expect": c.Expect, "pattern": c.Pattern, "why": c.Why}
			if c.In != "" {
				res["in"] = c.In
			}
			target, ok := text, true
			if c.In != "" {
				target, ok = pulls[c.In]
			}
			if !ok {
				res["ok"], res["detail"] = false, c.In+" not pulled yet"
			} else {
				passed, detail, err := c.Run(target)
				if err != nil {
					detail = err.Error()
				}
				if c.In != "" {
					detail += " in " + c.In
				}
				res["ok"], res["detail"] = passed, detail
			}
			results = append(results, res)
		}
		checks = map[string]any{"scenario": sc.Name, "steps": len(sc.Steps), "results": results}
	}
	var topicsJSON any = map[string]any{}
	if len(topics) > 0 {
		topicsJSON = json.RawMessage(topics)
	}
	return map[string]any{
		"session": session, "task": task, "topics": topicsJSON, "events": events, "entries": entries,
		"handoff": h, "checks": checks,
	}, nil
}

func (s *Server) config(w http.ResponseWriter, _ *http.Request) {
	all := eval.LoadScenarios(s.Scenarios)
	scenarios := []map[string]any{}
	for _, name := range slices.Sorted(maps.Keys(all)) {
		sc := all[name]
		scenarios = append(scenarios, map[string]any{"name": sc.Name, "task": sc.Task, "steps": len(sc.Steps)})
	}
	writeJSON(w, map[string]any{
		"thresholds": map[string]any{
			"visible_score":          classifier.MinVisibleScore,
			"visible_question_score": classifier.MinVisibleQuestionScore,
			"relation_min_prob":      classifier.RelationMinProb,
			"close_min_prob":         pipeline.CloseMinProb,
			"new_topic_min_prob":     classifier.NewTopicMinProb,
			"question_min_prob":      classifier.QuestionMinProb,
		},
		"relevance_levels": classifier.RelevanceLevels,
		"kinds":            classifier.KindCriteria,
		"relations":        classifier.RelationCriteria,
		"scenarios":        scenarios,
		"classifier":       os.Getenv("WETINK_CLASSIFIER_BASE_URL"),
		"namer":            os.Getenv("WETINK_TOPIC_NAMER_MODEL"),
	})
}

func (s *Server) stream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	c := make(chan []byte, 1000)
	s.mu.Lock()
	s.clients[c] = true
	sys := s.system
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.clients, c)
		s.mu.Unlock()
	}()
	if sys != nil {
		b, _ := json.Marshal(sys)
		fmt.Fprintf(w, "event: system\ndata: %s\n\n", b)
	}
	flusher.Flush()
	keepalive := time.NewTicker(15 * time.Second)
	defer keepalive.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case line := <-c:
			w.Write(line)
			flusher.Flush()
		case <-keepalive.C:
			w.Write([]byte(": keepalive\n\n"))
			flusher.Flush()
		}
	}
}

// startRun starts a scenario run (wetink eval) in a fresh session.
func (s *Server) startRun(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Scenario string   `json:"scenario"`
		Pace     *float64 `json:"pace"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "bad body", http.StatusBadRequest)
		return
	}
	sc := eval.LoadScenarios(s.Scenarios)[body.Scenario]
	if sc == nil {
		http.Error(w, "unknown scenario", http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	if s.run != nil && s.run.Status == "running" {
		session := s.run.Session
		s.mu.Unlock()
		http.Error(w, "run "+session+" is still going", http.StatusConflict)
		return
	}
	s.mu.Unlock()
	pace := 1.0
	if body.Pace != nil {
		pace = min(max(*body.Pace, 0), 10)
	}
	session := sc.Name + "-" + time.Now().Format("150405")
	exe, err := os.Executable()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	cmd := exec.Command(exe, "eval", sc.Path, "--session", session, "--pace", strconv.FormatFloat(pace, 'f', -1, 64))
	stdout, _ := cmd.StdoutPipe()
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	run := &runState{ID: cmd.Process.Pid, Scenario: sc.Name, Session: session,
		Started: float64(time.Now().UnixNano()) / 1e9, Status: "running", cmd: cmd}
	s.mu.Lock()
	s.run = run
	s.mu.Unlock()
	s.send("run", *run)
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			s.send("log", map[string]any{"session": session, "line": sc.Text()})
		}
		status := "passed"
		if err := cmd.Wait(); err != nil {
			status = "exit " + strconv.Itoa(cmd.ProcessState.ExitCode())
		}
		s.mu.Lock()
		run.Status = status
		snapshot := *run
		s.mu.Unlock()
		s.send("run", snapshot)
	}()
	writeJSON(w, map[string]any{"session": session, "pace": pace})
}

// Handler serves the page and the API.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(page)
	})
	mux.HandleFunc("GET /api/config", s.config)
	mux.HandleFunc("GET /api/sessions", func(w http.ResponseWriter, r *http.Request) {
		rows, err := queryMaps(r.Context(), s.DB, selectSessions)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if rows == nil {
			rows = []map[string]any{}
		}
		writeJSON(w, rows)
	})
	mux.HandleFunc("GET /api/sessions/{session}", func(w http.ResponseWriter, r *http.Request) {
		session := r.PathValue("session")
		if err := envelope.CheckName(session, "session"); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		state, err := s.sessionState(r.Context(), session, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, state)
	})
	mux.HandleFunc("GET /api/system", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, s.systemStatus(r.Context())) })
	mux.HandleFunc("GET /api/stream", s.stream)
	mux.HandleFunc("POST /api/run", s.startRun)
	return mux
}
