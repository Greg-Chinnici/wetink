// End to end: tap -> NATS JetStream -> consumer (+ classifier) -> Postgres -> handoff, pull
// and MCP tools. Needs `docker compose up`; skipped otherwise. The classifier runs against the
// in-process mock gateway.
package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"wetink/internal/classifier"
	"wetink/internal/consumer"
	"wetink/internal/envelope"
	"wetink/internal/eval"
	"wetink/internal/handoff"
	"wetink/internal/master"
	"wetink/internal/mcpserver"
	"wetink/internal/mockgw"
	"wetink/internal/oai"
	"wetink/internal/segment"
	"wetink/internal/store"
	"wetink/internal/tap"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

var (
	natsURL     = env("NATS_URL", "nats://dev-token@localhost:4222")
	databaseURL = env("DATABASE_URL", "postgresql://wetink:wetink@localhost:5432/wetink")
)

type harness struct {
	t       *testing.T
	ctx     context.Context
	session string
	db      *pgxpool.Pool
	gw      *httptest.Server
}

func setup(t *testing.T) *harness {
	t.Helper()
	ctx := context.Background()
	nc, err := nats.Connect(natsURL, nats.Timeout(time.Second))
	if err != nil {
		t.Skipf("NATS not reachable (%v); run `docker compose up -d`", err)
	}
	// A running `wetink consumer` reads every session's items and works on test sessions too.
	if js, err := jetstream.New(nc); err == nil {
		if c, err := js.Consumer(ctx, envelope.StreamName, "store"); err == nil {
			if info, err := c.Info(ctx); err == nil && info.NumWaiting > 0 {
				nc.Close()
				t.Fatal("a `wetink consumer` is running (durable \"store\" is being read); stop it before the integration tests")
			}
		}
	}
	nc.Close()
	cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	db, err := store.Open(cctx, databaseURL)
	if err != nil {
		t.Skipf("Postgres not reachable (%v); run `docker compose up -d`", err)
	}
	h := &harness{t: t, ctx: ctx, db: db, session: "t-" + tap.NewID("x")[2:14]}
	h.gw = httptest.NewServer(mockgw.Handler())
	t.Cleanup(func() {
		h.gw.Close()
		if nc, err := nats.Connect(natsURL); err == nil {
			if js, err := jetstream.New(nc); err == nil {
				_ = js.DeleteConsumer(ctx, envelope.StreamName, "test-"+h.session) // best effort
			}
			nc.Close()
		}
		// Keep test sessions out of the dashboard, and their messages out of the stream: a
		// long-lived consumer (`wetink consumer`) would otherwise store them on its next start.
		if err := eval.Forget(ctx, db, natsURL, h.session); err != nil {
			t.Errorf("cleanup: %v", err)
		}
		db.Close()
	})
	return h
}

// markerNamer names new topics from a `[topic: X]` marker, like the mock does for known ones.
func markerNamer(_ context.Context, text string) (string, error) {
	if m := regexp.MustCompile(`\[topic:\s*([^\]]+)\]`).FindStringSubmatch(text); m != nil {
		return m[1], nil
	}
	return "general", nil
}

// classifier is the production path: a decision model on /v1/systemone, served by the mock.
func (h *harness) classifier() classifier.Classifier {
	return classifier.NewSystemOne(h.gw.URL+"/v1", "", "", markerNamer, 4, 10*time.Second)
}

func (h *harness) gateway() *oai.Client { return oai.New(h.gw.URL+"/v1", "mock", 10*time.Second) }

// consumer runs a consumer for the session until the returned stop is called.
func (h *harness) consumer(c classifier.Classifier) (stop func()) {
	ctx, cancel := context.WithCancel(h.ctx)
	done := make(chan error, 1)
	go func() {
		done <- consumer.Run(ctx, consumer.Config{
			NatsURL: natsURL, DatabaseURL: databaseURL,
			Durable: "test-" + h.session, Subject: envelope.Subject(h.session),
		}, c)
	}()
	return func() {
		cancel()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) {
				h.t.Errorf("consumer: %v", err)
			}
		case <-time.After(5 * time.Second):
			h.t.Error("consumer did not stop")
		}
	}
}

func (h *harness) tap(master string) *tap.Tap {
	tp, err := tap.New(h.session, master, natsURL, tap.Options{})
	if err != nil {
		h.t.Fatal(err)
	}
	tp.Start()
	return tp
}

func (h *harness) publish(master string, items ...map[string]any) {
	tp := h.tap(master)
	for _, it := range items {
		tp.Publish(it, 0)
	}
	tp.Close(5 * time.Second)
}

func message(text string) map[string]any {
	return map[string]any{
		"type": "message", "id": tap.NewID("msg"), "role": "assistant",
		"content": []any{map[string]any{"type": "output_text", "text": text, "annotations": []any{}}},
	}
}

type eventRow struct {
	master, itemID, typ string
	seq                 int64
}

func (h *harness) events() []eventRow {
	rows, _ := h.db.Query(h.ctx, "SELECT master, seq, item_id, item->>'type' FROM events WHERE session = $1 ORDER BY seq, id", h.session)
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (eventRow, error) {
		var e eventRow
		return e, r.Scan(&e.master, &e.seq, &e.itemID, &e.typ)
	})
	if err != nil {
		h.t.Fatal(err)
	}
	return out
}

func (h *harness) waitEvents(n int) []eventRow {
	deadline := time.Now().Add(10 * time.Second)
	for {
		got := h.events()
		if len(got) >= n || time.Now().After(deadline) {
			return got
		}
		time.Sleep(100 * time.Millisecond)
	}
}

type entryRow struct {
	text, master, topic, kind, status string
	relation, relatedText             *string
}

// waitProcessed waits until n events of the session are processed; returns its entries.
func (h *harness) waitProcessed(n int) []entryRow {
	deadline := time.Now().Add(15 * time.Second)
	for {
		var done int
		if err := h.db.QueryRow(h.ctx, "SELECT count(*) FROM events WHERE session = $1 AND classified_at IS NOT NULL", h.session).Scan(&done); err != nil {
			h.t.Fatal(err)
		}
		if done >= n || time.Now().After(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	rows, _ := h.db.Query(h.ctx, `SELECT e.text, e.master, coalesce(e.topic, ''), coalesce(e.kind, ''), e.status, e.relation, r.text
		FROM entries e LEFT JOIN entries r ON r.id = e.related_id WHERE e.session = $1 ORDER BY e.id`, h.session)
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (entryRow, error) {
		var e entryRow
		return e, r.Scan(&e.text, &e.master, &e.topic, &e.kind, &e.status, &e.relation, &e.relatedText)
	})
	if err != nil {
		h.t.Fatal(err)
	}
	return out
}

func (h *harness) handoff(o handoff.Options) string {
	r, err := handoff.Snapshot(h.ctx, h.db, h.session, o)
	if err != nil {
		h.t.Fatal(err)
	}
	return r.Text()
}

func TestMasterItemsLandInEvents(t *testing.T) {
	h := setup(t)
	stop := h.consumer(h.classifier())
	tp := h.tap("master-a")
	if _, err := master.Run(h.ctx, tp, h.gateway(), "ping", master.Options{Model: "mock"}); err != nil {
		t.Fatal(err)
	}
	tp.Close(5 * time.Second)
	got := h.waitEvents(3)
	stop()
	var summary []string
	for _, e := range got {
		summary = append(summary, fmt.Sprintf("%s %d %s", e.master, e.seq, e.typ))
	}
	want := []string{"master-a 1 function_call", "master-a 2 function_call_output", "master-a 3 message"}
	if !slices.Equal(summary, want) {
		t.Errorf("events %v", summary)
	}
}

func TestDuplicatesAndConsumerRestart(t *testing.T) {
	h := setup(t)
	var items []map[string]any
	for i := range 6 {
		items = append(items, map[string]any{"type": "message", "id": fmt.Sprintf("msg_%s_%d", h.session, i), "content": []any{}})
	}
	stop := h.consumer(h.classifier())
	h.publish("master-a", items[:3]...)
	if n := len(h.waitEvents(3)); n != 3 {
		t.Fatalf("%d events", n)
	}
	stop()

	// Consumer is down: publish more, plus a repeat that JetStream should drop.
	h.publish("master-a", append(items[3:], items[0])...)
	// Bypass JetStream dedup with a fresh Nats-Msg-Id so Postgres must catch it.
	nc, _ := nats.Connect(natsURL)
	js, _ := jetstream.New(nc)
	e := envelope.Envelope{Session: h.session, Master: "master-a", Seq: 99, Item: items[1], Via: envelope.ViaTap}
	data, _ := e.Marshal()
	if _, err := js.Publish(h.ctx, e.Subject(), data, jetstream.WithMsgID("bypass-1")); err != nil {
		t.Fatal(err)
	}
	nc.Close()

	stop = h.consumer(h.classifier())
	h.waitEvents(6)
	time.Sleep(time.Second) // let any stray duplicates arrive
	got := h.events()
	stop()
	var ids, want []string
	for _, e := range got {
		ids = append(ids, e.itemID)
	}
	for _, it := range items {
		want = append(want, it["id"].(string))
	}
	slices.Sort(ids)
	slices.Sort(want)
	if !slices.Equal(ids, want) {
		t.Errorf("stored %v, want %v", ids, want)
	}
}

func TestEntriesLifecycle(t *testing.T) {
	h := setup(t)
	plan := message("[topic: db] We decided to use Postgres 17.\n\n[topic: db] Maybe we need sharding later.\n\n[topic: db] Which backup tool should we use?")
	echo := message("[topic: db] We decided on Postgres 17 too. [rel: restates: Postgres]")
	revision := message("[topic: db] We decided to use SQLite instead. [rel: replaces: Postgres]")
	answer := message("[topic: db] We will use Restic for backups. [rel: answers: backup tool]")
	rival := message("[topic: db] We decided to use MySQL. [rel: contradicts: SQLite]")
	call := map[string]any{"type": "function_call", "id": tap.NewID("fc"), "call_id": tap.NewID("call"),
		"name": "lookup", "arguments": `{"query": "[topic: ports] postgres"}`}
	output := map[string]any{"type": "function_call_output", "call_id": call["call_id"], "output": "5432"}

	stop := h.consumer(h.classifier())
	for _, step := range []struct {
		master string
		it     map[string]any
	}{{"master-a", plan}, {"master-b", echo}, {"master-a", revision}, {"master-b", answer},
		{"master-b", rival}, {"master-a", call}, {"master-a", output}} {
		h.publish(step.master, step.it) // one at a time: order matters
	}
	rows := h.waitProcessed(7)
	text := h.handoff(handoff.Options{})
	stop()

	got := map[string]entryRow{}
	for _, r := range rows {
		key := strings.SplitN(r.text, "] ", 2)[1]
		got[strings.SplitN(key, " [rel", 2)[0]] = r
	}
	rel := func(r entryRow) string {
		if r.relation == nil {
			return ""
		}
		return *r.relation
	}
	check := func(key, status, relation string) {
		r, ok := got[key]
		if !ok || r.status != status || (relation != "*" && rel(r) != relation) {
			t.Errorf("%q: %+v (relation %s), want %s %s", key, r, rel(r), status, relation)
		}
	}
	check("We decided to use Postgres 17.", "superseded", "*")
	check("Maybe we need sharding later.", "active", "*")
	check("Which backup tool should we use?", "resolved", "*")
	check("We decided on Postgres 17 too.", "duplicate", "restates")
	check("We decided to use SQLite instead.", "active", "replaces")
	check("We will use Restic for backups.", "active", "answers")
	check("We decided to use MySQL.", "active", "contradicts")
	if got["Maybe we need sharding later."].kind != "hypothesis" || got["Which backup tool should we use?"].kind != "open_question" {
		t.Error("kinds")
	}
	if len(rows) != 8 { // 3 parts, 4 one-part messages, 1 tool exchange; the call alone has none
		t.Errorf("%d entries", len(rows))
	}
	if !strings.Contains(text, "## db (decisions from master-a and master-b; check that they agree)") {
		t.Errorf("no several-deciders flag:\n%s", text)
	}
	for _, line := range strings.Split(text, "\n") {
		if (strings.Contains(line, "SQLite instead") || strings.Contains(line, "use MySQL")) && !strings.Contains(line, "conflicts with #") {
			t.Errorf("conflict not marked: %s", line)
		}
	}
	if !strings.Contains(text, "Restic") || !strings.Contains(text, `lookup({"query": "[topic: ports] postgres"}) returned`) {
		t.Errorf("missing entries:\n%s", text)
	}
	for _, gone := range []string{"Postgres 17.", "Postgres 17 too", "sharding", "Which backup tool"} {
		if strings.Contains(text, gone) {
			t.Errorf("%q still shown", gone)
		}
	}
}

func TestRankingCapsAndNoise(t *testing.T) {
	h := setup(t)
	var items []map[string]any
	for i, s := range []int{3, 5, 4} {
		items = append(items, message(fmt.Sprintf("[topic: perf] Benchmark %d ran. [score: %d]", i, s)))
	}
	items = append(items, message("[topic: perf] Okay, continuing. [score: 2]"), message("[topic: perf] Is the cache warm? [score: 3]"))
	stop := h.consumer(h.classifier())
	h.publish("master-a", items...)
	h.waitProcessed(5)
	capped := h.handoff(handoff.Options{PerTopic: 2})
	small := h.handoff(handoff.Options{MaxChars: 150})
	stop()
	perf := strings.Split(strings.SplitN(strings.SplitN(capped, "## perf\n", 2)[1], "\n\n", 2)[0], "\n")
	if len(perf) != 2 || !strings.Contains(perf[0], "Benchmark 1") || !strings.Contains(perf[1], "Benchmark 2") {
		t.Errorf("ranking %q", perf)
	}
	if strings.Contains(capped, "continuing") || !strings.Contains(capped, "Is the cache warm?") {
		t.Errorf("noise or question:\n%s", capped)
	}
	if !strings.Contains(small, "lower-priority entries were left out for length") {
		t.Errorf("budget:\n%s", small)
	}
}

type flaky struct{ calls, failures atomic.Int32 }

func (f *flaky) Classify(context.Context, string, string, classifier.Topics) (classifier.Classification, error) {
	if f.calls.Add(1) <= f.failures.Load() {
		return classifier.Classification{}, errors.New("provider down")
	}
	return classifier.Classification{Topic: "general", Kind: "fact", Final: true, Score: 3}, nil
}

func (f *flaky) Relate(context.Context, string, []classifier.Candidate) (classifier.Relation, error) {
	return classifier.NewRelation(1), nil
}

func TestClassifierFailureIsRetried(t *testing.T) {
	h := setup(t)
	c := &flaky{}
	c.failures.Store(1)
	stop := h.consumer(c)
	h.publish("master-a", message("NATS listens on 4222."))
	if n := len(h.waitEvents(1)); n != 1 { // raw item stored even while classification fails
		t.Fatalf("%d events", n)
	}
	rows := h.waitProcessed(1)
	stop()
	if c.calls.Load() != 2 || len(rows) != 1 || rows[0].topic != "general" || rows[0].kind != "fact" || rows[0].status != "active" {
		t.Errorf("calls %d rows %+v", c.calls.Load(), rows)
	}
}

func httpDo(t *testing.T, method, url, body string, headers map[string]string) (int, string) {
	req, _ := http.NewRequest(method, url, strings.NewReader(body))
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func TestHandoffStartsAFreshMaster(t *testing.T) {
	h := setup(t)
	api := httptest.NewServer((&handoff.API{DB: h.db, Token: "secret"}).Handler())
	defer api.Close()
	auth := map[string]string{"Authorization": "Bearer secret"}
	base := api.URL + "/v1/sessions/" + h.session
	if code, _ := httpDo(t, "GET", base+"/handoff", "", nil); code != 401 {
		t.Errorf("no token: %d", code)
	}
	code, body := httpDo(t, "PUT", base, `{"task": "Ship the MVP.", "topics": {"DB": "storage choices"}}`, auth)
	if code != 200 || !strings.Contains(body, `"topics":{"db":"storage choices"}`) {
		t.Errorf("put: %d %s", code, body)
	}
	stop := h.consumer(h.classifier())
	h.publish("master-a", message("[topic: db] We decided to use Postgres 17."))
	h.publish("master-b", message("[topic: ports] Postgres listens on 5432."))
	h.waitProcessed(2)
	stop()

	_, body = httpDo(t, "GET", base+"/handoff", "", auth)
	var full handoff.Result
	var raw struct {
		Input []json.RawMessage `json:"input"`
	}
	if err := json.Unmarshal([]byte(body), &full); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(body), &raw); err != nil {
		t.Fatal(err)
	}
	_, without := httpDo(t, "GET", base+"/handoff?exclude_master=master-b", "", auth)
	if code, _ := httpDo(t, "GET", base+"/handoff?per_topic=0", "", auth); code != 400 {
		t.Errorf("per_topic=0: %d", code)
	}
	var last int64
	if err := h.db.QueryRow(h.ctx, "SELECT max(stream_seq) FROM events WHERE session = $1", h.session).Scan(&last); err != nil {
		t.Fatal(err)
	}
	if full.UptoSeq != last || full.Pending != 0 || full.Items != 2 || len(raw.Input) != 1 {
		t.Errorf("handoff %+v (last %d)", full, last)
	}
	var msg struct {
		Type, Role string
		Content    []struct{ Type, Text string }
	}
	if err := json.Unmarshal(raw.Input[0], &msg); err != nil {
		t.Fatal(err)
	}
	text := msg.Content[0].Text
	if msg.Type != "message" || msg.Role != "user" || msg.Content[0].Type != "input_text" {
		t.Errorf("item %+v", msg)
	}
	for _, want := range []string{"The task: Ship the MVP.", "[decision from master-a, seq 1, #", "We decided to use Postgres 17.", "[fact from master-b, seq 1, #"} {
		if !strings.Contains(text, want) {
			t.Errorf("missing %q in\n%s", want, text)
		}
	}
	if !strings.Contains(without, `"items":1`) || strings.Contains(without, "master-b") {
		t.Errorf("exclude: %s", without)
	}
	// A fresh master accepts the handoff as the start of its input.
	resp, err := h.gateway().Response(h.ctx, map[string]any{
		"model": "mock",
		"input": []any{raw.Input[0], map[string]any{"role": "user", "content": "What did we decide for the db?"}},
	})
	if err != nil || resp["status"] != "completed" || !strings.Contains(oai.OutputText(resp), "What did we decide") {
		t.Errorf("fresh master: %v %v", resp, err)
	}
}

func TestMastersPullAndShareMidRun(t *testing.T) {
	h := setup(t)
	srv := mcpserver.New(h.db, natsURL, "")
	defer srv.Close()
	mcpHTTP := httptest.NewServer(srv.Handler())
	defer mcpHTTP.Close()

	stop := h.consumer(h.classifier())
	h.publish("master-a",
		message("[topic: storage] We decided to use SQLite for the MVP."),
		message("[topic: http_api] Which status code should redirects use?"))
	h.waitProcessed(2)

	// master-b pulls mid-run, then answers with a declared decision.
	tp := h.tap("master-b")
	tools, err := master.ConnectShared(h.ctx, mcpHTTP.URL+"/mcp", h.session, "master-b", "")
	if err != nil {
		t.Fatal(err)
	}
	if !tools.Has("get_shared_context") || !tools.Has("share_context") || len(tools.Tools) != 2 {
		t.Fatalf("tools %v", tools.Tools)
	}
	pulled, err := master.Run(h.ctx, tp, h.gateway(), "[tool: get_shared_context {}]", master.Options{Model: "mock", Shared: tools})
	if err != nil {
		t.Fatal(err)
	}
	share := `[tool: share_context {"text": "Redirects use 302 so links can be edited.", "kind": "decision", "topic": "http_api"}]`
	shared, err := master.Run(h.ctx, tp, h.gateway(), share, master.Options{Model: "mock", Shared: tools})
	if err != nil {
		t.Fatal(err)
	}
	tools.Close()
	tp.Close(5 * time.Second)
	h.waitProcessed(9)
	stop()

	m := regexp.MustCompile(`since_seq=(\d+)`).FindStringSubmatch(pulled)
	if m == nil || !strings.Contains(pulled, "We decided to use SQLite for the MVP.") {
		t.Fatalf("pulled:\n%s", pulled)
	}
	if !strings.Contains(shared, "Shared as a decision") {
		t.Errorf("shared: %s", shared)
	}
	var since int64
	if _, err := fmt.Sscan(m[1], &since); err != nil {
		t.Fatal(err)
	}
	changes, err := handoff.Pull(h.ctx, h.db, h.session, handoff.PullOptions{SinceSeq: since, ExcludeMaster: "master-a"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(changes.Text, "[decision from master-b, declared, #") || !strings.Contains(changes.Text, "Redirects use 302 so links can be edited.") {
		t.Errorf("changes:\n%s", changes.Text)
	}
	// The same pull over HTTP.
	api := httptest.NewServer((&handoff.API{DB: h.db}).Handler())
	defer api.Close()
	_, body := httpDo(t, "GET", fmt.Sprintf("%s/v1/sessions/%s/context?since_seq=%d&exclude_master=master-a", api.URL, h.session, since), "", nil)
	var overHTTP handoff.Changes
	if err := json.Unmarshal([]byte(body), &overHTTP); err != nil {
		t.Fatal(err)
	}
	if overHTTP.Text != changes.Text {
		t.Errorf("HTTP pull differs:\n%s", body)
	}
	// One entry from master-b's tool use: the share. The pulled context and the tap's copies of
	// the tool calls made none.
	rows, _ := h.db.Query(h.ctx, `SELECT e.via, e.item->>'type', x.kind, x.topic, x.score, x.declared, x.status
		FROM entries x JOIN events e ON e.id = x.event_id
		WHERE x.session = $1 AND x.master = 'master-b' AND e.item->>'type' <> 'message'`, h.session)
	toolEntries, _ := pgx.CollectRows(rows, func(r pgx.CollectableRow) (string, error) {
		var via, typ, kind, topic, status string
		var score float64
		var declared bool
		err := r.Scan(&via, &typ, &kind, &topic, &score, &declared, &status)
		return fmt.Sprint(via, typ, kind, topic, score, declared, status), err
	})
	if !slices.Equal(toolEntries, []string{fmt.Sprint("mcp", "function_call", "decision", "http_api", 5.0, true, "active")}) {
		t.Errorf("tool entries %v", toolEntries)
	}
}

func TestShareTopicVariantsMapToKnownTopics(t *testing.T) {
	h := setup(t)
	srv := mcpserver.New(h.db, natsURL, "")
	defer srv.Close()
	stop := h.consumer(h.classifier())
	h.publish("master-a", message("[topic: storage] We decided to use SQLite for the MVP."))
	h.waitProcessed(1)
	// A variant of a known topic goes to the topic the classifier picks; a topic the
	// classifier also finds new (the mock says "general") keeps the master's name.
	for _, args := range []map[string]any{
		{"text": "[topic: storage] Postgres replaces SQLite.", "kind": "decision", "topic": "Storage Layer"},
		{"text": "Redirects are served from a CDN.", "kind": "decision", "topic": "edge"},
	} {
		if _, err := srv.Publisher.Publish(h.ctx, h.session, "master-b", args); err != nil {
			t.Fatal(err)
		}
	}
	h.waitProcessed(3)
	stop()
	rows, _ := h.db.Query(h.ctx, `SELECT topic FROM entries WHERE session = $1 AND declared ORDER BY id`, h.session)
	topics, _ := pgx.CollectRows(rows, pgx.RowTo[string])
	if !slices.Equal(topics, []string{"storage", "edge"}) {
		t.Errorf("declared topics %v", topics)
	}
	pulled, err := handoff.Pull(h.ctx, h.db, h.session, handoff.PullOptions{ExcludeMaster: "master-c"})
	if err != nil || !strings.Contains(pulled.Text, "Topics: edge; storage. Use one of these") {
		t.Errorf("pull: %v\n%s", err, pulled.Text)
	}
}

func TestShareReplacesNamedEntries(t *testing.T) {
	h := setup(t)
	srv := mcpserver.New(h.db, natsURL, "")
	defer srv.Close()
	mcpHTTP := httptest.NewServer(srv.Handler())
	defer mcpHTTP.Close()
	stop := h.consumer(h.classifier())
	defer stop()
	h.publish("master-a", message("[topic: storage] We decided to use SQLite for the MVP."))
	h.waitProcessed(1)

	tools, err := master.ConnectShared(h.ctx, mcpHTTP.URL+"/mcp", h.session, "master-b", "")
	if err != nil {
		t.Fatal(err)
	}
	defer tools.Close()
	entryID := regexp.MustCompile(`entry #(\d+)`)
	share := func(args string) (string, string) {
		out, err := tools.Call(h.ctx, "share_context", args)
		if err != nil {
			t.Fatal(err)
		}
		m := entryID.FindStringSubmatch(out)
		if m == nil {
			t.Fatalf("no entry id in %q", out)
		}
		return m[1], out
	}
	first, _ := share(`{"text": "[topic: storage] Use Postgres 16.", "kind": "decision", "topic": "storage"}`)
	second, _ := share(`{"text": "[topic: storage] Use the users table in Postgres.", "kind": "decision", "topic": "storage"}`)
	// Revising both at once: the master's own entries are superseded.
	_, out := share(`{"text": "[topic: storage] Use Postgres 17 with one accounts table.", "kind": "decision", "topic": "storage", "replaces": [` + first + `, "#` + second + `"]}`)
	if !strings.Contains(out, "It replaces your #"+first+", #"+second) {
		t.Errorf("replace result: %s", out)
	}
	// Naming another master's entry is a conflict, not a retraction.
	var sqlite int64
	if err := h.db.QueryRow(h.ctx, `SELECT id FROM entries WHERE session = $1 AND master = 'master-a'`, h.session).Scan(&sqlite); err != nil {
		t.Fatal(err)
	}
	_, out = share(fmt.Sprintf(`{"text": "[topic: storage] Not SQLite: it cannot take concurrent writers.", "kind": "decision", "topic": "storage", "replaces": [%d]}`, sqlite))
	if !strings.Contains(out, fmt.Sprintf("It conflicts with #%d from master-a", sqlite)) {
		t.Errorf("conflict result: %s", out)
	}
	if out, err := tools.Call(h.ctx, "share_context", `{"text": "x", "kind": "decision", "replaces": ["first"]}`); err != nil || entryID.MatchString(out) {
		t.Errorf("bad replaces accepted: %q %v", out, err)
	}
	rows, _ := h.db.Query(h.ctx, `SELECT master, status FROM entries WHERE session = $1 ORDER BY id`, h.session)
	statuses, _ := pgx.CollectRows(rows, func(r pgx.CollectableRow) (string, error) {
		var m, st string
		err := r.Scan(&m, &st)
		return m + ":" + st, err
	})
	want := []string{"master-a:active", "master-b:superseded", "master-b:superseded", "master-b:active", "master-b:active"}
	if !slices.Equal(statuses, want) {
		t.Errorf("statuses %v, want %v", statuses, want)
	}
}

func TestStreamedMessageSharesPartsBeforeItEnds(t *testing.T) {
	h := setup(t)
	prompt := "[tool: none][topic: storage] We decided to use SQLite.\n\n[topic: ids] IDs are base62.\n\n[topic: api] Redirects return 302."
	stop := h.consumer(h.classifier())
	tp := h.tap("master-a")
	answer, err := master.Run(h.ctx, tp, h.gateway(), prompt, master.Options{Model: "mock"})
	if err != nil {
		t.Fatal(err)
	}
	tp.Close(5 * time.Second)
	h.waitProcessed(5) // call, output, 2 snapshots, message
	stop()
	rows, _ := h.db.Query(h.ctx, `SELECT x.text, coalesce(e.rev, 0) FROM entries x JOIN events e ON e.id = x.event_id
		WHERE x.session = $1 AND e.item->>'type' = 'message' ORDER BY x.part`, h.session)
	var texts []string
	var revs []int
	var text string
	var rev int
	if _, err := pgx.ForEachRow(rows, []any{&text, &rev}, func() error { texts = append(texts, text); revs = append(revs, rev); return nil }); err != nil {
		t.Fatal(err)
	}
	// Each part was created once, by the first event in which it was final.
	if !slices.Equal(texts, segment.Segment(answer)) || !slices.Equal(revs, []int{1, 2, 0}) {
		t.Errorf("texts %q revs %v", texts, revs)
	}
}

// topicAware is a slow classifier: "NEW" entries create topic "fresh"; other entries say
// whether "fresh" was among the topics they were classified with.
type topicAware struct{ calls atomic.Int32 }

func (c *topicAware) Classify(_ context.Context, text, _ string, topics classifier.Topics) (classifier.Classification, error) {
	c.calls.Add(1)
	time.Sleep(50 * time.Millisecond)
	topic := "stale"
	switch {
	case strings.HasPrefix(text, "NEW"):
		topic = "fresh"
	case topics.Has("fresh"):
		topic = "saw_fresh"
	}
	return classifier.Classification{Topic: topic, Kind: "fact", Final: true, Score: 4}, nil
}

func (c *topicAware) Relate(context.Context, string, []classifier.Candidate) (classifier.Relation, error) {
	return classifier.NewRelation(1), nil
}

func TestClassifyAheadSeesEarlierTopics(t *testing.T) {
	h := setup(t)
	c := &topicAware{}
	// One message: the first part creates a topic; the later parts were classified ahead,
	// before it existed, and must be classified again.
	h.publish("master-a", message("A first fact.\n\nNEW topic here.\n\nLater fact one.\n\nLater fact two."))
	stop := h.consumer(c)
	rows := h.waitProcessed(1)
	stop()
	var topics []string
	for _, r := range rows {
		topics = append(topics, r.topic)
	}
	if !slices.Equal(topics, []string{"stale", "fresh", "saw_fresh", "saw_fresh"}) {
		t.Errorf("topics %v", topics)
	}
	if c.calls.Load() < 5 {
		t.Errorf("only %d classifications: nothing was classified ahead", c.calls.Load())
	}
}
