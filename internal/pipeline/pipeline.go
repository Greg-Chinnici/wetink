// Package pipeline turns stored events into classified entries with a lifecycle.
//
// For each event, in stream order (see docs/shared-context.md):
//  1. Split the item into parts (segment); each part becomes an entry. Tool exchanges are one
//     entry (the call is rendered with its output); bare function_call items have no entries.
//  2. Classify each entry: topic (the session's topics, then recently used ones, else a new
//     one), kind and relevance.
//  3. If the entry would be visible (not a hypothesis, relevance >= 3), relate it to the
//     active entries on its topic and apply the result (Decide):
//     restates    -> the new entry is a duplicate and stays hidden;
//     replaces    -> the older entry is superseded if the same master wrote it, otherwise
//     this is a conflict between masters (contradicts);
//     answers     -> the open question is resolved;
//     contradicts -> both stay visible and the handoff marks the conflict;
//     new         -> nothing else changes.
//
// Parts of one item are processed in order, so a later part can relate to an earlier one.
//
// The shared-context tools (mcpserver) are special:
//   - share_context: the MCP server publishes the call itself (via "mcp"); it becomes one
//     declared entry, with the master's kind (and topic, if given) and top relevance. It is
//     still related to existing entries, so it can replace, answer or restate them.
//   - The tap's copies of these tool calls and their outputs never become entries: a pulled
//     context would otherwise come back as the pulling master's own entries.
//
// Streamed messages: a snapshot (rev) of a message still being written creates entries for
// its final parts (segment.StreamedParts) right away; later snapshots and the finished item
// add only the parts not seen yet. Entries are keyed by (master, item_id, part), not by event.
package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"

	"wetink/internal/classifier"
	"wetink/internal/envelope"
	"wetink/internal/item"
	"wetink/internal/ojson"
	"wetink/internal/pystr"
	"wetink/internal/segment"
	"wetink/internal/store"
)

const (
	// Closing (superseding, resolving) or hiding (duplicate) an entry needs this probability.
	// Correct cases scored 0.78-0.95 on the scenarios; a load-test fact "replacing" a decision
	// scored 0.51 and hid the session's current storage decision.
	CloseMinProb          = 0.7
	RelateTopicCandidates = 6
	RelateOtherCandidates = 4
	CandidateChars        = 400
	RecentTopics          = 30

	PullTool  = "get_shared_context"
	ShareTool = "share_context"
	// A declared entry is what its master chose to tell the others: always shown.
	DeclaredScore = 5.0
)

var ShareKinds = []string{"decision", "fact", "open_question", "artifact"}

func IsWetinkTool(name string) bool { return name == PullTool || name == ShareTool }

func isShareKind(k string) bool {
	for _, x := range ShareKinds {
		if x == k {
			return true
		}
	}
	return false
}

const selectCall = `
SELECT item FROM events
WHERE session = $1 AND master = $2 AND item->>'type' = 'function_call'
  AND item->>'call_id' = $3
LIMIT 1`

const insertEntry = `
INSERT INTO entries (event_id, item_id, part, session, master, seq, stream_seq, text)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT DO NOTHING`

const selectItemParts = `SELECT part, text FROM entries WHERE master = $1 AND item_id = $2`

const selectPending = `
SELECT id, text FROM entries WHERE master = $1 AND item_id = $2 AND status = 'pending'
ORDER BY part`

const selectSessionTopics = `SELECT topics FROM sessions WHERE session = $1`

// The most recently used topics, in order of first use: a stable order, so the choices the
// model sees do not shift between entries (and entries classified ahead stay valid).
const selectRecentTopics = `
SELECT topic FROM (
    SELECT topic, min(stream_seq) AS first, max(stream_seq) AS last
    FROM entries WHERE session = $1 AND topic IS NOT NULL GROUP BY topic
    ORDER BY last DESC LIMIT $2
) t ORDER BY first, topic`

// What a new entry is related to: recent entries on its topic, plus the session's open
// questions and the same master's recent entries on any topic, since topic assignment is not
// perfect and answers and revisions (usually by the same master) must still connect. Only
// visible, active entries.
const selectCandidates = `
WITH shown AS (
    SELECT id, kind, master, text, topic, stream_seq, part FROM context_entries
    WHERE session = @session AND id <> @id
), picked AS (
    (SELECT * FROM shown WHERE topic = @topic
     ORDER BY stream_seq DESC, part DESC LIMIT @n_topic)
    UNION
    (SELECT * FROM shown WHERE kind = 'open_question'
     ORDER BY stream_seq DESC, part DESC LIMIT @n_other)
    UNION
    (SELECT * FROM shown WHERE master = @master
     ORDER BY stream_seq DESC, part DESC LIMIT @n_other)
)
SELECT id, kind, master, text FROM picked ORDER BY stream_seq DESC, part DESC`

const updateEntry = `
UPDATE entries
SET topic = $1, kind = $2, score = $3, confidence = $4, status = $5, relation = $6,
    related_id = $7, relation_prob = $8, trace = $9, declared = $10, classified_at = now()
WHERE id = $11 AND status = 'pending'`

const closeEntry = `
UPDATE entries SET status = $1, closed_by = $2 WHERE id = $3 AND status = 'active'`

type EventRow struct {
	ID        int64
	Session   string
	Master    string
	Seq       int64
	StreamSeq int64
	Item      map[string]any
	Via       string
	Rev       int // > 0: a snapshot of a message still being written
}

type EventResult struct {
	Entries, Visible, Closed, Conflicts int
}

// ShareArgs returns the arguments of a share_context call published by the MCP server.
func ShareArgs(it map[string]any, via string) (map[string]any, bool) {
	if via != envelope.ViaMCP || item.String(it, "type") != "function_call" || item.String(it, "name") != ShareTool {
		return nil, false
	}
	var args map[string]any
	raw, _ := it["arguments"].(string)
	if raw == "" {
		raw = "{}"
	}
	if err := json.Unmarshal([]byte(raw), &args); err != nil || args == nil {
		return nil, false
	}
	if pystr.Strip(textArg(args)) == "" {
		return nil, false
	}
	return args, true
}

func textArg(args map[string]any) string {
	switch v := args["text"].(type) {
	case nil:
		return ""
	case string:
		return v
	default:
		b, _ := json.Marshal(v)
		return string(b)
	}
}

// ItemParts are an item's entry texts; for a snapshot (partial), only the final parts.
func ItemParts(it, call map[string]any, via string, partial bool) []string {
	switch item.String(it, "type") {
	case "message":
		text := item.TextOf(it["content"])
		if partial {
			return segment.StreamedParts(text)
		}
		return segment.Segment(text)
	case "function_call":
		if args, ok := ShareArgs(it, via); ok {
			return []string{pystr.Strip(textArg(args))}
		}
		return nil
	case "function_call_output":
		if call != nil && IsWetinkTool(item.String(call, "name")) {
			return nil
		}
		if text, ok := item.Render(it, call); ok {
			return []string{text}
		}
	}
	return nil
}

// Label is how an entry is shown to the relation question: kind, author, (shortened) text.
func Label(kind, master, text string) string {
	short := text
	if pystr.Len(text) > CandidateChars {
		short = pystr.Prefix(text, CandidateChars) + " ..."
	}
	return fmt.Sprintf("[%s from %s] %s", strings.ReplaceAll(kind, "_", " "), master, short)
}

// SessionTopics are the session's declared topics (with descriptions), then recent ones.
func SessionTopics(ctx context.Context, db store.DB, session string) (classifier.Topics, error) {
	var topics classifier.Topics
	var raw []byte
	err := db.QueryRow(ctx, selectSessionTopics, session).Scan(&raw)
	if err != nil && err != pgx.ErrNoRows {
		return nil, err
	}
	if len(raw) > 0 {
		v, err := ojson.Decode(raw)
		if err != nil {
			return nil, err
		}
		if obj, ok := v.(*ojson.Object); ok {
			for _, k := range obj.Keys() {
				d, _ := obj.Get(k)
				t := classifier.Topic{Name: k}
				if s, ok := d.(string); ok {
					t.Desc = &s
				}
				topics = append(topics, t)
			}
		}
	}
	rows, err := db.Query(ctx, selectRecentTopics, session, RecentTopics)
	if err != nil {
		return nil, err
	}
	names, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, err
	}
	for _, n := range names {
		topics.Add(n)
	}
	return topics, nil
}

type pending struct {
	id   int64
	text string
}

func ensureEntries(ctx context.Context, db store.DB, ev EventRow) ([]pending, error) {
	var call map[string]any
	if item.String(ev.Item, "type") == "function_call_output" {
		err := db.QueryRow(ctx, selectCall, ev.Session, ev.Master, item.String(ev.Item, "call_id")).Scan(&call)
		if err != nil && err != pgx.ErrNoRows {
			return nil, err
		}
	}
	itemID := item.String(ev.Item, "id")
	parts := ItemParts(ev.Item, call, ev.Via, ev.Rev > 0)
	err := pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		for part, text := range parts {
			if _, err := tx.Exec(ctx, insertEntry, ev.ID, itemID, part, ev.Session, ev.Master, ev.Seq, ev.StreamSeq, text); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if ev.Rev > 0 || len(parts) > 1 {
		// Parts made from earlier snapshots must match; segment.StreamedParts guarantees it.
		rows, err := db.Query(ctx, selectItemParts, ev.Master, itemID)
		if err != nil {
			return nil, err
		}
		stored := map[int]string{}
		var part int
		var text string
		_, err = pgx.ForEachRow(rows, []any{&part, &text}, func() error { stored[part] = text; return nil })
		if err != nil {
			return nil, err
		}
		for i, text := range parts {
			if stored[i] != text {
				slog.Warn("streamed part changed", "item", itemID, "part", i)
			}
		}
	}
	rows, err := db.Query(ctx, selectPending, ev.Master, itemID)
	if err != nil {
		return nil, err
	}
	var out []pending
	var p pending
	_, err = pgx.ForEachRow(rows, []any{&p.id, &p.text}, func() error { out = append(out, p); return nil })
	return out, err
}

// Outcome is what a relation does to the new entry (and maybe an older one).
type Outcome struct {
	Status   string // of the new entry: active or duplicate
	Relation string
	Related  *int64
	Close    *Close
	Also     []Close // further entries closed (a share that replaces several)
}

type Close struct {
	Status string // superseded | resolved
	Target int64
}

var NewOutcome = Outcome{Status: "active", Relation: "new"}

// CandidateInfo is a candidate's kind and author.
type CandidateInfo struct{ Kind, Master string }

// Decide turns a relation into an outcome. When unsure, keep things visible: hiding a
// current decision is worse than showing a stale one, so closing or hiding needs
// CloseMinProb and a target of a compatible kind.
func Decide(kind, master string, rel classifier.Relation, candidates map[int64]CandidateInfo) Outcome {
	if rel.Relation == "new" || rel.Target == nil {
		return NewOutcome
	}
	target, ok := candidates[*rel.Target]
	if !ok {
		return NewOutcome
	}
	id := *rel.Target
	sure := rel.Probability >= CloseMinProb
	switch rel.Relation {
	case "replaces", "contradicts":
		if target.Master != master {
			// One master cannot retract another's entry: that is a conflict, shown as such.
			return Outcome{Status: "active", Relation: "contradicts", Related: &id}
		}
		// A master revising its own entry, with the same kind (a load-test fact does not
		// revoke a decision). Replacing and contradicting one's own entry are the same change.
		if (sure || rel.ChangeProbability >= CloseMinProb) && kind == target.Kind {
			return Outcome{Status: "active", Relation: "replaces", Related: &id, Close: &Close{"superseded", id}}
		}
	case "answers":
		if sure && target.Kind == "open_question" {
			return Outcome{Status: "active", Relation: "answers", Related: &id, Close: &Close{"resolved", id}}
		}
	case "restates":
		// Adopting a fact or suggestion as a decision is a commitment, not a repeat.
		if sure && (kind != "decision" || target.Kind == "decision") {
			return Outcome{Status: "duplicate", Relation: "restates", Related: &id}
		}
	}
	return NewOutcome
}

// ReplaceIDs are the entry ids a share_context call names in replaces (41 or "#41").
func ReplaceIDs(share map[string]any) []int64 {
	raw, _ := share["replaces"].([]any)
	var ids []int64
	for _, v := range raw {
		switch x := v.(type) {
		case float64:
			if x == math.Trunc(x) && x > 0 {
				ids = append(ids, int64(x))
			}
		case string:
			if n, err := strconv.ParseInt(strings.TrimPrefix(strings.TrimSpace(x), "#"), 10, 64); err == nil && n > 0 {
				ids = append(ids, n)
			}
		}
	}
	return ids
}

const selectReplaced = `
SELECT id, kind, master FROM entries
WHERE session = $1 AND id = ANY($2) AND status = 'active'
ORDER BY id`

// explicitOutcome is the outcome of a share that names the entries it replaces: the master's
// own active entries are superseded (whatever their kind: the master says so). Naming another
// master's entry is a conflict with it, since one master cannot retract another's. Unknown
// or closed ids are ignored; nil when none is left.
func explicitOutcome(ctx context.Context, db store.DB, ev EventRow, ids []int64, candidates map[int64]CandidateInfo) (*Outcome, map[string]any, error) {
	rows, err := db.Query(ctx, selectReplaced, ev.Session, ids)
	if err != nil {
		return nil, nil, err
	}
	var own, others []int64
	var id int64
	var kind, master string
	_, err = pgx.ForEachRow(rows, []any{&id, &kind, &master}, func() error {
		candidates[id] = CandidateInfo{kind, master}
		if master == ev.Master {
			own = append(own, id)
		} else {
			others = append(others, id)
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	trace := map[string]any{"explicit": ids, "superseded": own, "conflicts": others}
	switch {
	case len(own) > 0:
		o := Outcome{Status: "active", Relation: "replaces", Related: &own[0], Close: &Close{"superseded", own[0]}}
		for _, x := range own[1:] {
			o.Also = append(o.Also, Close{"superseded", x})
		}
		return &o, trace, nil
	case len(others) > 0:
		return &Outcome{Status: "active", Relation: "contradicts", Related: &others[0]}, trace, nil
	}
	return nil, trace, nil
}

func tags(ctx context.Context, c classifier.Classifier, text string, ev EventRow, topics classifier.Topics, share map[string]any) (classifier.Classification, error) {
	if share == nil {
		return c.Classify(ctx, text, ev.Master, topics)
	}
	kind, _ := share["kind"].(string)
	topic, _ := share["topic"].(string)
	if topic != "" {
		topic = classifier.NormalizeTopic(topic)
	}
	var t classifier.Classification
	if !isShareKind(kind) || topic == "" || !topics.Has(topic) {
		// Without a kind or topic the classifier supplies it. A topic the session does not use
		// yet is often a variant of an existing one ("storage_layer" for "storage"): the
		// classifier picks among the known topics, and the master's name is kept only for a
		// topic the classifier also finds new.
		var err error
		if t, err = c.Classify(ctx, text, ev.Master, topics); err != nil {
			return t, err
		}
		if topics.Has(topic) || (topic != "" && !topics.Has(t.Topic)) {
			t.Topic = topic
		}
	} else {
		t = classifier.Classification{Topic: topic}
	}
	if !isShareKind(kind) {
		kind = t.Kind
	}
	trace := map[string]any{}
	for k, v := range t.Trace {
		trace[k] = v
	}
	trace["declared"] = share
	one := 1.0
	t.Kind, t.Final, t.Score, t.Confidence, t.Trace = kind, classifier.FinalKinds[kind], DeclaredScore, &one, trace
	return t, nil
}

func apply(ctx context.Context, db store.DB, entryID int64, ev EventRow, t classifier.Classification,
	rel classifier.Relation, candidates map[int64]CandidateInfo, result *EventResult, declared bool, explicit *Outcome) error {
	outcome := Decide(t.Kind, ev.Master, rel, candidates)
	if explicit != nil {
		outcome = *explicit
	}
	shown := classifier.Visible(t.Kind, t.Score)
	var relate any
	if rel.Trace != nil {
		cands := map[string]any{}
		for id, c := range candidates {
			cands[fmt.Sprint(id)] = map[string]any{"kind": c.Kind, "master": c.Master}
		}
		r := map[string]any{}
		for k, v := range rel.Trace {
			r[k] = v
		}
		r["model_relation"], r["model_target"], r["candidates"] = rel.Relation, rel.Target, cands
		relate = r
	}
	var closeV any
	if outcome.Close != nil {
		closeV = []any{outcome.Close.Status, outcome.Close.Target}
	}
	if len(outcome.Also) > 0 {
		all := []any{closeV}
		for _, c := range outcome.Also {
			all = append(all, []any{c.Status, c.Target})
		}
		closeV = all
	}
	var classify any
	if t.Trace != nil {
		classify = t.Trace
	}
	trace := map[string]any{
		"classify": classify,
		"relate":   relate,
		"outcome":  map[string]any{"status": outcome.Status, "relation": outcome.Relation, "related": outcome.Related, "close": closeV},
	}
	err := pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, updateEntry, t.Topic, t.Kind, t.Score, t.Confidence, outcome.Status,
			outcome.Relation, outcome.Related, rel.Probability, trace, declared, entryID); err != nil {
			return err
		}
		var closes []Close
		if outcome.Close != nil {
			closes = append(closes, *outcome.Close)
		}
		for _, c := range append(closes, outcome.Also...) {
			tag, err := tx.Exec(ctx, closeEntry, c.Status, entryID, c.Target)
			if err != nil {
				return err
			}
			result.Closed += int(tag.RowsAffected())
		}
		return nil
	})
	if err != nil {
		return err
	}
	if outcome.Status == "active" && shown {
		result.Visible++
	}
	if outcome.Relation == "contradicts" {
		result.Conflicts++
		slog.Warn("conflict", "session", ev.Session, "topic", t.Topic, "entry", entryID, "master", ev.Master,
			"contradicts", *outcome.Related, "other_master", candidates[*outcome.Related].Master)
	}
	return nil
}

// relateAndApply relates a classified entry to the current context and stores the outcome.
func relateAndApply(ctx context.Context, db store.DB, c classifier.Classifier, ev EventRow, entryID int64,
	text string, t classifier.Classification, share map[string]any, result *EventResult) error {
	rel := classifier.NewRelation(1.0)
	candidates := map[int64]CandidateInfo{}
	if ids := ReplaceIDs(share); len(ids) > 0 {
		explicit, trace, err := explicitOutcome(ctx, db, ev, ids, candidates)
		if err != nil {
			return err
		}
		if explicit != nil {
			rel.Trace = trace
			if err := apply(ctx, db, entryID, ev, t, rel, candidates, result, true, explicit); err != nil {
				return err
			}
			result.Entries++
			return nil
		}
		// None of the ids is an active entry: relate as usual.
	}
	if classifier.Visible(t.Kind, t.Score) {
		rows, err := db.Query(ctx, selectCandidates, pgx.NamedArgs{
			"session": ev.Session, "id": entryID, "topic": t.Topic, "master": ev.Master,
			"n_topic": RelateTopicCandidates, "n_other": RelateOtherCandidates,
		})
		if err != nil {
			return err
		}
		var existing []classifier.Candidate
		var id int64
		var kind, master, ctext string
		_, err = pgx.ForEachRow(rows, []any{&id, &kind, &master, &ctext}, func() error {
			candidates[id] = CandidateInfo{kind, master}
			existing = append(existing, classifier.Candidate{ID: id, Text: Label(kind, master, ctext)})
			return nil
		})
		if err != nil {
			return err
		}
		if len(existing) > 0 {
			if rel, err = c.Relate(ctx, Label(t.Kind, ev.Master, text), existing); err != nil {
				return err
			}
		}
	}
	if err := apply(ctx, db, entryID, ev, t, rel, candidates, result, share != nil, nil); err != nil {
		return err
	}
	result.Entries++
	return nil
}

// ProcessEvent creates, classifies and relates the entries of one event. Safe to retry.
func ProcessEvent(ctx context.Context, db store.DB, ev EventRow, c classifier.Classifier) (EventResult, error) {
	var result EventResult
	_, err := ProcessEvents(ctx, db, []EventRow{ev}, c, 1, func(_ EventRow, r EventResult) error {
		result = r
		return nil
	})
	return result, err
}

type work struct {
	event int // index into the events
	id    int64
	text  string
	share map[string]any
}

type tagged struct {
	tags    classifier.Classification
	err     error
	version int // of the topics it was classified with
}

// ProcessEvents processes one session's events, in stream order, and calls done after each
// event's entries are all stored. Relating and storing is strictly in order: each entry is
// related to the context as it stood before it. Classification depends only on the entry and
// the session's topics, so up to `ahead` entries are classified ahead, concurrently; one is
// classified again if an earlier entry added a topic since.
//
// It returns how many events were completed; on an error, the events from that index on are
// not done (their entries may exist, pending, which is safe to retry).
func ProcessEvents(ctx context.Context, db store.DB, events []EventRow, c classifier.Classifier, ahead int,
	done func(EventRow, EventResult) error) (int, error) {
	if len(events) == 0 {
		return 0, nil
	}
	ahead = max(ahead, 1)
	var items []work
	prepared := len(events)
	var prepErr error
	for i, ev := range events {
		pend, err := ensureEntries(ctx, db, ev)
		if err != nil {
			prepared, prepErr = i, err
			break
		}
		share, _ := ShareArgs(ev.Item, ev.Via)
		for _, p := range pend {
			items = append(items, work{i, p.id, p.text, share})
		}
	}
	topics, err := SessionTopics(ctx, db, events[0].Session)
	if err != nil {
		return 0, err
	}
	version := 0

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	futures := make([]chan tagged, len(items))
	launched := 0
	launch := func() {
		k := launched
		launched++
		ch := make(chan tagged, 1)
		futures[k] = ch
		snapshot, v, w := slices.Clone(topics), version, items[k]
		go func() {
			t, err := tags(ctx, c, w.text, events[w.event], snapshot, w.share)
			ch <- tagged{t, err, v}
		}()
	}
	for launched < len(items) && launched < ahead {
		launch()
	}

	k := 0
	for i, ev := range events[:prepared] {
		var result EventResult
		for ; k < len(items) && items[k].event == i; k++ {
			r := <-futures[k]
			if r.err == nil && r.version != version {
				r.tags, r.err = tags(ctx, c, items[k].text, ev, topics, items[k].share)
			}
			if r.err != nil {
				return i, r.err
			}
			if !topics.Has(r.tags.Topic) {
				topics.Add(r.tags.Topic)
				version++
			}
			if err := relateAndApply(ctx, db, c, ev, items[k].id, items[k].text, r.tags, items[k].share, &result); err != nil {
				return i, err
			}
			if launched < len(items) {
				launch()
			}
		}
		if err := done(ev, result); err != nil {
			return i, err
		}
	}
	return prepared, prepErr
}
