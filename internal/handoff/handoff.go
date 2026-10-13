// Package handoff renders a session's shared context: as a portable Open Responses `input`
// array for a master that starts (Snapshot), or as the changes since a stream sequence for a
// running master (Pull). The HTTP API is in api.go.
//
// A handoff holds at most one `user` message that renders the shared context as text (see
// docs/shared-context.md):
//   - rebuilt from Postgres only (no provider state, no previous_response_id);
//   - active entries only: superseded, answered and duplicate entries and hypotheses are left
//     out; open questions get their own section;
//   - one attributed line per entry with a stable id (#41); conflicting entries say so;
//   - tool exchanges are text, never call/output items, and other masters' output is never
//     replayed as `assistant` messages;
//   - fitted to max_chars in priority order: conflicts, decisions, facts and artifacts by
//     relevance, then open questions.
//
// upto_seq is the highest stream sequence such that every item of the session at or below it
// has been processed. Later items may already be included; pending counts the ones that are
// not processed yet.
package handoff

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"

	"wetink/internal/classifier"
	"wetink/internal/ojson"
	"wetink/internal/pystr"
	"wetink/internal/store"
)

const (
	DefaultPerTopic      = 8
	MaxPerTopic          = 20
	DefaultMaxChars      = 8000
	ArtifactPreviewChars = 600
	EntryChars           = 1500
)

const selectSession = `SELECT task FROM sessions WHERE session = $1`

const upsertSession = `
INSERT INTO sessions (session, task, topics) VALUES ($1, $2, $3)
ON CONFLICT (session) DO UPDATE SET task = EXCLUDED.task, topics = EXCLUDED.topics`

// Ranked entries, plus every active entry in a conflict (shown past the per-topic cap).
const selectContext = `
SELECT c.id, c.topic, c.kind, c.score, c.master, c.seq, c.stream_seq, c.text, c.topic_rank,
       c.declared
FROM context_ranked c
WHERE c.session = @session
  AND (@exclude::text IS NULL OR c.master <> @exclude)
  AND (@topic::text IS NULL OR c.topic = @topic)
  AND (c.topic_rank <= @per_topic OR c.id IN (
        SELECT unnest(ARRAY[x.id, x.related_id]) FROM entries x
        WHERE x.session = @session AND x.status = 'active' AND x.relation = 'contradicts'))
ORDER BY c.topic, c.topic_rank`

// Conflicts between entries that are both still active.
const selectConflicts = `
SELECT a.id, b.id FROM entries a JOIN entries b ON b.id = a.related_id
WHERE a.session = $1 AND a.relation = 'contradicts' AND a.status = 'active'
  AND b.status = 'active'`

// Highest stream_seq with no unprocessed item of the session at or below it.
const selectWatermark = `
SELECT coalesce(
           (SELECT min(stream_seq) - 1 FROM events
            WHERE session = $1 AND classified_at IS NULL),
           (SELECT max(stream_seq) FROM events WHERE session = $1),
           0),
       (SELECT count(*) FROM events WHERE session = $1 AND classified_at IS NULL)`

var kindPriority = map[string]int{"decision": 0, "fact": 1, "artifact": 1, "open_question": 2}

func priority(kind string) int {
	if p, ok := kindPriority[kind]; ok {
		return p
	}
	return 3
}

type Entry struct {
	ID        int64   `json:"id"`
	Topic     string  `json:"topic"`
	Kind      string  `json:"kind"`
	Score     float64 `json:"score"`
	Master    string  `json:"master"`
	Seq       int64   `json:"seq"`
	StreamSeq int64   `json:"stream_seq"`
	Text      string  `json:"text"`
	TopicRank int64   `json:"topic_rank"`
	Declared  bool    `json:"declared"` // shared deliberately with share_context
}

func scanEntries(rows pgx.Rows) ([]Entry, error) {
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Entry, error) {
		var e Entry
		err := r.Scan(&e.ID, &e.Topic, &e.Kind, &e.Score, &e.Master, &e.Seq, &e.StreamSeq, &e.Text, &e.TopicRank, &e.Declared)
		return e, err
	})
}

func render(e Entry, conflicts []int64) string {
	text := e.Text
	if e.Kind == "artifact" && pystr.Len(text) > ArtifactPreviewChars {
		text = pystr.Prefix(text, ArtifactPreviewChars) + fmt.Sprintf("\n[preview; full text in entry #%d]", e.ID)
	} else if pystr.Len(text) > EntryChars {
		text = pystr.Prefix(text, EntryChars) + fmt.Sprintf(" [truncated; full text in entry #%d]", e.ID)
	}
	// A declared entry's seq numbers the MCP server's items, so it is left out.
	seq := fmt.Sprintf("seq %d", e.Seq)
	if e.Declared {
		seq = "declared"
	}
	tags := []string{strings.ReplaceAll(e.Kind, "_", " ") + " from " + e.Master, seq, fmt.Sprintf("#%d", e.ID)}
	if len(conflicts) > 0 {
		ids := slices.Sorted(slices.Values(conflicts))
		refs := make([]string, len(ids))
		for i, id := range ids {
			refs[i] = fmt.Sprintf("#%d", id)
		}
		tags = append(tags, "conflicts with "+strings.Join(refs, ", "))
	}
	if e.Kind == "open_question" {
		tags = append(tags, "topic "+e.Topic)
	}
	// Indent continuation lines so multi-line entries stay under their header.
	return "- [" + strings.Join(tags, ", ") + "] " + strings.ReplaceAll(text, "\n", "\n  ")
}

func deciders(entries []Entry) []string {
	set := map[string]bool{}
	for _, e := range entries {
		if e.Kind == "decision" {
			set[e.Master] = true
		}
	}
	return slices.Sorted(maps.Keys(set))
}

func heading(topic string, entries []Entry) string {
	// Decision models catch restatements and revisions, but not every disagreement between
	// masters (e.g. "SQLite" vs "Redis" for the same table). Several masters deciding on one
	// topic is flagged so the receiving master checks them.
	d := deciders(entries)
	if len(d) > 1 {
		who := strings.Join(d[:len(d)-1], ", ") + " and " + d[len(d)-1]
		return fmt.Sprintf("## %s (decisions from %s; check that they agree)\n", topic, who)
	}
	return "## " + topic + "\n"
}

// Result is a handoff: the session's shared context as an Open Responses input array.
type Result struct {
	Session  string  `json:"session"`
	UptoSeq  int64   `json:"upto_seq"`
	Pending  int64   `json:"pending"`
	Items    int     `json:"items"`
	EntryIDs []int64 `json:"entry_ids"`
	Input    []any   `json:"input"`
}

// Text is the handoff message's text ("" when empty).
func (r Result) Text() string {
	if len(r.Input) == 0 {
		return ""
	}
	msg := r.Input[0].(*ojson.Object)
	content, _ := msg.Get("content")
	part := content.([]any)[0].(*ojson.Object)
	text, _ := part.Get("text")
	return text.(string)
}

// conflictPairs maps each entry in a conflict to the entries it conflicts with; with ids,
// only conflicts between entries in ids count.
func conflictPairs(conflicts [][2]int64, ids map[int64]bool) map[int64][]int64 {
	pairs := map[int64][]int64{}
	add := func(a, b int64) {
		if !slices.Contains(pairs[a], b) {
			pairs[a] = append(pairs[a], b)
		}
	}
	for _, c := range conflicts {
		if ids == nil || (ids[c[0]] && ids[c[1]]) {
			add(c[0], c[1])
			add(c[1], c[0])
		}
	}
	return pairs
}

// Build renders entries (ordered by topic and rank) into a handoff.
func Build(session string, entries []Entry, conflicts [][2]int64, uptoSeq, pending int64, task *string, maxChars int) Result {
	ids := map[int64]bool{}
	for _, e := range entries {
		ids[e.ID] = true
	}
	pairs := conflictPairs(conflicts, ids)
	lines := map[int64]string{}
	for _, e := range entries {
		lines[e.ID] = render(e, pairs[e.ID])
	}

	// Fill the budget in priority order, then render in reading order.
	ranked := slices.Clone(entries)
	slices.SortStableFunc(ranked, func(a, b Entry) int {
		_, ca := pairs[a.ID]
		_, cb := pairs[b.ID]
		return cmp.Or(
			cmpBool(!ca, !cb),
			cmp.Compare(priority(a.Kind), priority(b.Kind)),
			cmp.Compare(a.TopicRank, b.TopicRank),
			cmp.Compare(-a.Score, -b.Score),
		)
	})
	chosen := map[int64]bool{}
	used := 0
	for _, e := range ranked {
		cost := pystr.Len(lines[e.ID]) + 1
		if used+cost <= maxChars {
			chosen[e.ID] = true
			used += cost
		}
	}
	var shown []Entry
	for _, e := range entries {
		if chosen[e.ID] {
			shown = append(shown, e)
		}
	}

	byTopic := map[string][]Entry{}
	var topics []string
	for _, e := range shown {
		if e.Kind == "open_question" {
			continue
		}
		if _, ok := byTopic[e.Topic]; !ok {
			topics = append(topics, e.Topic)
		}
		byTopic[e.Topic] = append(byTopic[e.Topic], e)
	}
	disputed := func(t string) bool {
		for _, e := range byTopic[t] {
			if _, ok := pairs[e.ID]; ok {
				return true
			}
		}
		return len(deciders(byTopic[t])) > 1
	}
	best := func(t string) float64 {
		m := byTopic[t][0].Score
		for _, e := range byTopic[t] {
			m = max(m, e.Score)
		}
		return m
	}
	// Topics with conflicts or several deciders first, then by their most relevant entry.
	order := slices.Clone(topics)
	slices.SortStableFunc(order, func(a, b string) int {
		return cmp.Or(cmpBool(!disputed(a), !disputed(b)), cmp.Compare(-best(a), -best(b)), strings.Compare(a, b))
	})
	var sections []string
	for _, t := range order {
		ls := make([]string, len(byTopic[t]))
		for i, e := range byTopic[t] {
			ls[i] = lines[e.ID]
		}
		sections = append(sections, heading(t, byTopic[t])+strings.Join(ls, "\n"))
	}
	var questions []string
	for _, e := range shown {
		if e.Kind == "open_question" {
			questions = append(questions, lines[e.ID])
		}
	}
	if len(questions) > 0 {
		sections = append(sections, "## Open questions\n"+
			"Unresolved; answer them if you can, and do not treat them as settled.\n"+
			strings.Join(questions, "\n"))
	}

	// Named up front as well: a small reader model missed the flag in the section heading.
	var disputes []string
	for _, t := range order {
		if !disputed(t) {
			continue
		}
		set := map[string]bool{}
		for _, e := range byTopic[t] {
			if _, ok := pairs[e.ID]; ok || e.Kind == "decision" {
				set[e.Master] = true
			}
		}
		disputes = append(disputes, fmt.Sprintf("%s (%s)", t, strings.Join(slices.Sorted(maps.Keys(set)), ", ")))
	}

	result := Result{Session: session, UptoSeq: uptoSeq, Pending: pending, Items: len(shown), EntryIDs: []int64{}, Input: []any{}}
	for _, e := range shown {
		result.EntryIDs = append(result.EntryIDs, e.ID)
	}
	if len(shown) > 0 {
		var intro strings.Builder
		fmt.Fprintf(&intro, "Shared context from the other agents working on this task (session %s, up to stream sequence %d).", session, uptoSeq)
		if task != nil && *task != "" {
			intro.WriteString("\nThe task: " + *task)
		}
		intro.WriteString("\n\nEach entry names the agent that reported it. Treat entries as that agent's " +
			"results and decisions, not your own conclusions, and check anything critical " +
			"before relying on it. Declared entries were shared deliberately by their agent; " +
			"the others were picked out of its work. Entries marked as conflicting disagree " +
			"and are unresolved.")
		if leftOut := len(entries) - len(shown); leftOut > 0 {
			fmt.Fprintf(&intro, " %d lower-priority entries were left out for length.", leftOut)
		}
		if len(disputes) > 0 {
			intro.WriteString("\n\nAgents may disagree on: " + strings.Join(disputes, "; ") + ". Resolve these first.")
		}
		text := intro.String() + "\n\n" + strings.Join(sections, "\n\n")
		result.Input = append(result.Input, ojson.New().
			Set("type", "message").
			Set("role", "user").
			Set("content", []any{ojson.New().Set("type", "input_text").Set("text", text)}))
	}
	return result
}

func cmpBool(a, b bool) int {
	switch {
	case a == b:
		return 0
	case !a:
		return -1
	}
	return 1
}

// Options select what a handoff includes.
type Options struct {
	PerTopic      int
	MaxChars      int
	ExcludeMaster string // leave out this master's entries
	Topic         string // only this topic
}

func (o Options) withDefaults() Options {
	if o.PerTopic == 0 {
		o.PerTopic = DefaultPerTopic
	}
	if o.MaxChars == 0 {
		o.MaxChars = DefaultMaxChars
	}
	return o
}

func nullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// Snapshot builds a session's handoff from one repeatable-read snapshot, so the watermark
// matches the entries.
func Snapshot(ctx context.Context, db store.TxDB, session string, o Options) (Result, error) {
	o = o.withDefaults()
	var result Result
	err := pgx.BeginTxFunc(ctx, db, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		var upto, pending int64
		if err := tx.QueryRow(ctx, selectWatermark, session).Scan(&upto, &pending); err != nil {
			return err
		}
		var task *string
		if err := tx.QueryRow(ctx, selectSession, session).Scan(&task); err != nil && err != pgx.ErrNoRows {
			return err
		}
		rows, err := tx.Query(ctx, selectContext, pgx.NamedArgs{
			"session": session, "per_topic": o.PerTopic, "exclude": nullable(o.ExcludeMaster), "topic": nullable(o.Topic),
		})
		if err != nil {
			return err
		}
		entries, err := scanEntries(rows)
		if err != nil {
			return err
		}
		conflicts, err := queryConflicts(ctx, tx, session)
		if err != nil {
			return err
		}
		result = Build(session, entries, conflicts, upto, pending, task, o.MaxChars)
		return nil
	})
	return result, err
}

func queryConflicts(ctx context.Context, db store.DB, session string) ([][2]int64, error) {
	rows, err := db.Query(ctx, selectConflicts, session)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) ([2]int64, error) {
		var c [2]int64
		err := r.Scan(&c[0], &c[1])
		return c, err
	})
}

// PutSession declares a session's task and topics (name -> description or nil, in order).
func PutSession(ctx context.Context, db store.DB, session string, task *string, topics *ojson.Object) error {
	if topics == nil {
		topics = ojson.New()
	}
	_, err := db.Exec(ctx, upsertSession, session, task, topics)
	return err
}

// ParseTopics accepts {"name": "description" | null, ...} or ["name", ...] and normalizes
// the names, keeping their order.
func ParseTopics(raw any) (*ojson.Object, error) {
	out := ojson.New()
	bad := fmt.Errorf("topics must be a list of names or an object of name -> description")
	switch t := raw.(type) {
	case nil:
		return out, nil
	case []any:
		for _, v := range t {
			name, ok := v.(string)
			if !ok {
				return nil, bad
			}
			out.Set(classifier.NormalizeTopic(name), nil)
		}
	case *ojson.Object:
		for _, k := range t.Keys() {
			v, _ := t.Get(k)
			if _, ok := v.(string); v != nil && !ok {
				return nil, bad
			}
			out.Set(classifier.NormalizeTopic(k), v)
		}
	default:
		return nil, bad
	}
	return out, nil
}
