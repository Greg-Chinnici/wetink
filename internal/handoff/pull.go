package handoff

// Pull: a session's shared context, or what changed in it since a stream sequence. Backs the
// get_shared_context tool (mcpserver) and GET /v1/sessions/{session}/context.
//   - since=0: the current shared context, rendered like a handoff.
//   - since=N (the upto_seq of an earlier call): only the changes after it, i.e. visible
//     entries from items in (N, upto], and earlier entries those items superseded or answered.
//     upto is the processing watermark (every item at or below it is processed), so across a
//     chain of calls each entry is reported once.
// The caller's own entries are left out (it knows them); changes others made to them are not.

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"

	"wetink/internal/classifier"
	"wetink/internal/pystr"
	"wetink/internal/store"
)

const selectNew = `
SELECT c.id, c.topic, c.kind, c.score, c.master, c.seq, c.stream_seq, c.text, c.topic_rank,
       c.declared
FROM context_ranked c
WHERE c.session = @session AND c.stream_seq > @since AND c.stream_seq <= @upto
  AND (@exclude::text IS NULL OR c.master <> @exclude)
  AND (@topic::text IS NULL OR c.topic = @topic)
ORDER BY c.stream_seq, c.id`

// Earlier entries that later items closed. Closures by the caller itself are left out.
const selectClosed = `
SELECT o.id, o.kind, o.master, o.status, c.id, c.master
FROM entries o JOIN entries c ON c.id = o.closed_by
WHERE o.session = @session AND o.stream_seq <= @since
  AND c.stream_seq > @since AND c.stream_seq <= @upto
  AND (@exclude::text IS NULL OR c.master <> @exclude)
  AND (@topic::text IS NULL OR o.topic = @topic)
ORDER BY c.stream_seq, o.id`

// The session's topics: declared ones (with their descriptions) and ones entries use.
const selectTopics = `
SELECT topic, max(descr) FROM (
    SELECT t.key AS topic, t.value #>> '{}' AS descr
    FROM sessions s, jsonb_each(s.topics) t WHERE s.session = $1
    UNION ALL SELECT topic, NULL FROM entries WHERE session = $1 AND topic IS NOT NULL
) t GROUP BY topic ORDER BY topic`

type topicRow struct {
	Name string
	Desc *string
}

// topicsLine lists the session's topics so a master can reuse their names when it shares.
func topicsLine(topics []topicRow) string {
	if len(topics) == 0 {
		return ""
	}
	names := make([]string, len(topics))
	for i, t := range topics {
		names[i] = t.Name
		if t.Desc != nil && *t.Desc != "" {
			names[i] += " (" + *t.Desc + ")"
		}
	}
	return "Topics: " + strings.Join(names, "; ") + ". Use one of these as the topic when you share_context, if one fits."
}

type Closed struct {
	ID       int64  `json:"id"`
	Kind     string `json:"kind"`
	Master   string `json:"master"`
	Status   string `json:"status"` // superseded | resolved
	By       int64  `json:"by"`
	ByMaster string `json:"by_master"`
}

type ClosedRef struct {
	ID     int64  `json:"id"`
	Status string `json:"status"`
	By     int64  `json:"by"`
}

// Changes is the result of a pull.
type Changes struct {
	Session  string      `json:"session"`
	SinceSeq int64       `json:"since_seq"`
	UptoSeq  int64       `json:"upto_seq"`
	Pending  int64       `json:"pending"`
	EntryIDs []int64     `json:"entry_ids"`
	Closed   []ClosedRef `json:"closed"`
	Text     string      `json:"text"`
}

func again(upto int64) string {
	return fmt.Sprintf("Call get_shared_context with since_seq=%d for later changes.", upto)
}

// BuildChanges renders the changes after since, up to upto.
func BuildChanges(session string, since, upto, pending int64, newEntries []Entry, closed []Closed,
	conflicts [][2]int64, topic string, maxChars int) Changes {
	pairs := conflictPairs(conflicts, nil)
	lines := map[int64]string{}
	for _, e := range newEntries {
		lines[e.ID] = render(e, pairs[e.ID])
	}
	// Fill the budget by priority, then list in stream order.
	ranked := slices.Clone(newEntries)
	slices.SortStableFunc(ranked, func(a, b Entry) int {
		_, ca := pairs[a.ID]
		_, cb := pairs[b.ID]
		return cmp.Or(cmpBool(!ca, !cb), cmp.Compare(priority(a.Kind), priority(b.Kind)), cmp.Compare(a.StreamSeq, b.StreamSeq))
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
	for _, e := range newEntries {
		if chosen[e.ID] {
			shown = append(shown, e)
		}
	}

	scope := "session " + session
	if topic != "" {
		scope += ", topic " + topic
	}
	still := ""
	if pending > 0 {
		still = fmt.Sprintf(" (%d newer items still processing)", pending)
	}
	var text string
	if len(shown) == 0 && len(closed) == 0 {
		text = fmt.Sprintf("No changes to the shared context (%s) after stream sequence %d; up to date to %d%s. %s",
			scope, since, upto, still, again(upto))
	} else {
		parts := []string{fmt.Sprintf("Changes to the shared context (%s) after stream sequence %d, up to %d%s. "+
			"Each entry names the agent that reported it; declared entries were shared deliberately.", scope, since, upto, still)}
		if len(shown) > 0 {
			ls := make([]string, len(shown))
			for i, e := range shown {
				ls[i] = lines[e.ID]
			}
			s := "New:\n" + strings.Join(ls, "\n")
			if leftOut := len(newEntries) - len(shown); leftOut > 0 {
				s += fmt.Sprintf("\n(%d more left out for length; pass a topic to narrow.)", leftOut)
			}
			parts = append(parts, s)
		}
		if len(closed) > 0 {
			verb := map[string]string{"superseded": "superseded by", "resolved": "answered by"}
			ls := make([]string, len(closed))
			for i, c := range closed {
				v := verb[c.Status]
				if v == "" {
					v = c.Status
				}
				ls[i] = fmt.Sprintf("- #%d (%s from %s) was %s #%d from %s",
					c.ID, strings.ReplaceAll(c.Kind, "_", " "), c.Master, v, c.By, c.ByMaster)
			}
			parts = append(parts, "No longer current:\n"+strings.Join(ls, "\n"))
		}
		parts = append(parts, again(upto))
		text = strings.Join(parts, "\n\n")
	}
	out := Changes{Session: session, SinceSeq: since, UptoSeq: upto, Pending: pending, EntryIDs: []int64{}, Closed: []ClosedRef{}, Text: text}
	for _, e := range shown {
		out.EntryIDs = append(out.EntryIDs, e.ID)
	}
	for _, c := range closed {
		out.Closed = append(out.Closed, ClosedRef{c.ID, c.Status, c.By})
	}
	return out
}

// PullOptions select what a pull returns.
type PullOptions struct {
	Topic         string
	SinceSeq      int64
	ExcludeMaster string
	MaxChars      int
}

// Pull returns the session's shared context (SinceSeq 0) or the changes after SinceSeq.
func Pull(ctx context.Context, db store.TxDB, session string, o PullOptions) (Changes, error) {
	if o.MaxChars == 0 {
		o.MaxChars = DefaultMaxChars
	}
	if o.Topic != "" {
		o.Topic = classifier.NormalizeTopic(o.Topic)
	}
	var out Changes
	full := false
	var line string // the topics, for full pulls
	err := pgx.BeginTxFunc(ctx, db, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, selectTopics, session)
		if err != nil {
			return err
		}
		topicRows, err := pgx.CollectRows(rows, pgx.RowToStructByPos[topicRow])
		if err != nil {
			return err
		}
		topics := make([]string, len(topicRows))
		for i, t := range topicRows {
			topics[i] = t.Name
		}
		line = topicsLine(topicRows)
		var upto, pending int64
		if err := tx.QueryRow(ctx, selectWatermark, session).Scan(&upto, &pending); err != nil {
			return err
		}
		if o.Topic != "" && !slices.Contains(topics, o.Topic) {
			known := strings.Join(topics, ", ")
			if known == "" {
				known = "none yet"
			}
			out = Changes{Session: session, SinceSeq: o.SinceSeq, UptoSeq: upto, Pending: pending,
				EntryIDs: []int64{}, Closed: []ClosedRef{},
				Text: fmt.Sprintf("Unknown topic '%s'. Known topics: %s.", o.Topic, known)}
			return nil
		}
		if o.SinceSeq <= 0 {
			full = true
			return nil
		}
		upto = max(upto, o.SinceSeq)
		args := pgx.NamedArgs{"session": session, "since": o.SinceSeq, "upto": upto,
			"exclude": nullable(o.ExcludeMaster), "topic": nullable(o.Topic)}
		rows, err = tx.Query(ctx, selectNew, args)
		if err != nil {
			return err
		}
		newEntries, err := scanEntries(rows)
		if err != nil {
			return err
		}
		rows, err = tx.Query(ctx, selectClosed, args)
		if err != nil {
			return err
		}
		closed, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (Closed, error) {
			var c Closed
			err := r.Scan(&c.ID, &c.Kind, &c.Master, &c.Status, &c.By, &c.ByMaster)
			return c, err
		})
		if err != nil {
			return err
		}
		conflicts, err := queryConflicts(ctx, tx, session)
		if err != nil {
			return err
		}
		out = BuildChanges(session, o.SinceSeq, upto, pending, newEntries, closed, conflicts, o.Topic, o.MaxChars)
		return nil
	})
	if err != nil || !full {
		return out, err
	}

	snap, err := Snapshot(ctx, db, session, Options{MaxChars: o.MaxChars, ExcludeMaster: o.ExcludeMaster, Topic: o.Topic})
	if err != nil {
		return out, err
	}
	var text string
	if t := snap.Text(); t != "" {
		text = t + "\n\n" + again(snap.UptoSeq)
	} else {
		scope := "session " + session
		if o.Topic != "" {
			scope += ", topic " + o.Topic
		}
		text = fmt.Sprintf("The shared context (%s) has no entries from other agents yet, up to stream sequence %d. %s",
			scope, snap.UptoSeq, again(snap.UptoSeq))
	}
	if line != "" && o.Topic == "" {
		text += "\n\n" + line
	}
	return Changes{Session: session, SinceSeq: 0, UptoSeq: snap.UptoSeq, Pending: snap.Pending,
		EntryIDs: snap.EntryIDs, Closed: []ClosedRef{}, Text: text}, nil
}
