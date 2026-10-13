package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go/jetstream"

	"wetink/internal/envelope"
	"wetink/internal/store"
)

const insertEvent = `
INSERT INTO events (session, master, seq, item_id, item, stream_seq, published_at, via, rev)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT DO NOTHING
RETURNING id`

type seqKey struct{ session, master, via string }

// SeqTracker detects per-master sequence gaps, e.g. from a tap buffer overflow or restart.
// The tap and the MCP server number their items separately.
type SeqTracker struct{ last map[seqKey]int64 }

func (s *SeqTracker) Load(ctx context.Context, db store.DB) error {
	s.last = map[seqKey]int64{}
	rows, err := db.Query(ctx, "SELECT session, master, via, max(seq) FROM events GROUP BY 1, 2, 3")
	if err != nil {
		return err
	}
	var k seqKey
	var seq int64
	_, err = pgx.ForEachRow(rows, []any{&k.session, &k.master, &k.via, &seq}, func() error {
		s.last[k] = seq
		return nil
	})
	return err
}

func (s *SeqTracker) Observe(env envelope.Envelope) {
	if s.last == nil {
		s.last = map[seqKey]int64{}
	}
	k := seqKey{env.Session, env.Master, env.ViaOrTap()}
	last := s.last[k]
	if env.Seq > last+1 {
		slog.Warn("seq gap", "session", k.session, "master", k.master, "via", k.via, "from", last, "to", env.Seq)
	} else if env.Seq <= last {
		slog.Warn("seq out of order", "session", k.session, "master", k.master, "via", k.via, "seq", env.Seq, "after", last)
	}
	s.last[k] = max(last, env.Seq)
}

// Message is what ingest needs from a JetStream message (an interface for tests).
type Message interface {
	Data() []byte
	Metadata() (*jetstream.MsgMetadata, error)
	Ack() error
	Term() error
}

// logAckErr logs a failed ack or term; JetStream redelivers after AckWait either way.
func logAckErr(op string, err error) {
	if err != nil {
		slog.Warn("jetstream "+op+" failed", "err", err)
	}
}

// StoreBatch inserts a batch of messages as raw events in one transaction and acks them. It
// returns the sessions that got new events, and how many were new and duplicates.
func StoreBatch(ctx context.Context, db store.DB, msgs []Message, seqs *SeqTracker) (sessions []string, inserted, dup int, err error) {
	type parsed struct {
		msg Message
		env envelope.Envelope
		md  *jetstream.MsgMetadata
	}
	var batch []parsed
	for _, m := range msgs {
		md, err := m.Metadata()
		if err != nil {
			return nil, 0, 0, err
		}
		env, err := envelope.Unmarshal(m.Data())
		if err != nil {
			slog.Error("dropping malformed message", "stream_seq", md.Sequence.Stream, "err", err)
			logAckErr("term", m.Term())
			continue
		}
		batch = append(batch, parsed{m, env, md})
	}
	var fresh []envelope.Envelope
	err = pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		fresh = fresh[:0]
		for _, p := range batch {
			var rev any
			if p.env.Rev > 0 {
				rev = p.env.Rev
			}
			var id int64
			err := tx.QueryRow(ctx, insertEvent, p.env.Session, p.env.Master, p.env.Seq, p.env.ItemID(),
				p.env.Item, int64(p.md.Sequence.Stream), p.md.Timestamp, p.env.ViaOrTap(), rev).Scan(&id)
			if errors.Is(err, pgx.ErrNoRows) {
				continue // stored before: a redelivery or a repeat
			}
			if err != nil {
				return err
			}
			fresh = append(fresh, p.env)
		}
		return nil
	})
	if err != nil {
		return nil, 0, 0, fmt.Errorf("insert events: %w", err) // not acked: redelivered
	}
	for _, p := range batch {
		logAckErr("ack", p.msg.Ack())
	}
	seen := map[string]bool{}
	for _, env := range fresh {
		seqs.Observe(env)
		if !seen[env.Session] {
			seen[env.Session] = true
			sessions = append(sessions, env.Session)
		}
	}
	return sessions, len(fresh), len(batch) - len(fresh), nil
}

// ingest fetches from JetStream until ctx is cancelled, storing each batch and waking the
// workers of the sessions that got new events.
func ingest(ctx context.Context, pool *pgxpool.Pool, cons jetstream.Consumer, cfg Config, sched *scheduler) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	var seqs SeqTracker
	if err := seqs.Load(ctx, conn); err != nil {
		return err
	}
	slog.Info("consumer reading", "durable", cfg.Durable, "subject", cfg.Subject, "subjects", cfg.Subjects,
		"max_sessions", cfg.MaxSessions, "ahead", cfg.Ahead)
	for ctx.Err() == nil {
		batch, err := cons.Fetch(cfg.Batch, jetstream.FetchMaxWait(time.Second))
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			slog.Warn("fetch failed", "err", err)
			time.Sleep(time.Second)
			continue
		}
		var msgs []Message
		for m := range batch.Messages() {
			msgs = append(msgs, m)
		}
		if len(msgs) == 0 {
			continue
		}
		sessions, inserted, dup, err := StoreBatch(ctx, conn, msgs, &seqs)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			slog.Error("store failed; messages will be redelivered", "err", err)
			continue
		}
		slog.Debug("stored", "events", inserted, "dup", dup)
		for _, s := range sessions {
			sched.notify(ctx, s)
		}
	}
	return nil
}
