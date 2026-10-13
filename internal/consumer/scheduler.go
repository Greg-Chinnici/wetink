package consumer

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"wetink/internal/classifier"
	"wetink/internal/pipeline"
)

const (
	workerBatch = 100              // events loaded at a time per session
	idleTimeout = 2 * time.Second  // a worker with nothing to do exits after this
	sweepEvery  = 5 * time.Second  // look for sessions with unprocessed events (restarts, other consumers)
	maxBackoff  = 30 * time.Second // between retries of a failing event
)

const selectPending = `
SELECT id, session, master, seq, stream_seq, item, via, coalesce(rev, 0), attempts
FROM events WHERE session = $1 AND classified_at IS NULL
ORDER BY stream_seq LIMIT $2`

const selectPendingSessions = `SELECT DISTINCT session FROM events WHERE classified_at IS NULL`

const markProcessed = `
UPDATE events SET classified_at = now(), classify_error = $1
WHERE id = $2 AND classified_at IS NULL`

const addAttempt = `UPDATE events SET attempts = attempts + 1 WHERE id = $1 RETURNING attempts`

// sessionLock is the advisory lock key of a session: one worker per session across consumer
// processes.
const sessionLock = `hashtextextended($1, 6833)`

// scheduler runs one worker per session with unprocessed events, up to MaxSessions at a time.
type scheduler struct {
	pool *pgxpool.Pool
	c    classifier.Classifier
	cfg  Config

	mu      sync.Mutex
	workers map[string]chan struct{} // session -> wake signal
	slots   chan struct{}
	waiting atomic.Int32
	wg      sync.WaitGroup
}

func newScheduler(pool *pgxpool.Pool, c classifier.Classifier, cfg Config) *scheduler {
	return &scheduler{pool: pool, c: c, cfg: cfg, workers: map[string]chan struct{}{},
		slots: make(chan struct{}, cfg.MaxSessions)}
}

// start begins sweeping for sessions with unprocessed events (now and every sweepEvery).
func (s *scheduler) start(ctx context.Context) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			s.sweep(ctx)
			select {
			case <-ctx.Done():
				return
			case <-time.After(sweepEvery):
			}
		}
	}()
}

func (s *scheduler) wait() { s.wg.Wait() }

func (s *scheduler) sweep(ctx context.Context) {
	rows, err := s.pool.Query(ctx, selectPendingSessions)
	if err != nil {
		if ctx.Err() == nil {
			slog.Warn("sweep failed", "err", err)
		}
		return
	}
	sessions, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return
	}
	for _, session := range sessions {
		s.notify(ctx, session)
	}
}

// notify wakes the session's worker, starting one if needed.
func (s *scheduler) notify(ctx context.Context, session string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if wake, ok := s.workers[session]; ok {
		select {
		case wake <- struct{}{}:
		default:
		}
		return
	}
	wake := make(chan struct{}, 1)
	s.workers[session] = wake
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.work(ctx, session, wake)
	}()
}

// leave removes the worker unconditionally (it stops even if work arrived; a sweep finds it).
func (s *scheduler) leave(session string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.workers, session)
}

// exit removes the worker unless a wake-up arrived meanwhile (then it keeps going).
func (s *scheduler) exit(session string, wake chan struct{}) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-wake:
		return false
	default:
		delete(s.workers, session)
		return true
	}
}

func (s *scheduler) work(ctx context.Context, session string, wake chan struct{}) {
	s.waiting.Add(1)
	select {
	case s.slots <- struct{}{}:
		s.waiting.Add(-1)
	case <-ctx.Done():
		s.waiting.Add(-1)
		s.leave(session)
		return
	}
	defer func() { <-s.slots }()

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		s.leave(session)
		return
	}
	defer conn.Release()
	var locked bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock("+sessionLock+")", session).Scan(&locked); err != nil || !locked {
		// Another consumer is processing this session; a later sweep checks again.
		s.leave(session)
		return
	}
	defer func() {
		if _, err := conn.Exec(context.Background(), "SELECT pg_advisory_unlock("+sessionLock+")", session); err != nil {
			conn.Conn().Close(context.Background()) // closing the connection releases the lock
		}
	}()

	for ctx.Err() == nil {
		n, err := s.processPending(ctx, conn, session)
		if err != nil {
			if ctx.Err() == nil {
				slog.Error("session worker failed", "session", session, "err", err)
			}
			s.leave(session)
			return
		}
		if s.waiting.Load() > 0 {
			// Others wait for a slot: hand this one over (queueing again if there was work).
			// exit fails when a wake-up came in meanwhile; it consumed it, so keep going.
			if s.exit(session, wake) {
				if n > 0 {
					go s.notify(ctx, session)
				}
				return
			}
			continue
		}
		if n > 0 {
			continue
		}
		select {
		case <-wake:
		case <-time.After(idleTimeout):
			if s.exit(session, wake) {
				return
			}
		case <-ctx.Done():
		}
	}
	s.leave(session)
}

type eventRow struct {
	pipeline.EventRow
	attempts int
}

// processPending processes the session's unprocessed events, in order, until one fails. It
// returns how many it completed or gave up on.
func (s *scheduler) processPending(ctx context.Context, conn *pgxpool.Conn, session string) (int, error) {
	rows, err := conn.Query(ctx, selectPending, session, workerBatch)
	if err != nil {
		return 0, err
	}
	pending, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (eventRow, error) {
		var e eventRow
		err := r.Scan(&e.ID, &e.Session, &e.Master, &e.Seq, &e.StreamSeq, &e.Item, &e.Via, &e.Rev, &e.attempts)
		return e, err
	})
	if err != nil || len(pending) == 0 {
		return 0, err
	}
	events := make([]pipeline.EventRow, len(pending))
	for i, e := range pending {
		events[i] = e.EventRow
	}
	done, perr := pipeline.ProcessEvents(ctx, conn, events, s.c, s.cfg.Ahead, func(ev pipeline.EventRow, _ pipeline.EventResult) error {
		_, err := conn.Exec(ctx, markProcessed, nil, ev.ID)
		return err
	})
	if perr == nil || ctx.Err() != nil {
		return done, ctx.Err()
	}
	// The event at index done failed; later ones wait, to keep the session's order.
	failed := events[done]
	var attempts int
	if err := conn.QueryRow(ctx, addAttempt, failed.ID).Scan(&attempts); err != nil {
		return done, err
	}
	if attempts >= MaxAttempts {
		slog.Error("giving up on event", "session", session, "event", failed.ID, "attempts", attempts, "err", perr)
		msg := perr.Error()
		if len(msg) > 500 {
			msg = msg[:500]
		}
		_, err := conn.Exec(ctx, markProcessed, msg, failed.ID)
		return done + 1, err
	}
	delay := min(time.Duration(1<<attempts)*time.Second, maxBackoff)
	slog.Warn("processing event failed; retrying", "session", session, "event", failed.ID, "attempt", attempts, "in", delay, "err", perr)
	select {
	case <-time.After(delay):
	case <-ctx.Done():
	}
	if errors.Is(perr, context.Canceled) {
		return done, nil
	}
	return max(done, 1), nil // retry next round (non-zero: keep the worker going)
}
