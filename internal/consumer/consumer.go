// Package consumer stores envelopes from JetStream in the events log and builds shared
// context, many sessions at a time.
//
// Two stages, with the events table as the work queue between them:
//
//   - Ingest (ingest.go): fetch batches from a durable JetStream consumer, insert the raw items
//     in one transaction, then ack. Messages are acked once stored, so slow classification
//     never runs into the ack deadline; inserts are idempotent on (master, item_id, rev).
//   - Workers (scheduler.go): one per session with unprocessed events, up to MaxSessions at
//     a time. A worker holds a Postgres advisory lock on its session (so several consumer
//     processes can share the load) and processes the session's events in stream order
//     (pipeline.ProcessEvents: in-order relating, classification ahead). A failing event
//     blocks only its session; it is retried with backoff and given up after MaxAttempts,
//     marked with its error, so the handoff watermark can move past it.
//
// Sessions are independent, so they run in parallel; within a session, order is kept: each
// entry is related to the context as it stood before it.
package consumer

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"wetink/internal/classifier"
	"wetink/internal/envelope"
	"wetink/internal/store"
)

const (
	DefaultBatch       = 100
	DefaultMaxSessions = 16
	// Entries classified ahead of the one being related, per session. The model server's
	// slots are shared by all sessions (the classifier's own concurrency limit applies).
	DefaultAhead = 4
	MaxAttempts  = 5
)

// Config of a consumer run.
type Config struct {
	NatsURL     string
	DatabaseURL string
	Durable     string   // default "store"
	Subject     string   // default every session
	Subjects    []string // several subjects instead of Subject
	Batch       int      // messages per fetch
	MaxSessions int      // sessions processed at once
	Ahead       int      // entries classified ahead within a session
}

func (cfg *Config) defaults() {
	if cfg.Durable == "" {
		cfg.Durable = "store"
	}
	if cfg.Subject == "" && len(cfg.Subjects) == 0 {
		cfg.Subject = envelope.StreamSubjects
	}
	if cfg.Batch == 0 {
		cfg.Batch = DefaultBatch
	}
	if cfg.MaxSessions == 0 {
		cfg.MaxSessions = DefaultMaxSessions
	}
	if cfg.Ahead == 0 {
		cfg.Ahead = DefaultAhead
	}
}

// Run consumes until ctx is cancelled.
func Run(ctx context.Context, cfg Config, c classifier.Classifier) error {
	cfg.defaults()
	poolCfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return err
	}
	// A connection per busy session (it holds the session's lock), plus ingest and sweeps.
	poolCfg.MaxConns = int32(cfg.MaxSessions + 4)
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := store.ApplySchema(ctx, pool); err != nil {
		return err
	}

	nc, err := nats.Connect(cfg.NatsURL, nats.Name("consumer-"+cfg.Durable), nats.MaxReconnects(-1))
	if err != nil {
		return err
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	if err := envelope.EnsureStream(ctx, js); err != nil {
		return err
	}
	filter := cfg.Subject
	if len(cfg.Subjects) > 0 {
		filter = ""
	}
	// Create or update, so config changes reach an existing durable. Messages are acked once
	// stored, so redelivery only covers ingest failures, which are retried indefinitely.
	cons, err := js.CreateOrUpdateConsumer(ctx, envelope.StreamName, jetstream.ConsumerConfig{
		Durable:        cfg.Durable,
		FilterSubject:  filter,
		FilterSubjects: cfg.Subjects,
		AckPolicy:      jetstream.AckExplicitPolicy,
		DeliverPolicy:  jetstream.DeliverAllPolicy,
		AckWait:        30 * time.Second,
		MaxDeliver:     -1,
	})
	if err != nil {
		return err
	}

	sched := newScheduler(pool, c, cfg)
	sched.start(ctx)
	err = ingest(ctx, pool, cons, cfg, sched)
	sched.wait()
	// Acks are sent asynchronously; flush them before closing.
	if ferr := nc.FlushTimeout(2 * time.Second); err == nil && ctx.Err() == nil {
		err = ferr
	}
	if ctx.Err() != nil {
		return nil
	}
	return err
}
