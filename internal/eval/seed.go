package eval

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"wetink/internal/envelope"
	"wetink/internal/store"
)

const dropAll = `
DROP TABLE IF EXISTS entries, events, sessions, context_view CASCADE;
DROP FUNCTION IF EXISTS wetink_notify() CASCADE;`

func jetStream(natsURL string) (*nats.Conn, jetstream.JetStream, error) {
	nc, err := nats.Connect(natsURL)
	if err != nil {
		return nil, nil, err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, nil, err
	}
	return nc, js, nil
}

// Reset deletes all shared-context data: the Postgres tables and the JetStream stream.
func Reset(ctx context.Context, db *pgxpool.Pool, natsURL string) error {
	if _, err := db.Exec(ctx, dropAll); err != nil {
		return err
	}
	if err := store.ApplySchema(ctx, db); err != nil {
		return err
	}
	nc, js, err := jetStream(natsURL)
	if err != nil {
		return err
	}
	defer nc.Close()
	if err := js.DeleteStream(ctx, envelope.StreamName); err != nil && !errors.Is(err, jetstream.ErrStreamNotFound) {
		return err
	}
	return envelope.EnsureStream(ctx, js)
}

// Forget deletes one session: its rows, and its messages in the stream (a consumer for the
// session would otherwise replay them).
func Forget(ctx context.Context, db *pgxpool.Pool, natsURL, session string) error {
	for _, table := range []string{"entries", "events", "sessions"} {
		if _, err := db.Exec(ctx, "DELETE FROM "+table+" WHERE session = $1", session); err != nil {
			return err
		}
	}
	nc, js, err := jetStream(natsURL)
	if err != nil {
		return err
	}
	defer nc.Close()
	s, err := js.Stream(ctx, envelope.StreamName)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	return s.Purge(ctx, jetstream.WithPurgeSubject(envelope.Subject(session)))
}
