// Package store is the Postgres side: the schema (schema.sql, applied idempotently at
// startup) and the connection helpers shared by every service.
package store

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var Schema string

// DB is what the queries need; *pgx.Conn, *pgxpool.Pool and pgx.Tx all satisfy it.
type DB interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Begin(ctx context.Context) (pgx.Tx, error)
}

// TxDB can also start transactions with options (isolation level); pools and connections.
type TxDB interface {
	DB
	BeginTx(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error)
}

// schemaLock serialises schema changes between services starting at the same time.
const schemaLock = 0x776574696e6b // "wetink"

// ApplySchema creates or migrates the schema.
func ApplySchema(ctx context.Context, db DB) error {
	return pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", schemaLock); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, Schema); err != nil {
			return fmt.Errorf("apply schema: %w", err)
		}
		return nil
	})
}

// Open connects a pool and applies the schema.
func Open(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, err
	}
	if err := ApplySchema(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}
