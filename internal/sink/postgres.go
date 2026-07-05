package sink

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// event_store tables are identical except for the TOAST compression method
// applied to the jsonb payload column, so the same stream of events can be
// used to compare pglz vs lz4 on-disk size and write cost.
var eventStoreDDL = []string{
	`CREATE TABLE IF NOT EXISTS event_store_pglz (
		id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
		topic      text        NOT NULL,
		partition  int         NOT NULL,
		"offset"   bigint      NOT NULL,
		key        text,
		payload    jsonb       NOT NULL,
		created_at timestamptz NOT NULL DEFAULT now(),
		UNIQUE (topic, partition, "offset")
	)`,
	`ALTER TABLE event_store_pglz ALTER COLUMN payload SET COMPRESSION pglz`,
	`CREATE TABLE IF NOT EXISTS event_store_lz4 (
		id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
		topic      text        NOT NULL,
		partition  int         NOT NULL,
		"offset"   bigint      NOT NULL,
		key        text,
		payload    jsonb       NOT NULL,
		created_at timestamptz NOT NULL DEFAULT now(),
		UNIQUE (topic, partition, "offset")
	)`,
	`ALTER TABLE event_store_lz4 ALTER COLUMN payload SET COMPRESSION lz4`,
}

const insertSQL = `INSERT INTO %s (topic, partition, "offset", key, payload)
	VALUES ($1, $2, $3, $4, $5)
	ON CONFLICT (topic, partition, "offset") DO NOTHING`

// Postgres stores every event in both event_store tables.
type Postgres struct {
	pool *pgxpool.Pool
}

func NewPostgres(ctx context.Context, dsn string) (*Postgres, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	for _, stmt := range eventStoreDDL {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			pool.Close()
			return nil, fmt.Errorf("ensure event_store schema: %w", err)
		}
	}
	return &Postgres{pool: pool}, nil
}

func (p *Postgres) Store(ctx context.Context, evt Event) error {
	payload, err := json.Marshal(evt.Decoded)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	batch := &pgx.Batch{}
	for _, table := range []string{"event_store_pglz", "event_store_lz4"} {
		batch.Queue(fmt.Sprintf(insertSQL, table),
			evt.Topic, evt.Partition, evt.Offset, string(evt.Key), payload)
	}
	if err := p.pool.SendBatch(ctx, batch).Close(); err != nil {
		return fmt.Errorf("insert event (offset=%d partition=%d): %w", evt.Offset, evt.Partition, err)
	}
	return nil
}

func (p *Postgres) Close() error {
	p.pool.Close()
	return nil
}
