package sink

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The event_store schema is managed by goose migrations (see migrations/);
// run `make migrate-up` before consuming with this sink.
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
	// Fail fast with a clear message if migrations haven't been applied.
	var exists bool
	err = pool.QueryRow(ctx,
		`SELECT to_regclass('event_store_pglz') IS NOT NULL AND to_regclass('event_store_lz4') IS NOT NULL`,
	).Scan(&exists)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("check event_store schema: %w", err)
	}
	if !exists {
		pool.Close()
		return nil, fmt.Errorf("event_store tables not found: run migrations first (make migrate-up)")
	}
	return &Postgres{pool: pool}, nil
}

func (p *Postgres) Store(ctx context.Context, evts []Event) error {
	if len(evts) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, evt := range evts {
		payload, err := json.Marshal(evt.Decoded)
		if err != nil {
			return fmt.Errorf("marshal payload (offset=%d partition=%d): %w", evt.Offset, evt.Partition, err)
		}
		for _, table := range []string{"event_store_pglz", "event_store_lz4"} {
			batch.Queue(fmt.Sprintf(insertSQL, table),
				evt.Topic, evt.Partition, evt.Offset, string(evt.Key), payload)
		}
	}
	// One network round-trip for the whole batch (2 inserts per event).
	if err := p.pool.SendBatch(ctx, batch).Close(); err != nil {
		return fmt.Errorf("insert batch of %d events: %w", len(evts), err)
	}
	return nil
}

func (p *Postgres) Close() error {
	p.pool.Close()
	return nil
}
