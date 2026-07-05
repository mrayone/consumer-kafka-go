-- +goose Up
-- Two identical event_store tables differing only in the TOAST compression
-- method of the jsonb payload column, so the same stream of events can be
-- used to compare pglz vs lz4 on-disk size and write cost.
CREATE TABLE event_store_pglz (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    topic      text        NOT NULL,
    partition  int         NOT NULL,
    "offset"   bigint      NOT NULL,
    key        text,
    payload    jsonb       NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (topic, partition, "offset")
);

ALTER TABLE event_store_pglz ALTER COLUMN payload SET COMPRESSION pglz;

CREATE TABLE event_store_lz4 (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    topic      text        NOT NULL,
    partition  int         NOT NULL,
    "offset"   bigint      NOT NULL,
    key        text,
    payload    jsonb       NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (topic, partition, "offset")
);

ALTER TABLE event_store_lz4 ALTER COLUMN payload SET COMPRESSION lz4;

-- +goose Down
DROP TABLE event_store_lz4;
DROP TABLE event_store_pglz;
