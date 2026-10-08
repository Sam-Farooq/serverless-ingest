-- Three tables, and the reason each one exists.
--
-- ingest_objects  the ledger. One row per (bucket, key, etag) that committed.
--                 Its primary key is what makes an SQS redelivery a no-op, so
--                 it is inserted in the same transaction as the rows it counts.
-- readings        the data. event_id is the producer's own id, not a surrogate,
--                 because the deduplication has to survive a replay.
-- rejected_rows   rows that will never be valid. Kept, not dropped, because the
--                 question asked after an alarm is always "what did it look
--                 like", and the answer has to be in Postgres and not in a log
--                 that aged out.
--
-- No DEFAULT clauses anywhere. Every timestamp is passed by the caller, which
-- keeps the statements portable enough to run against sqlite in the tests.

CREATE TABLE IF NOT EXISTS ingest_objects (
    bucket        TEXT        NOT NULL,
    object_key    TEXT        NOT NULL,
    etag          TEXT        NOT NULL,
    rows_loaded   INTEGER     NOT NULL,
    rows_rejected INTEGER     NOT NULL,
    loaded_at     TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (bucket, object_key, etag)
);

CREATE TABLE IF NOT EXISTS readings (
    event_id    TEXT          NOT NULL PRIMARY KEY,
    device_id   TEXT          NOT NULL,
    recorded_at TIMESTAMPTZ   NOT NULL,
    metric      TEXT          NOT NULL,
    value       NUMERIC(12,4) NOT NULL,
    source_key  TEXT          NOT NULL
);

CREATE INDEX IF NOT EXISTS readings_device_recorded_idx
    ON readings (device_id, recorded_at);

CREATE TABLE IF NOT EXISTS rejected_rows (
    id          BIGSERIAL PRIMARY KEY,
    bucket      TEXT        NOT NULL,
    object_key  TEXT        NOT NULL,
    etag        TEXT        NOT NULL,
    line_number INTEGER     NOT NULL,
    reason      TEXT        NOT NULL,
    payload     TEXT        NOT NULL,
    rejected_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS rejected_rows_object_idx
    ON rejected_rows (bucket, object_key, etag);
