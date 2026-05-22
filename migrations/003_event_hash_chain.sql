-- Add tamper-evident hash chain fields to canonical event log
ALTER TABLE events
ADD COLUMN IF NOT EXISTS previous_hash CHAR(64),
ADD COLUMN IF NOT EXISTS event_hash CHAR(64);

-- Enforce hash format when present (64-char lowercase hex SHA-256)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'chk_previous_hash_format'
      AND conrelid = 'events'::regclass
  ) THEN
    ALTER TABLE events
    ADD CONSTRAINT chk_previous_hash_format
    CHECK (previous_hash IS NULL OR previous_hash ~ '^[0-9a-f]{64}$');
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'chk_event_hash_format'
      AND conrelid = 'events'::regclass
  ) THEN
    ALTER TABLE events
    ADD CONSTRAINT chk_event_hash_format
    CHECK (event_hash IS NULL OR event_hash ~ '^[0-9a-f]{64}$');
  END IF;
END $$;

-- Hash must be present for all non-genesis events (sequence_id > 1)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'chk_hash_required_non_genesis'
      AND conrelid = 'events'::regclass
  ) THEN
    ALTER TABLE events
    ADD CONSTRAINT chk_hash_required_non_genesis
    CHECK (
      (sequence_id = 1 AND event_hash IS NOT NULL)
      OR (sequence_id > 1 AND previous_hash IS NOT NULL AND event_hash IS NOT NULL)
    );
  END IF;
END $$;

-- Exactly one genesis event per aggregate stream (sequence_id=1)
CREATE UNIQUE INDEX IF NOT EXISTS uq_events_genesis_per_aggregate
ON events(tenant_id, aggregate_id)
WHERE sequence_id = 1;

-- Foreign keys cannot target a partial unique index.
-- Ensure a full-table UNIQUE constraint exists on the referenced key.
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'uq_events_stream_event_hash_full'
      AND conrelid = 'events'::regclass
  ) THEN
    ALTER TABLE events
    ADD CONSTRAINT uq_events_stream_event_hash_full
    UNIQUE (tenant_id, aggregate_id, event_hash);
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'fk_events_previous_hash'
      AND conrelid = 'events'::regclass
  ) THEN
    ALTER TABLE events
    ADD CONSTRAINT fk_events_previous_hash
    FOREIGN KEY (tenant_id, aggregate_id, previous_hash)
    REFERENCES events(tenant_id, aggregate_id, event_hash)
    DEFERRABLE INITIALLY DEFERRED;
  END IF;
END $$;

-- Helpful index for replay verification and audit scans
CREATE INDEX IF NOT EXISTS idx_events_stream_sequence_hash
ON events(tenant_id, aggregate_id, sequence_id, event_hash);
