-- Create marker DLQ table
CREATE TABLE IF NOT EXISTS des_marker_dlq (
    id SERIAL PRIMARY KEY,
    catalog_entry_id INTEGER NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    resolved BOOLEAN DEFAULT FALSE,
    resolved_at TIMESTAMP WITH TIME ZONE,

    CONSTRAINT fk_catalog_entry
        FOREIGN KEY (catalog_entry_id)
        REFERENCES des_source_catalog(id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_dlq_unresolved ON des_marker_dlq(resolved, created_at)
WHERE resolved = FALSE;

-- Add marker status enum if not exists
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'marker_status') THEN
        CREATE TYPE marker_status AS ENUM (
            'pending',
            'marking',
            'marked',
            'failed',
            'retry'
        );
    END IF;
END $$;

-- Ensure catalog table has proper columns
ALTER TABLE des_source_catalog 
    ADD COLUMN IF NOT EXISTS des_status VARCHAR(32),
    ADD COLUMN IF NOT EXISTS retry_count INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_error TEXT;

-- Index for marker queries
CREATE INDEX IF NOT EXISTS idx_catalog_marker_candidates 
ON des_source_catalog(created_at, des_status, des_name)
WHERE des_status IS NULL OR des_status IN ('retry', 'pending');
