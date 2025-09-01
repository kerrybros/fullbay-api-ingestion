-- =====================================================
-- Raw Data Table for Fullbay API JSON Storage
-- =====================================================
-- This table stores the complete JSON response from the Fullbay API
-- for backup, debugging, and reprocessing purposes

CREATE TABLE IF NOT EXISTS fullbay_raw_data (
    id SERIAL PRIMARY KEY,
    fullbay_invoice_id VARCHAR(50) UNIQUE NOT NULL,
    raw_json_data JSONB NOT NULL,
    ingestion_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    processed BOOLEAN DEFAULT FALSE,
    processing_errors TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_fullbay_raw_data_invoice_id ON fullbay_raw_data(fullbay_invoice_id);
CREATE INDEX IF NOT EXISTS idx_fullbay_raw_data_processed ON fullbay_raw_data(processed);
CREATE INDEX IF NOT EXISTS idx_fullbay_raw_data_ingestion ON fullbay_raw_data(ingestion_timestamp);
CREATE INDEX IF NOT EXISTS idx_fullbay_raw_data_json_gin ON fullbay_raw_data USING GIN(raw_json_data);

-- Comments for documentation
COMMENT ON TABLE fullbay_raw_data IS 'Stores complete JSON responses from Fullbay API for backup and reprocessing';
COMMENT ON COLUMN fullbay_raw_data.fullbay_invoice_id IS 'Unique Fullbay invoice identifier';
COMMENT ON COLUMN fullbay_raw_data.raw_json_data IS 'Complete JSON response from Fullbay API';
COMMENT ON COLUMN fullbay_raw_data.processed IS 'Flag indicating if this record has been processed into line items';
COMMENT ON COLUMN fullbay_raw_data.processing_errors IS 'Any errors encountered during processing';
