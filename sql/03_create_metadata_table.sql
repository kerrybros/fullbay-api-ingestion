-- =====================================================
-- Metadata Table for Ingestion Tracking
-- =====================================================
-- This table tracks each ingestion run with statistics and error handling

CREATE TABLE IF NOT EXISTS ingestion_metadata (
    id SERIAL PRIMARY KEY,
    execution_id VARCHAR(255) UNIQUE NOT NULL,
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time TIMESTAMP WITH TIME ZONE,
    status VARCHAR(50) NOT NULL, -- 'RUNNING', 'SUCCESS', 'ERROR', 'PARTIAL'
    
    -- Processing Statistics
    records_processed INTEGER DEFAULT 0,
    records_inserted INTEGER DEFAULT 0,
    records_updated INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    raw_records_stored INTEGER DEFAULT 0,
    line_items_created INTEGER DEFAULT 0,
    
    -- API Statistics
    api_endpoint VARCHAR(255),
    api_response_time_ms INTEGER,
    api_status_code INTEGER,
    api_error_count INTEGER DEFAULT 0,
    
    -- Database Statistics
    db_connection_time_ms INTEGER,
    db_query_count INTEGER DEFAULT 0,
    db_error_count INTEGER DEFAULT 0,
    
    -- Error Information
    error_message TEXT,
    error_type VARCHAR(100),
    error_stack_trace TEXT,
    
    -- Environment Information
    environment VARCHAR(50) NOT NULL,
    lambda_function_name VARCHAR(255),
    lambda_function_version VARCHAR(50),
    aws_request_id VARCHAR(255),
    
    -- Performance Metrics
    total_duration_seconds DECIMAL(10,2),
    memory_usage_mb INTEGER,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_ingestion_metadata_execution_id ON ingestion_metadata(execution_id);
CREATE INDEX IF NOT EXISTS idx_ingestion_metadata_start_time ON ingestion_metadata(start_time);
CREATE INDEX IF NOT EXISTS idx_ingestion_metadata_status ON ingestion_metadata(status);
CREATE INDEX IF NOT EXISTS idx_ingestion_metadata_environment ON ingestion_metadata(environment);
CREATE INDEX IF NOT EXISTS idx_ingestion_metadata_created_at ON ingestion_metadata(created_at);

-- Comments for documentation
COMMENT ON TABLE ingestion_metadata IS 'Tracks each ingestion run with statistics and error handling';
COMMENT ON COLUMN ingestion_metadata.execution_id IS 'Unique identifier for each ingestion run';
COMMENT ON COLUMN ingestion_metadata.status IS 'Status of the ingestion run: RUNNING, SUCCESS, ERROR, PARTIAL';
COMMENT ON COLUMN ingestion_metadata.records_processed IS 'Total number of records processed from API';
COMMENT ON COLUMN ingestion_metadata.line_items_created IS 'Total number of line items created in the database';
COMMENT ON COLUMN ingestion_metadata.api_response_time_ms IS 'API response time in milliseconds';
COMMENT ON COLUMN ingestion_metadata.total_duration_seconds IS 'Total execution time in seconds';
