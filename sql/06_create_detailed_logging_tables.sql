-- =====================================================
-- Detailed Logging Tables for Enhanced Monitoring
-- =====================================================
-- These tables provide granular logging for debugging and monitoring

-- API Request/Response Logging
CREATE TABLE IF NOT EXISTS api_request_log (
    id SERIAL PRIMARY KEY,
    execution_id VARCHAR(255) NOT NULL,
    request_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Request Details
    api_endpoint VARCHAR(255) NOT NULL,
    http_method VARCHAR(10) NOT NULL,
    request_headers JSONB,
    request_params JSONB,
    request_body TEXT,
    
    -- Response Details
    response_status_code INTEGER,
    response_headers JSONB,
    response_body_size INTEGER,
    response_time_ms INTEGER,
    
    -- Error Information
    error_message TEXT,
    error_type VARCHAR(100),
    retry_count INTEGER DEFAULT 0,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (execution_id) REFERENCES ingestion_metadata(execution_id) ON DELETE CASCADE
);

-- Database Query Logging
CREATE TABLE IF NOT EXISTS database_query_log (
    id SERIAL PRIMARY KEY,
    execution_id VARCHAR(255) NOT NULL,
    query_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Query Details
    query_type VARCHAR(50) NOT NULL, -- 'SELECT', 'INSERT', 'UPDATE', 'DELETE'
    table_name VARCHAR(100),
    query_text TEXT,
    query_params JSONB,
    
    -- Performance Metrics
    execution_time_ms INTEGER,
    rows_affected INTEGER,
    query_size_bytes INTEGER,
    
    -- Error Information
    error_message TEXT,
    error_type VARCHAR(100),
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (execution_id) REFERENCES ingestion_metadata(execution_id) ON DELETE CASCADE
);

-- Data Processing Logging
CREATE TABLE IF NOT EXISTS data_processing_log (
    id SERIAL PRIMARY KEY,
    execution_id VARCHAR(255) NOT NULL,
    processing_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Processing Details
    processing_stage VARCHAR(100) NOT NULL, -- 'API_FETCH', 'JSON_PARSE', 'DATA_TRANSFORM', 'DB_INSERT'
    record_count INTEGER,
    batch_size INTEGER,
    
    -- Performance Metrics
    processing_time_ms INTEGER,
    memory_usage_mb INTEGER,
    
    -- Validation Results
    validation_errors JSONB,
    data_quality_score DECIMAL(3,2), -- 0.00 to 1.00
    
    -- Error Information
    error_message TEXT,
    error_type VARCHAR(100),
    affected_records JSONB,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (execution_id) REFERENCES ingestion_metadata(execution_id) ON DELETE CASCADE
);

-- Performance Metrics Logging
CREATE TABLE IF NOT EXISTS performance_metrics_log (
    id SERIAL PRIMARY KEY,
    execution_id VARCHAR(255) NOT NULL,
    metric_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- System Metrics
    cpu_usage_percent DECIMAL(5,2),
    memory_usage_mb INTEGER,
    disk_usage_mb INTEGER,
    network_io_mb INTEGER,
    
    -- Lambda Specific Metrics
    lambda_duration_ms INTEGER,
    lambda_memory_allocated_mb INTEGER,
    lambda_memory_used_mb INTEGER,
    lambda_billed_duration_ms INTEGER,
    
    -- Custom Metrics
    custom_metrics JSONB,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (execution_id) REFERENCES ingestion_metadata(execution_id) ON DELETE CASCADE
);

-- Error Tracking Table
CREATE TABLE IF NOT EXISTS error_tracking_log (
    id SERIAL PRIMARY KEY,
    execution_id VARCHAR(255) NOT NULL,
    error_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Error Details
    error_level VARCHAR(20) NOT NULL, -- 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'
    error_category VARCHAR(100), -- 'API', 'DATABASE', 'DATA_PROCESSING', 'SYSTEM'
    error_message TEXT NOT NULL,
    error_type VARCHAR(100),
    error_code VARCHAR(50),
    
    -- Context Information
    context_data JSONB, -- Additional context about the error
    stack_trace TEXT,
    user_agent VARCHAR(255),
    ip_address INET,
    
    -- Resolution Tracking
    resolved BOOLEAN DEFAULT FALSE,
    resolution_notes TEXT,
    resolved_by VARCHAR(100),
    resolved_at TIMESTAMP WITH TIME ZONE,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (execution_id) REFERENCES ingestion_metadata(execution_id) ON DELETE CASCADE
);

-- Data Quality Monitoring
CREATE TABLE IF NOT EXISTS data_quality_log (
    id SERIAL PRIMARY KEY,
    execution_id VARCHAR(255) NOT NULL,
    quality_check_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Quality Check Details
    check_type VARCHAR(100) NOT NULL, -- 'COMPLETENESS', 'ACCURACY', 'CONSISTENCY', 'VALIDITY'
    check_description TEXT,
    
    -- Results
    total_records_checked INTEGER,
    records_passed INTEGER,
    records_failed INTEGER,
    quality_score DECIMAL(5,2), -- 0.00 to 100.00
    
    -- Issues Found
    issues_found JSONB, -- Detailed list of quality issues
    critical_issues_count INTEGER,
    warning_issues_count INTEGER,
    
    -- Recommendations
    recommendations TEXT,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (execution_id) REFERENCES ingestion_metadata(execution_id) ON DELETE CASCADE
);

-- Business Logic Validation
CREATE TABLE IF NOT EXISTS business_validation_log (
    id SERIAL PRIMARY KEY,
    execution_id VARCHAR(255) NOT NULL,
    validation_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Validation Details
    validation_rule VARCHAR(100) NOT NULL,
    validation_description TEXT,
    business_impact VARCHAR(50), -- 'LOW', 'MEDIUM', 'HIGH', 'CRITICAL'
    
    -- Results
    records_validated INTEGER,
    records_passed INTEGER,
    records_failed INTEGER,
    validation_score DECIMAL(5,2),
    
    -- Failed Records Details
    failed_records JSONB,
    failure_reasons JSONB,
    
    -- Actions Taken
    auto_corrected BOOLEAN DEFAULT FALSE,
    manual_review_required BOOLEAN DEFAULT FALSE,
    escalation_required BOOLEAN DEFAULT FALSE,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (execution_id) REFERENCES ingestion_metadata(execution_id) ON DELETE CASCADE
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_api_request_log_execution_id ON api_request_log(execution_id);
CREATE INDEX IF NOT EXISTS idx_api_request_log_timestamp ON api_request_log(request_timestamp);
CREATE INDEX IF NOT EXISTS idx_api_request_log_status_code ON api_request_log(response_status_code);

CREATE INDEX IF NOT EXISTS idx_database_query_log_execution_id ON database_query_log(execution_id);
CREATE INDEX IF NOT EXISTS idx_database_query_log_timestamp ON database_query_log(query_timestamp);
CREATE INDEX IF NOT EXISTS idx_database_query_log_type ON database_query_log(query_type);

CREATE INDEX IF NOT EXISTS idx_data_processing_log_execution_id ON data_processing_log(execution_id);
CREATE INDEX IF NOT EXISTS idx_data_processing_log_stage ON data_processing_log(processing_stage);
CREATE INDEX IF NOT EXISTS idx_data_processing_log_timestamp ON data_processing_log(processing_timestamp);

CREATE INDEX IF NOT EXISTS idx_performance_metrics_log_execution_id ON performance_metrics_log(execution_id);
CREATE INDEX IF NOT EXISTS idx_performance_metrics_log_timestamp ON performance_metrics_log(metric_timestamp);

CREATE INDEX IF NOT EXISTS idx_error_tracking_log_execution_id ON error_tracking_log(execution_id);
CREATE INDEX IF NOT EXISTS idx_error_tracking_log_level ON error_tracking_log(error_level);
CREATE INDEX IF NOT EXISTS idx_error_tracking_log_category ON error_tracking_log(error_category);
CREATE INDEX IF NOT EXISTS idx_error_tracking_log_resolved ON error_tracking_log(resolved);

CREATE INDEX IF NOT EXISTS idx_data_quality_log_execution_id ON data_quality_log(execution_id);
CREATE INDEX IF NOT EXISTS idx_data_quality_log_type ON data_quality_log(check_type);
CREATE INDEX IF NOT EXISTS idx_data_quality_log_timestamp ON data_quality_log(quality_check_timestamp);

CREATE INDEX IF NOT EXISTS idx_business_validation_log_execution_id ON business_validation_log(execution_id);
CREATE INDEX IF NOT EXISTS idx_business_validation_log_rule ON business_validation_log(validation_rule);
CREATE INDEX IF NOT EXISTS idx_business_validation_log_impact ON business_validation_log(business_impact);

-- Comments for documentation
COMMENT ON TABLE api_request_log IS 'Detailed logging of all API requests and responses';
COMMENT ON TABLE database_query_log IS 'Logging of all database queries for performance monitoring';
COMMENT ON TABLE data_processing_log IS 'Logging of data processing stages and performance';
COMMENT ON TABLE performance_metrics_log IS 'System and Lambda performance metrics';
COMMENT ON TABLE error_tracking_log IS 'Comprehensive error tracking and resolution';
COMMENT ON TABLE data_quality_log IS 'Data quality monitoring and validation results';
COMMENT ON TABLE business_validation_log IS 'Business logic validation and rule checking';
