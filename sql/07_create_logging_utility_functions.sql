-- =====================================================
-- Logging Utility Functions
-- =====================================================
-- Functions to help implement comprehensive logging

-- Function to log API requests
CREATE OR REPLACE FUNCTION log_api_request(
    p_execution_id VARCHAR(255),
    p_api_endpoint VARCHAR(255),
    p_http_method VARCHAR(10),
    p_request_headers JSONB DEFAULT NULL,
    p_request_params JSONB DEFAULT NULL,
    p_request_body TEXT DEFAULT NULL,
    p_response_status_code INTEGER DEFAULT NULL,
    p_response_headers JSONB DEFAULT NULL,
    p_response_body_size INTEGER DEFAULT NULL,
    p_response_time_ms INTEGER DEFAULT NULL,
    p_error_message TEXT DEFAULT NULL,
    p_error_type VARCHAR(100) DEFAULT NULL,
    p_retry_count INTEGER DEFAULT 0
) RETURNS INTEGER AS $$
DECLARE
    log_id INTEGER;
BEGIN
    INSERT INTO api_request_log (
        execution_id,
        api_endpoint,
        http_method,
        request_headers,
        request_params,
        request_body,
        response_status_code,
        response_headers,
        response_body_size,
        response_time_ms,
        error_message,
        error_type,
        retry_count
    ) VALUES (
        p_execution_id,
        p_api_endpoint,
        p_http_method,
        p_request_headers,
        p_request_params,
        p_request_body,
        p_response_status_code,
        p_response_headers,
        p_response_body_size,
        p_response_time_ms,
        p_error_message,
        p_error_type,
        p_retry_count
    ) RETURNING id INTO log_id;
    
    RETURN log_id;
END;
$$ LANGUAGE plpgsql;

-- Function to log database queries
CREATE OR REPLACE FUNCTION log_database_query(
    p_execution_id VARCHAR(255),
    p_query_type VARCHAR(50),
    p_table_name VARCHAR(100),
    p_query_text TEXT,
    p_query_params JSONB DEFAULT NULL,
    p_execution_time_ms INTEGER DEFAULT NULL,
    p_rows_affected INTEGER DEFAULT NULL,
    p_query_size_bytes INTEGER DEFAULT NULL,
    p_error_message TEXT DEFAULT NULL,
    p_error_type VARCHAR(100) DEFAULT NULL
) RETURNS INTEGER AS $$
DECLARE
    log_id INTEGER;
BEGIN
    INSERT INTO database_query_log (
        execution_id,
        query_type,
        table_name,
        query_text,
        query_params,
        execution_time_ms,
        rows_affected,
        query_size_bytes,
        error_message,
        error_type
    ) VALUES (
        p_execution_id,
        p_query_type,
        p_table_name,
        p_query_text,
        p_query_params,
        p_execution_time_ms,
        p_rows_affected,
        p_query_size_bytes,
        p_error_message,
        p_error_type
    ) RETURNING id INTO log_id;
    
    RETURN log_id;
END;
$$ LANGUAGE plpgsql;

-- Function to log data processing stages
CREATE OR REPLACE FUNCTION log_data_processing(
    p_execution_id VARCHAR(255),
    p_processing_stage VARCHAR(100),
    p_record_count INTEGER DEFAULT NULL,
    p_batch_size INTEGER DEFAULT NULL,
    p_processing_time_ms INTEGER DEFAULT NULL,
    p_memory_usage_mb INTEGER DEFAULT NULL,
    p_validation_errors JSONB DEFAULT NULL,
    p_data_quality_score DECIMAL(3,2) DEFAULT NULL,
    p_error_message TEXT DEFAULT NULL,
    p_error_type VARCHAR(100) DEFAULT NULL,
    p_affected_records JSONB DEFAULT NULL
) RETURNS INTEGER AS $$
DECLARE
    log_id INTEGER;
BEGIN
    INSERT INTO data_processing_log (
        execution_id,
        processing_stage,
        record_count,
        batch_size,
        processing_time_ms,
        memory_usage_mb,
        validation_errors,
        data_quality_score,
        error_message,
        error_type,
        affected_records
    ) VALUES (
        p_execution_id,
        p_processing_stage,
        p_record_count,
        p_batch_size,
        p_processing_time_ms,
        p_memory_usage_mb,
        p_validation_errors,
        p_data_quality_score,
        p_error_message,
        p_error_type,
        p_affected_records
    ) RETURNING id INTO log_id;
    
    RETURN log_id;
END;
$$ LANGUAGE plpgsql;

-- Function to log performance metrics
CREATE OR REPLACE FUNCTION log_performance_metrics(
    p_execution_id VARCHAR(255),
    p_cpu_usage_percent DECIMAL(5,2) DEFAULT NULL,
    p_memory_usage_mb INTEGER DEFAULT NULL,
    p_disk_usage_mb INTEGER DEFAULT NULL,
    p_network_io_mb INTEGER DEFAULT NULL,
    p_lambda_duration_ms INTEGER DEFAULT NULL,
    p_lambda_memory_allocated_mb INTEGER DEFAULT NULL,
    p_lambda_memory_used_mb INTEGER DEFAULT NULL,
    p_lambda_billed_duration_ms INTEGER DEFAULT NULL,
    p_custom_metrics JSONB DEFAULT NULL
) RETURNS INTEGER AS $$
DECLARE
    log_id INTEGER;
BEGIN
    INSERT INTO performance_metrics_log (
        execution_id,
        cpu_usage_percent,
        memory_usage_mb,
        disk_usage_mb,
        network_io_mb,
        lambda_duration_ms,
        lambda_memory_allocated_mb,
        lambda_memory_used_mb,
        lambda_billed_duration_ms,
        custom_metrics
    ) VALUES (
        p_execution_id,
        p_cpu_usage_percent,
        p_memory_usage_mb,
        p_disk_usage_mb,
        p_network_io_mb,
        p_lambda_duration_ms,
        p_lambda_memory_allocated_mb,
        p_lambda_memory_used_mb,
        p_lambda_billed_duration_ms,
        p_custom_metrics
    ) RETURNING id INTO log_id;
    
    RETURN log_id;
END;
$$ LANGUAGE plpgsql;

-- Function to log errors
CREATE OR REPLACE FUNCTION log_error(
    p_execution_id VARCHAR(255),
    p_error_level VARCHAR(20),
    p_error_category VARCHAR(100),
    p_error_message TEXT,
    p_error_type VARCHAR(100) DEFAULT NULL,
    p_error_code VARCHAR(50) DEFAULT NULL,
    p_context_data JSONB DEFAULT NULL,
    p_stack_trace TEXT DEFAULT NULL,
    p_user_agent VARCHAR(255) DEFAULT NULL,
    p_ip_address INET DEFAULT NULL
) RETURNS INTEGER AS $$
DECLARE
    log_id INTEGER;
BEGIN
    INSERT INTO error_tracking_log (
        execution_id,
        error_level,
        error_category,
        error_message,
        error_type,
        error_code,
        context_data,
        stack_trace,
        user_agent,
        ip_address
    ) VALUES (
        p_execution_id,
        p_error_level,
        p_error_category,
        p_error_message,
        p_error_type,
        p_error_code,
        p_context_data,
        p_stack_trace,
        p_user_agent,
        p_ip_address
    ) RETURNING id INTO log_id;
    
    RETURN log_id;
END;
$$ LANGUAGE plpgsql;

-- Function to log data quality checks
CREATE OR REPLACE FUNCTION log_data_quality_check(
    p_execution_id VARCHAR(255),
    p_check_type VARCHAR(100),
    p_check_description TEXT,
    p_total_records_checked INTEGER,
    p_records_passed INTEGER,
    p_records_failed INTEGER,
    p_quality_score DECIMAL(5,2),
    p_issues_found JSONB DEFAULT NULL,
    p_critical_issues_count INTEGER DEFAULT 0,
    p_warning_issues_count INTEGER DEFAULT 0,
    p_recommendations TEXT DEFAULT NULL
) RETURNS INTEGER AS $$
DECLARE
    log_id INTEGER;
BEGIN
    INSERT INTO data_quality_log (
        execution_id,
        check_type,
        check_description,
        total_records_checked,
        records_passed,
        records_failed,
        quality_score,
        issues_found,
        critical_issues_count,
        warning_issues_count,
        recommendations
    ) VALUES (
        p_execution_id,
        p_check_type,
        p_check_description,
        p_total_records_checked,
        p_records_passed,
        p_records_failed,
        p_quality_score,
        p_issues_found,
        p_critical_issues_count,
        p_warning_issues_count,
        p_recommendations
    ) RETURNING id INTO log_id;
    
    RETURN log_id;
END;
$$ LANGUAGE plpgsql;

-- Function to log business validation
CREATE OR REPLACE FUNCTION log_business_validation(
    p_execution_id VARCHAR(255),
    p_validation_rule VARCHAR(100),
    p_validation_description TEXT,
    p_business_impact VARCHAR(50),
    p_records_validated INTEGER,
    p_records_passed INTEGER,
    p_records_failed INTEGER,
    p_validation_score DECIMAL(5,2),
    p_failed_records JSONB DEFAULT NULL,
    p_failure_reasons JSONB DEFAULT NULL,
    p_auto_corrected BOOLEAN DEFAULT FALSE,
    p_manual_review_required BOOLEAN DEFAULT FALSE,
    p_escalation_required BOOLEAN DEFAULT FALSE
) RETURNS INTEGER AS $$
DECLARE
    log_id INTEGER;
BEGIN
    INSERT INTO business_validation_log (
        execution_id,
        validation_rule,
        validation_description,
        business_impact,
        records_validated,
        records_passed,
        records_failed,
        validation_score,
        failed_records,
        failure_reasons,
        auto_corrected,
        manual_review_required,
        escalation_required
    ) VALUES (
        p_execution_id,
        p_validation_rule,
        p_validation_description,
        p_business_impact,
        p_records_validated,
        p_records_passed,
        p_records_failed,
        p_validation_score,
        p_failed_records,
        p_failure_reasons,
        p_auto_corrected,
        p_manual_review_required,
        p_escalation_required
    ) RETURNING id INTO log_id;
    
    RETURN log_id;
END;
$$ LANGUAGE plpgsql;

-- Function to get execution summary
CREATE OR REPLACE FUNCTION get_execution_summary(p_execution_id VARCHAR(255))
RETURNS TABLE (
    execution_id VARCHAR(255),
    status VARCHAR(50),
    start_time TIMESTAMP WITH TIME ZONE,
    end_time TIMESTAMP WITH TIME ZONE,
    total_duration_seconds DECIMAL(10,2),
    records_processed INTEGER,
    records_inserted INTEGER,
    records_failed INTEGER,
    api_requests_count INTEGER,
    api_errors_count INTEGER,
    db_queries_count INTEGER,
    db_errors_count INTEGER,
    processing_stages_count INTEGER,
    errors_count INTEGER,
    quality_checks_count INTEGER,
    validations_count INTEGER,
    avg_api_response_time_ms INTEGER,
    avg_db_query_time_ms INTEGER,
    avg_processing_time_ms INTEGER
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        im.execution_id,
        im.status,
        im.start_time,
        im.end_time,
        im.total_duration_seconds,
        im.records_processed,
        im.records_inserted,
        im.records_failed,
        COUNT(DISTINCT arl.id)::INTEGER as api_requests_count,
        COUNT(DISTINCT CASE WHEN arl.error_message IS NOT NULL THEN arl.id END)::INTEGER as api_errors_count,
        COUNT(DISTINCT dql.id)::INTEGER as db_queries_count,
        COUNT(DISTINCT CASE WHEN dql.error_message IS NOT NULL THEN dql.id END)::INTEGER as db_errors_count,
        COUNT(DISTINCT dpl.id)::INTEGER as processing_stages_count,
        COUNT(DISTINCT etl.id)::INTEGER as errors_count,
        COUNT(DISTINCT dql2.id)::INTEGER as quality_checks_count,
        COUNT(DISTINCT bvl.id)::INTEGER as validations_count,
        AVG(arl.response_time_ms)::INTEGER as avg_api_response_time_ms,
        AVG(dql.execution_time_ms)::INTEGER as avg_db_query_time_ms,
        AVG(dpl.processing_time_ms)::INTEGER as avg_processing_time_ms
    FROM ingestion_metadata im
    LEFT JOIN api_request_log arl ON im.execution_id = arl.execution_id
    LEFT JOIN database_query_log dql ON im.execution_id = dql.execution_id
    LEFT JOIN data_processing_log dpl ON im.execution_id = dpl.execution_id
    LEFT JOIN error_tracking_log etl ON im.execution_id = etl.execution_id
    LEFT JOIN data_quality_log dql2 ON im.execution_id = dql2.execution_id
    LEFT JOIN business_validation_log bvl ON im.execution_id = bvl.execution_id
    WHERE im.execution_id = p_execution_id
    GROUP BY 
        im.execution_id,
        im.status,
        im.start_time,
        im.end_time,
        im.total_duration_seconds,
        im.records_processed,
        im.records_inserted,
        im.records_failed;
END;
$$ LANGUAGE plpgsql;

-- Function to get recent executions with errors
CREATE OR REPLACE FUNCTION get_recent_executions_with_errors(p_hours_back INTEGER DEFAULT 24)
RETURNS TABLE (
    execution_id VARCHAR(255),
    start_time TIMESTAMP WITH TIME ZONE,
    status VARCHAR(50),
    error_count INTEGER,
    critical_errors INTEGER,
    error_summary TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        im.execution_id,
        im.start_time,
        im.status,
        COUNT(etl.id) as error_count,
        COUNT(CASE WHEN etl.error_level = 'CRITICAL' THEN 1 END) as critical_errors,
        STRING_AGG(DISTINCT etl.error_message, '; ' ORDER BY etl.error_message) as error_summary
    FROM ingestion_metadata im
    LEFT JOIN error_tracking_log etl ON im.execution_id = etl.execution_id
    WHERE im.start_time >= CURRENT_TIMESTAMP - INTERVAL '1 hour' * p_hours_back
    AND (etl.id IS NOT NULL OR im.status = 'ERROR')
    GROUP BY im.execution_id, im.start_time, im.status
    ORDER BY im.start_time DESC;
END;
$$ LANGUAGE plpgsql;

-- Function to get performance trends
CREATE OR REPLACE FUNCTION get_performance_trends(p_days_back INTEGER DEFAULT 7)
RETURNS TABLE (
    execution_date DATE,
    total_executions INTEGER,
    avg_duration_seconds DECIMAL(10,2),
    avg_api_response_time_ms INTEGER,
    avg_db_query_time_ms INTEGER,
    success_rate DECIMAL(5,2),
    error_rate DECIMAL(5,2)
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        DATE(im.start_time) as execution_date,
        COUNT(*) as total_executions,
        AVG(im.total_duration_seconds) as avg_duration_seconds,
        AVG(pml.lambda_duration_ms)::INTEGER as avg_api_response_time_ms,
        AVG(dql.execution_time_ms)::INTEGER as avg_db_query_time_ms,
        (COUNT(CASE WHEN im.status = 'SUCCESS' THEN 1 END) * 100.0 / COUNT(*))::DECIMAL(5,2) as success_rate,
        (COUNT(CASE WHEN im.status = 'ERROR' THEN 1 END) * 100.0 / COUNT(*))::DECIMAL(5,2) as error_rate
    FROM ingestion_metadata im
    LEFT JOIN performance_metrics_log pml ON im.execution_id = pml.execution_id
    LEFT JOIN database_query_log dql ON im.execution_id = dql.execution_id
    WHERE im.start_time >= CURRENT_DATE - INTERVAL '1 day' * p_days_back
    GROUP BY DATE(im.start_time)
    ORDER BY execution_date DESC;
END;
$$ LANGUAGE plpgsql;
