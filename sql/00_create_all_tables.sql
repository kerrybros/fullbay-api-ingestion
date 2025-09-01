-- =====================================================
-- Fullbay API Database Schema - Complete Setup
-- =====================================================
-- This script creates all tables, indexes, functions, and triggers
-- for the Fullbay API data ingestion system

-- Set timezone for consistent timestamp handling
SET timezone = 'UTC';

-- =====================================================
-- 1. Raw Data Table
-- =====================================================
-- Stores complete JSON responses from Fullbay API

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

-- Indexes for raw data table
CREATE INDEX IF NOT EXISTS idx_fullbay_raw_data_invoice_id ON fullbay_raw_data(fullbay_invoice_id);
CREATE INDEX IF NOT EXISTS idx_fullbay_raw_data_processed ON fullbay_raw_data(processed);
CREATE INDEX IF NOT EXISTS idx_fullbay_raw_data_ingestion ON fullbay_raw_data(ingestion_timestamp);
CREATE INDEX IF NOT EXISTS idx_fullbay_raw_data_json_gin ON fullbay_raw_data USING GIN(raw_json_data);

-- =====================================================
-- 2. Line Items Table
-- =====================================================
-- Stores flattened line items from Fullbay invoices

CREATE TABLE IF NOT EXISTS fullbay_line_items (
    id SERIAL PRIMARY KEY,
    raw_data_id INTEGER REFERENCES fullbay_raw_data(id) ON DELETE CASCADE,
    
    -- === INVOICE LEVEL INFO (Repeated on each row) ===
    fullbay_invoice_id VARCHAR(50) NOT NULL,
    invoice_number VARCHAR(50),
    invoice_date DATE,
    due_date DATE,
    exported BOOLEAN DEFAULT FALSE,
    
    -- === SHOP INFO (Repeated on each row) ===
    shop_title VARCHAR(255),
    shop_email VARCHAR(255),
    shop_physical_address TEXT,
    
    -- === CUSTOMER INFO (Repeated on each row) ===
    customer_id INTEGER,
    customer_title VARCHAR(255),
    customer_external_id VARCHAR(50),
    customer_main_phone VARCHAR(20),
    customer_secondary_phone VARCHAR(20),
    customer_billing_employee VARCHAR(255),
    customer_billing_email VARCHAR(255),
    customer_billing_address TEXT,
    
    -- === SERVICE ORDER INFO (Repeated on each row) ===
    fullbay_service_order_id VARCHAR(50),
    repair_order_number VARCHAR(50),
    service_order_created TIMESTAMP WITH TIME ZONE,
    service_order_start_date TIMESTAMP WITH TIME ZONE,
    service_order_completion_date TIMESTAMP WITH TIME ZONE,
    service_order_description TEXT,
    
    -- === UNIT/VEHICLE INFO (Repeated on each row) ===
    unit_id VARCHAR(50),
    unit_number VARCHAR(50),
    unit_nickname VARCHAR(100),
    unit_type VARCHAR(100),
    unit_subtype VARCHAR(100),
    unit_year VARCHAR(10),
    unit_make VARCHAR(100),
    unit_model VARCHAR(100),
    unit_vin VARCHAR(50),
    unit_license_plate VARCHAR(20),
    
    -- === PRIMARY TECHNICIAN INFO (Repeated on each row) ===
    primary_technician VARCHAR(255),
    primary_technician_number VARCHAR(50),
    parts_manager VARCHAR(255),
    parts_manager_number VARCHAR(50),
    
    -- === COMPLAINT/WORK ORDER INFO (Repeated on each row) ===
    fullbay_complaint_id INTEGER,
    complaint_type VARCHAR(100),
    complaint_subtype VARCHAR(100),
    complaint_note TEXT,
    complaint_cause TEXT,
    complaint_cause_type VARCHAR(100),
    complaint_authorized BOOLEAN,
    complaint_severity VARCHAR(50),
    complaint_mileage_rate VARCHAR(50),
    complaint_labor_rate VARCHAR(50),
    
    -- === CORRECTION/SERVICE INFO (Repeated on each row) ===
    fullbay_correction_id INTEGER,
    correction_title VARCHAR(255),
    global_component VARCHAR(255),
    global_system VARCHAR(255),
    global_service VARCHAR(255),
    unit_service VARCHAR(255),
    recommended_correction TEXT,
    actual_correction TEXT,
    correction_performed VARCHAR(50),
    correction_pre_authorized BOOLEAN,
    correction_pre_paid BOOLEAN,
    
    -- === LINE ITEM DETAILS (Unique per row) ===
    line_item_type VARCHAR(20) NOT NULL, -- 'PART', 'LABOR', 'SUPPLY', 'FREIGHT', 'MISC', 'SUBLET'
    
    -- For Parts
    fullbay_part_id INTEGER,
    part_description TEXT,
    shop_part_number VARCHAR(100),
    vendor_part_number VARCHAR(100),
    part_category VARCHAR(255),
    part_manufacturer VARCHAR(255),
    
    -- For Labor/Services  
    labor_description TEXT,
    labor_rate_type VARCHAR(50),
    assigned_technician VARCHAR(255),
    assigned_technician_number VARCHAR(50),
    assigned_technician_actual_hours DECIMAL(8,4),
    assigned_technician_portion INTEGER, -- Percentage of work for this technician
    
    -- === QUANTITIES ===
    quantity DECIMAL(10,3),
    to_be_returned_quantity DECIMAL(10,3),
    returned_quantity DECIMAL(10,3),
    
    -- === HOURS (for labor items) ===
    labor_hours DECIMAL(8,2),
    actual_hours DECIMAL(8,2),
    
    -- === FINANCIAL DETAILS (Per Line Item) ===
    unit_cost DECIMAL(10,2),
    unit_price DECIMAL(10,2),
    line_total_cost DECIMAL(10,2),
    line_total_price DECIMAL(10,2),
    price_overridden BOOLEAN DEFAULT FALSE,
    
    -- === TAX INFO ===
    taxable BOOLEAN DEFAULT TRUE,
    tax_rate DECIMAL(5,2),
    tax_amount DECIMAL(10,2),
    
    -- === CLASSIFICATION ===
    inventory_item BOOLEAN DEFAULT FALSE,
    core_type VARCHAR(50),
    sublet BOOLEAN DEFAULT FALSE,
    
    -- === QUICKBOOKS INTEGRATION ===
    quickbooks_account VARCHAR(255),
    quickbooks_item VARCHAR(255),
    quickbooks_item_type VARCHAR(100),
    quickbooks_id VARCHAR(50),
    
    -- === INVOICE TOTALS (Repeated on each row for context) ===
    invoice_misc_charge_total DECIMAL(10,2),
    invoice_service_call_total DECIMAL(10,2),
    invoice_mileage_total DECIMAL(10,2),
    invoice_mileage_cost_total DECIMAL(10,2),
    invoice_parts_total DECIMAL(10,2),
    invoice_labor_hours_total DECIMAL(8,2),
    invoice_labor_total DECIMAL(10,2),
    invoice_sublet_labor_total DECIMAL(10,2),
    invoice_supplies_total DECIMAL(10,2),
    invoice_subtotal DECIMAL(10,2),
    invoice_tax_total DECIMAL(10,2),
    invoice_total DECIMAL(10,2),
    invoice_balance DECIMAL(10,2),
    
    -- === SERVICE ORDER TOTALS (Repeated on each row for context) ===
    so_total_parts_cost DECIMAL(10,2),
    so_total_parts_price DECIMAL(10,2),
    so_total_labor_hours DECIMAL(8,2),
    so_total_labor_cost DECIMAL(10,2),
    so_subtotal DECIMAL(10,2),
    so_tax_total DECIMAL(10,2),
    so_total_amount DECIMAL(10,2),
    
    -- === METADATA ===
    ingestion_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ingestion_source VARCHAR(100) DEFAULT 'fullbay_api',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for line items table
CREATE INDEX IF NOT EXISTS idx_fullbay_line_items_invoice_id ON fullbay_line_items(fullbay_invoice_id);
CREATE INDEX IF NOT EXISTS idx_fullbay_line_items_invoice_date ON fullbay_line_items(invoice_date);
CREATE INDEX IF NOT EXISTS idx_fullbay_line_items_customer_id ON fullbay_line_items(customer_id);
CREATE INDEX IF NOT EXISTS idx_fullbay_line_items_repair_order ON fullbay_line_items(repair_order_number);
CREATE INDEX IF NOT EXISTS idx_fullbay_line_items_unit_vin ON fullbay_line_items(unit_vin);
CREATE INDEX IF NOT EXISTS idx_fullbay_line_items_line_type ON fullbay_line_items(line_item_type);
CREATE INDEX IF NOT EXISTS idx_fullbay_line_items_part_number ON fullbay_line_items(shop_part_number);
CREATE INDEX IF NOT EXISTS idx_fullbay_line_items_technician ON fullbay_line_items(assigned_technician);
CREATE INDEX IF NOT EXISTS idx_fullbay_line_items_ingestion ON fullbay_line_items(ingestion_timestamp);
CREATE INDEX IF NOT EXISTS idx_fullbay_line_items_complaint_id ON fullbay_line_items(fullbay_complaint_id);
CREATE INDEX IF NOT EXISTS idx_fullbay_line_items_correction_id ON fullbay_line_items(fullbay_correction_id);
CREATE INDEX IF NOT EXISTS idx_fullbay_line_items_part_id ON fullbay_line_items(fullbay_part_id);

-- =====================================================
-- 3. Metadata Table
-- =====================================================
-- Tracks each ingestion run with statistics

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

-- Indexes for metadata table
CREATE INDEX IF NOT EXISTS idx_ingestion_metadata_execution_id ON ingestion_metadata(execution_id);
CREATE INDEX IF NOT EXISTS idx_ingestion_metadata_start_time ON ingestion_metadata(start_time);
CREATE INDEX IF NOT EXISTS idx_ingestion_metadata_status ON ingestion_metadata(status);
CREATE INDEX IF NOT EXISTS idx_ingestion_metadata_environment ON ingestion_metadata(environment);
CREATE INDEX IF NOT EXISTS idx_ingestion_metadata_created_at ON ingestion_metadata(created_at);

-- =====================================================
-- 4. Summary Tables
-- =====================================================

-- Daily Summary Table
CREATE TABLE IF NOT EXISTS daily_summary (
    id SERIAL PRIMARY KEY,
    summary_date DATE NOT NULL,
    
    -- Invoice Counts
    total_invoices INTEGER DEFAULT 0,
    exported_invoices INTEGER DEFAULT 0,
    unexported_invoices INTEGER DEFAULT 0,
    
    -- Financial Totals
    total_revenue DECIMAL(12,2) DEFAULT 0,
    total_parts_revenue DECIMAL(12,2) DEFAULT 0,
    total_labor_revenue DECIMAL(12,2) DEFAULT 0,
    total_supplies_revenue DECIMAL(12,2) DEFAULT 0,
    total_misc_revenue DECIMAL(12,2) DEFAULT 0,
    total_sublet_revenue DECIMAL(12,2) DEFAULT 0,
    
    -- Labor Hours
    total_labor_hours DECIMAL(10,2) DEFAULT 0,
    total_actual_hours DECIMAL(10,2) DEFAULT 0,
    
    -- Parts Statistics
    total_parts_quantity DECIMAL(10,2) DEFAULT 0,
    unique_parts_count INTEGER DEFAULT 0,
    
    -- Customer Statistics
    unique_customers INTEGER DEFAULT 0,
    unique_vehicles INTEGER DEFAULT 0,
    
    -- Technician Statistics
    unique_technicians INTEGER DEFAULT 0,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(summary_date)
);

-- Monthly Summary Table
CREATE TABLE IF NOT EXISTS monthly_summary (
    id SERIAL PRIMARY KEY,
    summary_year INTEGER NOT NULL,
    summary_month INTEGER NOT NULL,
    
    -- Invoice Counts
    total_invoices INTEGER DEFAULT 0,
    exported_invoices INTEGER DEFAULT 0,
    unexported_invoices INTEGER DEFAULT 0,
    
    -- Financial Totals
    total_revenue DECIMAL(12,2) DEFAULT 0,
    total_parts_revenue DECIMAL(12,2) DEFAULT 0,
    total_labor_revenue DECIMAL(12,2) DEFAULT 0,
    total_supplies_revenue DECIMAL(12,2) DEFAULT 0,
    total_misc_revenue DECIMAL(12,2) DEFAULT 0,
    total_sublet_revenue DECIMAL(12,2) DEFAULT 0,
    
    -- Labor Hours
    total_labor_hours DECIMAL(10,2) DEFAULT 0,
    total_actual_hours DECIMAL(10,2) DEFAULT 0,
    
    -- Parts Statistics
    total_parts_quantity DECIMAL(10,2) DEFAULT 0,
    unique_parts_count INTEGER DEFAULT 0,
    
    -- Customer Statistics
    unique_customers INTEGER DEFAULT 0,
    unique_vehicles INTEGER DEFAULT 0,
    
    -- Technician Statistics
    unique_technicians INTEGER DEFAULT 0,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(summary_year, summary_month)
);

-- Customer Summary Table
CREATE TABLE IF NOT EXISTS customer_summary (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    customer_title VARCHAR(255),
    
    -- Invoice Statistics
    total_invoices INTEGER DEFAULT 0,
    first_invoice_date DATE,
    last_invoice_date DATE,
    
    -- Financial Totals
    total_revenue DECIMAL(12,2) DEFAULT 0,
    total_parts_revenue DECIMAL(12,2) DEFAULT 0,
    total_labor_revenue DECIMAL(12,2) DEFAULT 0,
    total_supplies_revenue DECIMAL(12,2) DEFAULT 0,
    total_misc_revenue DECIMAL(12,2) DEFAULT 0,
    total_sublet_revenue DECIMAL(12,2) DEFAULT 0,
    
    -- Vehicle Statistics
    unique_vehicles INTEGER DEFAULT 0,
    vehicle_list TEXT, -- JSON array of VINs
    
    -- Labor Statistics
    total_labor_hours DECIMAL(10,2) DEFAULT 0,
    total_actual_hours DECIMAL(10,2) DEFAULT 0,
    
    -- Parts Statistics
    total_parts_quantity DECIMAL(10,2) DEFAULT 0,
    unique_parts_count INTEGER DEFAULT 0,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(customer_id)
);

-- Vehicle Summary Table
CREATE TABLE IF NOT EXISTS vehicle_summary (
    id SERIAL PRIMARY KEY,
    unit_vin VARCHAR(50) NOT NULL,
    unit_number VARCHAR(50),
    unit_year VARCHAR(10),
    unit_make VARCHAR(100),
    unit_model VARCHAR(100),
    unit_type VARCHAR(100),
    
    -- Invoice Statistics
    total_invoices INTEGER DEFAULT 0,
    first_invoice_date DATE,
    last_invoice_date DATE,
    
    -- Financial Totals
    total_revenue DECIMAL(12,2) DEFAULT 0,
    total_parts_revenue DECIMAL(12,2) DEFAULT 0,
    total_labor_revenue DECIMAL(12,2) DEFAULT 0,
    total_supplies_revenue DECIMAL(12,2) DEFAULT 0,
    total_misc_revenue DECIMAL(12,2) DEFAULT 0,
    total_sublet_revenue DECIMAL(12,2) DEFAULT 0,
    
    -- Labor Statistics
    total_labor_hours DECIMAL(10,2) DEFAULT 0,
    total_actual_hours DECIMAL(10,2) DEFAULT 0,
    
    -- Parts Statistics
    total_parts_quantity DECIMAL(10,2) DEFAULT 0,
    unique_parts_count INTEGER DEFAULT 0,
    
    -- Customer Information
    primary_customer_id INTEGER,
    primary_customer_title VARCHAR(255),
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(unit_vin)
);

-- Indexes for summary tables
CREATE INDEX IF NOT EXISTS idx_daily_summary_date ON daily_summary(summary_date);
CREATE INDEX IF NOT EXISTS idx_monthly_summary_year_month ON monthly_summary(summary_year, summary_month);
CREATE INDEX IF NOT EXISTS idx_customer_summary_customer_id ON customer_summary(customer_id);
CREATE INDEX IF NOT EXISTS idx_vehicle_summary_vin ON vehicle_summary(unit_vin);

-- =====================================================
-- 5. Detailed Logging Tables
-- =====================================================

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

-- =====================================================
-- 6. Duplicate Check Placeholder
-- =====================================================

-- Placeholder table for tracking processed invoices
CREATE TABLE IF NOT EXISTS processed_invoices_tracker (
    id SERIAL PRIMARY KEY,
    fullbay_invoice_id VARCHAR(50) UNIQUE NOT NULL,
    first_processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    processing_count INTEGER DEFAULT 1,
    status VARCHAR(50) DEFAULT 'PROCESSED', -- 'PROCESSED', 'SKIPPED', 'ERROR'
    skip_reason TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for logging tables
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

-- Indexes for duplicate check table
CREATE INDEX IF NOT EXISTS idx_processed_invoices_tracker_invoice_id ON processed_invoices_tracker(fullbay_invoice_id);
CREATE INDEX IF NOT EXISTS idx_processed_invoices_tracker_status ON processed_invoices_tracker(status);
CREATE INDEX IF NOT EXISTS idx_processed_invoices_tracker_last_processed ON processed_invoices_tracker(last_processed_at);

-- =====================================================
-- 7. Utility Functions and Triggers
-- =====================================================

-- Function to update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create triggers for updated_at columns
CREATE TRIGGER update_fullbay_raw_data_updated_at 
    BEFORE UPDATE ON fullbay_raw_data 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_fullbay_line_items_updated_at 
    BEFORE UPDATE ON fullbay_line_items 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_ingestion_metadata_updated_at 
    BEFORE UPDATE ON ingestion_metadata 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_daily_summary_updated_at 
    BEFORE UPDATE ON daily_summary 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_monthly_summary_updated_at 
    BEFORE UPDATE ON monthly_summary 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_customer_summary_updated_at 
    BEFORE UPDATE ON customer_summary 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_vehicle_summary_updated_at 
    BEFORE UPDATE ON vehicle_summary 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_processed_invoices_tracker_updated_at 
    BEFORE UPDATE ON processed_invoices_tracker 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- =====================================================
-- 8. Comments for Documentation
-- =====================================================

COMMENT ON TABLE fullbay_raw_data IS 'Stores complete JSON responses from Fullbay API for backup and reprocessing';
COMMENT ON TABLE fullbay_line_items IS 'Flattened line items from Fullbay invoices for reporting and analysis';
COMMENT ON TABLE ingestion_metadata IS 'Tracks each ingestion run with statistics and error handling';
COMMENT ON TABLE daily_summary IS 'Daily aggregated statistics for reporting';
COMMENT ON TABLE monthly_summary IS 'Monthly aggregated statistics for reporting';
COMMENT ON TABLE customer_summary IS 'Customer-level aggregated statistics';
COMMENT ON TABLE vehicle_summary IS 'Vehicle-level aggregated statistics';
COMMENT ON TABLE api_request_log IS 'Detailed logging of all API requests and responses';
COMMENT ON TABLE database_query_log IS 'Logging of all database queries for performance monitoring';
COMMENT ON TABLE data_processing_log IS 'Logging of data processing stages and performance';
COMMENT ON TABLE performance_metrics_log IS 'System and Lambda performance metrics';
COMMENT ON TABLE error_tracking_log IS 'Comprehensive error tracking and resolution';
COMMENT ON TABLE data_quality_log IS 'Data quality monitoring and validation results';
COMMENT ON TABLE business_validation_log IS 'Business logic validation and rule checking';
COMMENT ON TABLE processed_invoices_tracker IS 'Tracks processed invoices to prevent duplicates - PLACEHOLDER FOR FUTURE IMPLEMENTATION';

COMMENT ON COLUMN fullbay_line_items.line_item_type IS 'Type of line item: PART, LABOR, SUPPLY, FREIGHT, MISC, SUBLET';
COMMENT ON COLUMN fullbay_line_items.assigned_technician_portion IS 'Percentage of work assigned to this technician (0-100)';
COMMENT ON COLUMN fullbay_line_items.quantity IS 'Quantity of parts/supplies or hours for labor';
COMMENT ON COLUMN fullbay_line_items.unit_cost IS 'Cost per unit (internal)';
COMMENT ON COLUMN fullbay_line_items.unit_price IS 'Price per unit (customer-facing)';

-- =====================================================
-- Setup Complete
-- =====================================================

SELECT 'Database schema setup completed successfully!' as status;
