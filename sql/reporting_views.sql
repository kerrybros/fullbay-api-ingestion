-- ===============================================
-- FULLBAY DATA INGESTION - REPORTING VIEWS
-- ===============================================
-- 
-- These views provide business intelligence and operational monitoring
-- capabilities for the Fullbay data ingestion system.
--

-- ===============================================
-- 1. INGESTION MONITORING VIEWS
-- ===============================================

-- View: Ingestion Performance Summary
CREATE OR REPLACE VIEW v_ingestion_performance AS
SELECT 
    DATE(ingestion_timestamp) as ingestion_date,
    COUNT(*) as total_raw_records,
    COUNT(CASE WHEN processed = true THEN 1 END) as processed_records,
    COUNT(CASE WHEN processed = false THEN 1 END) as pending_records,
    COUNT(CASE WHEN processing_errors IS NOT NULL THEN 1 END) as error_records,
    
    -- Calculate processing success rate
    ROUND(
        (COUNT(CASE WHEN processed = true THEN 1 END) * 100.0) / 
        NULLIF(COUNT(*), 0), 2
    ) as success_rate_percent,
    
    MIN(ingestion_timestamp) as first_ingestion,
    MAX(ingestion_timestamp) as last_ingestion
FROM fullbay_raw_data
GROUP BY DATE(ingestion_timestamp)
ORDER BY ingestion_date DESC;

-- View: Current Processing Status
CREATE OR REPLACE VIEW v_current_processing_status AS
SELECT 
    COUNT(*) as total_invoices,
    COUNT(CASE WHEN processed = true THEN 1 END) as processed_invoices,
    COUNT(CASE WHEN processed = false THEN 1 END) as pending_invoices,
    COUNT(CASE WHEN processing_errors IS NOT NULL THEN 1 END) as error_invoices,
    
    -- Line items created
    (SELECT COUNT(*) FROM fullbay_line_items) as total_line_items,
    
    -- Average line items per invoice
    ROUND(
        (SELECT COUNT(*) FROM fullbay_line_items) * 1.0 / 
        NULLIF(COUNT(CASE WHEN processed = true THEN 1 END), 0), 2
    ) as avg_line_items_per_invoice,
    
    -- Latest processing activity
    MAX(ingestion_timestamp) as last_ingestion_time,
    MAX(CASE WHEN processed = true THEN ingestion_timestamp END) as last_successful_processing
FROM fullbay_raw_data;

-- View: Error Analysis
CREATE OR REPLACE VIEW v_processing_errors AS
SELECT 
    fullbay_invoice_id,
    ingestion_timestamp,
    processing_errors,
    -- Categorize errors
    CASE 
        WHEN processing_errors LIKE '%missing%' THEN 'Missing Data'
        WHEN processing_errors LIKE '%format%' OR processing_errors LIKE '%parse%' THEN 'Format Error'
        WHEN processing_errors LIKE '%constraint%' OR processing_errors LIKE '%duplicate%' THEN 'Data Constraint'
        ELSE 'Other Error'
    END as error_category,
    
    -- Extract JSON for analysis if needed
    raw_json_data
FROM fullbay_raw_data
WHERE processing_errors IS NOT NULL
ORDER BY ingestion_timestamp DESC;

-- ===============================================
-- 2. BUSINESS INTELLIGENCE VIEWS
-- ===============================================

-- View: Invoice Summary Report
CREATE OR REPLACE VIEW v_invoice_summary AS
SELECT 
    fullbay_invoice_id,
    invoice_number,
    invoice_date,
    customer_title,
    repair_order_number,
    unit_make,
    unit_model,
    unit_year,
    unit_vin,
    
    -- Financial totals (from context repeated on each row)
    MAX(so_total_amount) as invoice_total,
    MAX(so_subtotal) as subtotal,
    MAX(so_tax_total) as tax_total,
    
    -- Line item breakdown
    COUNT(*) as total_line_items,
    COUNT(CASE WHEN line_item_type = 'PART' THEN 1 END) as parts_count,
    COUNT(CASE WHEN line_item_type = 'SUPPLY' THEN 1 END) as supplies_count,
    COUNT(CASE WHEN line_item_type = 'LABOR' THEN 1 END) as labor_count,
    
    -- Parts and labor totals from line items
    SUM(CASE WHEN line_item_type IN ('PART', 'SUPPLY') THEN line_total_price ELSE 0 END) as parts_total,
    SUM(CASE WHEN line_item_type = 'LABOR' THEN line_total_price ELSE 0 END) as labor_total,
    
    -- Technician count
    COUNT(DISTINCT assigned_technician) FILTER (WHERE assigned_technician IS NOT NULL) as technician_count,
    
    -- Ingestion metadata
    MAX(ingestion_timestamp) as ingested_at
FROM fullbay_line_items
GROUP BY 
    fullbay_invoice_id, invoice_number, invoice_date, customer_title,
    repair_order_number, unit_make, unit_model, unit_year, unit_vin
ORDER BY invoice_date DESC, invoice_number DESC;

-- View: Customer Analysis
CREATE OR REPLACE VIEW v_customer_analysis AS
SELECT 
    customer_id,
    customer_title,
    customer_external_id,
    
    -- Invoice metrics
    COUNT(DISTINCT fullbay_invoice_id) as total_invoices,
    MIN(invoice_date) as first_invoice_date,
    MAX(invoice_date) as last_invoice_date,
    
    -- Financial metrics
    SUM(CASE WHEN line_item_type IN ('PART', 'SUPPLY') THEN line_total_price ELSE 0 END) as total_parts_revenue,
    SUM(CASE WHEN line_item_type = 'LABOR' THEN line_total_price ELSE 0 END) as total_labor_revenue,
    SUM(line_total_price) as total_revenue,
    
    -- Service metrics
    COUNT(DISTINCT repair_order_number) as total_repair_orders,
    COUNT(DISTINCT unit_vin) as unique_vehicles_serviced,
    COUNT(*) as total_line_items,
    
    -- Average metrics
    ROUND(SUM(line_total_price) / COUNT(DISTINCT fullbay_invoice_id), 2) as avg_invoice_amount,
    ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT fullbay_invoice_id), 2) as avg_line_items_per_invoice
FROM fullbay_line_items
WHERE customer_id IS NOT NULL
GROUP BY customer_id, customer_title, customer_external_id
ORDER BY total_revenue DESC;

-- View: Parts Analysis
CREATE OR REPLACE VIEW v_parts_analysis AS
SELECT 
    shop_part_number,
    part_description,
    part_category,
    
    -- Usage metrics
    COUNT(*) as usage_count,
    SUM(quantity) as total_quantity_used,
    COUNT(DISTINCT fullbay_invoice_id) as invoices_used_on,
    
    -- Financial metrics
    AVG(unit_cost) as avg_unit_cost,
    AVG(unit_price) as avg_selling_price,
    SUM(line_total_cost) as total_cost,
    SUM(line_total_price) as total_revenue,
    
    -- Profit analysis
    SUM(line_total_price - line_total_cost) as total_profit,
    ROUND(
        (SUM(line_total_price - line_total_cost) / NULLIF(SUM(line_total_price), 0)) * 100, 2
    ) as profit_margin_percent,
    
    -- Recent usage
    MAX(invoice_date) as last_used_date,
    COUNT(CASE WHEN invoice_date >= CURRENT_DATE - INTERVAL '30 days' THEN 1 END) as used_last_30_days
FROM fullbay_line_items
WHERE line_item_type IN ('PART', 'SUPPLY') 
  AND shop_part_number IS NOT NULL
GROUP BY shop_part_number, part_description, part_category
ORDER BY total_revenue DESC;

-- View: Technician Performance
CREATE OR REPLACE VIEW v_technician_performance AS
SELECT 
    assigned_technician,
    assigned_technician_number,
    
    -- Work volume metrics
    COUNT(*) as total_labor_entries,
    COUNT(DISTINCT fullbay_invoice_id) as invoices_worked_on,
    COUNT(DISTINCT repair_order_number) as repair_orders_worked_on,
    
    -- Time metrics
    SUM(actual_hours) as total_hours_worked,
    AVG(actual_hours) as avg_hours_per_job,
    
    -- Financial metrics
    SUM(line_total_price) as total_labor_revenue,
    AVG(line_total_price) as avg_job_value,
    ROUND(SUM(line_total_price) / NULLIF(SUM(actual_hours), 0), 2) as revenue_per_hour,
    
    -- Recent activity
    MAX(invoice_date) as last_work_date,
    COUNT(CASE WHEN invoice_date >= CURRENT_DATE - INTERVAL '30 days' THEN 1 END) as jobs_last_30_days,
    SUM(CASE WHEN invoice_date >= CURRENT_DATE - INTERVAL '30 days' THEN actual_hours ELSE 0 END) as hours_last_30_days
FROM fullbay_line_items
WHERE line_item_type = 'LABOR' 
  AND assigned_technician IS NOT NULL
GROUP BY assigned_technician, assigned_technician_number
ORDER BY total_labor_revenue DESC;

-- View: Fleet Analysis (by Unit)
CREATE OR REPLACE VIEW v_fleet_analysis AS
SELECT 
    unit_vin,
    unit_number,
    unit_make,
    unit_model,
    unit_year,
    customer_title as fleet_owner,
    
    -- Service history
    COUNT(DISTINCT fullbay_invoice_id) as total_invoices,
    COUNT(DISTINCT repair_order_number) as total_repair_orders,
    MIN(invoice_date) as first_service_date,
    MAX(invoice_date) as last_service_date,
    
    -- Financial totals
    SUM(line_total_price) as total_service_cost,
    SUM(CASE WHEN line_item_type IN ('PART', 'SUPPLY') THEN line_total_price ELSE 0 END) as total_parts_cost,
    SUM(CASE WHEN line_item_type = 'LABOR' THEN line_total_price ELSE 0 END) as total_labor_cost,
    
    -- Service patterns
    ROUND(SUM(line_total_price) / COUNT(DISTINCT fullbay_invoice_id), 2) as avg_service_cost,
    COUNT(*) as total_line_items,
    
    -- Recent activity
    COUNT(CASE WHEN invoice_date >= CURRENT_DATE - INTERVAL '90 days' THEN 1 END) as services_last_90_days,
    SUM(CASE WHEN invoice_date >= CURRENT_DATE - INTERVAL '90 days' THEN line_total_price ELSE 0 END) as cost_last_90_days
FROM fullbay_line_items
WHERE unit_vin IS NOT NULL
GROUP BY unit_vin, unit_number, unit_make, unit_model, unit_year, customer_title
ORDER BY total_service_cost DESC;

-- ===============================================
-- 3. DATA QUALITY MONITORING VIEWS
-- ===============================================

-- View: Data Quality Check
CREATE OR REPLACE VIEW v_data_quality_check AS
SELECT 
    'Missing Invoice Numbers' as check_name,
    COUNT(*) as issue_count,
    ROUND((COUNT(*) * 100.0) / (SELECT COUNT(*) FROM fullbay_line_items), 2) as issue_percent
FROM fullbay_line_items
WHERE invoice_number IS NULL OR invoice_number = ''

UNION ALL

SELECT 
    'Missing Customer Info' as check_name,
    COUNT(*) as issue_count,
    ROUND((COUNT(*) * 100.0) / (SELECT COUNT(*) FROM fullbay_line_items), 2) as issue_percent
FROM fullbay_line_items
WHERE customer_id IS NULL OR customer_title IS NULL

UNION ALL

SELECT 
    'Missing Unit Info' as check_name,
    COUNT(*) as issue_count,
    ROUND((COUNT(*) * 100.0) / (SELECT COUNT(*) FROM fullbay_line_items), 2) as issue_percent
FROM fullbay_line_items
WHERE unit_vin IS NULL OR unit_make IS NULL

UNION ALL

SELECT 
    'Zero or Negative Prices' as check_name,
    COUNT(*) as issue_count,
    ROUND((COUNT(*) * 100.0) / (SELECT COUNT(*) FROM fullbay_line_items), 2) as issue_percent
FROM fullbay_line_items
WHERE line_total_price <= 0

UNION ALL

SELECT 
    'Missing Labor Hours' as check_name,
    COUNT(*) as issue_count,
    ROUND((COUNT(*) * 100.0) / (SELECT COUNT(*) FROM fullbay_line_items WHERE line_item_type = 'LABOR'), 2) as issue_percent
FROM fullbay_line_items
WHERE line_item_type = 'LABOR' AND (actual_hours IS NULL OR actual_hours <= 0)

UNION ALL

SELECT 
    'Missing Part Numbers' as check_name,
    COUNT(*) as issue_count,
    ROUND((COUNT(*) * 100.0) / (SELECT COUNT(*) FROM fullbay_line_items WHERE line_item_type IN ('PART', 'SUPPLY')), 2) as issue_percent
FROM fullbay_line_items
WHERE line_item_type IN ('PART', 'SUPPLY') AND (shop_part_number IS NULL OR shop_part_number = '')

ORDER BY issue_count DESC;

-- View: Recent Activity Summary (for monitoring dashboards)
CREATE OR REPLACE VIEW v_recent_activity AS
SELECT 
    DATE(ingestion_timestamp) as activity_date,
    COUNT(DISTINCT fullbay_invoice_id) as invoices_processed,
    COUNT(*) as line_items_created,
    SUM(line_total_price) as total_value_processed,
    COUNT(DISTINCT customer_id) as unique_customers,
    COUNT(DISTINCT assigned_technician) FILTER (WHERE assigned_technician IS NOT NULL) as active_technicians,
    
    -- Breakdown by type
    COUNT(CASE WHEN line_item_type = 'PART' THEN 1 END) as parts_processed,
    COUNT(CASE WHEN line_item_type = 'SUPPLY' THEN 1 END) as supplies_processed,
    COUNT(CASE WHEN line_item_type = 'LABOR' THEN 1 END) as labor_entries_processed,
    
    -- Quality metrics
    COUNT(CASE WHEN line_total_price <= 0 THEN 1 END) as zero_price_items,
    COUNT(CASE WHEN customer_id IS NULL THEN 1 END) as missing_customer_items
FROM fullbay_line_items
WHERE ingestion_timestamp >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY DATE(ingestion_timestamp)
ORDER BY activity_date DESC;

-- ===============================================
-- 4. INDEXES FOR PERFORMANCE
-- ===============================================

-- Indexes for reporting performance
CREATE INDEX IF NOT EXISTS idx_line_items_invoice_date ON fullbay_line_items(invoice_date);
CREATE INDEX IF NOT EXISTS idx_line_items_customer_analysis ON fullbay_line_items(customer_id, invoice_date);
CREATE INDEX IF NOT EXISTS idx_line_items_parts_analysis ON fullbay_line_items(shop_part_number, line_item_type);
CREATE INDEX IF NOT EXISTS idx_line_items_technician_analysis ON fullbay_line_items(assigned_technician, line_item_type);
CREATE INDEX IF NOT EXISTS idx_line_items_unit_analysis ON fullbay_line_items(unit_vin, invoice_date);
CREATE INDEX IF NOT EXISTS idx_line_items_recent_activity ON fullbay_line_items(ingestion_timestamp, line_item_type);

-- ===============================================
-- 5. USAGE EXAMPLES
-- ===============================================

-- Example queries to demonstrate the views:

/*
-- Check current system status
SELECT * FROM v_current_processing_status;

-- Review recent ingestion performance
SELECT * FROM v_ingestion_performance ORDER BY ingestion_date DESC LIMIT 7;

-- Find top customers by revenue
SELECT * FROM v_customer_analysis LIMIT 10;

-- Monitor data quality issues
SELECT * FROM v_data_quality_check WHERE issue_count > 0;

-- Check technician productivity
SELECT * FROM v_technician_performance LIMIT 10;

-- Analyze recent activity
SELECT * FROM v_recent_activity LIMIT 7;

-- Find most profitable parts
SELECT * FROM v_parts_analysis WHERE total_revenue > 1000 ORDER BY profit_margin_percent DESC LIMIT 20;
*/