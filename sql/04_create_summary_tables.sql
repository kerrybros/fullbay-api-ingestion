-- =====================================================
-- Summary Tables for Common Reporting Queries
-- =====================================================
-- These tables provide pre-aggregated data for common reporting needs

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

-- Comments for documentation
COMMENT ON TABLE daily_summary IS 'Daily aggregated statistics for reporting';
COMMENT ON TABLE monthly_summary IS 'Monthly aggregated statistics for reporting';
COMMENT ON TABLE customer_summary IS 'Customer-level aggregated statistics';
COMMENT ON TABLE vehicle_summary IS 'Vehicle-level aggregated statistics';
