-- =====================================================
-- Line Items Table for Flattened Fullbay Data
-- =====================================================
-- This table stores all line items (parts, labor, supplies, etc.) from Fullbay invoices
-- in a flattened, queryable format for reporting and analysis

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

-- Indexes for performance
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

-- Comments for documentation
COMMENT ON TABLE fullbay_line_items IS 'Flattened line items from Fullbay invoices for reporting and analysis';
COMMENT ON COLUMN fullbay_line_items.line_item_type IS 'Type of line item: PART, LABOR, SUPPLY, FREIGHT, MISC, SUBLET';
COMMENT ON COLUMN fullbay_line_items.assigned_technician_portion IS 'Percentage of work assigned to this technician (0-100)';
COMMENT ON COLUMN fullbay_line_items.quantity IS 'Quantity of parts/supplies or hours for labor';
COMMENT ON COLUMN fullbay_line_items.unit_cost IS 'Cost per unit (internal)';
COMMENT ON COLUMN fullbay_line_items.unit_price IS 'Price per unit (customer-facing)';
