-- =====================================================
-- Line Items Table for Flattened Fullbay Data
-- =====================================================
-- This table stores all line items (parts, labor, supplies, etc.) from Fullbay invoices
-- in a flattened, queryable format for reporting and analysis
-- ORIGINAL 72-COLUMN SCHEMA (restored from database.py + labor_hours - so_supplies_total)

CREATE TABLE IF NOT EXISTS fullbay_line_items (
    id SERIAL PRIMARY KEY,
    raw_data_id INTEGER REFERENCES fullbay_raw_data(id) ON DELETE CASCADE,
    
    -- === INVOICE LEVEL INFO (7 columns) ===
    fullbay_invoice_id VARCHAR(50) NOT NULL,
    invoice_number VARCHAR(50),
    invoice_date DATE,
    due_date DATE,
    shop_title VARCHAR(255),
    shop_email VARCHAR(255),
    shop_address TEXT,
    
    -- === CUSTOMER INFO (6 columns) ===
    customer_id INTEGER,
    customer VARCHAR(255),
    customer_external_id VARCHAR(50),
    customer_main_phone VARCHAR(50),
    customer_secondary_phone VARCHAR(50),
    customer_billing_address TEXT,
    
    -- === SERVICE ORDER INFO (5 columns) ===
    fullbay_service_order_id VARCHAR(50),
    so_number VARCHAR(50),
    service_order_created TIMESTAMP WITH TIME ZONE,
    service_order_start_date TIMESTAMP WITH TIME ZONE,
    service_order_completion_date TIMESTAMP WITH TIME ZONE,
    
    -- === UNIT/VEHICLE INFO (8 columns) ===
    unit_id VARCHAR(50),
    unit VARCHAR(50),
    unit_type VARCHAR(100),
    unit_year VARCHAR(10),
    unit_make VARCHAR(100),
    unit_model VARCHAR(100),
    unit_vin VARCHAR(50),
    unit_license_plate VARCHAR(20),
    
    -- === PRIMARY TECHNICIAN INFO (2 columns) ===
    primary_technician VARCHAR(255),
    primary_technician_number VARCHAR(50),
    
    -- === COMPLAINT/WORK ORDER INFO (6 columns) ===
    fullbay_complaint_id INTEGER,
    complaint_type VARCHAR(100),
    complaint_subtype VARCHAR(100),
    complaint_note TEXT,
    complaint_cause TEXT,
    complaint_authorized BOOLEAN,
    
    -- === CORRECTION/SERVICE INFO (7 columns) ===
    fullbay_correction_id INTEGER,
    correction_title VARCHAR(255),
    component VARCHAR(255),
    system VARCHAR(255),
    global_service VARCHAR(255),
    recommended_correction TEXT,
    service_description TEXT,
    
    -- === LINE ITEM DETAILS (16 columns) ===
    line_item_type VARCHAR(20) NOT NULL, -- 'PART', 'LABOR', 'SUPPLY', 'FREIGHT', 'MISC'
    
    -- For Parts
    fullbay_part_id INTEGER,
    part_description TEXT,
    shop_part_number VARCHAR(100),
    vendor_part_number VARCHAR(100),
    part_category VARCHAR(255),
    
    -- For Labor/Services  
    labor_description TEXT,
    labor_rate_type VARCHAR(50),
    assigned_technician VARCHAR(255),
    assigned_technician_number VARCHAR(50),
    
    -- === QUANTITIES ===
    quantity DECIMAL(10,3),
    to_be_returned_quantity DECIMAL(10,3),
    returned_quantity DECIMAL(10,3),
    
    -- === HOURS (for labor items) ===
    so_hours DECIMAL(8,2),
    labor_hours DECIMAL(8,2), -- Proportionally split total labor hours
    technician_portion INTEGER, -- Percentage of work for this technician
    
    -- === FINANCIAL DETAILS (Per Line Item) ===
    unit_cost DECIMAL(10,2),
    unit_price DECIMAL(10,2),
    line_total DECIMAL(10,2),
    price_overridden BOOLEAN DEFAULT FALSE,
    
    -- === TAX INFO ===
    taxable BOOLEAN DEFAULT TRUE,
    tax_rate DECIMAL(5,2),
    line_tax DECIMAL(10,2),  -- Calculated tax amount for this line
    sales_total DECIMAL(10,2),  -- Line total + line tax
    
    -- === CLASSIFICATION ===
    inventory_item BOOLEAN DEFAULT FALSE,
    core_type VARCHAR(50),
    sublet BOOLEAN DEFAULT FALSE,
    
    -- === METADATA (2 columns) ===
    ingestion_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ingestion_source VARCHAR(100) DEFAULT 'fullbay_api'
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_fullbay_line_items_invoice_id ON fullbay_line_items(fullbay_invoice_id);
CREATE INDEX IF NOT EXISTS idx_fullbay_line_items_invoice_date ON fullbay_line_items(invoice_date);
CREATE INDEX IF NOT EXISTS idx_fullbay_line_items_customer_id ON fullbay_line_items(customer_id);
CREATE INDEX IF NOT EXISTS idx_fullbay_line_items_so_number ON fullbay_line_items(so_number);
CREATE INDEX IF NOT EXISTS idx_fullbay_line_items_unit_vin ON fullbay_line_items(unit_vin);
CREATE INDEX IF NOT EXISTS idx_fullbay_line_items_line_type ON fullbay_line_items(line_item_type);
CREATE INDEX IF NOT EXISTS idx_fullbay_line_items_part_number ON fullbay_line_items(shop_part_number);
CREATE INDEX IF NOT EXISTS idx_fullbay_line_items_technician ON fullbay_line_items(assigned_technician);
CREATE INDEX IF NOT EXISTS idx_fullbay_line_items_ingestion ON fullbay_line_items(ingestion_timestamp);
CREATE INDEX IF NOT EXISTS idx_fullbay_line_items_complaint_id ON fullbay_line_items(fullbay_complaint_id);
CREATE INDEX IF NOT EXISTS idx_fullbay_line_items_correction_id ON fullbay_line_items(fullbay_correction_id);
CREATE INDEX IF NOT EXISTS idx_fullbay_line_items_part_id ON fullbay_line_items(fullbay_part_id);

-- Comments for documentation
COMMENT ON TABLE fullbay_line_items IS 'Flattened line items from Fullbay invoices for reporting and analysis - ORIGINAL 74-COLUMN SCHEMA';
COMMENT ON COLUMN fullbay_line_items.line_item_type IS 'Type of line item: PART, LABOR, SUPPLY, FREIGHT, MISC';
COMMENT ON COLUMN fullbay_line_items.technician_portion IS 'Percentage of work assigned to this technician (0-100)';
COMMENT ON COLUMN fullbay_line_items.quantity IS 'Quantity of parts/supplies or hours for labor';
COMMENT ON COLUMN fullbay_line_items.unit_cost IS 'Cost per unit (internal)';
COMMENT ON COLUMN fullbay_line_items.unit_price IS 'Price per unit (customer-facing)';
