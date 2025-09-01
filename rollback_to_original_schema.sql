-- =====================================================
-- ROLLBACK TO ORIGINAL 75-COLUMN SCHEMA
-- This script reverts the fullbay_line_items table to the original design
-- =====================================================

-- First, backup the current data
CREATE TABLE IF NOT EXISTS fullbay_line_items_backup AS 
SELECT * FROM fullbay_line_items;

-- Drop the current table
DROP TABLE IF EXISTS fullbay_line_items;

-- Recreate the table with the original 75-column structure
CREATE TABLE fullbay_line_items (
    -- Primary key and reference
    id SERIAL PRIMARY KEY,
    raw_data_id INTEGER REFERENCES fullbay_raw_data(id) ON DELETE CASCADE,
    
    -- === INVOICE LEVEL INFO (8 columns) ===
    fullbay_invoice_id VARCHAR(50) NOT NULL,
    invoice_number VARCHAR(50),
    invoice_date DATE,
    due_date DATE,
    exported BOOLEAN DEFAULT FALSE,
    shop_title VARCHAR(255),
    shop_email VARCHAR(255),
    shop_address TEXT,
    
    -- === CUSTOMER INFO (6 columns) ===
    customer_id INTEGER,
    customer_title VARCHAR(255),
    customer_external_id VARCHAR(50),
    customer_main_phone VARCHAR(20),
    customer_secondary_phone VARCHAR(20),
    customer_billing_address TEXT,
    
    -- === SERVICE ORDER INFO (5 columns) ===
    fullbay_service_order_id VARCHAR(50),
    repair_order_number VARCHAR(50),
    service_order_created TIMESTAMP WITH TIME ZONE,
    service_order_start_date TIMESTAMP WITH TIME ZONE,
    service_order_completion_date TIMESTAMP WITH TIME ZONE,
    
    -- === UNIT/VEHICLE INFO (8 columns) ===
    unit_id VARCHAR(50),
    unit_number VARCHAR(50),
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
    
    -- === CORRECTION/SERVICE INFO (8 columns) ===
    fullbay_correction_id INTEGER,
    correction_title VARCHAR(255),
    global_component VARCHAR(255),
    global_system VARCHAR(255),
    global_service VARCHAR(255),
    recommended_correction TEXT,
    actual_correction TEXT,
    correction_performed VARCHAR(50),
    
    -- === LINE ITEM DETAILS (15 columns) ===
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
    labor_hours DECIMAL(8,2),
    actual_hours DECIMAL(8,2),
    technician_portion INTEGER, -- Percentage of work for this technician
    
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
    
    -- === SERVICE ORDER TOTALS (7 columns) ===
    so_total_parts_cost DECIMAL(10,2),
    so_total_parts_price DECIMAL(10,2),
    so_total_labor_hours DECIMAL(8,2),
    so_total_labor_cost DECIMAL(10,2),
    so_subtotal DECIMAL(10,2),
    so_tax_total DECIMAL(10,2),
    so_total_amount DECIMAL(10,2),
    
    -- === METADATA (2 columns) ===
    ingestion_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ingestion_source VARCHAR(100) DEFAULT 'fullbay_api'
);

-- Create indexes for better performance
CREATE INDEX idx_fullbay_line_items_invoice_id ON fullbay_line_items(fullbay_invoice_id);
CREATE INDEX idx_fullbay_line_items_invoice_date ON fullbay_line_items(invoice_date);
CREATE INDEX idx_fullbay_line_items_customer_id ON fullbay_line_items(customer_id);
CREATE INDEX idx_fullbay_line_items_repair_order ON fullbay_line_items(repair_order_number);
CREATE INDEX idx_fullbay_line_items_unit_vin ON fullbay_line_items(unit_vin);
CREATE INDEX idx_fullbay_line_items_line_type ON fullbay_line_items(line_item_type);
CREATE INDEX idx_fullbay_line_items_part_number ON fullbay_line_items(shop_part_number);
CREATE INDEX idx_fullbay_line_items_technician ON fullbay_line_items(assigned_technician);
CREATE INDEX idx_fullbay_line_items_ingestion ON fullbay_line_items(ingestion_timestamp);

-- Insert data from backup, mapping only the original columns
INSERT INTO fullbay_line_items (
    id, raw_data_id, fullbay_invoice_id, invoice_number, invoice_date, due_date, exported,
    shop_title, shop_email, shop_address,
    customer_id, customer_title, customer_external_id, customer_main_phone, 
    customer_secondary_phone, customer_billing_address,
    fullbay_service_order_id, repair_order_number, service_order_created, 
    service_order_start_date, service_order_completion_date,
    unit_id, unit_number, unit_type, unit_year, unit_make, unit_model, unit_vin, unit_license_plate,
    primary_technician, primary_technician_number,
    fullbay_complaint_id, complaint_type, complaint_subtype, complaint_note, 
    complaint_cause, complaint_authorized,
    fullbay_correction_id, correction_title, global_component, global_system, 
    global_service, recommended_correction, actual_correction, correction_performed,
    line_item_type, fullbay_part_id, part_description, shop_part_number, 
    vendor_part_number, part_category,
    labor_description, labor_rate_type, assigned_technician, assigned_technician_number,
    quantity, to_be_returned_quantity, returned_quantity,
    labor_hours, actual_hours, technician_portion,
    unit_cost, unit_price, line_total_cost, line_total_price, price_overridden,
    taxable, tax_rate, tax_amount, inventory_item, core_type, sublet,
    quickbooks_account, quickbooks_item, quickbooks_item_type,
    so_total_parts_cost, so_total_parts_price, so_total_labor_hours, 
    so_total_labor_cost, so_subtotal, so_tax_total, so_total_amount,
    ingestion_timestamp, ingestion_source
)
SELECT 
    id, raw_data_id, fullbay_invoice_id, invoice_number, invoice_date, due_date, exported,
    shop_title, shop_email, shop_physical_address as shop_address,
    customer_id, customer_title, customer_external_id, customer_main_phone, 
    customer_secondary_phone, customer_billing_address,
    fullbay_service_order_id, repair_order_number, service_order_created, 
    service_order_start_date, service_order_completion_date,
    unit_id, unit_number, unit_type, unit_year, unit_make, unit_model, unit_vin, unit_license_plate,
    primary_technician, primary_technician_number,
    fullbay_complaint_id, complaint_type, complaint_subtype, complaint_note, 
    complaint_cause, complaint_authorized,
    fullbay_correction_id, correction_title, global_component, global_system, 
    global_service, recommended_correction, actual_correction, correction_performed,
    line_item_type, fullbay_part_id, part_description, shop_part_number, 
    vendor_part_number, part_category,
    labor_description, labor_rate_type, assigned_technician, assigned_technician_number,
    quantity, to_be_returned_quantity, returned_quantity,
    labor_hours, actual_hours, assigned_technician_portion as technician_portion,
    unit_cost, unit_price, line_total_cost, line_total_price, price_overridden,
    taxable, tax_rate, tax_amount, inventory_item, core_type, sublet,
    quickbooks_account, quickbooks_item, quickbooks_item_type,
    so_total_parts_cost, so_total_parts_price, so_total_labor_hours, 
    so_total_labor_cost, so_subtotal, so_tax_total, so_total_amount,
    ingestion_timestamp, ingestion_source
FROM fullbay_line_items_backup;

-- Verify the rollback
SELECT 
    'Original Schema Rollback Complete' as status,
    COUNT(*) as total_rows,
    COUNT(DISTINCT fullbay_invoice_id) as unique_invoices
FROM fullbay_line_items;

-- Show the new column count
SELECT 
    COUNT(*) as column_count,
    'Original 75-column structure restored' as message
FROM information_schema.columns 
WHERE table_name = 'fullbay_line_items' 
AND table_schema = 'public';
