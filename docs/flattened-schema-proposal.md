# Flattened Database Schema Proposal

Based on the current revenue report structure, here's a proposed single-table schema that flattens the Fullbay Invoice API JSON into a line-by-line format.

## 🎯 **Concept**

**One JSON Response** → **Multiple Flat Records** (one per part + one per service/labor)

Each row contains:
- Complete service order context (repeated)
- Individual line item details (unique per row)
- Financial details for that specific line item

## 🗃️ **Single Table Schema: `fullbay_line_items`**

```sql
CREATE TABLE fullbay_line_items (
    -- Primary Key
    id SERIAL PRIMARY KEY,
    
    -- === INVOICE LEVEL INFO (Repeated on each row) ===
    fullbay_invoice_id VARCHAR(50) NOT NULL,
    invoice_number VARCHAR(50),
    invoice_date DATE,
    due_date DATE,
    exported BOOLEAN DEFAULT FALSE,
    
    -- === SHOP INFO (Repeated on each row) ===
    shop_title VARCHAR(255),
    shop_email VARCHAR(255),
    shop_address TEXT,
    
    -- === CUSTOMER INFO (Repeated on each row) ===
    customer_id INTEGER,
    customer_title VARCHAR(255),
    customer_external_id VARCHAR(50),
    customer_main_phone VARCHAR(20),
    customer_secondary_phone VARCHAR(20),
    customer_billing_address TEXT,
    
    -- === SERVICE ORDER INFO (Repeated on each row) ===
    fullbay_service_order_id VARCHAR(50),
    repair_order_number VARCHAR(50),
    service_order_created DATE,
    service_order_start_date DATE,
    service_order_completion_date DATE,
    
    -- === UNIT/VEHICLE INFO (Repeated on each row) ===
    unit_id VARCHAR(50),
    unit_number VARCHAR(50),
    unit_type VARCHAR(100),
    unit_year VARCHAR(10),
    unit_make VARCHAR(100),
    unit_model VARCHAR(100),
    unit_vin VARCHAR(50),
    unit_license_plate VARCHAR(20),
    
    -- === TECHNICIAN INFO (Repeated on each row) ===
    primary_technician VARCHAR(255),
    primary_technician_number VARCHAR(50),
    
    -- === COMPLAINT/WORK ORDER INFO (Repeated on each row) ===
    fullbay_complaint_id INTEGER,
    complaint_type VARCHAR(100),
    complaint_subtype VARCHAR(100),
    complaint_note TEXT,
    complaint_cause TEXT,
    complaint_authorized BOOLEAN,
    
    -- === CORRECTION/SERVICE INFO (Repeated on each row) ===
    fullbay_correction_id INTEGER,
    correction_title VARCHAR(255),
    global_component VARCHAR(255),
    global_system VARCHAR(255),
    global_service VARCHAR(255),
    recommended_correction TEXT,
    actual_correction TEXT,
    correction_performed VARCHAR(50),
    
    -- === LINE ITEM DETAILS (Unique per row) ===
    line_item_type VARCHAR(20) NOT NULL, -- 'PART', 'LABOR', 'SUPPLY', 'MISC'
    
    -- For Parts
    fullbay_part_id INTEGER,
    part_description TEXT,
    shop_part_number VARCHAR(100),
    vendor_part_number VARCHAR(100),
    part_category VARCHAR(255),
    
    -- For Labor/Services  
    labor_description TEXT,
    labor_rate_type VARCHAR(50),
    
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
    raw_json_data JSONB, -- Store original JSON for reference
    
    -- === INDEXES FOR PERFORMANCE ===
    CONSTRAINT idx_fullbay_line_items_invoice_id 
        FOREIGN KEY (fullbay_invoice_id) REFERENCES ... -- if needed
);

-- Indexes for common queries
CREATE INDEX idx_fullbay_line_items_invoice_date ON fullbay_line_items(invoice_date);
CREATE INDEX idx_fullbay_line_items_customer_id ON fullbay_line_items(customer_id);
CREATE INDEX idx_fullbay_line_items_repair_order ON fullbay_line_items(repair_order_number);
CREATE INDEX idx_fullbay_line_items_unit_vin ON fullbay_line_items(unit_vin);
CREATE INDEX idx_fullbay_line_items_line_type ON fullbay_line_items(line_item_type);
CREATE INDEX idx_fullbay_line_items_part_number ON fullbay_line_items(shop_part_number);
CREATE INDEX idx_fullbay_line_items_technician ON fullbay_line_items(primary_technician);
CREATE INDEX idx_fullbay_line_items_ingestion ON fullbay_line_items(ingestion_timestamp);
```

## 📊 **Data Example from Your JSON**

Your sample invoice would create ~35-40 rows like:

| invoice_number | repair_order_number | customer_title | unit_vin | line_item_type | part_description | quantity | unit_cost | line_total_price |
|----------------|-------------------|----------------|----------|----------------|------------------|----------|-----------|------------------|
| 14282 | 15448 | GAETANO #3 | 3AKBHKDV0KSKC8657 | PART | DEF AWNING KIT | 1 | 206.40 | 412.80 |
| 14282 | 15448 | GAETANO #3 | 3AKBHKDV0KSKC8657 | PART | GASKET, EXHAUST | 2 | 11.79 | 32.42 |
| 14282 | 15448 | GAETANO #3 | 3AKBHKDV0KSKC8657 | LABOR | COMPUTER CHECK | 10 | 165.00 | 1650.00 |
| 14282 | 15448 | GAETANO #3 | 3AKBHKDV0KSKC8657 | PART | OIL - 1540 | 40 | 3.06 | 6.48 |

## ✅ **Advantages of This Approach**

1. **Familiar Format** - Matches your current revenue report
2. **Simple Queries** - Single table for all reporting needs
3. **Excel Export Ready** - Easy to export for analysis
4. **BI Tool Compatible** - Works with Power BI, Tableau, etc.
5. **Fast Implementation** - Much simpler than normalized schema
6. **Flexible Reporting** - Can aggregate at any level (customer, unit, part, etc.)

## ⚠️ **Trade-offs**

1. **Data Duplication** - Service order info repeated on each line
2. **Storage Space** - Larger than normalized approach
3. **Update Complexity** - Changing service order details affects many rows

## 🔄 **Data Processing Logic**

For each Invoice JSON:
1. Extract service order context (once)
2. Loop through each Complaint
3. Loop through each Correction in complaint
4. For each Part in correction → Create PART row
5. For labor hours in correction → Create LABOR row
6. Populate all context fields on each row

## 📈 **Reporting Examples**

```sql
-- Parts usage report
SELECT shop_part_number, part_description, SUM(quantity), SUM(line_total_price)
FROM fullbay_line_items 
WHERE line_item_type = 'PART'
GROUP BY shop_part_number, part_description;

-- Technician productivity
SELECT primary_technician, SUM(actual_hours), COUNT(*), SUM(line_total_price)
FROM fullbay_line_items
WHERE line_item_type = 'LABOR'
GROUP BY primary_technician;

-- Customer revenue
SELECT customer_title, COUNT(DISTINCT fullbay_invoice_id), SUM(line_total_price)
FROM fullbay_line_items
GROUP BY customer_title;
```

## 🤔 **Questions**

1. Does this flattened structure match your reporting needs better?
2. Are there any fields from your current revenue report that I missed?
3. Should we proceed with this single-table approach instead of normalized?