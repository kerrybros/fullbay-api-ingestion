# Database Schema Proposal for Fullbay API Data

Based on the sample JSON response, here's the proposed normalized database schema to handle the flattened data structure.

## 📋 **Schema Overview**

One Fullbay API response will create records across **7 main tables** with proper foreign key relationships.

## 🗃️ **Table Structures**

### 1. **service_orders** (Main Parent Table)
```sql
CREATE TABLE service_orders (
    id SERIAL PRIMARY KEY,
    
    -- Invoice Level Info
    fullbay_invoice_id VARCHAR(50) UNIQUE NOT NULL,
    invoice_number VARCHAR(50),
    invoice_date DATE,
    due_date DATE,
    exported BOOLEAN DEFAULT FALSE,
    
    -- Customer Info (from top level)
    customer_title VARCHAR(255),
    customer_billing_employee VARCHAR(255),
    customer_billing_email VARCHAR(255),
    customer_billing_address TEXT,
    
    -- Shop Info
    shop_title VARCHAR(255),
    shop_email VARCHAR(255),
    shop_physical_address TEXT,
    
    -- Financial Totals
    misc_charge_total DECIMAL(10,2) DEFAULT 0,
    service_call_total DECIMAL(10,2) DEFAULT 0,
    mileage_total DECIMAL(10,2) DEFAULT 0,
    mileage_cost_total DECIMAL(10,2) DEFAULT 0,
    parts_total DECIMAL(10,2) DEFAULT 0,
    labor_hours_total DECIMAL(8,2) DEFAULT 0,
    labor_total DECIMAL(10,2) DEFAULT 0,
    sublet_labor_total DECIMAL(10,2),
    supplies_total DECIMAL(10,2) DEFAULT 0,
    sub_total DECIMAL(10,2) DEFAULT 0,
    tax_total DECIMAL(10,2) DEFAULT 0,
    total DECIMAL(10,2) DEFAULT 0,
    balance DECIMAL(10,2) DEFAULT 0,
    
    -- Service Order Details (from nested ServiceOrder)
    fullbay_service_order_id VARCHAR(50),
    repair_order_number VARCHAR(50),
    technician VARCHAR(255),
    technician_number VARCHAR(50),
    parts_manager VARCHAR(255),
    parts_manager_number VARCHAR(50),
    
    -- Customer Authorization
    customer_authorized_on_hours_only BOOLEAN DEFAULT FALSE,
    customer_threshold DECIMAL(10,2),
    pre_authorized BOOLEAN DEFAULT FALSE,
    
    -- Timing
    unit_available_datetime TIMESTAMP WITH TIME ZONE,
    unit_must_be_accessed_at_available_datetime BOOLEAN DEFAULT FALSE,
    unit_return_datetime TIMESTAMP WITH TIME ZONE,
    unit_return_asap BOOLEAN DEFAULT TRUE,
    
    -- Service Order Financial Details
    so_labor_hours_total DECIMAL(8,2),
    so_actual_hours_total DECIMAL(8,2),
    so_labor_total DECIMAL(10,2),
    so_parts_cost_total DECIMAL(10,2),
    so_parts_total DECIMAL(10,2),
    
    -- Status & Workflow
    hot BOOLEAN DEFAULT FALSE,
    follow_in_use_schedule BOOLEAN DEFAULT FALSE,
    unscheduled BOOLEAN DEFAULT FALSE,
    
    -- References
    quickbooks_id VARCHAR(50),
    authorization_number VARCHAR(100),
    po_number VARCHAR(100),
    parts_po_number VARCHAR(100),
    promise_to_pay_date DATE,
    
    -- Timestamps
    all_parts_priced_datetime TIMESTAMP WITH TIME ZONE,
    start_datetime TIMESTAMP WITH TIME ZONE,
    completion_datetime TIMESTAMP WITH TIME ZONE,
    created_by_technician VARCHAR(255),
    created_by_technician_number VARCHAR(50),
    created TIMESTAMP WITH TIME ZONE,
    
    -- Metadata
    raw_invoice_data JSONB NOT NULL,
    raw_service_order_data JSONB,
    ingestion_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ingestion_source VARCHAR(100) DEFAULT 'fullbay_api'
);
```

### 2. **service_order_customers** (Customer Details)
```sql
CREATE TABLE service_order_customers (
    id SERIAL PRIMARY KEY,
    service_order_id INTEGER REFERENCES service_orders(id) ON DELETE CASCADE,
    
    customer_id INTEGER NOT NULL,
    title VARCHAR(255),
    external_id VARCHAR(50),
    main_phone VARCHAR(20),
    secondary_phone VARCHAR(20),
    
    -- Context: 'invoice_level' or 'service_order_level'
    customer_context VARCHAR(50) NOT NULL,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
```

### 3. **service_order_units** (Vehicle Information)
```sql
CREATE TABLE service_order_units (
    id SERIAL PRIMARY KEY,
    service_order_id INTEGER REFERENCES service_orders(id) ON DELETE CASCADE,
    
    customer_unit_id VARCHAR(50),
    number VARCHAR(50),
    nickname VARCHAR(255),
    type VARCHAR(100),
    sub_type VARCHAR(100),
    year VARCHAR(10),
    make VARCHAR(100),
    model VARCHAR(100),
    vin VARCHAR(50),
    license_plate VARCHAR(20),
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
```

### 4. **service_order_addresses** (Multiple Address Types)
```sql
CREATE TABLE service_order_addresses (
    id SERIAL PRIMARY KEY,
    service_order_id INTEGER REFERENCES service_orders(id) ON DELETE CASCADE,
    
    address_type VARCHAR(50) NOT NULL, -- 'billing', 'remit_to', 'ship_to', 'pickup'
    title VARCHAR(255),
    line1 TEXT,
    line2 TEXT,
    city VARCHAR(100),
    state VARCHAR(10),
    country VARCHAR(100),
    postal_code VARCHAR(20),
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
```

### 5. **service_order_tax_lines** (Tax Information)
```sql
CREATE TABLE service_order_tax_lines (
    id SERIAL PRIMARY KEY,
    service_order_id INTEGER REFERENCES service_orders(id) ON DELETE CASCADE,
    
    tax_title VARCHAR(255),
    tax_rate DECIMAL(5,2),
    tax_total DECIMAL(10,2),
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
```

### 6. **service_order_complaints** (Main Work Items)
```sql
CREATE TABLE service_order_complaints (
    id SERIAL PRIMARY KEY,
    service_order_id INTEGER REFERENCES service_orders(id) ON DELETE CASCADE,
    
    fullbay_complaint_id INTEGER UNIQUE NOT NULL,
    mileage_rate VARCHAR(50),
    labor_rate VARCHAR(50),
    type VARCHAR(100),
    sub_type VARCHAR(100),
    authorized BOOLEAN DEFAULT FALSE,
    severity VARCHAR(50),
    note TEXT,
    cause TEXT,
    cause_type VARCHAR(100),
    
    -- Financial Details
    labor_hours_total DECIMAL(8,2) DEFAULT 0,
    actual_hours_total DECIMAL(8,2) DEFAULT 0,
    labor_taxable BOOLEAN,
    labor_total DECIMAL(10,2) DEFAULT 0,
    parts_cost_total DECIMAL(10,2) DEFAULT 0,
    parts_total DECIMAL(10,2) DEFAULT 0,
    mileage_taxable BOOLEAN,
    mileage_total DECIMAL(10,2) DEFAULT 0,
    mileage_cost_total DECIMAL(10,2) DEFAULT 0,
    service_call_taxable BOOLEAN,
    service_call_total DECIMAL(10,2) DEFAULT 0,
    
    -- Metadata
    sublet BOOLEAN DEFAULT FALSE,
    part_category VARCHAR(255),
    quickbooks_account VARCHAR(255),
    quickbooks_item VARCHAR(255),
    quickbooks_item_type VARCHAR(100),
    
    created TIMESTAMP WITH TIME ZONE,
    ingestion_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
```

### 7. **service_order_corrections** (Specific Work Performed)
```sql
CREATE TABLE service_order_corrections (
    id SERIAL PRIMARY KEY,
    service_order_id INTEGER REFERENCES service_orders(id) ON DELETE CASCADE,
    complaint_id INTEGER REFERENCES service_order_complaints(id) ON DELETE CASCADE,
    
    fullbay_correction_id INTEGER UNIQUE NOT NULL,
    global_component VARCHAR(255),
    global_system VARCHAR(255),
    global_service VARCHAR(255),
    unit_service VARCHAR(255),
    labor_rate VARCHAR(50),
    title VARCHAR(255),
    recommended_correction TEXT,
    actual_correction TEXT,
    correction_performed VARCHAR(50),
    
    -- Authorization & Payment
    pre_authorized BOOLEAN DEFAULT FALSE,
    pre_paid BOOLEAN DEFAULT FALSE,
    
    -- Financial Details
    labor_hours_total DECIMAL(8,2) DEFAULT 0,
    labor_total DECIMAL(10,2) DEFAULT 0,
    taxable BOOLEAN DEFAULT FALSE,
    parts_cost_total DECIMAL(10,2) DEFAULT 0,
    parts_total DECIMAL(10,2) DEFAULT 0,
    
    created TIMESTAMP WITH TIME ZONE,
    ingestion_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
```

### 8. **service_order_parts** (Individual Parts Used)
```sql
CREATE TABLE service_order_parts (
    id SERIAL PRIMARY KEY,
    service_order_id INTEGER REFERENCES service_orders(id) ON DELETE CASCADE,
    complaint_id INTEGER REFERENCES service_order_complaints(id) ON DELETE CASCADE,
    correction_id INTEGER REFERENCES service_order_corrections(id) ON DELETE CASCADE,
    
    fullbay_part_id INTEGER UNIQUE NOT NULL,
    description TEXT,
    shop_part_number VARCHAR(100),
    vendor_part_number VARCHAR(100),
    
    -- Quantities
    quantity DECIMAL(8,2),
    to_be_returned_quantity DECIMAL(8,2),
    returned_quantity DECIMAL(8,2),
    
    -- Pricing
    cost DECIMAL(10,2),
    selling_price DECIMAL(10,2),
    selling_price_overridden BOOLEAN DEFAULT FALSE,
    
    -- Classifications
    taxable BOOLEAN DEFAULT TRUE,
    inventory BOOLEAN DEFAULT FALSE,
    core_type VARCHAR(50),
    sublet BOOLEAN DEFAULT FALSE,
    part_category VARCHAR(255),
    
    -- QuickBooks Integration
    quickbooks_account VARCHAR(255),
    quickbooks_item VARCHAR(255),
    quickbooks_item_type VARCHAR(100),
    
    created TIMESTAMP WITH TIME ZONE,
    ingestion_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
```

### 9. **service_order_technicians** (Assigned Technicians)
```sql
CREATE TABLE service_order_technicians (
    id SERIAL PRIMARY KEY,
    service_order_id INTEGER REFERENCES service_orders(id) ON DELETE CASCADE,
    complaint_id INTEGER REFERENCES service_order_complaints(id) ON DELETE CASCADE,
    
    fullbay_assignment_id INTEGER UNIQUE NOT NULL,
    technician VARCHAR(255),
    technician_number VARCHAR(50),
    actual_hours DECIMAL(8,2),
    portion INTEGER, -- Percentage of work
    quickbooks_labor_item VARCHAR(255),
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
```

## 📊 **Data Volume Example**

From your single API response, we would create:
- **1** service_orders record
- **2** service_order_customers records (invoice + service order level)
- **1** service_order_units record
- **4** service_order_addresses records (billing, remit_to, ship_to, pickup)
- **1** service_order_tax_lines record  
- **11** service_order_complaints records
- **11** service_order_corrections records
- **35+** service_order_parts records (counting all parts across all corrections)
- **11** service_order_technicians records

**Total: ~75+ records** from one API response!

## 🔑 **Key Relationships**

```
service_orders (1)
├── service_order_customers (1:many)
├── service_order_units (1:1) 
├── service_order_addresses (1:many)
├── service_order_tax_lines (1:many)
└── service_order_complaints (1:many)
    ├── service_order_corrections (1:many)
    │   └── service_order_parts (1:many)
    └── service_order_technicians (1:many)
```

## 📝 **Questions for You**

1. **Does this schema structure make sense for your reporting needs?**
2. **Are there any additional fields from the JSON that you specifically need?**
3. **Do you want separate tables for addresses, or would JSON columns work better?**
4. **Should I include the MiscCharges array (empty in your sample)?**
5. **Any specific indexing requirements for reporting queries?**

Once you approve this schema, I'll update the codebase to implement the flattening logic!