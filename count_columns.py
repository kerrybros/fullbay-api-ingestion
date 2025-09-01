#!/usr/bin/env python3
"""
Count the columns in our CREATE TABLE SQL to verify we have exactly 73 columns.
"""

def count_columns_in_sql():
    """Count columns in our CREATE TABLE SQL."""
    
    # This is the exact SQL from our script
    create_table_sql = """
        CREATE TABLE fullbay_line_items (
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
            customer_main_phone VARCHAR(50),
            customer_secondary_phone VARCHAR(50),
            customer_billing_address TEXT,
            
            -- === SERVICE ORDER INFO (5 columns) ===
            fullbay_service_order_id VARCHAR(50),
            repair_order_number VARCHAR(50),
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
            global_component VARCHAR(255),
            global_system VARCHAR(255),
            global_service VARCHAR(255),
            recommended_correction TEXT,
            service_description TEXT,
            
            -- === LINE ITEM DETAILS (15 columns) ===
            line_item_type VARCHAR(20) NOT NULL,
            
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
            so_hours DECIMAL(8,2),
            technician_portion INTEGER,
            
            -- === FINANCIAL DETAILS (Per Line Item) ===
            unit_cost DECIMAL(10,2),
            unit_price DECIMAL(10,2),
            line_total_cost DECIMAL(10,2),
            line_total_price DECIMAL(10,2),
            price_overridden BOOLEAN DEFAULT FALSE,
            
            -- === TAX INFO ===
            taxable BOOLEAN DEFAULT TRUE,
            tax_rate DECIMAL(5,2),
            line_tax DECIMAL(10,2),
            sales_total DECIMAL(10,2),
            
            -- === CLASSIFICATION ===
            inventory_item BOOLEAN DEFAULT FALSE,
            core_type VARCHAR(50),
            sublet BOOLEAN DEFAULT FALSE,
            
            -- === QUICKBOOKS INTEGRATION ===
            quickbooks_account VARCHAR(255),
            quickbooks_item VARCHAR(255),
            quickbooks_item_type VARCHAR(100),
            
            -- === SERVICE ORDER TOTALS (8 columns) ===
            so_total_parts_cost DECIMAL(10,2),
            so_total_parts_price DECIMAL(10,2),
            so_total_labor_hours DECIMAL(8,2),
            so_total_labor_cost DECIMAL(10,2),
            so_supplies_total DECIMAL(10,2),
            so_subtotal DECIMAL(10,2),
            so_tax_total DECIMAL(10,2),
            so_total_amount DECIMAL(10,2),
            
            -- === METADATA (2 columns) ===
            ingestion_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            ingestion_source VARCHAR(100) DEFAULT 'fullbay_api'
        );
        """
    
    # Extract column names from the SQL
    lines = create_table_sql.split('\n')
    columns = []
    
    for line in lines:
        line = line.strip()
        if line and not line.startswith('--') and not line.startswith('CREATE') and not line.startswith('(') and not line.startswith(')') and not line.startswith(';'):
            # Check if this line contains a column definition
            if any(keyword in line.upper() for keyword in ['VARCHAR', 'INTEGER', 'BOOLEAN', 'DECIMAL', 'TEXT', 'TIMESTAMP', 'SERIAL']):
                # Extract column name (first word before space)
                parts = line.split()
                if parts:
                    column_name = parts[0]
                    if column_name not in ['PRIMARY', 'KEY', 'REFERENCES', 'ON', 'DELETE', 'CASCADE']:
                        columns.append(column_name)
    
    print("🔍 COLUMN COUNT ANALYSIS")
    print("=" * 50)
    print(f"Total columns found: {len(columns)}")
    print("\nColumns:")
    for i, col in enumerate(columns, 1):
        print(f"  {i:2d}. {col}")
    
    # Expected breakdown
    expected = {
        'INVOICE LEVEL INFO': 8,
        'CUSTOMER INFO': 6,
        'SERVICE ORDER INFO': 5,
        'UNIT/VEHICLE INFO': 8,
        'PRIMARY TECHNICIAN INFO': 2,
        'COMPLAINT/WORK ORDER INFO': 6,
        'CORRECTION/SERVICE INFO': 7,
        'LINE ITEM DETAILS': 15,
        'SERVICE ORDER TOTALS': 8,
        'METADATA': 2
    }
    
    total_expected = sum(expected.values())
    print(f"\nExpected total: {total_expected}")
    print(f"Actual total: {len(columns)}")
    
    if len(columns) == total_expected:
        print("✅ Column count matches expected!")
    else:
        print(f"❌ Column count mismatch! Expected {total_expected}, got {len(columns)}")

if __name__ == "__main__":
    count_columns_in_sql()
