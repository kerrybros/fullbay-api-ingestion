#!/usr/bin/env python3
"""
Create the final 73-column table schema.
This script will drop and recreate the table with exactly 73 columns.
"""

import psycopg2
from psycopg2.extras import RealDictCursor

def create_final_73_column_table():
    """Create the final table with exactly 73 columns."""
    print("🏗️ CREATING FINAL 73-COLUMN TABLE")
    print("=" * 50)
    
    db_params = {
        'host': 'fullbay-postgres-db.cc74yq2i4j8n.us-east-1.rds.amazonaws.com',
        'port': 5432,
        'dbname': 'fullbay_data',
        'user': 'postgres',
        'password': '5255Tillman'
    }
    
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        print("🗑️ DROPPING EXISTING TABLE:")
        print("-" * 40)
        cursor.execute("DROP TABLE IF EXISTS fullbay_line_items CASCADE")
        print("  ✅ Dropped existing table")
        
        print("\n🏗️ CREATING NEW 73-COLUMN TABLE:")
        print("-" * 40)
        
        # Create table with exactly 73 columns
        create_table_sql = """
        CREATE TABLE fullbay_line_items (
            -- === PRIMARY KEY & REFERENCE (2 columns) ===
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
            unit_license_plate VARCHAR(50),
            
            -- === PRIMARY TECHNICIAN INFO (2 columns) ===
            primary_technician VARCHAR(255),
            primary_technician_number VARCHAR(50),
            
            -- === COMPLAINT INFO (6 columns) ===
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
            
            -- === LINE ITEM CLASSIFICATION (1 column) ===
            line_item_type VARCHAR(50) NOT NULL,
            
            -- === PART INFO (5 columns) ===
            fullbay_part_id INTEGER,
            part_description TEXT,
            shop_part_number VARCHAR(100),
            vendor_part_number VARCHAR(100),
            part_category VARCHAR(100),
            
            -- === LABOR INFO (4 columns) ===
            labor_description TEXT,
            labor_rate_type VARCHAR(50),
            assigned_technician VARCHAR(255),
            assigned_technician_number VARCHAR(50),
            
            -- === QUANTITIES (3 columns) ===
            quantity DECIMAL(10,2),
            to_be_returned_quantity DECIMAL(10,2),
            returned_quantity DECIMAL(10,2),
            
            -- === HOURS (2 columns) ===
            so_hours DECIMAL(8,2),
            technician_portion INTEGER,
            
            -- === FINANCIAL DETAILS (3 columns) ===
            unit_cost DECIMAL(10,2),
            unit_price DECIMAL(10,2),
            line_total_price DECIMAL(10,2),
            price_overridden BOOLEAN DEFAULT FALSE,
            
            -- === TAX CALCULATION (5 columns) ===
            taxable BOOLEAN DEFAULT TRUE,
            tax_rate DECIMAL(5,2),
            line_tax DECIMAL(10,2),
            sales_total DECIMAL(10,2),
            
            -- === CLASSIFICATION (3 columns) ===
            inventory_item BOOLEAN DEFAULT FALSE,
            core_type VARCHAR(50),
            sublet BOOLEAN DEFAULT FALSE,
            
            -- === SERVICE ORDER TOTALS (1 column) ===
            so_supplies_total DECIMAL(10,2),
            
            -- === METADATA (2 columns) ===
            ingestion_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            ingestion_source VARCHAR(50) DEFAULT 'fullbay_api'
        )
        """
        
        cursor.execute(create_table_sql)
        print("  ✅ Created new table")
        
        # Create indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_fullbay_line_items_invoice_id ON fullbay_line_items(fullbay_invoice_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_fullbay_line_items_service_order_id ON fullbay_line_items(fullbay_service_order_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_fullbay_line_items_raw_data_id ON fullbay_line_items(raw_data_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_fullbay_line_items_line_item_type ON fullbay_line_items(line_item_type)")
        print("  ✅ Created indexes")
        
        # Commit changes
        conn.commit()
        
        print("\n📊 FINAL TABLE STATUS:")
        print("-" * 40)
        
        # Check final columns
        cursor.execute("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns 
            WHERE table_name = 'fullbay_line_items' 
            ORDER BY ordinal_position
        """)
        
        final_columns = cursor.fetchall()
        print(f"Final column count: {len(final_columns)}")
        
        if len(final_columns) == 73:
            print("\n✅ SUCCESS: Database schema now has exactly 73 columns!")
        else:
            print(f"\n⚠️ WARNING: Expected 73 columns, got {len(final_columns)}")
            
        # Show all columns
        for i, col in enumerate(final_columns, 1):
            print(f"  {i:2d}. {col['column_name']} ({col['data_type']})")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error creating table: {e}")
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    create_final_73_column_table()
