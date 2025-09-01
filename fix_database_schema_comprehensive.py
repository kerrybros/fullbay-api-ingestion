#!/usr/bin/env python3
"""
Comprehensive fix for the database schema.
This script will recreate the fullbay_line_items table with exactly 73 columns
matching our current requirements.
"""

import psycopg2
from psycopg2.extras import RealDictCursor

def fix_database_schema_comprehensive():
    """Completely fix the database schema by recreating the table."""
    print("🔧 COMPREHENSIVE DATABASE SCHEMA FIX")
    print("=" * 60)
    
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
        
        print("📊 CURRENT TABLE STATUS:")
        print("-" * 40)
        
        # Check current columns
        cursor.execute("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns 
            WHERE table_name = 'fullbay_line_items' 
            ORDER BY ordinal_position
        """)
        
        current_columns = cursor.fetchall()
        print(f"Current column count: {len(current_columns)}")
        
        print("\n🗑️  DROPPING EXISTING TABLE:")
        print("-" * 40)
        
        # Drop the existing table
        cursor.execute("DROP TABLE IF EXISTS fullbay_line_items CASCADE")
        print("  ✅ Dropped existing fullbay_line_items table")
        
        print("\n🏗️  CREATING NEW TABLE WITH CORRECT SCHEMA:")
        print("-" * 40)
        
        # Create the table with the correct 73-column schema
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
        
        cursor.execute(create_table_sql)
        print("  ✅ Created new fullbay_line_items table")
        
        # Create indexes
        create_indexes = [
            "CREATE INDEX idx_fullbay_line_items_invoice_id ON fullbay_line_items(fullbay_invoice_id);",
            "CREATE INDEX idx_fullbay_line_items_invoice_date ON fullbay_line_items(invoice_date);",
            "CREATE INDEX idx_fullbay_line_items_customer_id ON fullbay_line_items(customer_id);",
            "CREATE INDEX idx_fullbay_line_items_repair_order ON fullbay_line_items(repair_order_number);",
            "CREATE INDEX idx_fullbay_line_items_unit_vin ON fullbay_line_items(unit_vin);",
            "CREATE INDEX idx_fullbay_line_items_line_type ON fullbay_line_items(line_item_type);",
            "CREATE INDEX idx_fullbay_line_items_part_number ON fullbay_line_items(shop_part_number);",
            "CREATE INDEX idx_fullbay_line_items_technician ON fullbay_line_items(assigned_technician);",
            "CREATE INDEX idx_fullbay_line_items_ingestion ON fullbay_line_items(ingestion_timestamp);"
        ]
        
        for index_sql in create_indexes:
            cursor.execute(index_sql)
        
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
        
        # Show final columns
        for col in final_columns:
            print(f"  {col['column_name']} ({col['data_type']})")
        
        if len(final_columns) == 73:
            print("\n✅ SUCCESS: Database schema now has exactly 73 columns!")
        else:
            print(f"\n⚠️  WARNING: Expected 73 columns, got {len(final_columns)}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error fixing database schema: {e}")
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    fix_database_schema_comprehensive()
