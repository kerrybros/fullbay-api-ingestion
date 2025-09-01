#!/usr/bin/env python3
"""
Rollback to original 75-column schema and export April 2025 data
FIXED VERSION - handles VARCHAR length issues
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import csv
from datetime import datetime

def rollback_and_export():
    """Rollback schema and export data."""
    print("ROLLBACK TO ORIGINAL 75-COLUMN SCHEMA (FIXED)")
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
        
        print("Connected to database")
        
        # Check current state
        cursor.execute("SELECT COUNT(*) as current_rows FROM fullbay_line_items")
        current_rows = cursor.fetchone()['current_rows']
        print(f"Current rows: {current_rows}")
        
        cursor.execute("""
            SELECT COUNT(*) as current_columns 
            FROM information_schema.columns 
            WHERE table_name = 'fullbay_line_items' 
            AND table_schema = 'public'
        """)
        current_columns = cursor.fetchone()['current_columns']
        print(f"Current columns: {current_columns}")
        
        if current_columns == 75:
            print("✅ Schema is already at 75 columns - no rollback needed")
        else:
            print(f"🔄 Rolling back from {current_columns} to 75 columns...")
            
            # Create backup
            cursor.execute("CREATE TABLE IF NOT EXISTS fullbay_line_items_backup AS SELECT * FROM fullbay_line_items")
            print("✅ Backup created")
            
            # Drop current table
            cursor.execute("DROP TABLE fullbay_line_items")
            print("✅ Old table dropped")
            
            # Recreate with original structure (FIXED - increased phone field sizes)
            create_table_sql = """
            CREATE TABLE fullbay_line_items (
                id SERIAL PRIMARY KEY,
                raw_data_id INTEGER REFERENCES fullbay_raw_data(id) ON DELETE CASCADE,
                fullbay_invoice_id VARCHAR(50) NOT NULL,
                invoice_number VARCHAR(50),
                invoice_date DATE,
                due_date DATE,
                exported BOOLEAN DEFAULT FALSE,
                shop_title VARCHAR(255),
                shop_email VARCHAR(255),
                shop_address TEXT,
                customer_id INTEGER,
                customer_title VARCHAR(255),
                customer_external_id VARCHAR(50),
                customer_main_phone VARCHAR(50),
                customer_secondary_phone VARCHAR(50),
                customer_billing_address TEXT,
                fullbay_service_order_id VARCHAR(50),
                repair_order_number VARCHAR(50),
                service_order_created TIMESTAMP WITH TIME ZONE,
                service_order_start_date TIMESTAMP WITH TIME ZONE,
                service_order_completion_date TIMESTAMP WITH TIME ZONE,
                unit_id VARCHAR(50),
                unit_number VARCHAR(50),
                unit_type VARCHAR(100),
                unit_year VARCHAR(10),
                unit_make VARCHAR(100),
                unit_model VARCHAR(100),
                unit_vin VARCHAR(50),
                unit_license_plate VARCHAR(20),
                primary_technician VARCHAR(255),
                primary_technician_number VARCHAR(50),
                fullbay_complaint_id INTEGER,
                complaint_type VARCHAR(100),
                complaint_subtype VARCHAR(100),
                complaint_note TEXT,
                complaint_cause TEXT,
                complaint_authorized BOOLEAN,
                fullbay_correction_id INTEGER,
                correction_title VARCHAR(255),
                global_component VARCHAR(255),
                global_system VARCHAR(255),
                global_service VARCHAR(255),
                recommended_correction TEXT,
                actual_correction TEXT,
                correction_performed VARCHAR(50),
                line_item_type VARCHAR(20) NOT NULL,
                fullbay_part_id INTEGER,
                part_description TEXT,
                shop_part_number VARCHAR(100),
                vendor_part_number VARCHAR(100),
                part_category VARCHAR(255),
                labor_description TEXT,
                labor_rate_type VARCHAR(50),
                assigned_technician VARCHAR(255),
                assigned_technician_number VARCHAR(50),
                quantity DECIMAL(10,3),
                to_be_returned_quantity DECIMAL(10,3),
                returned_quantity DECIMAL(10,3),
                labor_hours DECIMAL(8,2),
                actual_hours DECIMAL(8,2),
                technician_portion INTEGER,
                unit_cost DECIMAL(10,2),
                unit_price DECIMAL(10,2),
                line_total_cost DECIMAL(10,2),
                line_total_price DECIMAL(10,2),
                price_overridden BOOLEAN DEFAULT FALSE,
                taxable BOOLEAN DEFAULT TRUE,
                tax_rate DECIMAL(5,2),
                tax_amount DECIMAL(10,2),
                inventory_item BOOLEAN DEFAULT FALSE,
                core_type VARCHAR(50),
                sublet BOOLEAN DEFAULT FALSE,
                quickbooks_account VARCHAR(255),
                quickbooks_item VARCHAR(255),
                quickbooks_item_type VARCHAR(100),
                so_total_parts_cost DECIMAL(10,2),
                so_total_parts_price DECIMAL(10,2),
                so_total_labor_hours DECIMAL(8,2),
                so_total_labor_cost DECIMAL(10,2),
                so_subtotal DECIMAL(10,2),
                so_tax_total DECIMAL(10,2),
                so_total_amount DECIMAL(10,2),
                ingestion_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                ingestion_source VARCHAR(100) DEFAULT 'fullbay_api'
            )
            """
            
            cursor.execute(create_table_sql)
            print("✅ New table created with original structure")
            
            # Insert data from backup with proper field mapping
            insert_sql = """
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
                shop_title, shop_email, shop_physical_address,
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
                labor_hours, actual_hours, assigned_technician_portion,
                unit_cost, unit_price, line_total_cost, line_total_price, price_overridden,
                taxable, tax_rate, tax_amount, inventory_item, core_type, sublet,
                quickbooks_account, quickbooks_item, quickbooks_item_type,
                so_total_parts_cost, so_total_parts_price, so_total_labor_hours, 
                so_total_labor_cost, so_subtotal, so_tax_total, so_total_amount,
                ingestion_timestamp, ingestion_source
            FROM fullbay_line_items_backup
            """
            
            cursor.execute(insert_sql)
            print("✅ Data restored with original structure")
            
            conn.commit()
            print("✅ Rollback completed")
        
        # Now export April 2025 data with original structure
        print("\nEXPORTING APRIL 2025 DATA WITH ORIGINAL STRUCTURE")
        print("=" * 60)
        
        # Get all column names in order
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns 
            WHERE table_name = 'fullbay_line_items' 
            AND table_schema = 'public'
            ORDER BY ordinal_position
        """)
        
        columns = [row['column_name'] for row in cursor.fetchall()]
        
        # Query April 2025 data
        select_columns = ', '.join(columns)
        query = f"""
        SELECT {select_columns}
        FROM fullbay_line_items 
        WHERE invoice_date >= '2025-04-01' 
        AND invoice_date <= '2025-04-30'
        ORDER BY invoice_date, fullbay_invoice_id, id
        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        
        if not rows:
            print("No data found for April 2025")
            return
        
        print(f"Found {len(rows)} rows for April 2025")
        
        # Create CSV filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        csv_filename = f"april_2025_original_structure_{timestamp}.csv"
        
        # Write to CSV
        print(f"Writing to {csv_filename}...")
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=columns)
            writer.writeheader()
            
            for row in rows:
                writer.writerow(row)
        
        print(f"✅ Successfully exported {len(rows)} records to {csv_filename}")
        print(f"📊 Columns exported: {len(columns)}")
        
        # Verify final state
        cursor.execute("""
            SELECT COUNT(*) as final_columns 
            FROM information_schema.columns 
            WHERE table_name = 'fullbay_line_items' 
            AND table_schema = 'public'
        """)
        final_columns = cursor.fetchone()['final_columns']
        
        print(f"\n🎉 FINAL RESULTS:")
        print(f"  Schema columns: {final_columns} (target: 75)")
        print(f"  April 2025 rows: {len(rows)}")
        print(f"  CSV file: {csv_filename}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        if 'conn' in locals():
            conn.rollback()
        raise
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    rollback_and_export()
