#!/usr/bin/env python3
"""
Check the exact column count and identify extra columns
"""

import psycopg2
from psycopg2.extras import RealDictCursor

def check_column_count():
    """Check the exact column count and list all columns."""
    
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
        
        print("CHECKING COLUMN COUNT")
        print("=" * 40)
        
        # Get all columns with their position
        cursor.execute("""
            SELECT ordinal_position, column_name, data_type
            FROM information_schema.columns 
            WHERE table_name = 'fullbay_line_items' 
            AND table_schema = 'public'
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        
        print(f"Total columns: {len(columns)}")
        print("\nAll columns:")
        for i, col in enumerate(columns, 1):
            print(f"{i:2d}. {col['column_name']} ({col['data_type']})")
        
        # Expected 75 columns based on the original schema
        expected_columns = [
            'id', 'raw_data_id', 'fullbay_invoice_id', 'invoice_number', 'invoice_date', 
            'due_date', 'exported', 'shop_title', 'shop_email', 'shop_address',
            'customer_id', 'customer_title', 'customer_external_id', 'customer_main_phone', 
            'customer_secondary_phone', 'customer_billing_address',
            'fullbay_service_order_id', 'repair_order_number', 'service_order_created', 
            'service_order_start_date', 'service_order_completion_date',
            'unit_id', 'unit_number', 'unit_type', 'unit_year', 'unit_make', 'unit_model', 
            'unit_vin', 'unit_license_plate',
            'primary_technician', 'primary_technician_number',
            'fullbay_complaint_id', 'complaint_type', 'complaint_subtype', 'complaint_note', 
            'complaint_cause', 'complaint_authorized',
            'fullbay_correction_id', 'correction_title', 'global_component', 'global_system', 
            'global_service', 'recommended_correction', 'actual_correction', 'correction_performed',
            'line_item_type', 'fullbay_part_id', 'part_description', 'shop_part_number', 
            'vendor_part_number', 'part_category',
            'labor_description', 'labor_rate_type', 'assigned_technician', 'assigned_technician_number',
            'quantity', 'to_be_returned_quantity', 'returned_quantity',
            'labor_hours', 'actual_hours', 'technician_portion',
            'unit_cost', 'unit_price', 'line_total_cost', 'line_total_price', 'price_overridden',
            'taxable', 'tax_rate', 'tax_amount', 'inventory_item', 'core_type', 'sublet',
            'quickbooks_account', 'quickbooks_item', 'quickbooks_item_type',
            'so_total_parts_cost', 'so_total_parts_price', 'so_total_labor_hours', 
            'so_total_labor_cost', 'so_subtotal', 'so_tax_total', 'so_total_amount',
            'ingestion_timestamp', 'ingestion_source'
        ]
        
        current_column_names = [col['column_name'] for col in columns]
        
        print(f"\nExpected columns: {len(expected_columns)}")
        print(f"Current columns: {len(current_column_names)}")
        
        # Find extra columns
        extra_columns = [col for col in current_column_names if col not in expected_columns]
        missing_columns = [col for col in expected_columns if col not in current_column_names]
        
        if extra_columns:
            print(f"\n❌ EXTRA COLUMNS ({len(extra_columns)}):")
            for col in extra_columns:
                print(f"  - {col}")
        
        if missing_columns:
            print(f"\n❌ MISSING COLUMNS ({len(missing_columns)}):")
            for col in missing_columns:
                print(f"  - {col}")
        
        if not extra_columns and not missing_columns:
            print("\n✅ Schema matches exactly 75 columns!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    check_column_count()
