#!/usr/bin/env python3
"""
Verify the final structure after all schema changes
"""

import psycopg2
from psycopg2.extras import RealDictCursor

def verify_final_structure():
    """Verify the final structure after all changes."""
    print("VERIFYING FINAL SCHEMA STRUCTURE")
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
        
        # Get all columns
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'fullbay_line_items' 
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        print(f"📊 Total columns: {len(columns)}")
        
        # Show all column names
        print(f"\n📋 ALL COLUMN NAMES:")
        for i, col in enumerate(columns):
            print(f"   {i+1:2d}. {col['column_name']} ({col['data_type']})")
        
        # Check for specific columns that should be removed
        removed_columns = [
            'so_total_parts_cost', 'so_total_parts_price', 'so_total_labor_hours',
            'so_total_labor_cost', 'so_subtotal', 'so_tax_total', 'so_total_amount',
            'quickbooks_item_type', 'quickbooks_item', 'quickbooks_account', 'exported'
        ]
        
        print(f"\n🔍 CHECKING FOR REMOVED COLUMNS:")
        for col in removed_columns:
            cursor.execute(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'fullbay_line_items' 
                AND column_name = '{col}'
            """)
            result = cursor.fetchone()
            if result:
                print(f"   ❌ {col} - STILL EXISTS")
            else:
                print(f"   ✅ {col} - REMOVED")
        
        # Check for renamed columns
        renamed_columns = [
            ('service_order', 'repair_order_number'),
            ('unit', 'unit_number'),
            ('SOAI_id', 'fullbay_complaint_id'),
            ('system', 'global_system'),
            ('component', 'global_component'),
            ('total', 'line_total_price')
        ]
        
        print(f"\n🔄 CHECKING RENAMED COLUMNS:")
        for new_name, old_name in renamed_columns:
            cursor.execute(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'fullbay_line_items' 
                AND column_name = '{new_name}'
            """)
            result = cursor.fetchone()
            if result:
                print(f"   ✅ {old_name} → {new_name} - RENAMED")
            else:
                print(f"   ❌ {new_name} - NOT FOUND")
        
        # Get data count
        cursor.execute("""
            SELECT COUNT(*) as total_line_items
            FROM fullbay_line_items 
            WHERE invoice_date >= '2025-04-01' 
            AND invoice_date <= '2025-04-30'
        """)
        
        count_result = cursor.fetchone()
        print(f"\n📈 DATA COUNT:")
        print(f"   April 2025 Line Items: {count_result['total_line_items']:,}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        raise

if __name__ == "__main__":
    verify_final_structure()
