#!/usr/bin/env python3
"""
Fix missing columns in the fullbay_line_items table
"""

import psycopg2
from psycopg2.extras import RealDictCursor

def fix_missing_columns():
    """Identify and fix missing columns."""
    print("🔧 FIXING MISSING COLUMNS")
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
        
        # Check for missing columns
        missing_columns = []
        
        # Expected columns that should exist
        expected_columns = [
            'exported', 'repair_order_number', 'line_total_price', 'quickbooks_account',
            'quickbooks_item', 'quickbooks_item_type', 'so_total_parts_cost', 
            'so_total_parts_price', 'so_total_labor_hours', 'so_total_labor_cost',
            'so_subtotal', 'so_tax_total', 'so_total_amount'
        ]
        
        for col_name in expected_columns:
            cursor.execute("""
                SELECT column_name
                FROM information_schema.columns 
                WHERE table_name = 'fullbay_line_items' 
                AND table_schema = 'public'
                AND column_name = %s
            """, (col_name,))
            
            if not cursor.fetchone():
                missing_columns.append(col_name)
        
        if missing_columns:
            print(f"❌ Missing columns found: {missing_columns}")
            print()
            print("🔧 Adding missing columns...")
            
            for col_name in missing_columns:
                if col_name == 'exported':
                    cursor.execute("ALTER TABLE fullbay_line_items ADD COLUMN exported BOOLEAN DEFAULT FALSE")
                elif col_name == 'repair_order_number':
                    cursor.execute("ALTER TABLE fullbay_line_items ADD COLUMN repair_order_number VARCHAR(50)")
                elif col_name == 'line_total_price':
                    cursor.execute("ALTER TABLE fullbay_line_items ADD COLUMN line_total_price DECIMAL(10,2)")
                elif col_name == 'quickbooks_account':
                    cursor.execute("ALTER TABLE fullbay_line_items ADD COLUMN quickbooks_account VARCHAR(255)")
                elif col_name == 'quickbooks_item':
                    cursor.execute("ALTER TABLE fullbay_line_items ADD COLUMN quickbooks_item VARCHAR(255)")
                elif col_name == 'quickbooks_item_type':
                    cursor.execute("ALTER TABLE fullbay_line_items ADD COLUMN quickbooks_item_type VARCHAR(100)")
                elif col_name == 'so_total_parts_cost':
                    cursor.execute("ALTER TABLE fullbay_line_items ADD COLUMN so_total_parts_cost DECIMAL(10,2)")
                elif col_name == 'so_total_parts_price':
                    cursor.execute("ALTER TABLE fullbay_line_items ADD COLUMN so_total_parts_price DECIMAL(10,2)")
                elif col_name == 'so_total_labor_hours':
                    cursor.execute("ALTER TABLE fullbay_line_items ADD COLUMN so_total_labor_hours DECIMAL(8,2)")
                elif col_name == 'so_total_labor_cost':
                    cursor.execute("ALTER TABLE fullbay_line_items ADD COLUMN so_total_labor_cost DECIMAL(10,2)")
                elif col_name == 'so_subtotal':
                    cursor.execute("ALTER TABLE fullbay_line_items ADD COLUMN so_subtotal DECIMAL(10,2)")
                elif col_name == 'so_tax_total':
                    cursor.execute("ALTER TABLE fullbay_line_items ADD COLUMN so_tax_total DECIMAL(10,2)")
                elif col_name == 'so_total_amount':
                    cursor.execute("ALTER TABLE fullbay_line_items ADD COLUMN so_total_amount DECIMAL(10,2)")
                
                print(f"   ✅ Added {col_name}")
            
            conn.commit()
            print("   💾 Changes committed")
        else:
            print("   ✅ No missing columns found")
        
        # Get final column count
        cursor.execute("""
            SELECT COUNT(*) as count
            FROM information_schema.columns 
            WHERE table_name = 'fullbay_line_items' 
            AND table_schema = 'public'
        """)
        
        final_count = cursor.fetchone()['count']
        print(f"\n📊 Final column count: {final_count}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        if 'conn' in locals():
            conn.rollback()
        raise

if __name__ == "__main__":
    fix_missing_columns()
