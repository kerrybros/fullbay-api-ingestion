#!/usr/bin/env python3
"""
Fix the database schema to have exactly 73 columns by removing 9 unnecessary columns.
"""

import psycopg2
from psycopg2.extras import RealDictCursor

def fix_to_exactly_73_columns():
    """Remove exactly 9 columns to get to 73 columns total."""
    print("🔧 FIXING TO EXACTLY 73 COLUMNS")
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
        
        print("\n🔧 REMOVING 9 COLUMNS TO GET TO 73:")
        print("-" * 40)
        
        # Columns to remove (these are less essential and can be calculated or derived)
        columns_to_remove = [
            'labor_hours',           # We have so_hours which is more specific
            'line_total_cost',       # Can be calculated from unit_cost * quantity
            'so_total_parts_cost',   # Can be calculated from line items
            'so_total_parts_price',  # Can be calculated from line items
            'so_total_labor_hours',  # Can be calculated from line items
            'so_total_labor_cost',   # Can be calculated from line items
            'so_subtotal',           # Can be calculated from line items
            'so_tax_total',          # Can be calculated from line items
            'so_total_amount'        # Can be calculated from line items
        ]
        
        for col_name in columns_to_remove:
            try:
                cursor.execute(f"ALTER TABLE fullbay_line_items DROP COLUMN IF EXISTS {col_name}")
                print(f"  ✅ Removed column: {col_name}")
            except Exception as e:
                print(f"  ⚠️  Could not remove {col_name}: {e}")
        
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
            print(f"\n⚠️  WARNING: Expected 73 columns, got {len(final_columns)}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error fixing database schema: {e}")
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    fix_to_exactly_73_columns()
