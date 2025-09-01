#!/usr/bin/env python3
"""
Fix the database schema to match our current requirements.
This script will:
1. Remove old columns that shouldn't exist
2. Add missing columns
3. Ensure the schema matches our 73-column requirement
"""

import psycopg2
from psycopg2.extras import RealDictCursor

def fix_database_schema():
    """Fix the database schema to match our current requirements."""
    print("🔧 FIXING DATABASE SCHEMA")
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
        
        # Show current columns
        for col in current_columns:
            print(f"  {col['column_name']} ({col['data_type']})")
        
        print("\n🔧 APPLYING SCHEMA FIXES:")
        print("-" * 40)
        
        # Remove columns that shouldn't exist
        columns_to_remove = [
            'actual_correction',
            'correction_performed',
            'soai_id',
            'component',
            'system',
            'total',
            'tax_amount'
        ]
        
        for col_name in columns_to_remove:
            try:
                cursor.execute(f"ALTER TABLE fullbay_line_items DROP COLUMN IF EXISTS {col_name}")
                print(f"  ✅ Removed column: {col_name}")
            except Exception as e:
                print(f"  ⚠️  Could not remove {col_name}: {e}")
        
        # Add missing columns that should exist
        columns_to_add = [
            ('fullbay_complaint_id', 'INTEGER'),
            ('global_component', 'VARCHAR(255)'),
            ('global_system', 'VARCHAR(255)'),
            ('global_service', 'VARCHAR(255)'),
            ('line_total_price', 'DECIMAL(10,2)'),
            ('line_tax', 'DECIMAL(10,2)'),
            ('sales_total', 'DECIMAL(10,2)')
        ]
        
        for col_name, col_type in columns_to_add:
            try:
                cursor.execute(f"ALTER TABLE fullbay_line_items ADD COLUMN IF NOT EXISTS {col_name} {col_type}")
                print(f"  ✅ Added column: {col_name} ({col_type})")
            except Exception as e:
                print(f"  ⚠️  Could not add {col_name}: {e}")
        
        # Rename columns if needed
        try:
            cursor.execute("ALTER TABLE fullbay_line_items RENAME COLUMN IF EXISTS actual_hours TO so_hours")
            print("  ✅ Renamed actual_hours to so_hours")
        except Exception as e:
            print(f"  ⚠️  Could not rename actual_hours: {e}")
        
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
            print("\n✅ SUCCESS: Database schema now has 73 columns!")
        else:
            print(f"\n⚠️  WARNING: Expected 73 columns, got {len(final_columns)}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error fixing database schema: {e}")
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    fix_database_schema()
