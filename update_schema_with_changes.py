#!/usr/bin/env python3
"""
Update database schema with requested changes:
1. Remove specific columns
2. Rename columns
"""

import psycopg2
from psycopg2.extras import RealDictCursor

def update_schema():
    """Update the database schema with requested changes."""
    print("UPDATING DATABASE SCHEMA")
    print("=" * 40)
    
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
        
        print("🔧 Making schema changes...")
        
        # Step 1: Drop columns to be removed
        columns_to_drop = [
            'so_total_parts_cost',
            'so_total_parts_price', 
            'so_total_labor_hours',
            'so_total_labor_cost',
            'so_subtotal',
            'so_tax_total',
            'so_total_amount',
            'quickbooks_item_type',
            'quickbooks_item',
            'quickbooks_account',
            'exported'  # Added exported column to drop list
        ]
        
        print("🗑️  Dropping columns...")
        for column in columns_to_drop:
            try:
                cursor.execute(f"ALTER TABLE fullbay_line_items DROP COLUMN IF EXISTS {column}")
                print(f"   ✅ Dropped column: {column}")
            except Exception as e:
                print(f"   ⚠️  Could not drop {column}: {e}")
        
        # Step 2: Rename columns
        column_renames = [
            ('repair_order_number', 'service_order'),
            ('unit_number', 'unit'),
            ('fullbay_complaint_id', 'SOAI_id'),
            ('global_system', 'system'),
            ('global_component', 'component'),
            ('line_total_price', 'total')
        ]
        
        print("\n🔄 Renaming columns...")
        for old_name, new_name in column_renames:
            try:
                cursor.execute(f"ALTER TABLE fullbay_line_items RENAME COLUMN {old_name} TO {new_name}")
                print(f"   ✅ Renamed {old_name} → {new_name}")
            except Exception as e:
                print(f"   ⚠️  Could not rename {old_name}: {e}")
        
        # Commit changes
        conn.commit()
        print("\n✅ Schema changes completed successfully!")
        
        # Step 3: Verify the new structure
        print("\n📊 VERIFYING NEW STRUCTURE:")
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'fullbay_line_items' 
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        print(f"   Total columns: {len(columns)}")
        
        # Show first 20 columns
        print("\n🔍 First 20 columns:")
        for i, col in enumerate(columns[:20]):
            print(f"   {i+1:2d}. {col['column_name']} ({col['data_type']})")
        
        if len(columns) > 20:
            print(f"   ... and {len(columns)-20} more columns")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        raise

if __name__ == "__main__":
    update_schema()
