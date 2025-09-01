#!/usr/bin/env python3
"""
Add tax calculation columns to the fullbay_line_items table
- line_tax: Calculated tax amount for each line item
- sales_total: Line total + line tax
"""

import psycopg2
from psycopg2.extras import RealDictCursor

def add_tax_columns():
    """Add the line_tax and sales_total columns to the database."""
    print("💰 ADDING TAX CALCULATION COLUMNS")
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
        
        # Check current column count
        cursor.execute("""
            SELECT COUNT(*) as count
            FROM information_schema.columns 
            WHERE table_name = 'fullbay_line_items' 
            AND table_schema = 'public'
        """)
        
        current_columns = cursor.fetchone()['count']
        print(f"   📊 Current total columns: {current_columns}")
        
        # Check if columns already exist
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns 
            WHERE table_name = 'fullbay_line_items' 
            AND table_schema = 'public'
            AND column_name IN ('line_tax', 'sales_total')
            ORDER BY column_name
        """)
        
        existing_columns = [row['column_name'] for row in cursor.fetchall()]
        
        if 'line_tax' in existing_columns and 'sales_total' in existing_columns:
            print("   ✅ Both columns already exist")
            print("   ✅ No action needed")
            return
        
        print()
        print("💰 ADD COLUMN OPERATIONS:")
        print("-" * 40)
        
        # Add line_tax column if it doesn't exist
        if 'line_tax' not in existing_columns:
            print("   1. Adding line_tax column...")
            cursor.execute("ALTER TABLE fullbay_line_items ADD COLUMN line_tax DECIMAL(10,2)")
            print("      ✅ line_tax column added successfully")
        else:
            print("   1. ✅ line_tax column already exists")
        
        # Add sales_total column if it doesn't exist
        if 'sales_total' not in existing_columns:
            print("   2. Adding sales_total column...")
            cursor.execute("ALTER TABLE fullbay_line_items ADD COLUMN sales_total DECIMAL(10,2)")
            print("      ✅ sales_total column added successfully")
        else:
            print("   2. ✅ sales_total column already exists")
        
        # Verify the columns were added
        print("\n🔍 VERIFYING COLUMNS:")
        print("-" * 40)
        
        cursor.execute("""
            SELECT column_name, data_type, ordinal_position
            FROM information_schema.columns 
            WHERE table_name = 'fullbay_line_items' 
            AND table_schema = 'public'
            AND column_name IN ('line_tax', 'sales_total')
            ORDER BY column_name
        """)
        
        new_columns = cursor.fetchall()
        for col in new_columns:
            print(f"   ✅ {col['column_name']:<15} ({col['data_type']}) - Position {col['ordinal_position']}")
        
        # Get new column count
        cursor.execute("""
            SELECT COUNT(*) as count
            FROM information_schema.columns 
            WHERE table_name = 'fullbay_line_items' 
            AND table_schema = 'public'
        """)
        
        new_columns_count = cursor.fetchone()['count']
        print(f"\n   📊 New total columns: {new_columns_count}")
        print(f"   📈 Columns added: {new_columns_count - current_columns}")
        
        # Commit the changes
        conn.commit()
        print("   💾 Changes committed to database")
        
        print()
        print("🎉 TAX COLUMNS ADDITION COMPLETE!")
        print("=" * 50)
        print("   ✅ line_tax column added for tax calculations")
        print("   ✅ sales_total column added for line total + tax")
        print("   ✅ Ready for tax calculation functionality")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        if 'conn' in locals():
            conn.rollback()
        raise

if __name__ == "__main__":
    add_tax_columns()
