#!/usr/bin/env python3
"""
Add the so_supplies_total column to the fullbay_line_items table
This column will store the invoice-level supplies total for SHOP SUPPLIES line items
"""

import psycopg2
from psycopg2.extras import RealDictCursor

def add_supplies_total_column():
    """Add the so_supplies_total column to the database."""
    print("➕ ADDING SO_SUPPLIES_TOTAL COLUMN")
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
        
        # Check if the column already exists
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns 
            WHERE table_name = 'fullbay_line_items' 
            AND table_schema = 'public'
            AND column_name = 'so_supplies_total'
        """)
        
        if cursor.fetchone():
            print("   ✅ Column 'so_supplies_total' already exists")
            print("   ✅ No action needed")
            return
        
        # Get current column count
        cursor.execute("""
            SELECT COUNT(*) as count
            FROM information_schema.columns 
            WHERE table_name = 'fullbay_line_items' 
            AND table_schema = 'public'
        """)
        
        current_columns = cursor.fetchone()['count']
        print(f"   📊 Current total columns: {current_columns}")
        
        print()
        print("➕ ADD COLUMN OPERATION:")
        print("-" * 40)
        print("   1. Add so_supplies_total column")
        print("   2. Verify column has been added")
        print("   3. Confirm new column count")
        print()
        
        # Execute the column addition
        print("🚀 Adding column...")
        cursor.execute("ALTER TABLE fullbay_line_items ADD COLUMN so_supplies_total DECIMAL(10,2)")
        print("   ✅ Column added successfully")
        
        # Verify the column was added
        cursor.execute("""
            SELECT column_name, data_type, ordinal_position
            FROM information_schema.columns 
            WHERE table_name = 'fullbay_line_items' 
            AND table_schema = 'public'
            AND column_name = 'so_supplies_total'
        """)
        
        column_info = cursor.fetchone()
        if column_info:
            print(f"   ✅ Column 'so_supplies_total' successfully created")
            print(f"   📊 Data type: {column_info['data_type']}")
            print(f"   📍 Position: {column_info['ordinal_position']}")
        else:
            print("   ❌ Column 'so_supplies_total' not found - addition failed")
            return
        
        # Get new column count
        cursor.execute("""
            SELECT COUNT(*) as count
            FROM information_schema.columns 
            WHERE table_name = 'fullbay_line_items' 
            AND table_schema = 'public'
        """)
        
        new_columns = cursor.fetchone()['count']
        print(f"   📊 New total columns: {new_columns}")
        print(f"   📈 Columns added: {new_columns - current_columns}")
        
        # Commit the changes
        conn.commit()
        print("   💾 Changes committed to database")
        
        print()
        print("🎉 COLUMN ADDITION COMPLETE!")
        print("=" * 50)
        print("   ✅ so_supplies_total column has been added")
        print("   ✅ Table structure updated")
        print("   ✅ Ready for SHOP SUPPLIES line items")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        if 'conn' in locals():
            conn.rollback()
        raise

if __name__ == "__main__":
    add_supplies_total_column()
