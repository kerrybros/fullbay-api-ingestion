#!/usr/bin/env python3
"""
Rename the actual_hours column to so_hours in the fullbay_line_items table
"""

import psycopg2
from psycopg2.extras import RealDictCursor

def rename_actual_hours_to_so_hours():
    """Rename the actual_hours column to so_hours."""
    print("🔄 RENAMING ACTUAL_HOURS TO SO_HOURS")
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
        
        # Check if the column exists
        cursor.execute("""
            SELECT column_name, data_type, ordinal_position
            FROM information_schema.columns 
            WHERE table_name = 'fullbay_line_items' 
            AND table_schema = 'public'
            AND column_name = 'actual_hours'
        """)
        
        column_info = cursor.fetchone()
        
        if not column_info:
            print("   ❌ Column 'actual_hours' does not exist")
            print("   ✅ No action needed")
            return
        
        print(f"   📋 Column found: {column_info['column_name']}")
        print(f"   📊 Data type: {column_info['data_type']}")
        print(f"   📍 Position: {column_info['ordinal_position']}")
        
        # Check if so_hours already exists
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns 
            WHERE table_name = 'fullbay_line_items' 
            AND table_schema = 'public'
            AND column_name = 'so_hours'
        """)
        
        if cursor.fetchone():
            print("   ❌ Column 'so_hours' already exists")
            print("   ✅ Cannot rename - target name already exists")
            return
        
        print()
        print("🔄 RENAME OPERATION:")
        print("-" * 40)
        print("   1. Rename actual_hours to so_hours")
        print("   2. Verify column has been renamed")
        print("   3. Confirm column count remains the same")
        print()
        
        # Execute the column rename
        print("🚀 Renaming column...")
        cursor.execute("ALTER TABLE fullbay_line_items RENAME COLUMN actual_hours TO so_hours")
        print("   ✅ Column renamed successfully")
        
        # Verify the column was renamed
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns 
            WHERE table_name = 'fullbay_line_items' 
            AND table_schema = 'public'
            AND column_name = 'actual_hours'
        """)
        
        if cursor.fetchone():
            print("   ❌ Column 'actual_hours' still exists - rename failed")
        else:
            print("   ✅ Column 'actual_hours' successfully removed")
        
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns 
            WHERE table_name = 'fullbay_line_items' 
            AND table_schema = 'public'
            AND column_name = 'so_hours'
        """)
        
        if cursor.fetchone():
            print("   ✅ Column 'so_hours' successfully created")
        else:
            print("   ❌ Column 'so_hours' not found - rename failed")
        
        # Get column count to ensure it's the same
        cursor.execute("""
            SELECT COUNT(*) as count
            FROM information_schema.columns 
            WHERE table_name = 'fullbay_line_items' 
            AND table_schema = 'public'
        """)
        
        column_count = cursor.fetchone()['count']
        print(f"   📊 Total columns: {column_count}")
        
        # Commit the changes
        conn.commit()
        print("   💾 Changes committed to database")
        
        print()
        print("🎉 COLUMN RENAME COMPLETE!")
        print("=" * 50)
        print("   ✅ actual_hours → so_hours")
        print("   ✅ Table structure updated")
        print("   ✅ Ready for code updates")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        if 'conn' in locals():
            conn.rollback()
        raise

if __name__ == "__main__":
    rename_actual_hours_to_so_hours()
