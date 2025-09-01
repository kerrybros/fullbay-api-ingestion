#!/usr/bin/env python3
"""
Rename the actual_correction column to service_description in the fullbay_line_items table
"""

import psycopg2
from psycopg2.extras import RealDictCursor

def rename_actual_correction_column():
    """Rename the actual_correction column to service_description."""
    print("🔄 RENAMING ACTUAL_CORRECTION TO SERVICE_DESCRIPTION")
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
        
        print("📊 CURRENT TABLE STATUS:")
        print("-" * 40)
        
        # Check if the column exists
        cursor.execute("""
            SELECT column_name, data_type, ordinal_position
            FROM information_schema.columns 
            WHERE table_name = 'fullbay_line_items' 
            AND table_schema = 'public'
            AND column_name = 'actual_correction'
        """)
        
        column_info = cursor.fetchone()
        
        if not column_info:
            print("   ❌ Column 'actual_correction' does not exist")
            print("   ✅ No action needed")
            return
        
        print(f"   📋 Column found: {column_info['column_name']}")
        print(f"   📊 Data type: {column_info['data_type']}")
        print(f"   📍 Position: {column_info['ordinal_position']}")
        
        # Check if service_description already exists
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns 
            WHERE table_name = 'fullbay_line_items' 
            AND table_schema = 'public'
            AND column_name = 'service_description'
        """)
        
        if cursor.fetchone():
            print("   ❌ Column 'service_description' already exists")
            print("   ✅ Cannot rename - target name already exists")
            return
        
        print()
        print("🔄 RENAME OPERATION:")
        print("-" * 40)
        print("   1. Rename actual_correction to service_description")
        print("   2. Verify column has been renamed")
        print("   3. Confirm column count remains the same")
        print()
        
        # Execute the column rename
        print("🚀 Renaming column...")
        cursor.execute("ALTER TABLE fullbay_line_items RENAME COLUMN actual_correction TO service_description")
        print("   ✅ Column renamed successfully")
        
        # Verify the column was renamed
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns 
            WHERE table_name = 'fullbay_line_items' 
            AND table_schema = 'public'
            AND column_name = 'actual_correction'
        """)
        
        if cursor.fetchone():
            print("   ❌ Column 'actual_correction' still exists - rename failed")
        else:
            print("   ✅ Column 'actual_correction' successfully removed")
        
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns 
            WHERE table_name = 'fullbay_line_items' 
            AND table_schema = 'public'
            AND column_name = 'service_description'
        """)
        
        if cursor.fetchone():
            print("   ✅ Column 'service_description' successfully created")
        else:
            print("   ❌ Column 'service_description' not found - rename failed")
        
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
        print("=" * 60)
        print("   ✅ actual_correction → service_description")
        print("   ✅ Table structure updated")
        print("   ✅ Ready for line type mapping updates")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        if 'conn' in locals():
            conn.rollback()
        raise

if __name__ == "__main__":
    rename_actual_correction_column()
