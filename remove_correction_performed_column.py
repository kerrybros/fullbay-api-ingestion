#!/usr/bin/env python3
"""
Remove the correction_performed column from the fullbay_line_items table
"""

import psycopg2
from psycopg2.extras import RealDictCursor

def remove_correction_performed_column():
    """Remove the correction_performed column from the database."""
    print("🗑️  REMOVING CORRECTION_PERFORMED COLUMN")
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
            AND column_name = 'correction_performed'
        """)
        
        column_info = cursor.fetchone()
        
        if not column_info:
            print("   ❌ Column 'correction_performed' does not exist")
            print("   ✅ No action needed")
            return
        
        print(f"   📋 Column found: {column_info['column_name']}")
        print(f"   📊 Data type: {column_info['data_type']}")
        print(f"   📍 Position: {column_info['ordinal_position']}")
        
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
        print("🗑️  REMOVAL OPERATION:")
        print("-" * 40)
        print("   1. Drop the correction_performed column")
        print("   2. Verify column has been removed")
        print("   3. Confirm new column count")
        print()
        
        print("⚠️  WARNING: This will permanently remove the column and all its data!")
        print("   The action cannot be undone.")
        print()
        
        # Execute the column removal
        print("🚀 Removing column...")
        cursor.execute("ALTER TABLE fullbay_line_items DROP COLUMN correction_performed")
        print("   ✅ Column dropped successfully")
        
        # Verify the column was removed
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns 
            WHERE table_name = 'fullbay_line_items' 
            AND table_schema = 'public'
            AND column_name = 'correction_performed'
        """)
        
        if cursor.fetchone():
            print("   ❌ Column still exists - removal failed")
        else:
            print("   ✅ Column successfully removed")
        
        # Get new column count
        cursor.execute("""
            SELECT COUNT(*) as count
            FROM information_schema.columns 
            WHERE table_name = 'fullbay_line_items' 
            AND table_schema = 'public'
        """)
        
        new_columns = cursor.fetchone()['count']
        print(f"   📊 New total columns: {new_columns}")
        print(f"   📉 Columns removed: {current_columns - new_columns}")
        
        # Commit the changes
        conn.commit()
        print("   💾 Changes committed to database")
        
        print()
        print("🎉 COLUMN REMOVAL COMPLETE!")
        print("=" * 50)
        print("   ✅ correction_performed column has been removed")
        print("   ✅ Table structure updated")
        print("   ✅ Ready for your line type mapping updates")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        if 'conn' in locals():
            conn.rollback()
        raise

if __name__ == "__main__":
    remove_correction_performed_column()
