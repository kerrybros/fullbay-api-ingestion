#!/usr/bin/env python3
"""
Verify that actual_hours column was successfully renamed to so_hours
"""

import psycopg2
from psycopg2.extras import RealDictCursor

def verify_so_hours_rename():
    """Verify the column rename was successful."""
    print("🔍 VERIFYING SO_HOURS COLUMN RENAME")
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
        
        print("📊 COLUMN STATUS CHECK:")
        print("-" * 40)
        
        # Check if actual_hours still exists
        cursor.execute("""
            SELECT column_name, data_type, ordinal_position
            FROM information_schema.columns 
            WHERE table_name = 'fullbay_line_items' 
            AND table_schema = 'public'
            AND column_name = 'actual_hours'
        """)
        
        actual_hours_result = cursor.fetchone()
        if actual_hours_result:
            print(f"   ❌ actual_hours still exists at position {actual_hours_result['ordinal_position']}")
        else:
            print("   ✅ actual_hours column successfully removed")
        
        # Check if so_hours exists
        cursor.execute("""
            SELECT column_name, data_type, ordinal_position
            FROM information_schema.columns 
            WHERE table_name = 'fullbay_line_items' 
            AND table_schema = 'public'
            AND column_name = 'so_hours'
        """)
        
        so_hours_result = cursor.fetchone()
        if so_hours_result:
            print(f"   ✅ so_hours column exists at position {so_hours_result['ordinal_position']}")
            print(f"      Data type: {so_hours_result['data_type']}")
        else:
            print("   ❌ so_hours column not found")
        
        # Get total column count
        cursor.execute("""
            SELECT COUNT(*) as count
            FROM information_schema.columns 
            WHERE table_name = 'fullbay_line_items' 
            AND table_schema = 'public'
        """)
        
        column_count = cursor.fetchone()['count']
        print(f"\n📊 Total columns: {column_count}")
        
        # Check for other hour-related columns
        print("\n🔍 HOUR-RELATED COLUMNS:")
        print("-" * 40)
        
        cursor.execute("""
            SELECT column_name, ordinal_position, data_type
            FROM information_schema.columns 
            WHERE table_name = 'fullbay_line_items' 
            AND table_schema = 'public'
            AND column_name LIKE '%hour%'
            ORDER BY ordinal_position
        """)
        
        hour_columns = cursor.fetchall()
        for col in hour_columns:
            print(f"   {col['ordinal_position']:2d}. {col['column_name']:<20} ({col['data_type']})")
        
        print()
        print("🎯 RENAME VERIFICATION SUMMARY:")
        print("-" * 40)
        if not actual_hours_result and so_hours_result:
            print("   ✅ SUCCESS: Column rename completed successfully")
            print("   ✅ actual_hours → so_hours")
            print("   ✅ Database schema updated")
            print("   ✅ Code updates needed to complete the change")
        else:
            print("   ❌ FAILED: Column rename not completed")
            if actual_hours_result:
                print("      - actual_hours still exists")
            if not so_hours_result:
                print("      - so_hours not found")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        if 'conn' in locals():
            conn.rollback()
        raise

if __name__ == "__main__":
    verify_so_hours_rename()
