#!/usr/bin/env python3
"""
Wipe the database completely clean
- Removes all data from all tables
- Resets sequences
- Keeps table structure intact
- READY FOR FRESH DATA INGESTION
"""

import psycopg2
from psycopg2.extras import RealDictCursor

def wipe_database_completely():
    """Wipe all data from the database completely."""
    print("🗑️  DATABASE COMPLETE WIPE SCRIPT")
    print("=" * 50)
    print("⚠️  WARNING: This will DELETE ALL DATA from the database!")
    print("⚠️  This action cannot be undone!")
    print("⚠️  Only the table structure will remain.")
    print()
    
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
        
        print("📊 CURRENT DATABASE STATUS:")
        print("-" * 40)
        
        # Check current data counts
        cursor.execute("SELECT COUNT(*) as count FROM fullbay_line_items")
        line_items_count = cursor.fetchone()['count']
        
        cursor.execute("SELECT COUNT(*) as count FROM fullbay_raw_data")
        raw_data_count = cursor.fetchone()['count']
        
        cursor.execute("SELECT COUNT(*) as count FROM ingestion_metadata")
        metadata_count = cursor.fetchone()['count']
        
        print(f"   fullbay_line_items: {line_items_count:,} records")
        print(f"   fullbay_raw_data: {raw_data_count:,} records")
        print(f"   ingestion_metadata: {metadata_count:,} records")
        print()
        
        if line_items_count == 0 and raw_data_count == 0 and metadata_count == 0:
            print("✅ Database is already empty!")
            return
        
        print("🗑️  WIPE OPERATIONS TO PERFORM:")
        print("-" * 40)
        print("   1. Delete all records from fullbay_line_items")
        print("   2. Delete all records from fullbay_raw_data")
        print("   3. Delete all records from ingestion_metadata")
        print("   4. Reset all auto-increment sequences")
        print("   5. Verify database is completely empty")
        print()
        
        print("🔒 EXECUTION STATUS:")
        print("-" * 40)
        print("   ❌ NOT EXECUTED - Waiting for user confirmation")
        print("   📝 Script is ready to run when you say so")
        print()
        
        print("💡 TO EXECUTE THIS WIPE:")
        print("-" * 40)
        print("   1. Update your line type mapping code")
        print("   2. Tell me 'run the wipe script'")
        print("   3. I will execute this script")
        print("   4. Database will be completely clean")
        print("   5. You can then test with fresh data")
        print()
        
        print("📋 WIPE SQL COMMANDS (for reference):")
        print("-" * 40)
        wipe_commands = [
            "DELETE FROM fullbay_line_items;",
            "DELETE FROM fullbay_raw_data;",
            "DELETE FROM ingestion_metadata;",
            "ALTER SEQUENCE fullbay_line_items_id_seq RESTART WITH 1;",
            "ALTER SEQUENCE fullbay_raw_data_id_seq RESTART WITH 1;",
            "ALTER SEQUENCE ingestion_metadata_id_seq RESTART WITH 1;"
        ]
        
        for i, cmd in enumerate(wipe_commands, 1):
            print(f"   {i}. {cmd}")
        
        print()
        print("✅ WIPE SCRIPT READY - Waiting for your command to execute")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        raise

def execute_wipe():
    """Actually execute the database wipe."""
    print("🚀 EXECUTING DATABASE WIPE...")
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
        cursor = conn.cursor()
        
        print("🗑️  Step 1: Deleting all line items...")
        cursor.execute("DELETE FROM fullbay_line_items")
        line_items_deleted = cursor.rowcount
        print(f"   ✅ Deleted {line_items_deleted:,} line items")
        
        print("🗑️  Step 2: Deleting all raw data...")
        cursor.execute("DELETE FROM fullbay_raw_data")
        raw_data_deleted = cursor.rowcount
        print(f"   ✅ Deleted {raw_data_deleted:,} raw data records")
        
        print("🗑️  Step 3: Deleting all metadata...")
        cursor.execute("DELETE FROM ingestion_metadata")
        metadata_deleted = cursor.rowcount
        print(f"   ✅ Deleted {metadata_deleted:,} metadata records")
        
        print("🔄 Step 4: Resetting sequences...")
        cursor.execute("ALTER SEQUENCE fullbay_line_items_id_seq RESTART WITH 1")
        cursor.execute("ALTER SEQUENCE fullbay_raw_data_id_seq RESTART WITH 1")
        cursor.execute("ALTER SEQUENCE ingestion_metadata_id_seq RESTART WITH 1")
        print("   ✅ All sequences reset to 1")
        
        print("💾 Committing changes...")
        conn.commit()
        print("   ✅ Changes committed to database")
        
        print("🔍 Step 5: Verifying database is empty...")
        cursor.execute("SELECT COUNT(*) as count FROM fullbay_line_items")
        line_items_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) as count FROM fullbay_raw_data")
        raw_data_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) as count FROM ingestion_metadata")
        metadata_count = cursor.fetchone()[0]
        
        print(f"   📊 fullbay_line_items: {line_items_count:,} records")
        print(f"   📊 fullbay_raw_data: {raw_data_count:,} records")
        print(f"   📊 ingestion_metadata: {metadata_count:,} records")
        
        if line_items_count == 0 and raw_data_count == 0 and metadata_count == 0:
            print()
            print("🎉 DATABASE WIPE COMPLETE!")
            print("=" * 50)
            print("   ✅ All data has been removed")
            print("   ✅ All sequences have been reset")
            print("   ✅ Database is completely clean")
            print("   ✅ Ready for fresh data ingestion")
            print()
            print("💡 Next steps:")
            print("   1. Update your line type mapping code")
            print("   2. Test with a couple of days of data")
            print("   3. Verify the mapping is working correctly")
        else:
            print("❌ Database wipe incomplete - some data remains")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error during wipe execution: {e}")
        if 'conn' in locals():
            conn.rollback()
        raise

if __name__ == "__main__":
    # Show the wipe plan but don't execute
    wipe_database_completely()
    
    print()
    print("🔒 TO EXECUTE THE WIPE:")
    print("   Call: execute_wipe()")
    print("   Or tell me: 'run the wipe script'")
