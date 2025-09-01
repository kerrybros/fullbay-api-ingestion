#!/usr/bin/env python3
"""
Wipe Database Tables
Safely clear all data from the Fullbay ingestion tables for fresh processing.
"""

import os
import sys
import psycopg2
from datetime import datetime

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

# Set default environment variables for local testing
os.environ.setdefault('DB_HOST', 'fullbay-postgres-db.cc74yq2i4j8n.us-east-1.rds.amazonaws.com')
os.environ.setdefault('DB_USER', 'postgres')
os.environ.setdefault('DB_PASSWORD', '5255Tillman')
os.environ.setdefault('DB_NAME', 'fullbay_data')
os.environ.setdefault('DB_PORT', '5432')

def wipe_database():
    """Safely wipe all data from Fullbay ingestion tables."""
    print("🗑️  WIPING DATABASE TABLES")
    print("=" * 60)
    print(f"🕐 Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Connect to database
        conn = psycopg2.connect(
            host=os.environ['DB_HOST'],
            user=os.environ['DB_USER'],
            password=os.environ['DB_PASSWORD'],
            database=os.environ['DB_NAME'],
            port=os.environ['DB_PORT']
        )
        
        with conn.cursor() as cursor:
            # Get current record counts
            print("📊 CURRENT DATA COUNTS:")
            cursor.execute("SELECT COUNT(*) FROM fullbay_line_items")
            line_items_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM fullbay_raw_data")
            raw_data_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM ingestion_metadata")
            metadata_count = cursor.fetchone()[0]
            
            print(f"  📋 Line items: {line_items_count:,}")
            print(f"  📄 Raw data records: {raw_data_count:,}")
            print(f"  📊 Metadata records: {metadata_count:,}")
            
            if line_items_count == 0 and raw_data_count == 0:
                print("\n✅ Database is already empty - no action needed!")
                return
            
            # Confirm deletion
            print(f"\n⚠️  WARNING: This will delete ALL data from:")
            print(f"  • fullbay_line_items ({line_items_count:,} records)")
            print(f"  • fullbay_raw_data ({raw_data_count:,} records)")
            print(f"  • ingestion_metadata ({metadata_count:,} records)")
            
            response = input("\n❓ Are you sure you want to continue? (yes/no): ").lower().strip()
            
            if response not in ['yes', 'y']:
                print("❌ Database wipe cancelled.")
                return
            
            print("\n🗑️  DELETING DATA...")
            
            # Delete in correct order (respecting foreign key constraints)
            # 1. Delete line items first (they reference raw_data)
            if line_items_count > 0:
                cursor.execute("DELETE FROM fullbay_line_items")
                print(f"  ✅ Deleted {line_items_count:,} line items")
            
            # 2. Delete raw data
            if raw_data_count > 0:
                cursor.execute("DELETE FROM fullbay_raw_data")
                print(f"  ✅ Deleted {raw_data_count:,} raw data records")
            
            # 3. Delete metadata
            if metadata_count > 0:
                cursor.execute("DELETE FROM ingestion_metadata")
                print(f"  ✅ Deleted {metadata_count:,} metadata records")
            
            # Commit the changes
            conn.commit()
            
            # Verify deletion
            print("\n🔍 VERIFYING DELETION:")
            cursor.execute("SELECT COUNT(*) FROM fullbay_line_items")
            remaining_line_items = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM fullbay_raw_data")
            remaining_raw_data = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM ingestion_metadata")
            remaining_metadata = cursor.fetchone()[0]
            
            print(f"  📋 Remaining line items: {remaining_line_items}")
            print(f"  📄 Remaining raw data: {remaining_raw_data}")
            print(f"  📊 Remaining metadata: {remaining_metadata}")
            
            if remaining_line_items == 0 and remaining_raw_data == 0 and remaining_metadata == 0:
                print("\n✅ DATABASE SUCCESSFULLY WIPED!")
                print("  🚀 Ready for fresh data processing with new validation logic")
                print("  📈 Negative values and 0.0 labor hours will now be preserved")
            else:
                print("\n⚠️  WARNING: Some data may still remain in the database")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error wiping database: {e}")
        import traceback
        traceback.print_exc()

def reset_sequences():
    """Reset auto-increment sequences after data deletion."""
    print("\n🔄 RESETTING AUTO-INCREMENT SEQUENCES...")
    
    try:
        conn = psycopg2.connect(
            host=os.environ['DB_HOST'],
            user=os.environ['DB_USER'],
            password=os.environ['DB_PASSWORD'],
            database=os.environ['DB_NAME'],
            port=os.environ['DB_PORT']
        )
        
        with conn.cursor() as cursor:
            # Reset sequences for all tables
            sequences = [
                'fullbay_line_items_id_seq',
                'fullbay_raw_data_id_seq', 
                'ingestion_metadata_id_seq'
            ]
            
            for seq in sequences:
                try:
                    cursor.execute(f"ALTER SEQUENCE {seq} RESTART WITH 1")
                    print(f"  ✅ Reset sequence: {seq}")
                except Exception as e:
                    print(f"  ⚠️  Could not reset {seq}: {e}")
            
            conn.commit()
            print("✅ All sequences reset successfully!")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error resetting sequences: {e}")

if __name__ == "__main__":
    print("🗑️  FULLBAY DATABASE WIPE UTILITY")
    print("=" * 80)
    
    # Wipe the data
    wipe_database()
    
    # Reset sequences
    reset_sequences()
    
    print(f"\n🎉 Database wipe completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("\n📋 NEXT STEPS:")
    print("  1. Run process_may_2025.py to reprocess data with new validation logic")
    print("  2. Negative prices and quantities will be preserved (returns/credits)")
    print("  3. 0.0 labor hours will be preserved (no labor time recorded)")
    print("  4. Data quality metrics will reflect the new business logic")
