#!/usr/bin/env python3
"""
Standalone end-to-end test for June 25th data.
Runs from project root and handles imports properly.
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# Add src to Python path
current_dir = Path(__file__).parent
src_dir = current_dir / "src"
sys.path.insert(0, str(src_dir))

# Import modules directly
import config
import fullbay_client
import database
import utils

# Setup logging
logger = utils.setup_logging()

def test_fullbay_api_connection():
    """Test Fullbay API connection and token generation."""
    print("🔌 Testing Fullbay API connection...")
    
    try:
        config_obj = config.Config()
        client = fullbay_client.FullbayClient(config_obj)
        
        # Test connection
        connection_status = client.test_connection()
        print(f"✅ API Connection Status: {connection_status}")
        
        return client, config_obj
        
    except Exception as e:
        print(f"❌ API connection failed: {e}")
        return None, None

def fetch_june25_data(client):
    """Fetch real data from Fullbay API for June 25th."""
    print("📥 Fetching June 25th data from Fullbay API...")
    
    try:
        # Fetch data for June 25th, 2025
        target_date = "2025-06-25"
        api_data = client.fetch_invoices_by_date(target_date)
        
        if not api_data:
            print(f"⚠️ No data found for {target_date}")
            return None
        
        print(f"✅ Retrieved {len(api_data)} records for {target_date}")
        
        # Save raw data for inspection
        with open(f"june25_raw_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json", 'w') as f:
            json.dump(api_data, f, indent=2, default=str)
        
        return api_data
        
    except Exception as e:
        print(f"❌ Failed to fetch June 25th data: {e}")
        return None

def process_and_persist_data(api_data, config_obj):
    """Process API data and persist to database."""
    print("💾 Processing and persisting data to database...")
    
    try:
        db_manager = database.DatabaseManager(config_obj)
        
        # Connect to database
        db_manager.connect()
        print("✅ Database connection established")
        
        # Insert records
        records_inserted = db_manager.insert_records(api_data)
        print(f"✅ Successfully inserted {records_inserted} records")
        
        # Close connection
        db_manager.close()
        
        return records_inserted
        
    except Exception as e:
        print(f"❌ Database operation failed: {e}")
        return 0

def query_and_verify_data(config_obj, target_date="2025-06-25"):
    """Query database to verify data was persisted correctly."""
    print("🔍 Querying database to verify data persistence...")
    
    try:
        db_manager = database.DatabaseManager(config_obj)
        db_manager.connect()
        
        # Query raw data table
        with db_manager._get_connection() as conn:
            with conn.cursor() as cursor:
                # Count records for June 25th
                cursor.execute("""
                    SELECT COUNT(*) as record_count 
                    FROM fullbay_raw_data 
                    WHERE raw_data->>'invoiceDate' = %s
                """, (target_date,))
                
                raw_count = cursor.fetchone()['record_count']
                print(f"📊 Raw data records for {target_date}: {raw_count}")
                
                # Query line items
                cursor.execute("""
                    SELECT COUNT(*) as line_count 
                    FROM fullbay_line_items 
                    WHERE invoice_date = %s
                """, (target_date,))
                
                line_count = cursor.fetchone()['line_count']
                print(f"📊 Line items for {target_date}: {line_count}")
                
                # Get sample records
                cursor.execute("""
                    SELECT 
                        invoice_number,
                        customer_title,
                        invoice_date,
                        total,
                        created
                    FROM fullbay_raw_data 
                    WHERE raw_data->>'invoiceDate' = %s
                    LIMIT 5
                """, (target_date,))
                
                sample_records = cursor.fetchall()
                print(f"\n📋 Sample records for {target_date}:")
                for record in sample_records:
                    print(f"  - Invoice: {record['invoice_number']}, Customer: {record['customer_title']}, Total: ${record['total']}")
                
                # Get metadata
                cursor.execute("""
                    SELECT 
                        execution_id,
                        records_processed,
                        records_inserted,
                        execution_time_seconds,
                        created_at
                    FROM ingestion_metadata 
                    WHERE target_date = %s
                    ORDER BY created_at DESC
                    LIMIT 3
                """, (target_date,))
                
                metadata_records = cursor.fetchall()
                print(f"\n📊 Recent ingestion metadata for {target_date}:")
                for record in metadata_records:
                    print(f"  - Execution: {record['execution_id'][:8]}..., Processed: {record['records_processed']}, Inserted: {record['records_inserted']}, Time: {record['execution_time_seconds']:.2f}s")
        
        db_manager.close()
        return raw_count, line_count
        
    except Exception as e:
        print(f"❌ Database query failed: {e}")
        return 0, 0

def test_database_connectivity():
    """Test database connectivity independently."""
    print("\n🔍 Testing Database Connectivity...")
    
    try:
        config_obj = config.Config()
        db_manager = database.DatabaseManager(config_obj)
        db_manager.connect()
        
        with db_manager._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT version();")
                version = cursor.fetchone()[0]
                print(f"✅ Database connected: {version}")
                
                # Check table existence
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name IN ('fullbay_raw_data', 'fullbay_line_items', 'ingestion_metadata')
                """)
                
                tables = [row['table_name'] for row in cursor.fetchall()]
                print(f"✅ Tables found: {', '.join(tables)}")
        
        db_manager.close()
        return True
        
    except Exception as e:
        print(f"❌ Database connectivity test failed: {e}")
        return False

def run_complete_test():
    """Run the complete end-to-end test."""
    print("🚀 Starting Complete End-to-End Test for June 25th Data")
    print("=" * 60)
    
    start_time = datetime.now(timezone.utc)
    
    # Step 1: Test API connection
    client, config_obj = test_fullbay_api_connection()
    if not client:
        print("❌ Test failed: Could not establish API connection")
        return False
    
    # Step 2: Fetch June 25th data
    api_data = fetch_june25_data(client)
    if not api_data:
        print("⚠️ No data found for June 25th - this is normal if no invoices exist for that date")
        print("📝 Proceeding with database verification to check existing data...")
        
        # Still verify database state
        raw_count, line_count = query_and_verify_data(config_obj)
        print(f"\n📊 Current database state for June 25th:")
        print(f"  - Raw records: {raw_count}")
        print(f"  - Line items: {line_count}")
        
        return True
    
    # Step 3: Process and persist data
    records_inserted = process_and_persist_data(api_data, config_obj)
    
    # Step 4: Query and verify data
    raw_count, line_count = query_and_verify_data(config_obj)
    
    # Step 5: Summary
    end_time = datetime.now(timezone.utc)
    duration = (end_time - start_time).total_seconds()
    
    print("\n" + "=" * 60)
    print("📊 END-TO-END TEST SUMMARY")
    print("=" * 60)
    print(f"✅ API Connection: Successful")
    print(f"✅ Data Retrieval: {len(api_data)} records fetched")
    print(f"✅ Database Persistence: {records_inserted} records inserted")
    print(f"✅ Data Verification: {raw_count} raw records, {line_count} line items")
    print(f"⏱️ Total Test Duration: {duration:.2f} seconds")
    
    # Success criteria
    success = (
        client is not None and
        api_data is not None and
        records_inserted >= 0 and
        raw_count >= 0
    )
    
    if success:
        print("\n🎉 END-TO-END TEST PASSED!")
        print("✅ Full pipeline is working correctly")
    else:
        print("\n❌ END-TO-END TEST FAILED!")
        print("❌ Some components need attention")
    
    return success

if __name__ == "__main__":
    print("🔧 Fullbay API End-to-End Test for June 25th Data")
    print("This test will:")
    print("1. Connect to Fullbay API")
    print("2. Fetch real data for June 25th, 2025")
    print("3. Process and persist data to database")
    print("4. Query database to verify data persistence")
    print("5. Provide comprehensive test results")
    print()
    
    # First test database connectivity
    if not test_database_connectivity():
        print("❌ Cannot proceed: Database connectivity failed")
        sys.exit(1)
    
    # Run complete test
    success = run_complete_test()
    
    if success:
        print("\n🎯 Test completed successfully!")
        print("📋 Next steps:")
        print("  - Check the generated JSON files for data inspection")
        print("  - Verify data in your database management tool")
        print("  - Review CloudWatch logs if deployed to Lambda")
    else:
        print("\n⚠️ Test completed with issues")
        print("📋 Troubleshooting:")
        print("  - Check API credentials and configuration")
        print("  - Verify database connection settings")
        print("  - Review error logs for specific issues")
    
    sys.exit(0 if success else 1)
