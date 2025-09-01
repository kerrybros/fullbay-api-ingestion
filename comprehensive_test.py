#!/usr/bin/env python3
"""
Comprehensive Fullbay API Ingestion Test
Tests multiple dates and scenarios to validate the complete system.
"""

import os
import sys
import json
import time
from datetime import datetime, timedelta

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

# Set default environment variables for local testing
os.environ.setdefault('DB_HOST', 'fullbay-postgres-db.cc74yq2i4j8n.us-east-1.rds.amazonaws.com')
os.environ.setdefault('DB_USER', 'postgres')
os.environ.setdefault('DB_PASSWORD', '5255Tillman')
os.environ.setdefault('DB_NAME', 'fullbay_data')
os.environ.setdefault('DB_PORT', '5432')
os.environ.setdefault('FULLBAY_API_KEY', '4b9fcc18-1f24-09fb-275b-ad1974786395')

from config import Config
from fullbay_client import FullbayClient
from database import DatabaseManager

def test_date_processing(target_date, expected_min_invoices=0):
    """Test processing for a specific date."""
    print(f"\n📅 TESTING DATE: {target_date}")
    print("=" * 60)
    
    try:
        # Initialize components
        config = Config()
        client = FullbayClient(config)
        db_manager = DatabaseManager(config)
        
        # Test API connection
        print("🔌 Testing Fullbay API Connection...")
        status = client.get_api_status()
        print(f"✅ API Status: {status}")
        
        # Fetch data
        print(f"📥 Fetching invoices for {target_date}...")
        start_time = time.time()
        
        invoices = client.fetch_invoices_for_date(target_date)
        fetch_time = time.time() - start_time
        
        print(f"✅ Successfully retrieved {len(invoices)} invoices in {fetch_time:.2f}s")
        
        if len(invoices) < expected_min_invoices:
            print(f"⚠️  Warning: Only {len(invoices)} invoices found (expected at least {expected_min_invoices})")
            return False
        
        if invoices:
            # Save raw response for debugging
            filename = f"comprehensive_test_{target_date.replace('-', '')}.json"
            with open(filename, 'w') as f:
                json.dump(invoices, f, indent=2)
            print(f"💾 Raw API response saved to: {filename}")
            
            # Process and persist data
            print(f"💾 Processing and persisting data...")
            db_manager.connect()
            
            start_process_time = time.time()
            line_items_created = db_manager.insert_records(invoices)
            process_time = time.time() - start_process_time
            
            print(f"✅ Successfully processed {len(invoices)} invoices, created {line_items_created} line items in {process_time:.2f}s")
            
            # Query and verify data
            print(f"🔍 Querying and verifying data...")
            with db_manager._get_connection() as conn:
                with conn.cursor() as cursor:
                    # Check raw data
                    cursor.execute("""
                        SELECT COUNT(*) as count 
                        FROM fullbay_raw_data 
                        WHERE DATE(created_at) = CURRENT_DATE
                    """)
                    raw_count = cursor.fetchone()['count']
                    
                    # Check line items
                    cursor.execute("""
                        SELECT COUNT(*) as count 
                        FROM fullbay_line_items 
                        WHERE DATE(created_at) = CURRENT_DATE
                    """)
                    line_count = cursor.fetchone()['count']
                    
                    # Get line item type distribution
                    cursor.execute("""
                        SELECT 
                            line_item_type,
                            COUNT(*) as count,
                            SUM(line_total_price) as total_value
                        FROM fullbay_line_items 
                        WHERE DATE(created_at) = CURRENT_DATE
                        GROUP BY line_item_type
                        ORDER BY count DESC
                    """)
                    type_distribution = cursor.fetchall()
            
            print(f"📊 Raw data records for today: {raw_count}")
            print(f"📊 Line items for today: {line_count}")
            
            if type_distribution:
                print(f"📋 Line item type distribution:")
                for item in type_distribution:
                    print(f"  {item['line_item_type']}: {item['count']} items, ${item['total_value']:.2f}")
            
            db_manager.close()
            
            # Performance metrics
            total_time = fetch_time + process_time
            invoices_per_second = len(invoices) / total_time if total_time > 0 else 0
            line_items_per_second = line_items_created / total_time if total_time > 0 else 0
            
            print(f"⚡ Performance Metrics:")
            print(f"  📊 Total time: {total_time:.2f}s")
            print(f"  📊 Invoices/second: {invoices_per_second:.2f}")
            print(f"  📊 Line items/second: {line_items_per_second:.2f}")
            
            return True
            
        else:
            print("⚠️  No invoices found for this date")
            return False
            
    except Exception as e:
        print(f"❌ Error during test: {e}")
        import traceback
        traceback.print_exc()
        return False

def run_comprehensive_test():
    """Run comprehensive testing across multiple dates."""
    print("🚀 COMPREHENSIVE FULLBAY API INGESTION TEST")
    print("=" * 80)
    print(f"🕐 Test start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Test dates with expected minimum invoices
    test_dates = [
        ("2025-06-25", 1),    # Known to have 1 invoice
        ("2025-06-26", 20),   # Known to have 28 invoices
        ("2025-06-27", 100),  # Known to have 104 invoices
        ("2025-06-28", 0),    # Known to have 0 invoices
        ("2025-06-29", 0),    # Known to have 0 invoices
        ("2025-06-30", 100),  # Known to have 142 invoices
    ]
    
    results = []
    total_invoices = 0
    total_line_items = 0
    total_time = 0
    
    for target_date, expected_min in test_dates:
        start_time = time.time()
        success = test_date_processing(target_date, expected_min)
        test_time = time.time() - start_time
        
        results.append({
            'date': target_date,
            'success': success,
            'time': test_time,
            'expected_min': expected_min
        })
        
        if success:
            total_time += test_time
    
    # Final summary
    print(f"\n🎯 COMPREHENSIVE TEST SUMMARY")
    print("=" * 80)
    print(f"📊 Test Results:")
    
    successful_tests = 0
    for result in results:
        status = "✅ PASS" if result['success'] else "❌ FAIL"
        print(f"  {status} {result['date']}: {result['time']:.2f}s (expected min: {result['expected_min']})")
        if result['success']:
            successful_tests += 1
    
    print(f"\n📈 Overall Results:")
    print(f"  ✅ Successful tests: {successful_tests}/{len(results)}")
    print(f"  📊 Success rate: {(successful_tests/len(results)*100):.1f}%")
    print(f"  ⏱️  Total test time: {total_time:.2f}s")
    
    # Check final database state
    print(f"\n🔍 FINAL DATABASE STATE:")
    try:
        config = Config()
        db_manager = DatabaseManager(config)
        db_manager.connect()
        
        with db_manager._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) as count FROM fullbay_raw_data")
                raw_count = cursor.fetchone()['count']
                
                cursor.execute("SELECT COUNT(*) as count FROM fullbay_line_items")
                line_count = cursor.fetchone()['count']
                
                cursor.execute("SELECT SUM(line_total_price) as total_value FROM fullbay_line_items")
                total_value = cursor.fetchone()['total_value'] or 0
        
        print(f"  📄 Total raw data records: {raw_count}")
        print(f"  📋 Total line items: {line_count}")
        print(f"  💰 Total value: ${total_value:,.2f}")
        
        db_manager.close()
        
    except Exception as e:
        print(f"  ❌ Error checking final state: {e}")
    
    print(f"\n🎉 Comprehensive test completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    run_comprehensive_test()
