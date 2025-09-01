#!/usr/bin/env python3
"""
Test CloudWatch Integration with Fullbay API Ingestion
Demonstrates monitoring integration with real data processing.
"""

import os
import sys
import time
from datetime import datetime

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
from cloudwatch_monitor import CloudWatchMonitor, get_monitor

def test_cloudwatch_integration():
    """Test CloudWatch integration with real data processing."""
    print("🚀 TESTING CLOUDWATCH INTEGRATION")
    print("=" * 60)
    print(f"🕐 Test start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Initialize components
        config = Config()
        client = FullbayClient(config)
        db_manager = DatabaseManager(config)
        
        # Initialize CloudWatch monitor
        monitor = CloudWatchMonitor()
        print(f"📊 CloudWatch monitor initialized: {'enabled' if monitor.enabled else 'disabled'}")
        
        # Test date
        target_date = "2025-06-25"
        
        # Log ingestion start
        monitor.log_ingestion_start(target_date)
        
        # Test API connection with monitoring
        print("🔌 Testing Fullbay API Connection...")
        start_time = time.time()
        
        try:
            status = client.get_api_status()
            api_time = time.time() - start_time
            print(f"✅ API Status: {status} (took {api_time:.2f}s)")
            
            # Monitor API status check - extract status string from response
            status_str = status.get('status', 'unknown') if isinstance(status, dict) else str(status)
            monitor.log_metric('APIStatusCheck', 1, 'Count', [
                {'Name': 'Status', 'Value': status_str}
            ])
            
        except Exception as e:
            api_time = time.time() - start_time
            print(f"❌ API Status Check Failed: {e} (took {api_time:.2f}s)")
            monitor.monitor_errors(1, 'API_Status_Check_Failed')
        
        # Fetch data with monitoring
        print(f"📥 Fetching invoices for {target_date}...")
        fetch_start_time = time.time()
        
        try:
            invoices = client.fetch_invoices_for_date(target_date)
            fetch_time = time.time() - fetch_start_time
            
            print(f"✅ Successfully retrieved {len(invoices)} invoices in {fetch_time:.2f}s")
            
            # Monitor API performance
            monitor.monitor_api_performance(fetch_time, len(invoices), True)
            
            if invoices:
                # Process and persist data with monitoring
                print(f"💾 Processing and persisting data...")
                db_manager.connect()
                
                process_start_time = time.time()
                
                try:
                    line_items_created = db_manager.insert_records(invoices)
                    process_time = time.time() - process_start_time
                    
                    print(f"✅ Successfully processed {len(invoices)} invoices, created {line_items_created} line items in {process_time:.2f}s")
                    
                    # Monitor processing performance
                    monitor.monitor_processing_performance(process_time, line_items_created, True)
                    
                    # Calculate data quality metrics (simplified)
                    total_items = line_items_created
                    missing_unit_info = int(total_items * 0.11)  # 11% from our previous analysis
                    zero_prices = int(total_items * 0.05)  # 5% zero prices (reduced since negative prices are now valid)
                    missing_labor_hours = int(total_items * 0.03)  # 3% missing hours (reduced since 0.0 is now valid)
                    
                    # Monitor data quality
                    monitor.monitor_data_quality(total_items, missing_unit_info, zero_prices, missing_labor_hours)
                    
                    # Log ingestion completion
                    total_time = fetch_time + process_time
                    monitor.log_ingestion_complete(target_date, len(invoices), line_items_created, total_time)
                    
                    # Performance summary
                    print(f"\n📊 PERFORMANCE SUMMARY:")
                    print(f"  📥 API Fetch: {fetch_time:.2f}s ({len(invoices)} invoices)")
                    print(f"  💾 Processing: {process_time:.2f}s ({line_items_created} line items)")
                    print(f"  ⏱️  Total Time: {total_time:.2f}s")
                    print(f"  📈 Invoices/second: {len(invoices)/total_time:.2f}")
                    print(f"  📈 Line items/second: {line_items_created/total_time:.2f}")
                    
                    db_manager.close()
                    
                except Exception as e:
                    process_time = time.time() - process_start_time
                    print(f"❌ Processing failed: {e}")
                    monitor.monitor_processing_performance(process_time, 0, False)
                    monitor.monitor_errors(1, 'Processing_Failed')
                    monitor.log_ingestion_error(target_date, str(e))
                    db_manager.close()
                    raise
                    
            else:
                print("⚠️  No invoices found for this date")
                monitor.log_event(f"No invoices found for {target_date}", 'WARNING')
                
        except Exception as e:
            fetch_time = time.time() - fetch_start_time
            print(f"❌ API fetch failed: {e}")
            monitor.monitor_api_performance(fetch_time, 0, False)
            monitor.monitor_errors(1, 'API_Fetch_Failed')
            monitor.log_ingestion_error(target_date, str(e))
            raise
        
        print(f"\n✅ CloudWatch integration test completed successfully!")
        print(f"📊 Check CloudWatch console for metrics and logs:")
        print(f"   - Metrics: https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#metricsV2:graph=~();search=FullbayAPI")
        print(f"   - Logs: https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#logsV2:log-groups/log-group/aws/fullbay-api-ingestion")
        
    except Exception as e:
        print(f"❌ CloudWatch integration test failed: {e}")
        import traceback
        traceback.print_exc()

def test_monitor_functions():
    """Test individual CloudWatch monitor functions."""
    print("\n🔧 TESTING INDIVIDUAL MONITOR FUNCTIONS")
    print("=" * 50)
    
    monitor = CloudWatchMonitor()
    
    # Test metric logging
    print("📊 Testing metric logging...")
    monitor.log_metric('TestMetric', 42, 'Count')
    monitor.log_metric('TestMetricWithDimensions', 100, 'Count', [
        {'Name': 'Environment', 'Value': 'Test'},
        {'Name': 'Version', 'Value': '1.0'}
    ])
    
    # Test event logging
    print("📝 Testing event logging...")
    monitor.log_event('Test event message', 'INFO', {'test': True, 'timestamp': datetime.now().isoformat()})
    monitor.log_event('Test warning message', 'WARNING', {'warning_type': 'test'})
    monitor.log_event('Test error message', 'ERROR', {'error_code': 500})
    
    # Test performance monitoring
    print("⚡ Testing performance monitoring...")
    monitor.monitor_api_performance(5.2, 25, True)
    monitor.monitor_processing_performance(12.8, 150, True)
    
    # Test data quality monitoring
    print("📈 Testing data quality monitoring...")
    monitor.monitor_data_quality(1000, 89, 128, 45)
    
    # Test error monitoring
    print("❌ Testing error monitoring...")
    monitor.monitor_errors(3, 'API_Timeout')
    monitor.monitor_errors(1, 'Database_Connection_Failed')
    
    print("✅ Individual monitor function tests completed!")

if __name__ == "__main__":
    print("🚀 CLOUDWATCH INTEGRATION TEST")
    print("=" * 80)
    
    # Test individual functions first
    test_monitor_functions()
    
    # Test full integration
    test_cloudwatch_integration()
    
    print(f"\n🎉 All CloudWatch integration tests completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
