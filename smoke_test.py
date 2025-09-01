#!/usr/bin/env python3
"""
Smoke Test Script for Fullbay API Ingestion System
This script verifies database connectivity and basic functionality
"""

import os
import sys
import psycopg2
import psycopg2.extras
import json
from datetime import datetime, timezone

def get_db_connection():
    """Get database connection using environment variables"""
    return psycopg2.connect(
        host=os.environ.get('DB_HOST', 'fullbay-postgres-db.cc74yq2i4j8n.us-east-1.rds.amazonaws.com'),
        port=int(os.environ.get('DB_PORT', '5432')),
        dbname=os.environ.get('DB_NAME', 'fullbay_data'),
        user=os.environ.get('DB_USER', 'postgres'),
        password=os.environ.get('DB_PASSWORD', '5255Tillman')
    )

def test_connection():
    """Test basic database connectivity"""
    print("🔌 Testing database connection...")
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("SELECT version();")
            version = cursor.fetchone()[0]
            print(f"✅ Database connection successful")
            print(f"📊 PostgreSQL version: {version.split(',')[0]}")
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False

def test_tables_exist():
    """Test that all required tables exist"""
    print("\n📋 Testing table existence...")
    
    required_tables = [
        'fullbay_raw_data',
        'fullbay_line_items', 
        'ingestion_metadata',
        'daily_summary',
        'monthly_summary',
        'customer_summary',
        'vehicle_summary',
        'api_request_log',
        'database_query_log',
        'data_processing_log',
        'performance_metrics_log',
        'error_tracking_log',
        'data_quality_log',
        'business_validation_log',
        'processed_invoices_tracker'
    ]
    
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            for table in required_tables:
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = %s
                    );
                """, (table,))
                exists = cursor.fetchone()[0]
                if exists:
                    print(f"✅ Table exists: {table}")
                else:
                    print(f"❌ Table missing: {table}")
                    return False
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Error checking tables: {e}")
        return False

def test_functions_exist():
    """Test that utility functions exist"""
    print("\n⚙️  Testing function existence...")
    
    required_functions = [
        'update_updated_at_column',
        'refresh_daily_summary',
        'refresh_monthly_summary',
        'refresh_customer_summary',
        'refresh_vehicle_summary',
        'log_api_request',
        'log_database_query',
        'log_data_processing',
        'log_performance_metrics',
        'log_error',
        'log_data_quality_check',
        'log_business_validation',
        'get_execution_summary',
        'get_recent_executions_with_errors',
        'get_performance_trends',
        'check_invoice_already_processed',
        'mark_invoice_processed'
    ]
    
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            for func in required_functions:
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.routines 
                        WHERE routine_schema = 'public' 
                        AND routine_name = %s
                    );
                """, (func,))
                exists = cursor.fetchone()[0]
                if exists:
                    print(f"✅ Function exists: {func}")
                else:
                    print(f"❌ Function missing: {func}")
                    return False
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Error checking functions: {e}")
        return False

def test_logging_functions():
    """Test logging functions with sample data"""
    print("\n📝 Testing logging functions...")
    
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # Create a test execution record
            execution_id = f"smoke_test_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
            
            # Insert test metadata
            cursor.execute("""
                INSERT INTO ingestion_metadata (
                    execution_id, start_time, status, environment, 
                    records_processed, records_inserted
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """, (execution_id, datetime.now(timezone.utc), 'SUCCESS', 'development', 0, 0))
            
            # Test API request logging
            cursor.execute("""
                SELECT log_api_request(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (execution_id, '/test/endpoint', 'GET', None, None, None, 200, None, 1024, 150, None, None, 0))
            
            # Test database query logging
            cursor.execute("""
                SELECT log_database_query(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (execution_id, 'SELECT', 'test_table', 'SELECT * FROM test_table', None, 25, 1, 50, None, None))
            
            # Test error logging
            cursor.execute("""
                SELECT log_error(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (execution_id, 'INFO', 'TEST', 'Smoke test error message', 'TestError', 'TEST001', None, None, None, None))
            
            # Test data quality logging
            cursor.execute("""
                SELECT log_data_quality_check(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (execution_id, 'COMPLETENESS', 'Test completeness check', 100, 95, 5, 95.0, None, 0, 5, 'Test recommendations'))
            
            # Test business validation logging
            cursor.execute("""
                SELECT log_business_validation(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (execution_id, 'TEST_RULE', 'Test validation rule', 'LOW', 50, 48, 2, 96.0, None, None, False, False, False))
            
            # Test duplicate check function
            cursor.execute("SELECT check_invoice_already_processed(%s)", ('test_invoice_123',))
            result = cursor.fetchone()
            print(f"✅ Duplicate check result: {result}")
            
            # Test mark invoice processed
            cursor.execute("SELECT mark_invoice_processed(%s, %s, %s)", ('test_invoice_123', 'PROCESSED', None))
            tracker_id = cursor.fetchone()[0]
            print(f"✅ Invoice tracker ID: {tracker_id}")
            
            # Test execution summary function
            cursor.execute("SELECT * FROM get_execution_summary(%s)", (execution_id,))
            summary = cursor.fetchone()
            print(f"✅ Execution summary: {summary}")
            
            conn.commit()
            print("✅ All logging functions tested successfully")
            
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Error testing logging functions: {e}")
        return False

def test_sample_data_insert():
    """Test inserting sample data"""
    print("\n📊 Testing sample data insertion...")
    
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # Insert sample raw data
            sample_json = {
                "invoice_id": "test_invoice_456",
                "shop": {"title": "Test Shop"},
                "customer": {"title": "Test Customer"},
                "line_items": []
            }
            
            cursor.execute("""
                INSERT INTO fullbay_raw_data (
                    fullbay_invoice_id, raw_json_data, processed
                ) VALUES (%s, %s, %s)
            """, ('test_invoice_456', json.dumps(sample_json), True))
            
            # Insert sample line item
            cursor.execute("""
                INSERT INTO fullbay_line_items (
                    fullbay_invoice_id, line_item_type, part_description, 
                    quantity, unit_price, line_total_price
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """, ('test_invoice_456', 'PART', 'Test Part', 1.0, 100.00, 100.00))
            
            # Test summary refresh functions
            cursor.execute("SELECT refresh_daily_summary(%s)", (datetime.now().date(),))
            cursor.execute("SELECT refresh_customer_summary()")
            
            conn.commit()
            print("✅ Sample data inserted successfully")
            
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Error inserting sample data: {e}")
        return False

def cleanup_test_data():
    """Clean up test data"""
    print("\n🧹 Cleaning up test data...")
    
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # Delete test data
            cursor.execute("DELETE FROM fullbay_line_items WHERE fullbay_invoice_id LIKE 'test_%'")
            cursor.execute("DELETE FROM fullbay_raw_data WHERE fullbay_invoice_id LIKE 'test_%'")
            cursor.execute("DELETE FROM processed_invoices_tracker WHERE fullbay_invoice_id LIKE 'test_%'")
            cursor.execute("DELETE FROM ingestion_metadata WHERE execution_id LIKE 'smoke_test_%'")
            
            conn.commit()
            print("✅ Test data cleaned up")
            
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Error cleaning up test data: {e}")
        return False

def main():
    """Main smoke test function"""
    print("🚀 Starting Fullbay API Smoke Tests")
    print("=" * 50)
    
    tests = [
        ("Database Connection", test_connection),
        ("Table Existence", test_tables_exist),
        ("Function Existence", test_functions_exist),
        ("Logging Functions", test_logging_functions),
        ("Sample Data Insertion", test_sample_data_insert),
        ("Cleanup", cleanup_test_data)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n🧪 Running: {test_name}")
        if test_func():
            passed += 1
        else:
            print(f"❌ {test_name} failed")
    
    print("\n" + "=" * 50)
    print(f"📊 Smoke Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All smoke tests passed! System is ready for use.")
        print("\n📋 Next steps:")
        print("1. Set up AWS CloudWatch logging")
        print("2. Configure Lambda environment variables")
        print("3. Test with real Fullbay API data")
        print("4. Implement duplicate invoice check logic")
    else:
        print("⚠️  Some tests failed. Please check the errors above.")
        sys.exit(1)

if __name__ == "__main__":
    main()
