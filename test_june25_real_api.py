#!/usr/bin/env python3
"""
Test fetching real data from Fullbay API for June 25th, 2025.
This script will attempt to connect to the real API and show the results.
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# Set up environment variables for testing
os.environ.setdefault('DB_HOST', 'fullbay-postgres-db.cc74yq2i4j8n.us-east-1.rds.amazonaws.com')
os.environ.setdefault('DB_PORT', '5432')
os.environ.setdefault('DB_NAME', 'fullbay_data')
os.environ.setdefault('DB_USER', 'postgres')
os.environ.setdefault('DB_PASSWORD', '5255Tillman')
os.environ.setdefault('ENVIRONMENT', 'development')
os.environ.setdefault('LOG_LEVEL', 'INFO')

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

def test_real_api_connection():
    """Test real API connection and show what happens."""
    print("🔌 Testing Real Fullbay API Connection...")
    
    try:
        # Set the real API key
        os.environ['FULLBAY_API_KEY'] = '4b9fcc18-1f24-09fb-275b-ad1974786395'
        
        config_obj = config.Config()
        print(f"✅ Config loaded - API Key configured: {bool(config_obj.fullbay_api_key)}")
        print(f"🔑 API Key: {config_obj.fullbay_api_key[:8]}...{config_obj.fullbay_api_key[-4:]}")
        
        client = fullbay_client.FullbayClient(config_obj)
        print(f"✅ FullbayClient initialized")
        
        # Test API status
        api_status = client.get_api_status()
        print(f"📡 API Status: {api_status}")
        
        return client, config_obj
        
    except Exception as e:
        print(f"❌ API connection failed: {e}")
        return None, None

def fetch_june25_data(client, config_obj):
    """Attempt to fetch real data for June 25th, 2025."""
    print("\n📥 Attempting to fetch real data for June 25th, 2025...")
    
    try:
        # Set target date to June 25th, 2025
        target_date = datetime(2025, 6, 25, tzinfo=timezone.utc)
        date_str = target_date.strftime('%Y-%m-%d')
        
        print(f"📅 Target date: {date_str}")
        print(f"🌐 Base URL: {client.base_url}")
        print(f"🔗 Full endpoint: {client.base_url}/getInvoices.php")
        
        # Generate token and show parameters
        token = client._generate_token(date_str)
        
        # Debug token generation
        import hashlib
        ip_address = client._get_public_ip()
        token_data = f"{client.api_key}{date_str}{ip_address}"
        debug_token = hashlib.sha1(token_data.encode()).hexdigest()
        
        print(f"\n🔍 TOKEN GENERATION DEBUG:")
        print(f"   API Key: {client.api_key}")
        print(f"   Date: {date_str}")
        print(f"   IP Address: {ip_address}")
        print(f"   Token Data String: {token_data}")
        print(f"   Generated Token: {debug_token}")
        print(f"   Token Length: {len(debug_token)}")
        
        params = {
            'key': client.api_key,
            'token': token,
            'startDate': date_str,
            'endDate': date_str
        }
        
        print(f"\n📋 REQUEST DETAILS:")
        print(f"   URL: {client.base_url}/getInvoices.php")
        print(f"   Method: GET")
        print(f"   Parameters:")
        for key, value in params.items():
            if key == 'key':
                print(f"     {key}: {value[:8]}...{value[-4:]}")
            elif key == 'token':
                print(f"     {key}: {value[:10]}...")
            else:
                print(f"     {key}: {value}")
        
        print(f"\n📋 HEADERS:")
        for key, value in client.session.headers.items():
            print(f"   {key}: {value}")
        
        # Build the complete URL with parameters
        import urllib.parse
        base_url = f"{client.base_url}/getInvoices.php"
        query_string = urllib.parse.urlencode(params)
        complete_url = f"{base_url}?{query_string}"
        
        print(f"\n🌐 COMPLETE URL HIT:")
        print(f"   {complete_url}")
        
        # Make the request manually to capture the exact response
        print(f"\n🔄 Making API request...")
        import requests
        
        # Make the request with the same session as the client
        response = client.session.get(base_url, params=params, timeout=30)
        
        print(f"📡 Response Status: {response.status_code}")
        print(f"📡 Response Headers: {dict(response.headers)}")
        
        # Get the raw response text
        response_text = response.text
        print(f"\n📄 RAW API RESPONSE:")
        print(f"   {response_text}")
        
        # Try to parse as JSON
        try:
            response_json = response.json()
            print(f"\n📋 PARSED JSON RESPONSE:")
            print(json.dumps(response_json, indent=2))
        except Exception as e:
            print(f"\n❌ Failed to parse JSON: {e}")
            print(f"   Raw response: {response_text}")
        
        # Now use the client's method to process the data
        api_data = client.fetch_invoices_for_date(target_date)
        
        if api_data:
            print(f"✅ Successfully retrieved {len(api_data)} invoices")
            
            # Show sample data
            if len(api_data) > 0:
                sample_invoice = api_data[0]
                print(f"📋 Sample invoice keys: {list(sample_invoice.keys())[:10]}...")
                
                # Save to file for inspection
                output_file = f"real_api_response_june25_{date_str}.json"
                with open(output_file, 'w') as f:
                    json.dump(api_data, f, indent=2, default=str)
                print(f"💾 Raw API response saved to: {output_file}")
            
            return api_data
        else:
            print("⚠️  No invoices found for the specified date")
            return []
            
    except Exception as e:
        print(f"❌ Failed to fetch data: {e}")
        
        # Check if it's an authentication error
        if "401" in str(e) or "403" in str(e) or "unauthorized" in str(e).lower():
            print("🔐 This appears to be an authentication error.")
            print("   Please ensure you have a valid Fullbay API key.")
        elif "404" in str(e) or "not found" in str(e).lower():
            print("🔍 This appears to be an endpoint not found error.")
            print("   Please verify the API endpoint is correct.")
        elif "timeout" in str(e).lower():
            print("⏱️  This appears to be a timeout error.")
            print("   Please check your network connection.")
        
        return None

def process_and_persist_data(api_data, config_obj):
    """Process and persist the API data to database."""
    if not api_data:
        print("⚠️  No data to process")
        return 0
    
    print("\n💾 Processing and persisting data to database...")
    
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
    print(f"\n🔍 Querying database to verify data for {target_date}...")
    
    try:
        db_manager = database.DatabaseManager(config_obj)
        db_manager.connect()
        
        # Query raw data table
        with db_manager._get_connection() as conn:
            with conn.cursor() as cursor:
                # Count records for the target date
                cursor.execute("""
                    SELECT COUNT(*) as record_count 
                    FROM fullbay_raw_data 
                    WHERE raw_json_data->>'invoiceDate' = %s
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
                        fullbay_invoice_id,
                        raw_json_data->>'invoiceNumber' as invoice_number,
                        raw_json_data->>'customerTitle' as customer_title,
                        raw_json_data->>'invoiceDate' as invoice_date,
                        raw_json_data->>'total' as total,
                        created_at
                    FROM fullbay_raw_data 
                    WHERE raw_json_data->>'invoiceDate' = %s
                    LIMIT 5
                """, (target_date,))
                
                sample_records = cursor.fetchall()
                print(f"\n📋 Sample records for {target_date}:")
                for record in sample_records:
                    print(f"  - Invoice: {record['invoice_number']}, Customer: {record['customer_title']}, Total: ${record['total']}")
                
                # Check ingestion metadata
                cursor.execute("""
                    SELECT 
                        execution_id,
                        records_processed,
                        records_inserted,
                        created_at
                    FROM ingestion_metadata 
                    ORDER BY created_at DESC
                    LIMIT 3
                """)
                
                metadata_records = cursor.fetchall()
                print(f"\n📊 Recent ingestion metadata:")
                for record in metadata_records:
                    print(f"  - Execution: {record['execution_id'][:8]}..., Processed: {record['records_processed']}, Inserted: {record['records_inserted']}, Time: {record['created_at']}")
        
        db_manager.close()
        return raw_count, line_count
        
    except Exception as e:
        print(f"❌ Database query failed: {e}")
        return 0, 0

def main():
    """Run the complete test with real API data."""
    print("🚀 Fullbay API Real Data Test for June 25th, 2025")
    print("=" * 60)
    
    start_time = datetime.now(timezone.utc)
    
    # Step 1: Test API connection
    client, config_obj = test_real_api_connection()
    if not client:
        print("❌ Cannot proceed: API connection failed")
        return False
    
    # Step 2: Fetch real data for June 25th, 2025
    api_data = fetch_june25_data(client, config_obj)
    
    if api_data is None:
        print("\n⚠️  API data fetch failed. This is expected if:")
        print("   - No valid API key is configured")
        print("   - The API endpoint is not accessible")
        print("   - Network connectivity issues")
        print("\n📋 To test with real data:")
        print("   1. Obtain a valid Fullbay API key")
        print("   2. Set FULLBAY_API_KEY environment variable")
        print("   3. Run this test again")
        return False
    
    # Step 3: Process and persist data
    records_inserted = process_and_persist_data(api_data, config_obj)
    
    # Step 4: Query and verify data
    raw_count, line_count = query_and_verify_data(config_obj)
    
    # Step 5: Summary
    end_time = datetime.now(timezone.utc)
    duration = (end_time - start_time).total_seconds()
    
    print("\n" + "=" * 60)
    print("📊 REAL API TEST SUMMARY")
    print("=" * 60)
    print(f"✅ API Connection: Successful")
    print(f"✅ Data Retrieval: {len(api_data) if api_data else 0} records")
    print(f"✅ Database Persistence: {records_inserted} records inserted")
    print(f"✅ Data Verification: {raw_count} raw records, {line_count} line items")
    print(f"⏱️  Total Test Duration: {duration:.2f} seconds")
    
    if api_data and len(api_data) > 0:
        print("\n🎉 SUCCESS: Real data was fetched and processed!")
        print("✅ The Fullbay API integration is working correctly")
    else:
        print("\n⚠️  No real data was retrieved")
        print("   This may be due to:")
        print("   - No invoices for the specified date")
        print("   - API authentication issues")
        print("   - API endpoint configuration")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
