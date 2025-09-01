#!/usr/bin/env python3
"""
End-to-end test for June 26th, 2025 - Fetch real data from Fullbay API and persist to database.
"""

import sys
import os
import json
from datetime import datetime

# Set default environment variables for local testing
os.environ.setdefault('DB_HOST', 'fullbay-postgres-db.cc74yq2i4j8n.us-east-1.rds.amazonaws.com')
os.environ.setdefault('DB_USER', 'postgres')
os.environ.setdefault('DB_PASSWORD', '5255Tillman')
os.environ.setdefault('DB_NAME', 'fullbay_data')
os.environ.setdefault('DB_PORT', '5432')
os.environ.setdefault('FULLBAY_API_KEY', '4b9fcc18-1f24-09fb-275b-ad1974786395')

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from config import Config
from fullbay_client import FullbayClient
from database import DatabaseManager

def main():
    print("🚀 End-to-End Test: June 26th, 2025")
    print("=" * 50)
    
    # Set target date
    target_date = "2025-06-26"
    date_str = target_date.replace('-', '')
    
    # Load config
    config = Config()
    
    # Initialize Fullbay client with your real API key
    client = FullbayClient(config)
    
    try:
        print(f"📅 Fetching data for {target_date}...")
        
        # Generate token and show parameters
        token = client._generate_token(target_date)
        
        # Debug token generation
        import hashlib
        ip_address = client._get_public_ip()
        token_data = f"{client.api_key}{target_date}{ip_address}"
        debug_token = hashlib.sha1(token_data.encode()).hexdigest()
        
        print(f"\n🔍 TOKEN GENERATION DEBUG:")
        print(f"   API Key: {client.api_key}")
        print(f"   Date: {target_date}")
        print(f"   IP Address: {ip_address}")
        print(f"   Token Data String: {token_data}")
        print(f"   Generated Token: {debug_token}")
        print(f"   Token Length: {len(debug_token)}")
        
        params = {
            'key': client.api_key,
            'token': token,
            'startDate': target_date,
            'endDate': target_date
        }
        
        base_url = "https://app.fullbay.com/services/getInvoices.php"
        
        print(f"\n🌐 REQUEST DETAILS:")
        print(f"   URL: {base_url}")
        print(f"   Method: GET")
        print(f"   Parameters: key={client.api_key[:8]}..., token={token[:8]}..., date={target_date}")
        print(f"   Headers: {dict(client.session.headers)}")
        
        # Build complete URL for debugging
        import urllib.parse
        complete_url = f"{base_url}?{urllib.parse.urlencode(params)}"
        
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
                output_file = f"real_api_response_june26_{date_str}.json"
                with open(output_file, 'w') as f:
                    json.dump(api_data, f, indent=2, default=str)
                print(f"💾 Raw API response saved to: {output_file}")
            
            # Process and persist data
            print(f"\n💾 Processing and persisting data...")
            db_manager = DatabaseManager(config)
            db_manager.connect()
            
            try:
                # Insert records into database
                line_items_created = db_manager.insert_records(api_data)
                print(f"✅ Successfully processed {len(api_data)} invoices, created {line_items_created} line items")
                
                # Query and verify data
                print(f"\n🔍 Querying and verifying data...")
                query_and_verify_data(db_manager, target_date)
                
            finally:
                db_manager.close()
        else:
            print("❌ No data retrieved from API")
            
    except Exception as e:
        print(f"❌ Error during test: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

def query_and_verify_data(db_manager, target_date):
    """Query and verify the data was persisted correctly."""
    try:
        with db_manager._get_connection() as conn:
            with conn.cursor() as cursor:
                # Check raw data
                cursor.execute("""
                    SELECT COUNT(*) as count 
                    FROM fullbay_raw_data 
                    WHERE raw_json_data->>'invoiceDate' = %s
                """, (target_date,))
                raw_count = cursor.fetchone()['count']
                print(f"📊 Raw data records for {target_date}: {raw_count}")
                
                # Check line items
                cursor.execute("""
                    SELECT COUNT(*) as count 
                    FROM fullbay_line_items 
                    WHERE invoice_date = %s
                """, (target_date,))
                line_count = cursor.fetchone()['count']
                print(f"📊 Line items for {target_date}: {line_count}")
                
                # Check recent ingestion metadata
                cursor.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = 'ingestion_metadata' 
                    AND column_name IN ('execution_time_seconds', 'target_date')
                """)
                available_columns = [row['column_name'] for row in cursor.fetchall()]
                
                if 'target_date' in available_columns:
                    cursor.execute("""
                        SELECT COUNT(*) as count 
                        FROM ingestion_metadata 
                        WHERE target_date = %s
                    """, (target_date,))
                else:
                    cursor.execute("""
                        SELECT COUNT(*) as count 
                        FROM ingestion_metadata 
                        WHERE start_time >= NOW() - INTERVAL '1 hour'
                    """)
                
                metadata_count = cursor.fetchone()['count']
                print(f"📊 Ingestion metadata records: {metadata_count}")
                
                # Show sample line items
                if line_count > 0:
                    cursor.execute("""
                        SELECT 
                            line_item_type,
                            part_description,
                            labor_description,
                            line_total_price,
                            quantity
                        FROM fullbay_line_items 
                        WHERE invoice_date = %s
                        LIMIT 5
                    """, (target_date,))
                    
                    sample_items = cursor.fetchall()
                    print(f"\n📋 Sample line items:")
                    for i, item in enumerate(sample_items, 1):
                        desc = item['part_description'] or item['labor_description'] or 'N/A'
                        print(f"  {i}. {item['line_item_type']}: {desc[:50]}... (${item['line_total_price']}, Qty: {item['quantity']})")
                
    except Exception as e:
        print(f"❌ Error querying data: {e}")

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
