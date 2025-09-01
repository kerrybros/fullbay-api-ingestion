#!/usr/bin/env python3
"""
End-to-End Test: June 30th, 2025
Test the complete pipeline with real Fullbay API data for June 30th.
"""

import os
import sys
import json
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

def main():
    """Main function to test June 30th data processing."""
    print("🚀 End-to-End Test: June 30th, 2025")
    print("=" * 50)
    print(f"📅 Fetching data for 2025-06-30...")
    
    try:
        # Initialize components
        config = Config()
        client = FullbayClient(config)
        db_manager = DatabaseManager(config)
        
        # Test API connection
        print("🔌 Testing Fullbay API Connection...")
        status = client.get_api_status()
        print(f"✅ API Status: {status}")
        
        # Fetch data for June 30th
        target_date = "2025-06-30"
        print(f"📥 Fetching invoices for {target_date}...")
        
        invoices = client.fetch_invoices_for_date(target_date)
        print(f"✅ Successfully retrieved {len(invoices)} invoices")
        
        if invoices:
            print(f"📋 Sample invoice keys: {list(invoices[0].keys())}...")
            
            # Save raw response for debugging
            filename = f"real_api_response_june30_{target_date.replace('-', '')}.json"
            with open(filename, 'w') as f:
                json.dump(invoices, f, indent=2)
            print(f"💾 Raw API response saved to: {filename}")
            
            # Process and persist data
            print(f"💾 Processing and persisting data...")
            db_manager.connect()
            
            # Process the invoices
            line_items_created = db_manager.insert_records(invoices)
            
            print(f"✅ Successfully processed {len(invoices)} invoices, created {line_items_created} line items")
            
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
                    
                    # Get sample line items
                    cursor.execute("""
                        SELECT 
                            line_item_type,
                            part_description,
                            labor_description,
                            line_total_price,
                            quantity
                        FROM fullbay_line_items 
                        WHERE DATE(created_at) = CURRENT_DATE
                        ORDER BY id
                        LIMIT 5
                    """)
                    samples = cursor.fetchall()
            
            print(f"📊 Raw data records for today: {raw_count}")
            print(f"📊 Line items for today: {line_count}")
            
            if samples:
                print(f"📋 Sample line items:")
                for i, sample in enumerate(samples, 1):
                    item_type = sample['line_item_type']
                    description = sample['part_description'] or sample['labor_description'] or 'No description'
                    price = sample['line_total_price']
                    qty = sample['quantity']
                    qty_str = f"Qty: {qty}" if qty else "Qty: None"
                    print(f"  {i}. {item_type}: {description[:50]}... (${price:.2f}, {qty_str})")
            
            db_manager.close()
            
        else:
            print("⚠️  No invoices found for this date")
            
    except Exception as e:
        print(f"❌ Error during test: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
