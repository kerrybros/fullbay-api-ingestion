#!/usr/bin/env python3
"""
Debug script to test line item extraction and database insertion.
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

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from config import Config
from database import DatabaseManager

def main():
    print("🔍 Debugging Line Item Extraction and Database Insertion")
    print("=" * 60)
    
    # Load config
    config = Config()
    
    # Load the saved API response
    response_file = "real_api_response_june25_2025-06-25.json"
    if not os.path.exists(response_file):
        print(f"❌ Response file not found: {response_file}")
        return False
    
    with open(response_file, 'r') as f:
        api_data = json.load(f)
    
    print(f"📄 Loaded {len(api_data)} invoices from {response_file}")
    
    # Test with first invoice
    if api_data:
        invoice = api_data[0]
        print(f"\n🔍 Testing invoice: {invoice.get('primaryKey')} - {invoice.get('invoiceNumber')}")
        
        # Initialize database manager
        db_manager = DatabaseManager(config)
        db_manager.connect()
        
        try:
            # Test the flattening logic directly
            print("\n📋 Testing _flatten_invoice_to_line_items...")
            line_items = db_manager._flatten_invoice_to_line_items(invoice, 1)
            
            print(f"✅ Successfully created {len(line_items)} line items")
            
            if line_items:
                print("\n📋 Sample line items:")
                for i, item in enumerate(line_items[:3]):  # Show first 3
                    print(f"  {i+1}. Type: {item.get('line_item_type')}, "
                          f"Description: {item.get('part_description', item.get('labor_description', 'N/A'))[:50]}...")
                
                # Test database insertion
                print(f"\n💾 Testing database insertion...")
                with db_manager._get_connection() as conn:
                    with conn.cursor() as cursor:
                        # First, insert the raw data
                        print("  📥 Inserting raw data...")
                        raw_data_id = db_manager._store_raw_data(cursor, invoice)
                        print(f"  ✅ Raw data inserted with ID: {raw_data_id}")
                        
                        # Update line items with raw_data_id
                        for item in line_items:
                            item['raw_data_id'] = raw_data_id
                        
                        # Test line item insertion
                        print("  📥 Inserting line items...")
                        inserted_count = db_manager._insert_line_items(cursor, line_items)
                        print(f"  ✅ Successfully inserted {inserted_count} line items")
                        
                        # Commit the transaction
                        conn.commit()
                        print("  ✅ Transaction committed")
            
        except Exception as e:
            print(f"❌ Error during processing: {e}")
            import traceback
            traceback.print_exc()
        
        finally:
            db_manager.close()
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
