#!/usr/bin/env python3
"""
Script to get detailed error information for specific invoice processing errors.
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
from database import DatabaseManager

def get_error_details():
    """Get detailed error information for processing errors."""
    print("🔍 CHECKING ERROR DETAILS")
    print("=" * 50)
    
    try:
        # Set up database connection
        config = Config()
        db_manager = DatabaseManager(config)
        db_manager.connect()
        
        with db_manager._get_connection() as conn:
            with conn.cursor() as cursor:
                # Get all processing errors with full details
                print("\n❌ ALL PROCESSING ERRORS (Last 24 hours):")
                cursor.execute("""
                    SELECT 
                        fullbay_invoice_id,
                        processing_errors,
                        ingestion_timestamp,
                        raw_json_data
                    FROM fullbay_raw_data 
                    WHERE processing_errors IS NOT NULL
                    AND ingestion_timestamp >= NOW() - INTERVAL '24 hours'
                    ORDER BY ingestion_timestamp DESC
                """)
                
                errors = cursor.fetchall()
                if errors:
                    for i, error in enumerate(errors, 1):
                        print(f"\n🚨 ERROR #{i}:")
                        print(f"  📄 Invoice ID: {error['fullbay_invoice_id']}")
                        print(f"  🕐 Timestamp: {error['ingestion_timestamp']}")
                        print(f"  ❌ Error Message: {error['processing_errors']}")
                        
                        # Try to parse the raw JSON to get more context
                        try:
                            # Handle both string and dict formats
                            if isinstance(error['raw_json_data'], str):
                                raw_data = json.loads(error['raw_json_data'])
                            else:
                                raw_data = error['raw_json_data']
                                
                            print(f"  📋 Invoice Number: {raw_data.get('invoiceNumber', 'N/A')}")
                            print(f"  👤 Customer: {raw_data.get('customerTitle', 'N/A')}")
                            print(f"  📅 Invoice Date: {raw_data.get('invoiceDate', 'N/A')}")
                            print(f"  💰 Total Amount: ${raw_data.get('totalAmount', 'N/A')}")
                            
                            # Check Service Order information
                            service_order = raw_data.get('ServiceOrder', {})
                            if isinstance(service_order, dict):
                                service_order_number = service_order.get('serviceOrderNumber', 'N/A')
                                print(f"  🔧 Service Order Number: {service_order_number}")
                                print(f"  🔧 Service Order Keys: {list(service_order.keys())}")
                                print(f"  🔧 Service Order Data: {service_order}")
                            else:
                                print(f"  🔧 Service Order: {service_order}")
                                
                            # Check if there are line items in the raw data
                            if 'lineItems' in raw_data:
                                line_items_count = len(raw_data['lineItems'])
                                print(f"  📊 Line Items in Raw Data: {line_items_count}")
                                
                                # Show first few line items for debugging
                                if line_items_count > 0:
                                    print(f"  📋 Sample Line Items:")
                                    for i, item in enumerate(raw_data['lineItems'][:3]):
                                        item_type = item.get('lineItemType', 'UNKNOWN')
                                        description = item.get('partDescription') or item.get('laborDescription') or 'No description'
                                        price = item.get('lineTotalPrice', 0)
                                        print(f"    {i+1}. {item_type}: {description[:50]}... (${price})")
                            else:
                                print(f"  📊 Line Items in Raw Data: None found")
                                
                            # Show the complete raw data structure for debugging
                            print(f"\n  🔍 COMPLETE RAW DATA STRUCTURE:")
                            print(f"    Available keys: {list(raw_data.keys())}")
                            
                            # Check for alternative line item field names
                            line_item_fields = ['lineItems', 'line_items', 'items', 'parts', 'labor']
                            found_line_items = None
                            for field in line_item_fields:
                                if field in raw_data:
                                    found_line_items = field
                                    break
                            
                            if found_line_items:
                                items = raw_data[found_line_items]
                                if isinstance(items, list):
                                    print(f"    Found line items in field '{found_line_items}': {len(items)} items")
                                else:
                                    print(f"    Field '{found_line_items}' exists but is not a list: {type(items)}")
                            else:
                                print(f"    No line item fields found in: {line_item_fields}")
                                
                        except Exception as e:
                            print(f"  ⚠️  Could not parse raw JSON data: {e}")
                            print(f"  🔍 Raw data type: {type(error['raw_json_data'])}")
                            print(f"  🔍 Raw data preview: {str(error['raw_json_data'])[:200]}...")
                        
                        print("-" * 50)
                else:
                    print("✅ No processing errors found in the last 24 hours")
                
                # Check if the specific invoice exists in line items table
                print("\n🔍 CHECKING LINE ITEMS FOR ERROR INVOICES:")
                for error in errors:
                    invoice_id = error['fullbay_invoice_id']
                    cursor.execute("""
                        SELECT 
                            COUNT(*) as line_items_count,
                            SUM(line_total_price) as total_value
                        FROM fullbay_line_items 
                        WHERE fullbay_invoice_id = %s
                    """, (invoice_id,))
                    
                    line_item_stats = cursor.fetchone()
                    print(f"\n  📄 Invoice {invoice_id}:")
                    print(f"    📊 Line items created: {line_item_stats['line_items_count']}")
                    print(f"    💰 Total value: ${line_item_stats['total_value']:,.2f}" if line_item_stats['total_value'] else "    💰 Total value: $0.00")
                    
                    if line_item_stats['line_items_count'] == 0:
                        print(f"    ⚠️  NO LINE ITEMS CREATED - This explains the processing error")
                    else:
                        print(f"    ✅ Line items were created despite the error")
                
                # Get more context about the error
                print("\n📊 ERROR ANALYSIS:")
                if errors:
                    print(f"  📈 Total errors in last 24 hours: {len(errors)}")
                    
                    # Check if any line items were created for error invoices
                    error_invoice_ids = [error['fullbay_invoice_id'] for error in errors]
                    placeholders = ','.join(['%s'] * len(error_invoice_ids))
                    
                    cursor.execute(f"""
                        SELECT 
                            fullbay_invoice_id,
                            COUNT(*) as line_items_created
                        FROM fullbay_line_items 
                        WHERE fullbay_invoice_id IN ({placeholders})
                        GROUP BY fullbay_invoice_id
                    """, error_invoice_ids)
                    
                    line_items_created = cursor.fetchall()
                    created_count = len(line_items_created)
                    total_errors = len(errors)
                    
                    print(f"  📊 Invoices with line items created: {created_count}/{total_errors}")
                    print(f"  📊 Invoices with no line items: {total_errors - created_count}/{total_errors}")
                    
                    if created_count < total_errors:
                        print(f"  ⚠️  {total_errors - created_count} invoices failed to create line items")
                    else:
                        print(f"  ✅ All error invoices had line items created (errors may be minor)")
                else:
                    print("  ✅ No errors to analyze")
        
        db_manager.close()
        
    except Exception as e:
        print(f"❌ Error checking error details: {e}")

def main():
    """Main function to get error details."""
    print("🔍 FULLBAY API INGESTION ERROR DETAILS")
    print("=" * 60)
    print(f"🕐 Check time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    get_error_details()
    
    print("\n✅ Error details check completed!")

if __name__ == "__main__":
    main()
