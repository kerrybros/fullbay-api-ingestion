#!/usr/bin/env python3
"""
Debug script to investigate June 27th processing errors.
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

def debug_june27_errors():
    """Debug the specific errors from June 27th processing."""
    print("🔍 DEBUGGING JUNE 27TH ERRORS")
    print("=" * 50)
    
    try:
        # Set up database connection
        config = Config()
        db_manager = DatabaseManager(config)
        db_manager.connect()
        
        with db_manager._get_connection() as conn:
            with conn.cursor() as cursor:
                # Check the problematic invoice
                print("🔍 CHECKING PROBLEMATIC INVOICE 17889795:")
                cursor.execute("""
                    SELECT 
                        fullbay_invoice_id,
                        invoice_number,
                        customer_title,
                        total
                    FROM fullbay_raw_data 
                    WHERE fullbay_invoice_id = '17889795'
                """)
                
                result = cursor.fetchone()
                if result:
                    print(f"  📄 Invoice found: {result['invoice_number']}")
                    print(f"  👤 Customer: {result['customer_title']}")
                    print(f"  💰 Total: ${result['total']}")
                else:
                    print("  ❌ Invoice not found in raw_data")
                
                # Check database schema for line_items table
                print(f"\n🔍 CHECKING DATABASE SCHEMA:")
                cursor.execute("""
                    SELECT column_name, data_type, character_maximum_length
                    FROM information_schema.columns 
                    WHERE table_name = 'fullbay_line_items'
                    ORDER BY ordinal_position
                """)
                
                columns = cursor.fetchall()
                print(f"  📋 Line Items Table Schema:")
                for col in columns:
                    max_len = f"({col['character_maximum_length']})" if col['character_maximum_length'] else ""
                    print(f"    {col['column_name']}: {col['data_type']}{max_len}")
                
                # Check for any line items that were successfully inserted
                cursor.execute("""
                    SELECT COUNT(*) as count 
                    FROM fullbay_line_items 
                    WHERE fullbay_invoice_id = '17889795'
                """)
                
                line_count = cursor.fetchone()['count']
                print(f"\n📊 Line items for invoice 17889795: {line_count}")
                
                # Check the raw JSON data for the problematic invoice
                cursor.execute("""
                    SELECT raw_json_data
                    FROM fullbay_raw_data 
                    WHERE fullbay_invoice_id = '17889795'
                """)
                
                raw_result = cursor.fetchone()
                if raw_result:
                    raw_data = raw_result['raw_json_data']
                    if isinstance(raw_data, str):
                        raw_data = json.loads(raw_data)
                    
                    print(f"\n🔍 ANALYZING RAW DATA FOR INVOICE 17889795:")
                    print(f"  📄 Invoice Number: {raw_data.get('invoiceNumber')}")
                    print(f"  👤 Customer: {raw_data.get('customerTitle')}")
                    
                    # Check service order data
                    service_order = raw_data.get('ServiceOrder', {})
                    complaints = service_order.get('Complaints', [])
                    
                    print(f"  📋 Number of Complaints: {len(complaints)}")
                    
                    # Look for potential long values
                    for i, complaint in enumerate(complaints):
                        print(f"\n  📋 Complaint #{i+1}:")
                        print(f"    📝 Note: {complaint.get('note', 'N/A')[:100]}...")
                        
                        corrections = complaint.get('Corrections', [])
                        for j, correction in enumerate(corrections):
                            print(f"\n      🔧 Correction #{j+1}:")
                            title = correction.get('title', 'N/A')
                            print(f"        📝 Title: {title[:100]}...")
                            print(f"        📝 Title Length: {len(title)}")
                            
                            actual_correction = correction.get('actualCorrection', 'N/A')
                            print(f"        📋 Actual Correction: {actual_correction[:100]}...")
                            print(f"        📋 Actual Correction Length: {len(actual_correction)}")
                            
                            # Check parts
                            parts = correction.get('Parts', [])
                            for k, part in enumerate(parts):
                                print(f"\n          🔧 Part #{k+1}:")
                                description = part.get('description', 'N/A')
                                print(f"            📝 Description: {description[:100]}...")
                                print(f"            📝 Description Length: {len(description)}")
                                
                                shop_part_number = part.get('shopPartNumber', 'N/A')
                                print(f"            🔧 Shop Part Number: {shop_part_number}")
                                print(f"            🔧 Shop Part Number Length: {len(shop_part_number)}")
                
                # Check the correct column name for date filtering
                print(f"\n🔍 CHECKING CORRECT COLUMN NAMES:")
                cursor.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = 'fullbay_line_items' 
                    AND column_name LIKE '%date%'
                """)
                
                date_columns = cursor.fetchall()
                print(f"  📅 Date-related columns in line_items:")
                for col in date_columns:
                    print(f"    {col['column_name']}")
        
        db_manager.close()
        
    except Exception as e:
        print(f"❌ Error debugging June 27th errors: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Main function to debug June 27th errors."""
    print("🔍 JUNE 27TH ERROR DEBUGGING")
    print("=" * 60)
    print(f"🕐 Debug time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    debug_june27_errors()
    
    print("\n✅ June 27th error debugging completed!")

if __name__ == "__main__":
    main()
