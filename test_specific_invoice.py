#!/usr/bin/env python3
"""
Test script to process the specific error invoice and debug the database insertion.
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

def test_specific_invoice():
    """Test processing the specific error invoice."""
    print("🔍 TESTING SPECIFIC INVOICE PROCESSING")
    print("=" * 50)
    
    try:
        # Set up database connection
        config = Config()
        db_manager = DatabaseManager(config)
        db_manager.connect()
        
        with db_manager._get_connection() as conn:
            with conn.cursor() as cursor:
                # Get the specific error invoice
                cursor.execute("""
                    SELECT 
                        fullbay_invoice_id,
                        raw_json_data
                    FROM fullbay_raw_data 
                    WHERE fullbay_invoice_id = '17556190'
                """)
                
                result = cursor.fetchone()
                if not result:
                    print("❌ Invoice 17556190 not found in database")
                    return
                
                # Parse the raw data
                raw_data = result['raw_json_data']
                if isinstance(raw_data, str):
                    raw_data = json.loads(raw_data)
                
                print(f"📄 Processing Invoice: {raw_data.get('invoiceNumber')}")
                print(f"👤 Customer: {raw_data.get('customerTitle')}")
                print(f"📅 Date: {raw_data.get('invoiceDate')}")
                
                # Test the line item extraction
                print(f"\n🔧 TESTING LINE ITEM EXTRACTION:")
                
                # Get service order data
                service_order = raw_data.get('ServiceOrder', {})
                complaints = service_order.get('Complaints', [])
                
                print(f"  📊 Service Order ID: {service_order.get('primaryKey')}")
                print(f"  🔧 Repair Order Number: {service_order.get('repairOrderNumber')}")
                print(f"  📋 Number of Complaints: {len(complaints)}")
                
                # Process each complaint
                total_corrections = 0
                total_parts = 0
                total_labor_items = 0
                
                for i, complaint in enumerate(complaints):
                    print(f"\n  📋 Complaint #{i+1}:")
                    print(f"    📝 Note: {complaint.get('note', 'N/A')}")
                    print(f"    💰 Labor Total: ${complaint.get('laborTotal', 0)}")
                    print(f"    🔧 Parts Total: ${complaint.get('partsTotal', 0)}")
                    
                    corrections = complaint.get('Corrections', [])
                    print(f"    📊 Number of Corrections: {len(corrections)}")
                    
                    for j, correction in enumerate(corrections):
                        total_corrections += 1
                        print(f"\n      🔧 Correction #{j+1}:")
                        print(f"        📝 Title: {correction.get('title', 'N/A')}")
                        print(f"        💰 Labor Total: ${correction.get('laborTotal', 0)}")
                        print(f"        ⏱️  Labor Hours: {correction.get('laborHoursTotal', 0)}")
                        print(f"        📋 Actual Correction: {correction.get('actualCorrection', 'N/A')[:100]}...")
                        
                        # Check parts
                        parts = correction.get('Parts', [])
                        print(f"        🔧 Number of Parts: {len(parts)}")
                        total_parts += len(parts)
                        
                        # Check assigned technicians
                        assigned_technicians = complaint.get('AssignedTechnicians', [])
                        print(f"        👨‍🔧 Number of Technicians: {len(assigned_technicians)}")
                        
                        for tech in assigned_technicians:
                            print(f"          👨‍🔧 {tech.get('technician', 'N/A')} - {tech.get('actualHours', 0)} hours - {tech.get('portion', 0)}%")
                            total_labor_items += 1
                
                print(f"\n📊 SUMMARY:")
                print(f"  📋 Total Complaints: {len(complaints)}")
                print(f"  🔧 Total Corrections: {total_corrections}")
                print(f"  🔧 Total Parts: {total_parts}")
                print(f"  👨‍🔧 Total Labor Items: {total_labor_items}")
                
                # Now test the actual line item extraction
                print(f"\n🔧 TESTING ACTUAL LINE ITEM EXTRACTION:")
                try:
                    line_items = db_manager._flatten_invoice_to_line_items(raw_data, 1)
                    print(f"  ✅ Successfully extracted {len(line_items)} line items")
                    
                    if line_items:
                        print(f"  📋 Line Items Created:")
                        for i, item in enumerate(line_items[:5]):  # Show first 5
                            item_type = item.get('line_item_type', 'UNKNOWN')
                            description = item.get('part_description') or item.get('labor_description', 'No description')
                            price = item.get('line_total_price', 0)
                            print(f"    {i+1}. {item_type}: {description[:50]}... (${price})")
                        
                        # Test database insertion
                        print(f"\n💾 TESTING DATABASE INSERTION:")
                        try:
                            # First, store the raw data to get a raw_data_id
                            raw_data_id = db_manager._store_raw_data(cursor, raw_data)
                            print(f"  📄 Raw data stored with ID: {raw_data_id}")
                            
                            # Now try to insert the line items with the correct raw_data_id
                            line_items = db_manager._flatten_invoice_to_line_items(raw_data, raw_data_id)
                            print(f"  📋 Re-extracted {len(line_items)} line items with correct raw_data_id")
                            
                            # Now try to insert the line items
                            inserted_count = db_manager._insert_line_items(cursor, line_items)
                            print(f"  ✅ Successfully inserted {inserted_count} line items")
                            
                            # Commit the transaction
                            conn.commit()
                            
                            # Verify they're in the database
                            cursor.execute("""
                                SELECT COUNT(*) as count 
                                FROM fullbay_line_items 
                                WHERE fullbay_invoice_id = %s
                            """, (raw_data.get('primaryKey'),))
                            
                            db_count = cursor.fetchone()['count']
                            print(f"  📊 Line items in database for this invoice: {db_count}")
                            
                        except Exception as e:
                            print(f"  ❌ Error during database insertion: {e}")
                            import traceback
                            traceback.print_exc()
                    else:
                        print(f"  ❌ No line items were created - this explains the error!")
                        
                except Exception as e:
                    print(f"  ❌ Error during line item extraction: {e}")
                    import traceback
                    traceback.print_exc()
        
        db_manager.close()
        
    except Exception as e:
        print(f"❌ Error testing specific invoice: {e}")

def main():
    """Main function to test specific invoice processing."""
    print("🔍 FULLBAY API INGESTION - SPECIFIC INVOICE TEST")
    print("=" * 60)
    print(f"🕐 Test time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    test_specific_invoice()
    
    print("\n✅ Specific invoice test completed!")

if __name__ == "__main__":
    main()
