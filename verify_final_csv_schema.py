#!/usr/bin/env python3
"""
Verify the final CSV schema matches requirements
"""

import csv
import os

def verify_final_csv_schema():
    """Verify the final CSV schema."""
    print("VERIFYING FINAL CSV SCHEMA")
    print("=" * 40)
    
    # Find the most recent final schema CSV
    csv_files = [f for f in os.listdir('.') if f.startswith('april_2025_final_schema_') and f.endswith('.csv')]
    
    if not csv_files:
        print("❌ No final schema CSV files found")
        return
    
    # Get the most recent one
    latest_csv = sorted(csv_files)[-1]
    print(f"📁 Checking file: {latest_csv}")
    
    try:
        with open(latest_csv, 'r', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            header = next(reader)
            
            print(f"📊 Total columns: {len(header)}")
            
            # Check for requested changes
            print(f"\n🔍 CHECKING REQUESTED CHANGES:")
            
            # 1. Check if customer_title was changed to customer
            if 'customer' in header:
                print("   ✅ customer_title → customer (CHANGED)")
            else:
                print("   ❌ customer_title → customer (NOT CHANGED)")
            
            if 'customer_title' in header:
                print("   ❌ customer_title still exists")
            else:
                print("   ✅ customer_title removed")
            
            # 2. Check if primary_technician_number was removed
            if 'primary_technician_number' in header:
                print("   ❌ primary_technician_number still exists")
            else:
                print("   ✅ primary_technician_number removed")
            
            # 3. Check for other important columns
            print(f"\n📋 IMPORTANT COLUMNS CHECK:")
            important_cols = [
                'id', 'fullbay_invoice_id', 'invoice_number', 'invoice_date',
                'customer', 'service_order', 'unit', 'primary_technician',
                'line_item_type', 'total', 'ingestion_timestamp'
            ]
            
            for col in important_cols:
                if col in header:
                    print(f"   ✅ {col}")
                else:
                    print(f"   ❌ {col} - MISSING")
            
            # 4. Show first 15 columns
            print(f"\n📋 FIRST 15 COLUMNS:")
            for i, col in enumerate(header[:15]):
                print(f"   {i+1:2d}. {col}")
            
            # 5. Show last 10 columns
            if len(header) > 15:
                print(f"\n📋 LAST 10 COLUMNS:")
                for i, col in enumerate(header[-10:], len(header)-9):
                    print(f"   {i:2d}. {col}")
            
            # 6. Check for any unexpected columns
            print(f"\n🔍 CHECKING FOR UNEXPECTED COLUMNS:")
            unexpected_cols = [
                'exported', 'so_total_parts_cost', 'so_total_parts_price',
                'so_total_labor_hours', 'so_total_labor_cost', 'so_subtotal',
                'so_tax_total', 'so_total_amount', 'quickbooks_account',
                'quickbooks_item', 'quickbooks_item_type'
            ]
            
            for col in unexpected_cols:
                if col in header:
                    print(f"   ❌ {col} - SHOULD NOT EXIST")
                else:
                    print(f"   ✅ {col} - CORRECTLY REMOVED")
            
            # 7. Count rows
            row_count = sum(1 for row in reader)
            print(f"\n📈 DATA COUNT:")
            print(f"   Total rows: {row_count:,}")
            
            print(f"\n✅ VERIFICATION COMPLETE")
            print(f"📁 File: {latest_csv}")
            print(f"📊 Schema: {len(header)} columns, {row_count:,} rows")
            
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")

if __name__ == "__main__":
    verify_final_csv_schema()
