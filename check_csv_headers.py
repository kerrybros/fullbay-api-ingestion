#!/usr/bin/env python3
"""
Check CSV file headers
"""

import csv

def check_csv_headers():
    """Check the CSV file headers."""
    filename = "april_2025_updated_schema_20250829_115231.csv"
    
    try:
        with open(filename, 'r', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            header = next(reader)
            
            print(f"CSV FILE: {filename}")
            print(f"Total columns: {len(header)}")
            print("=" * 50)
            
            # Check for exported column
            if 'exported' in header:
                print("❌ EXPORTED COLUMN STILL EXISTS IN CSV")
                exported_index = header.index('exported')
                print(f"   Position: {exported_index + 1}")
            else:
                print("✅ EXPORTED COLUMN REMOVED FROM CSV")
            
            # Check for other removed columns
            removed_cols = ['so_total_parts_cost', 'quickbooks_item', 'exported']
            print(f"\n🔍 CHECKING REMOVED COLUMNS:")
            for col in removed_cols:
                if col in header:
                    print(f"   ❌ {col} - STILL EXISTS")
                else:
                    print(f"   ✅ {col} - REMOVED")
            
            # Check for renamed columns
            print(f"\n🔄 CHECKING RENAMED COLUMNS:")
            if 'service_order' in header:
                print(f"   ✅ service_order - RENAMED")
            if 'total' in header:
                print(f"   ✅ total - RENAMED")
            
            # Show first 10 columns
            print(f"\n📋 FIRST 10 COLUMNS:")
            for i, col in enumerate(header[:10]):
                print(f"   {i+1:2d}. {col}")
            
            if len(header) > 10:
                print(f"   ... and {len(header)-10} more columns")
                
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_csv_headers()
