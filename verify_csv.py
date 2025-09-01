#!/usr/bin/env python3
"""
Verify the CSV file structure and content
"""

import csv
import os

def verify_csv():
    """Verify the CSV file."""
    filename = "april_2025_final_complete_20250829_101829.csv"
    
    if not os.path.exists(filename):
        print(f"❌ File {filename} not found")
        return
    
    file_size = os.path.getsize(filename)
    print(f"📁 File: {filename}")
    print(f"📏 Size: {file_size:,} bytes ({file_size/1024/1024:.1f} MB)")
    
    try:
        with open(filename, 'r', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            
            # Read header
            header = next(reader)
            print(f"📊 Columns: {len(header)}")
            
            # Count rows
            row_count = 0
            for row in reader:
                row_count += 1
                if row_count % 1000 == 0:
                    print(f"   Processed {row_count} rows...")
            
            print(f"📈 Total Rows: {row_count:,}")
            print(f"📋 Total Records: {row_count:,} rows × {len(header)} columns")
            
            # Show first few column names
            print(f"\n🔍 First 10 columns:")
            for i, col in enumerate(header[:10]):
                print(f"   {i+1:2d}. {col}")
            
            if len(header) > 10:
                print(f"   ... and {len(header)-10} more columns")
            
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")

if __name__ == "__main__":
    verify_csv()
