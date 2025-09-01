#!/usr/bin/env python3
"""
Extract all warnings from the April 2025 ingestion log file
Updated to exclude false warnings that have been removed from validation logic
"""

import re
from datetime import datetime

def extract_warnings_from_log():
    """Extract all warnings from the log file."""
    log_file = "logs/april_2025_ingestion_20250828_140643.log"
    
    warnings = []
    
    try:
        with open(log_file, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                # Look for WARNING level log entries
                if 'WARNING' in line:
                    # Skip false warnings that have been removed from validation
                    if any(false_warning in line for false_warning in [
                        'Missing vehicle VIN',
                        'Missing vehicle make/model',
                        'Zero or negative invoice total'
                    ]):
                        continue
                    
                    warnings.append({
                        'line_number': line_num,
                        'timestamp': line.split(' - ')[0] if ' - ' in line else 'Unknown',
                        'message': line.strip(),
                        'type': 'WARNING'
                    })
                
                # Look for ERROR level log entries
                elif 'ERROR' in line and 'Error calculating data quality metrics' not in line:
                    warnings.append({
                        'line_number': line_num,
                        'timestamp': line.split(' - ')[0] if ' - ' in line else 'Unknown',
                        'message': line.strip(),
                        'type': 'ERROR'
                    })
        
        return warnings
        
    except FileNotFoundError:
        print(f"Log file not found: {log_file}")
        return []
    except Exception as e:
        print(f"Error reading log file: {e}")
        return []

def categorize_warnings(warnings):
    """Categorize warnings by type."""
    categories = {
        'data_quality': [],
        'database': [],
        'api': [],
        'other': []
    }
    
    for warning in warnings:
        message = warning['message'].lower()
        
        if 'data quality' in message or 'missing vehicle' in message:
            categories['data_quality'].append(warning)
        elif 'database' in message or 'connection' in message:
            categories['database'].append(warning)
        elif 'api' in message or 'fullbay' in message:
            categories['api'].append(warning)
        else:
            categories['other'].append(warning)
    
    return categories

def main():
    """Main function to extract and display warnings."""
    print("Extracting warnings from April 2025 ingestion log...")
    print("(Excluding false warnings that have been removed from validation)")
    print("=" * 80)
    
    warnings = extract_warnings_from_log()
    
    if not warnings:
        print("No warnings found in the log file.")
        return
    
    print(f"Found {len(warnings)} warnings/errors:")
    print()
    
    # Categorize warnings
    categories = categorize_warnings(warnings)
    
    # Display by category
    for category, category_warnings in categories.items():
        if category_warnings:
            print(f"📋 {category.upper().replace('_', ' ')} WARNINGS ({len(category_warnings)}):")
            print("-" * 60)
            
            for warning in category_warnings:
                print(f"  Line {warning['line_number']}: {warning['timestamp']}")
                print(f"  {warning['message']}")
                print()
    
    # Summary
    print("=" * 80)
    print("SUMMARY:")
    print(f"  Total warnings/errors: {len(warnings)}")
    print(f"  Data quality warnings: {len(categories['data_quality'])}")
    print(f"  Database warnings: {len(categories['database'])}")
    print(f"  API warnings: {len(categories['api'])}")
    print(f"  Other warnings: {len(categories['other'])}")
    
    print("\nNote: False warnings for missing VIN/make/model and zero/negative totals")
    print("have been removed from the validation logic as these are not actual issues.")
    
    # Save to file
    output_file = f"april_2025_warnings_filtered_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("APRIL 2025 INGESTION WARNINGS SUMMARY (FILTERED)\n")
        f.write("=" * 80 + "\n\n")
        f.write("Note: False warnings for missing VIN/make/model and zero/negative totals\n")
        f.write("have been removed from the validation logic as these are not actual issues.\n\n")
        
        for category, category_warnings in categories.items():
            if category_warnings:
                f.write(f"{category.upper().replace('_', ' ')} WARNINGS ({len(category_warnings)}):\n")
                f.write("-" * 60 + "\n")
                
                for warning in category_warnings:
                    f.write(f"Line {warning['line_number']}: {warning['timestamp']}\n")
                    f.write(f"{warning['message']}\n\n")
        
        f.write("=" * 80 + "\n")
        f.write("SUMMARY:\n")
        f.write(f"  Total warnings/errors: {len(warnings)}\n")
        f.write(f"  Data quality warnings: {len(categories['data_quality'])}\n")
        f.write(f"  Database warnings: {len(categories['database'])}\n")
        f.write(f"  API warnings: {len(categories['api'])}\n")
        f.write(f"  Other warnings: {len(categories['other'])}\n")
    
    print(f"\nDetailed warnings saved to: {output_file}")

if __name__ == "__main__":
    main()
