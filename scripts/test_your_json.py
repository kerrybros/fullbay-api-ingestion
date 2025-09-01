#!/usr/bin/env python3
"""
Quick Test Script for Your FullBay JSON

This script makes it easy to test any FullBay invoice JSON file with the flattening logic.
Just provide your JSON file path and it will process it and show you the results.

Usage:
    python scripts/test_your_json.py your_invoice.json
"""

import sys
import os
from flattening_demo_standalone import StandaloneFlatteningDemo

def main():
    """Main function to test JSON files."""
    
    if len(sys.argv) != 2:
        print("Usage: python scripts/test_your_json.py <json_file_path>")
        print("\nExample:")
        print("  python scripts/test_your_json.py my_invoice.json")
        print("\nThe script will:")
        print("  - Process your JSON through the flattening logic")
        print("  - Generate a CSV file with all flattened data")
        print("  - Show a summary of the transformation")
        print("  - Validate data quality and completeness")
        sys.exit(1)
    
    json_file = sys.argv[1]
    
    if not os.path.exists(json_file):
        print(f"❌ Error: File '{json_file}' not found!")
        print("Please check the file path and try again.")
        sys.exit(1)
    
    print("="*60)
    print("FullBay JSON Flattening Test")
    print("="*60)
    print(f"Testing file: {json_file}")
    print()
    
    # Create demo instance and run test
    demo = StandaloneFlatteningDemo()
    demo.run_demo(json_file)
    
    print("\n" + "="*60)
    print("Test completed! Check the generated CSV file.")
    print("="*60)

if __name__ == "__main__":
    main()
