#!/usr/bin/env python3
"""
Test script to verify the labor_hours field fix.
This will test a small sample to ensure the fix works before reprocessing all data.
"""

import os
import sys
from datetime import datetime, timezone

# Load environment variables from local config file if it exists
def load_local_env():
    """Load environment variables from local_config.env if it exists."""
    env_file = "local_config.env"
    if os.path.exists(env_file):
        print("🔧 Loading environment variables from local_config.env...")
        try:
            with open(env_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        key = key.strip()
                        value = value.strip()
                        
                        # Remove quotes if present
                        if value.startswith('"') and value.endswith('"'):
                            value = value[1:-1]
                        elif value.startswith("'") and value.endswith("'"):
                            value = value[1:-1]
                        
                        # Don't load placeholder values
                        if not value.endswith('_here'):
                            os.environ[key] = value
            print("✅ Environment variables loaded from local config")
        except Exception as e:
            print(f"⚠️  Error loading local config: {e}")

# Load local environment variables first
load_local_env()

# Add src directory to path
sys.path.append('src')

from config import Config
from fullbay_client import FullbayClient
from database import DatabaseManager

def test_labor_hours_fix():
    """Test the labor_hours field fix with a small sample."""
    print("🧪 TESTING LABOR HOURS FIX")
    print("=" * 40)
    
    try:
        # Test with DET shop (we know it has data)
        config = Config(shop_id="DET")
        fullbay_client = FullbayClient(config)
        db_manager = DatabaseManager(config)
        
        # Connect to database
        db_manager.connect()
        print("✅ Database connection established")
        
        # Test with a single day that we know has data
        test_date = datetime(2025, 1, 31, tzinfo=timezone.utc)  # Last day of January
        print(f"🔍 Testing with {test_date.strftime('%Y-%m-%d')} for DET shop...")
        
        # Fetch just one day's data
        invoices = fullbay_client.fetch_invoices_for_date(test_date)
        
        if not invoices:
            print("❌ No test data found for the test date")
            return False
        
        print(f"📊 Found {len(invoices)} invoices for testing")
        
        # Process just the first invoice to test the fix
        test_invoice = [invoices[0]]
        print(f"🧪 Testing with invoice: {test_invoice[0].get('invoiceNumber', 'Unknown')}")
        
        # Try to insert - this should now work without labor_hours errors
        records_inserted = db_manager.insert_records(test_invoice)
        
        if records_inserted > 0:
            print(f"✅ SUCCESS! Inserted {records_inserted} line items without labor_hours errors")
            print("🎉 The labor_hours field fix is working correctly!")
            return True
        else:
            print("❌ No records were inserted - there may still be an issue")
            return False
            
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False
        
    finally:
        if 'db_manager' in locals():
            db_manager.close()

def main():
    """Main function."""
    print("🔧 LABOR HOURS FIELD FIX TEST")
    print("=" * 50)
    print("This will test the fix for the labor_hours field insertion errors.")
    print("Testing with a small sample from DET shop January 31, 2025.")
    print()
    
    success = test_labor_hours_fix()
    
    if success:
        print("\n🎉 Labor hours fix test PASSED!")
        print("The field mapping issue has been resolved.")
        print("You can now reprocess data without labor_hours insertion errors.")
    else:
        print("\n❌ Labor hours fix test FAILED!")
        print("There may still be issues with the field mapping.")
        
    return success

if __name__ == "__main__":
    main()
