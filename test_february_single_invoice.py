#!/usr/bin/env python3
"""
Test script to debug February ingestion issues.
Tests processing a single invoice to identify the problem.
"""

import os
import sys
import logging
from datetime import datetime

# Add src directory to path
sys.path.append('src')

from config import Config
from database import DatabaseManager
from fullbay_client import FullbayClient

def load_local_env():
    """Load environment variables from local_config.env."""
    env_file = "local_config.env"
    if os.path.exists(env_file):
        with open(env_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key] = value
        print("✅ Environment variables loaded from local config")
    else:
        print("❌ local_config.env not found!")
        return False
    return True

def test_single_invoice_processing():
    """Test processing a single invoice from February."""
    print("🧪 TESTING SINGLE INVOICE PROCESSING")
    print("=" * 50)
    
    try:
        # Load environment
        if not load_local_env():
            return False
        
        # Test with DET shop for February 3rd (we know it has 10 invoices)
        shop_id = "DET"
        test_date = datetime(2025, 2, 3)
        
        print(f"🏪 Testing {shop_id} shop")
        print(f"📅 Testing date: {test_date.date()}")
        
        # Initialize configuration and clients
        config = Config(shop_id=shop_id)
        client = FullbayClient(config)
        db_manager = DatabaseManager(config)
        
        print("🔌 Connected to API and database")
        
        # Fetch invoices for this date
        print(f"📡 Fetching invoices for {test_date.date()}...")
        invoices = client.fetch_invoices_for_date(test_date)
        
        if not invoices:
            print("❌ No invoices found for test date")
            return False
        
        print(f"✅ Found {len(invoices)} invoices")
        
        # Test processing just the first invoice
        test_invoice = invoices[0]
        print(f"🧪 Testing invoice ID: {test_invoice.get('id', 'unknown')}")
        
        # Test raw data insertion
        print("📝 Testing raw data insertion...")
        try:
            db_manager.insert_raw_data(test_invoice, shop_id)
            print("✅ Raw data insertion successful")
        except Exception as e:
            print(f"❌ Raw data insertion failed: {e}")
            import traceback
            traceback.print_exc()
            return False
        
        # Test line item processing
        print("📋 Testing line item processing...")
        try:
            line_items_created = db_manager.process_and_insert_invoice(test_invoice, shop_id)
            print(f"✅ Line item processing successful: {line_items_created} items created")
        except Exception as e:
            print(f"❌ Line item processing failed: {e}")
            import traceback
            traceback.print_exc()
            return False
        
        # Close database connection
        db_manager.close()
        
        print("\n🎉 Single invoice test completed successfully!")
        print(f"📊 Results: 1 raw record, {line_items_created} line items")
        
        return True
        
    except Exception as e:
        print(f"💥 Critical error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_single_invoice_processing()
    if success:
        print("\n✅ Test passed - February ingestion should work")
    else:
        print("\n❌ Test failed - there's an issue with the ingestion process")
    sys.exit(0 if success else 1)
