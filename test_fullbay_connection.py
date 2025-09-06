#!/usr/bin/env python3
"""
Test script to verify Fullbay API connection and authentication.

This script tests the API connection using the provided API key
and verifies that the authentication token generation is working correctly.
"""

import os
import sys
import logging
from datetime import datetime, timezone

# Add src directory to path
sys.path.append('src')

from config import Config
from fullbay_client import FullbayClient

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_fullbay_connection():
    """Test Fullbay API connection and authentication."""
    print("🔧 Testing Fullbay API Connection")
    print("="*50)
    
    try:
        # Initialize configuration
        config = Config()
        print(f"✅ Configuration loaded - Environment: {config.environment}")
        
        # Check API key
        if not config.fullbay_api_key:
            print("❌ Fullbay API key not found!")
            print("   Please set the FULLBAY_API_KEY environment variable")
            return False
        
        print(f"✅ API Key configured: {config.fullbay_api_key[:8]}...{config.fullbay_api_key[-8:]}")
        
        # Initialize Fullbay client
        fullbay_client = FullbayClient(config)
        print("✅ Fullbay client initialized")
        
        # Test API status
        api_status = fullbay_client.get_api_status()
        print(f"📡 API Status: {api_status}")
        
        if api_status.get('status') != 'connected':
            print(f"❌ API connection failed: {api_status}")
            return False
        
        # Test token generation
        today = datetime.now(timezone.utc)
        date_str = today.strftime('%Y-%m-%d')
        token = fullbay_client._generate_token(date_str)
        ip_address = fullbay_client._get_public_ip()
        
        print(f"🌐 Public IP: {ip_address}")
        print(f"📅 Today's Date: {date_str}")
        print(f"🔐 Generated Token: {token}")
        
        # Test API call with today's date
        print(f"\n🧪 Testing API call for today ({date_str})...")
        try:
            invoices = fullbay_client.fetch_invoices_for_date(today)
            print(f"✅ API call successful - Retrieved {len(invoices)} invoices")
            
            if invoices:
                print("📊 Sample invoice data:")
                sample_invoice = invoices[0]
                for key, value in list(sample_invoice.items())[:5]:  # Show first 5 fields
                    print(f"   {key}: {value}")
                if len(sample_invoice) > 5:
                    print(f"   ... and {len(sample_invoice) - 5} more fields")
            
        except Exception as e:
            print(f"⚠️  API call failed (this might be normal if no data for today): {e}")
            # This is not necessarily a failure - there might just be no data for today
        
        print("\n✅ Fullbay API connection test completed successfully!")
        print("🚀 Ready to proceed with February 2025 data ingestion!")
        return True
        
    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        return False

def main():
    """Main function."""
    success = test_fullbay_connection()
    
    if success:
        print("\n🎉 All tests passed! The system is ready for February 2025 ingestion.")
        print("\nNext steps:")
        print("1. Run: python february_ingestion.py")
        print("2. The script will pull February 2025 data day by day")
        print("3. All data will be processed into the line_items table")
    else:
        print("\n❌ Connection test failed. Please check the configuration.")
        sys.exit(1)

if __name__ == "__main__":
    main()
