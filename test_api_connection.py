#!/usr/bin/env python3
"""
Test Fullbay API Connection
Quick test to verify API credentials and connection before running full ingestion.
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

try:
    from config import Config
    from fullbay_client import FullbayClient
    from database import DatabaseManager
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Make sure you're running this from the project root directory.")
    sys.exit(1)

def test_api_connection():
    """Test API connection and token generation."""
    print("🧪 FULLBAY API CONNECTION TEST")
    print("=" * 40)
    
    try:
        # Initialize configuration
        print("🔧 Loading configuration...")
        config = Config()
        
        # Check if API key is set
        if not config.fullbay_api_key or config.fullbay_api_key.endswith('_here'):
            print("❌ Fullbay API key not configured!")
            print("Please set FULLBAY_API_KEY in your local_config.env file")
            return False
        
        print(f"✅ API key configured: {config.fullbay_api_key[:8]}...{config.fullbay_api_key[-4:]}")
        
        # Initialize API client
        print("🔌 Initializing Fullbay API client...")
        fullbay_client = FullbayClient(config)
        print("✅ Fullbay client initialized")
        
        # Test token generation
        print("🔑 Testing token generation...")
        test_date = datetime(2025, 1, 1, tzinfo=timezone.utc)
        token = fullbay_client._generate_token(test_date.strftime('%Y-%m-%d'))
        print(f"✅ Token generated successfully: {token[:10]}...{token[-4:]}")
        
        # Test public IP retrieval
        print("🌐 Testing public IP retrieval...")
        ip = fullbay_client._get_public_ip()
        print(f"✅ Public IP retrieved: {ip}")
        
        # Test a small API call (just one day to avoid long wait)
        print("📡 Testing actual API call (January 1, 2025)...")
        print("⏳ This may take up to 16 minutes - please be patient...")
        
        try:
            invoices = fullbay_client.fetch_invoices_for_date(test_date)
            print(f"✅ API call successful! Found {len(invoices)} invoices for January 1, 2025")
            
            if invoices:
                print("📊 Sample invoice data structure:")
                sample_invoice = invoices[0]
                print(f"   - Invoice ID: {sample_invoice.get('id', 'N/A')}")
                print(f"   - Invoice Number: {sample_invoice.get('invoiceNumber', 'N/A')}")
                print(f"   - Customer: {sample_invoice.get('customer', {}).get('title', 'N/A')}")
                print(f"   - Date: {sample_invoice.get('invoiceDate', 'N/A')}")
            
            return True
            
        except Exception as e:
            print(f"❌ API call failed: {e}")
            print("This could be due to:")
            print("  - Invalid API key")
            print("  - Network connectivity issues") 
            print("  - Fullbay API service issues")
            return False
        
    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        return False

def test_database_connection():
    """Test database connection."""
    print("\n🗄️  DATABASE CONNECTION TEST")
    print("=" * 40)
    
    try:
        config = Config()
        db_manager = DatabaseManager(config)
        
        print("🔌 Testing database connection...")
        db_manager.connect()
        print("✅ Database connection successful!")
        
        # Test basic query
        with db_manager._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM fullbay_raw_data")
                raw_count = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM fullbay_line_items")
                line_count = cursor.fetchone()[0]
                
                print(f"📊 Current database state:")
                print(f"   - Raw data records: {raw_count:,}")
                print(f"   - Line items records: {line_count:,}")
        
        db_manager.close()
        return True
        
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False

def main():
    """Main function."""
    print("🔍 FULLBAY SYSTEM CONNECTION TEST")
    print("=" * 50)
    print("This will test both API and database connections before ingestion.")
    print()
    
    # Test database connection
    db_success = test_database_connection()
    
    # Test API connection
    api_success = test_api_connection()
    
    print("\n" + "=" * 50)
    print("📋 CONNECTION TEST RESULTS")
    print("=" * 50)
    print(f"Database Connection: {'✅ PASS' if db_success else '❌ FAIL'}")
    print(f"API Connection: {'✅ PASS' if api_success else '❌ FAIL'}")
    
    if db_success and api_success:
        print("\n🎉 ALL TESTS PASSED!")
        print("You can now run: python january_ingestion.py")
        return True
    else:
        print("\n❌ SOME TESTS FAILED!")
        print("Please fix the issues above before running ingestion.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
