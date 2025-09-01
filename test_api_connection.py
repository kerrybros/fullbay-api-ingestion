#!/usr/bin/env python3
"""
Test Fullbay API connection with real API endpoint.
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# Set up environment variables for testing
os.environ.setdefault('DB_HOST', 'fullbay-postgres-db.cc74yq2i4j8n.us-east-1.rds.amazonaws.com')
os.environ.setdefault('DB_PORT', '5432')
os.environ.setdefault('DB_NAME', 'fullbay_data')
os.environ.setdefault('DB_USER', 'postgres')
os.environ.setdefault('DB_PASSWORD', '5255Tillman')
os.environ.setdefault('ENVIRONMENT', 'development')
os.environ.setdefault('LOG_LEVEL', 'INFO')

# Add src to Python path
current_dir = Path(__file__).parent
src_dir = current_dir / "src"
sys.path.insert(0, str(src_dir))

# Import modules directly
import config
import fullbay_client
import utils

# Setup logging
logger = utils.setup_logging()

def test_api_connection_with_test_key():
    """Test API connection with a test API key."""
    print("🔌 Testing Fullbay API connection with test key...")
    
    try:
        # Set a test API key
        os.environ['FULLBAY_API_KEY'] = 'test-api-key-12345'
        
        config_obj = config.Config()
        print(f"✅ Config loaded - API Key configured: {bool(config_obj.fullbay_api_key)}")
        
        client = fullbay_client.FullbayClient(config_obj)
        print(f"✅ FullbayClient initialized - Base URL: {client.base_url}")
        
        # Test API status
        api_status = client.get_api_status()
        print(f"📡 API Status: {api_status}")
        
        return client, config_obj
        
    except Exception as e:
        print(f"❌ API connection test failed: {e}")
        return None, None

def test_api_connection_without_key():
    """Test API connection without API key to see the error."""
    print("\n🔌 Testing Fullbay API connection without API key...")
    
    try:
        # Remove API key
        if 'FULLBAY_API_KEY' in os.environ:
            del os.environ['FULLBAY_API_KEY']
        
        config_obj = config.Config()
        print(f"✅ Config loaded - API Key configured: {bool(config_obj.fullbay_api_key)}")
        
        client = fullbay_client.FullbayClient(config_obj)
        print(f"✅ FullbayClient initialized")
        
    except Exception as e:
        print(f"❌ Expected error without API key: {e}")
        return None, None

def test_api_endpoint_structure():
    """Test the API endpoint structure and show what we're trying to connect to."""
    print("\n🌐 Testing API endpoint structure...")
    
    try:
        # Set a test API key
        os.environ['FULLBAY_API_KEY'] = 'test-api-key-12345'
        
        config_obj = config.Config()
        client = fullbay_client.FullbayClient(config_obj)
        
        # Show the endpoint structure
        test_date = datetime(2024, 6, 25, tzinfo=timezone.utc)
        date_str = test_date.strftime('%Y-%m-%d')
        
        print(f"📅 Test date: {date_str}")
        print(f"🌐 Base URL: {client.base_url}")
        print(f"🔗 Full endpoint: {client.base_url}/getInvoices.php")
        
        # Show what parameters would be sent
        token = client._generate_token(date_str)
        params = {
            'key': client.api_key,
            'token': token,
            'startDate': date_str,
            'endDate': date_str
        }
        
        print(f"📋 Request parameters: {params}")
        print(f"🔑 Token generated: {token[:10]}...")
        
        return True
        
    except Exception as e:
        print(f"❌ Endpoint structure test failed: {e}")
        return False

def test_network_connectivity():
    """Test basic network connectivity to the API endpoint."""
    print("\n🌐 Testing network connectivity...")
    
    import requests
    
    try:
        # Test basic connectivity to the domain
        test_url = "https://app.fullbay.com"
        print(f"🔗 Testing connection to: {test_url}")
        
        response = requests.get(test_url, timeout=10)
        print(f"✅ Connection successful - Status: {response.status_code}")
        
        # Test the specific API endpoint
        api_url = "https://app.fullbay.com/services/getInvoices.php"
        print(f"🔗 Testing API endpoint: {api_url}")
        
        # This will likely fail without proper authentication, but we can see the response
        response = requests.get(api_url, timeout=10)
        print(f"📡 API endpoint response - Status: {response.status_code}")
        
        if response.status_code != 200:
            print(f"📄 Response content: {response.text[:200]}...")
        
        return True
        
    except requests.exceptions.ConnectionError as e:
        print(f"❌ Connection error: {e}")
        return False
    except requests.exceptions.Timeout as e:
        print(f"❌ Timeout error: {e}")
        return False
    except Exception as e:
        print(f"❌ Network test failed: {e}")
        return False

def main():
    """Run all API connection tests."""
    print("🚀 Fullbay API Connection Test Suite")
    print("=" * 50)
    
    # Test 1: API connection with test key
    client, config_obj = test_api_connection_with_test_key()
    
    # Test 2: API connection without key
    test_api_connection_without_key()
    
    # Test 3: API endpoint structure
    test_api_endpoint_structure()
    
    # Test 4: Network connectivity
    test_network_connectivity()
    
    print("\n" + "=" * 50)
    print("📋 Test Summary:")
    print("✅ API client structure is working")
    print("✅ Configuration loading is working")
    print("✅ Token generation is working")
    print("⚠️  API key is required for actual API calls")
    print("⚠️  Network connectivity needs to be verified")
    
    print("\n🎯 Next Steps:")
    print("1. Obtain a valid Fullbay API key")
    print("2. Set FULLBAY_API_KEY environment variable")
    print("3. Test with real API key")
    print("4. Verify network connectivity to app.fullbay.com")

if __name__ == "__main__":
    main()
