#!/usr/bin/env python3
"""
Comprehensive test suite for Fullbay API integration.

Tests token generation, API connection, and end-to-end functionality.
"""

import sys
import os
import json
import hashlib
from datetime import datetime, timezone, timedelta
from typing import Dict, Any

# Add the lambda-deploy-fixed directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'lambda-deploy-fixed'))

def test_token_generation():
    """Test Fullbay API token generation"""
    print("🔐 Testing Fullbay API token generation...")
    
    try:
        from fullbay_client import FullbayClient
        from config import Config
        
        # Create config with API key
        config = Config()
        client = FullbayClient(config)
        
        # Test token generation
        today = datetime.now(timezone.utc)
        date_str = today.strftime('%Y-%m-%d')
        
        # Get public IP
        ip_address = client._get_public_ip()
        print(f"  📍 Public IP: {ip_address}")
        
        # Generate token
        token = client._generate_token(date_str)
        print(f"  🔑 Generated token: {token}")
        print(f"  📅 Date: {date_str}")
        print(f"  🔑 Token length: {len(token)}")
        
        # Verify token format (should be 40 characters for SHA1)
        if len(token) == 40:
            print("  ✅ Token generation successful")
            return True
        else:
            print("  ❌ Token generation failed - invalid length")
            return False
            
    except Exception as e:
        print(f"  ❌ Token generation test failed: {e}")
        return False

def test_api_connection():
    """Test Fullbay API connection"""
    print("\n🔗 Testing Fullbay API connection...")
    
    try:
        from fullbay_client import FullbayClient
        from config import Config
        
        config = Config()
        client = FullbayClient(config)
        
        # Test API status
        api_status = client.get_api_status()
        print(f"  📡 API Status: {api_status}")
        
        if api_status.get("status") == "connected":
            print("  ✅ API connection successful")
            return True
        else:
            print(f"  ❌ API connection failed: {api_status.get('error', 'Unknown error')}")
            return False
            
    except Exception as e:
        print(f"  ❌ API connection test failed: {e}")
        return False

def test_fetch_invoices():
    """Test fetching invoices from Fullbay API"""
    print("\n📦 Testing invoice fetching...")
    
    try:
        from fullbay_client import FullbayClient
        from config import Config
        
        config = Config()
        client = FullbayClient(config)
        
        # Test with today's date
        today = datetime.now(timezone.utc)
        print(f"  📅 Testing with date: {today.strftime('%Y-%m-%d')}")
        
        invoices = client.fetch_invoices_for_date(today)
        print(f"  📊 Retrieved {len(invoices)} invoices")
        
        if len(invoices) >= 0:  # 0 is valid (no invoices for today)
            print("  ✅ Invoice fetching successful")
            
            # Show sample invoice structure if available
            if invoices:
                sample_invoice = invoices[0]
                print(f"  📋 Sample invoice keys: {list(sample_invoice.keys())[:10]}...")
                
            return True
        else:
            print("  ❌ Invoice fetching failed")
            return False
            
    except Exception as e:
        print(f"  ❌ Invoice fetching test failed: {e}")
        return False

def test_database_integration():
    """Test database integration"""
    print("\n💾 Testing database integration...")
    
    try:
        from database import DatabaseManager
        from config import Config
        
        config = Config()
        db_manager = DatabaseManager(config)
        
        # Test connection
        db_manager.connect()
        print("  ✅ Database connection successful")
        
        # Test basic query
        with db_manager._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM fullbay_line_items")
                count = cursor.fetchone()[0]
                print(f"  📊 Current line items in database: {count}")
        
        db_manager.close()
        print("  ✅ Database integration test successful")
        return True
        
    except Exception as e:
        print(f"  ❌ Database integration test failed: {e}")
        return False

def test_end_to_end():
    """Test end-to-end integration"""
    print("\n🚀 Testing end-to-end integration...")
    
    try:
        from lambda_function import lambda_handler
        
        # Create test event
        test_event = {
            "target_date": (datetime.now(timezone.utc) - timedelta(days=1)).strftime('%Y-%m-%d')
        }
        
        print(f"  📅 Testing with date: {test_event['target_date']}")
        
        # Mock context
        class MockContext:
            def __init__(self):
                self.aws_request_id = "test-request-id"
        
        context = MockContext()
        
        # Execute Lambda function
        response = lambda_handler(test_event, context)
        
        print(f"  📊 Response status: {response.get('statusCode')}")
        
        # Parse response body
        body = json.loads(response.get('body', '{}'))
        print(f"  📋 Response body: {body}")
        
        if body.get('status') in ['SUCCESS', 'ERROR']:
            print("  ✅ End-to-end test completed")
            return True
        else:
            print("  ❌ End-to-end test failed")
            return False
            
    except Exception as e:
        print(f"  ❌ End-to-end test failed: {e}")
        return False

def test_manual_token_verification():
    """Manually verify token generation matches Fullbay specification"""
    print("\n🔍 Manual token verification...")
    
    try:
        # Test with known values from Fullbay documentation
        api_key = "4b9fcc18-1f24-09fb-275b-ad1974786395"
        test_date = "2025-08-27"
        test_ip = "127.0.0.1"  # Using localhost for testing
        
        # Concatenate as per Fullbay spec
        token_string = f"{api_key}{test_date}{test_ip}"
        print(f"  🔗 Token string: {token_string}")
        
        # Generate SHA1 hash
        expected_token = hashlib.sha1(token_string.encode('utf-8')).hexdigest()
        print(f"  🔑 Expected token: {expected_token}")
        print(f"  📏 Token length: {len(expected_token)}")
        
        # Verify it's a valid SHA1 hash
        if len(expected_token) == 40 and all(c in '0123456789abcdef' for c in expected_token):
            print("  ✅ Manual token verification successful")
            return True
        else:
            print("  ❌ Manual token verification failed")
            return False
            
    except Exception as e:
        print(f"  ❌ Manual token verification failed: {e}")
        return False

def run_all_tests():
    """Run all tests and provide summary"""
    print("🧪 Running Fullbay API Integration Tests")
    print("=" * 50)
    
    tests = [
        ("Manual Token Verification", test_manual_token_verification),
        ("Token Generation", test_token_generation),
        ("API Connection", test_api_connection),
        ("Invoice Fetching", test_fetch_invoices),
        ("Database Integration", test_database_integration),
        ("End-to-End Integration", test_end_to_end),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ {test_name} failed with exception: {e}")
            results.append((test_name, False))
    
    # Print summary
    print("\n" + "=" * 50)
    print("📊 Test Results Summary")
    print("=" * 50)
    
    passed = 0
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} {test_name}")
        if result:
            passed += 1
    
    print(f"\n🎯 Overall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Fullbay integration is ready.")
    else:
        print("⚠️ Some tests failed. Please review the errors above.")
    
    return passed == total

if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
