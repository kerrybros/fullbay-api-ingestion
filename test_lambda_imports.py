#!/usr/bin/env python3
"""
Test script to check Lambda function imports
"""

import sys
import os

# Add the lambda-deploy-fixed directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'lambda-deploy-fixed'))

def test_imports():
    """Test all the imports used in the Lambda function"""
    try:
        print("Testing imports...")
        
        # Test basic imports
        import json
        import logging
        import os
        from datetime import datetime, timezone
        from typing import Dict, Any, Optional
        print("✅ Basic imports successful")
        
        # Test custom module imports
        from config import Config
        print("✅ Config import successful")
        
        from fullbay_client import FullbayClient
        print("✅ FullbayClient import successful")
        
        from database import DatabaseManager
        print("✅ DatabaseManager import successful")
        
        from utils import setup_logging, handle_errors
        print("✅ Utils import successful")
        
        print("🎉 All imports successful!")
        return True
        
    except Exception as e:
        print(f"❌ Import error: {e}")
        return False

if __name__ == "__main__":
    test_imports()
