#!/usr/bin/env python3
"""
Test DatabaseManager connection with detailed error reporting.
"""

import os
import sys
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
import database
import utils

# Setup logging
logger = utils.setup_logging()

def test_database_manager():
    """Test DatabaseManager connection with detailed error reporting."""
    print("🔌 Testing DatabaseManager connection...")
    print(f"Host: {os.environ.get('DB_HOST')}")
    print(f"Port: {os.environ.get('DB_PORT')}")
    print(f"Database: {os.environ.get('DB_NAME')}")
    print(f"User: {os.environ.get('DB_USER')}")
    print()
    
    try:
        # Create config
        config_obj = config.Config()
        print(f"✅ Config created successfully")
        print(f"   DB Host: {config_obj.db_host}")
        print(f"   DB Port: {config_obj.db_port}")
        print(f"   DB Name: {config_obj.db_name}")
        print(f"   DB User: {config_obj.db_user}")
        print()
        
        # Create DatabaseManager
        db_manager = database.DatabaseManager(config_obj)
        print(f"✅ DatabaseManager created successfully")
        
        # Try to connect
        print("🔄 Attempting to connect...")
        db_manager.connect()
        print("✅ DatabaseManager.connect() completed successfully")
        
        # Test a simple query
        with db_manager._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT version();")
                version = cursor.fetchone()['version']
                print(f"📊 Query successful: {version}")
        
        # Close connection
        db_manager.close()
        print("✅ DatabaseManager.close() completed successfully")
        
        return True
        
    except Exception as e:
        print(f"❌ DatabaseManager test failed: {e}")
        print(f"   Error type: {type(e).__name__}")
        import traceback
        print(f"   Traceback: {traceback.format_exc()}")
        return False

if __name__ == "__main__":
    success = test_database_manager()
    if success:
        print("\n🎉 DatabaseManager test passed!")
    else:
        print("\n❌ DatabaseManager test failed!")
