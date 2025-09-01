#!/usr/bin/env python3
"""
Script to fix phone number column sizes in the database.
"""

import os
import sys

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

# Set default environment variables for local testing
os.environ.setdefault('DB_HOST', 'fullbay-postgres-db.cc74yq2i4j8n.us-east-1.rds.amazonaws.com')
os.environ.setdefault('DB_USER', 'postgres')
os.environ.setdefault('DB_PASSWORD', '5255Tillman')
os.environ.setdefault('DB_NAME', 'fullbay_data')
os.environ.setdefault('DB_PORT', '5432')

from config import Config
from database import DatabaseManager

def fix_phone_columns():
    """Fix phone number column sizes in the database."""
    print("🔧 FIXING PHONE NUMBER COLUMN SIZES")
    print("=" * 50)
    
    try:
        # Set up database connection
        config = Config()
        db_manager = DatabaseManager(config)
        db_manager.connect()
        
        with db_manager._get_connection() as conn:
            with conn.cursor() as cursor:
                # Check current column sizes
                print("📋 CURRENT COLUMN SIZES:")
                cursor.execute("""
                    SELECT column_name, data_type, character_maximum_length
                    FROM information_schema.columns 
                    WHERE table_name = 'fullbay_line_items'
                    AND column_name IN ('customer_main_phone', 'customer_secondary_phone')
                """)
                
                current_columns = cursor.fetchall()
                for col in current_columns:
                    print(f"  {col['column_name']}: {col['data_type']}({col['character_maximum_length']})")
                
                # Update column sizes
                print(f"\n🔧 UPDATING COLUMN SIZES...")
                
                # Update customer_main_phone to VARCHAR(50)
                cursor.execute("""
                    ALTER TABLE fullbay_line_items 
                    ALTER COLUMN customer_main_phone TYPE VARCHAR(50)
                """)
                print(f"  ✅ Updated customer_main_phone to VARCHAR(50)")
                
                # Update customer_secondary_phone to VARCHAR(50)
                cursor.execute("""
                    ALTER TABLE fullbay_line_items 
                    ALTER COLUMN customer_secondary_phone TYPE VARCHAR(50)
                """)
                print(f"  ✅ Updated customer_secondary_phone to VARCHAR(50)")
                
                # Commit the changes
                conn.commit()
                
                # Verify the changes
                print(f"\n📋 UPDATED COLUMN SIZES:")
                cursor.execute("""
                    SELECT column_name, data_type, character_maximum_length
                    FROM information_schema.columns 
                    WHERE table_name = 'fullbay_line_items'
                    AND column_name IN ('customer_main_phone', 'customer_secondary_phone')
                """)
                
                updated_columns = cursor.fetchall()
                for col in updated_columns:
                    print(f"  {col['column_name']}: {col['data_type']}({col['character_maximum_length']})")
                
                print(f"\n✅ Phone number column sizes updated successfully!")
        
        db_manager.close()
        
    except Exception as e:
        print(f"❌ Error fixing phone columns: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    fix_phone_columns()
