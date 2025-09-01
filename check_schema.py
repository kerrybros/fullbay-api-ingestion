#!/usr/bin/env python3
"""
Simple script to check database schema.
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

def check_schema():
    """Check the database schema."""
    print("🔍 CHECKING DATABASE SCHEMA")
    print("=" * 50)
    
    try:
        # Set up database connection
        config = Config()
        db_manager = DatabaseManager(config)
        db_manager.connect()
        
        with db_manager._get_connection() as conn:
            with conn.cursor() as cursor:
                # Check raw_data table schema
                print("📋 FULLBAY_RAW_DATA TABLE SCHEMA:")
                cursor.execute("""
                    SELECT column_name, data_type, character_maximum_length
                    FROM information_schema.columns 
                    WHERE table_name = 'fullbay_raw_data'
                    ORDER BY ordinal_position
                """)
                
                raw_columns = cursor.fetchall()
                for col in raw_columns:
                    max_len = f"({col['character_maximum_length']})" if col['character_maximum_length'] else ""
                    print(f"  {col['column_name']}: {col['data_type']}{max_len}")
                
                print(f"\n📋 FULLBAY_LINE_ITEMS TABLE SCHEMA:")
                cursor.execute("""
                    SELECT column_name, data_type, character_maximum_length
                    FROM information_schema.columns 
                    WHERE table_name = 'fullbay_line_items'
                    ORDER BY ordinal_position
                """)
                
                line_columns = cursor.fetchall()
                for col in line_columns:
                    max_len = f"({col['character_maximum_length']})" if col['character_maximum_length'] else ""
                    print(f"  {col['column_name']}: {col['data_type']}{max_len}")
                
                # Check for any data in the tables
                print(f"\n📊 DATA COUNTS:")
                cursor.execute("SELECT COUNT(*) as count FROM fullbay_raw_data")
                raw_count = cursor.fetchone()['count']
                print(f"  Raw data records: {raw_count}")
                
                cursor.execute("SELECT COUNT(*) as count FROM fullbay_line_items")
                line_count = cursor.fetchone()['count']
                print(f"  Line items: {line_count}")
        
        db_manager.close()
        
    except Exception as e:
        print(f"❌ Error checking schema: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_schema()
