#!/usr/bin/env python3
"""
Database Schema Update Script
Drops and recreates fullbay_line_items table with the correct 74-column schema
"""

import sys
import os
sys.path.append('src')

from database import DatabaseManager
from config import Config

def update_database_schema():
    """Update the database schema to the correct 74-column version"""
    
    print("🔄 Updating database schema to 74-column version...")
    
    try:
        # Initialize database connection
        config = Config()
        db = DatabaseManager(config)
        
        if not db.test_connection():
            print("❌ Database connection failed")
            return False
            
        print("✅ Database connection successful")
        
        # Get connection from pool
        conn = db.connection_pool.getconn()
        cursor = conn.cursor()
        
        try:
            # Drop the existing table (this will delete all data)
            print("🗑️  Dropping existing fullbay_line_items table...")
            cursor.execute("DROP TABLE IF EXISTS fullbay_line_items CASCADE;")
            
            # Recreate the table with correct schema
            print("🏗️  Creating new fullbay_line_items table with 74-column schema...")
            
            # Read the correct schema from the SQL file
            with open('sql/02_create_line_items_table.sql', 'r') as f:
                schema_sql = f.read()
            
            # Execute the schema creation
            cursor.execute(schema_sql)
            
            # Commit the changes
            conn.commit()
            
            print("✅ Database schema updated successfully!")
            print("📊 New table has 74 columns with labor_hours splitting logic")
            
            # Verify the table was created correctly
            cursor.execute("""
                SELECT COUNT(*) as column_count
                FROM information_schema.columns 
                WHERE table_name = 'fullbay_line_items'
            """)
            
            result = cursor.fetchone()
            column_count = result[0] if result else 0
            
            print(f"✅ Verified: Table has {column_count} columns")
            
            if column_count == 74:
                print("🎉 Schema update completed successfully!")
                return True
            else:
                print(f"⚠️  Warning: Expected 74 columns, got {column_count}")
                return False
                
        except Exception as e:
            print(f"❌ Error updating schema: {e}")
            conn.rollback()
            return False
            
        finally:
            cursor.close()
            db.connection_pool.putconn(conn)
            
    except Exception as e:
        print(f"❌ Database connection error: {e}")
        return False

if __name__ == "__main__":
    success = update_database_schema()
    if success:
        print("\n🎯 Database is now ready for data ingestion with the correct schema!")
    else:
        print("\n💥 Schema update failed. Please check the errors above.")
        sys.exit(1)
