#!/usr/bin/env python3
"""
Database Setup Script for Fullbay API Ingestion System
This script creates all tables, indexes, functions, and triggers
"""

import os
import sys
import psycopg2
import psycopg2.extras
from pathlib import Path

def get_db_connection():
    """Get database connection using environment variables"""
    return psycopg2.connect(
        host=os.environ.get('DB_HOST', 'fullbay-postgres-db.cc74yq2i4j8n.us-east-1.rds.amazonaws.com'),
        port=int(os.environ.get('DB_PORT', '5432')),
        dbname=os.environ.get('DB_NAME', 'fullbay_data'),
        user=os.environ.get('DB_USER', 'postgres'),
        password=os.environ.get('DB_PASSWORD', '5255Tillman')
    )

def read_sql_file(file_path):
    """Read SQL file content"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        print(f"❌ SQL file not found: {file_path}")
        return None
    except Exception as e:
        print(f"❌ Error reading SQL file {file_path}: {e}")
        return None

def execute_sql_file(conn, file_path, description):
    """Execute SQL file and handle errors"""
    print(f"📄 Executing: {description}")
    
    sql_content = read_sql_file(file_path)
    if not sql_content:
        return False
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql_content)
            conn.commit()
            print(f"✅ Successfully executed: {description}")
            return True
    except Exception as e:
        conn.rollback()
        print(f"❌ Error executing {description}: {e}")
        return False

def main():
    """Main setup function"""
    print("🚀 Starting Fullbay API Database Setup")
    print("=" * 50)
    
    # Get database connection
    try:
        conn = get_db_connection()
        print("✅ Database connection established")
    except Exception as e:
        print(f"❌ Failed to connect to database: {e}")
        sys.exit(1)
    
    # Define SQL files to execute in order
    sql_files = [
        ('sql/01_create_raw_data_table.sql', 'Raw Data Table'),
        ('sql/02_create_line_items_table.sql', 'Line Items Table'),
        ('sql/03_create_metadata_table.sql', 'Metadata Table'),
        ('sql/04_create_summary_tables.sql', 'Summary Tables'),
        ('sql/05_create_utility_functions.sql', 'Utility Functions'),
        ('sql/06_create_detailed_logging_tables.sql', 'Detailed Logging Tables'),
        ('sql/07_create_logging_utility_functions.sql', 'Logging Utility Functions'),
        ('sql/08_create_duplicate_check_placeholder.sql', 'Duplicate Check Placeholder'),
    ]
    
    # Execute each SQL file
    success_count = 0
    total_files = len(sql_files)
    
    for file_path, description in sql_files:
        if execute_sql_file(conn, file_path, description):
            success_count += 1
        print()
    
    # Close connection
    conn.close()
    
    # Summary
    print("=" * 50)
    print(f"📊 Setup Summary: {success_count}/{total_files} files executed successfully")
    
    if success_count == total_files:
        print("🎉 Database setup completed successfully!")
        print("\n📋 Next steps:")
        print("1. Run smoke tests to verify connectivity")
        print("2. Test logging functionality")
        print("3. Implement duplicate invoice check logic")
        print("4. Set up AWS CloudWatch logging")
    else:
        print("⚠️  Some files failed to execute. Please check the errors above.")
        sys.exit(1)

if __name__ == "__main__":
    main()
