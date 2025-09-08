#!/usr/bin/env python3
"""
Quick script to check the current state of the database.
Shows tables, their row counts, and basic structure.
"""

import os
import sys
import psycopg2
import psycopg2.extras
from src.config import Config

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

def check_database_state():
    """Check and display the current state of the database."""
    
    # Load configuration
    try:
        config = Config()
        print(f"Connecting to database: {config.db_host}:{config.db_port}/{config.db_name}")
        print(f"User: {config.db_user}")
        print("-" * 50)
    except Exception as e:
        print(f"Error loading configuration: {e}")
        print("Make sure environment variables are set:")
        print("- DB_HOST")
        print("- DB_USER") 
        print("- DB_PASSWORD")
        return
    
    # Connect to database
    try:
        conn = psycopg2.connect(
            host=config.db_host,
            port=config.db_port,
            database=config.db_name,
            user=config.db_user,
            password=config.db_password,
            cursor_factory=psycopg2.extras.RealDictCursor
        )
        
        with conn.cursor() as cursor:
            print("✅ Database connection successful!")
            
            # Check PostgreSQL version
            cursor.execute("SELECT version();")
            version = cursor.fetchone()['version']
            print(f"PostgreSQL Version: {version.split(',')[0]}")
            print("-" * 50)
            
            # List all tables in public schema
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                ORDER BY table_name;
            """)
            
            tables = cursor.fetchall()
            
            if not tables:
                print("❌ No tables found in the database!")
                print("You need to run the SQL scripts to create tables:")
                print("1. sql/00_create_all_tables.sql")
                print("2. Or individual scripts: sql/01_create_raw_data_table.sql, etc.")
                return
            
            print(f"📋 Found {len(tables)} table(s):")
            for table in tables:
                table_name = table['table_name']
                
                # Get row count
                cursor.execute(f"SELECT COUNT(*) as count FROM {table_name};")
                count = cursor.fetchone()['count']
                
                # Get column count
                cursor.execute(f"""
                    SELECT COUNT(*) as col_count 
                    FROM information_schema.columns 
                    WHERE table_name = '{table_name}' AND table_schema = 'public';
                """)
                col_count = cursor.fetchone()['col_count']
                
                status = "📊" if count > 0 else "🔍"
                print(f"  {status} {table_name}: {count:,} rows, {col_count} columns")
            
            print("-" * 50)
            
            # Check specific expected tables
            expected_tables = ['fullbay_raw_data', 'fullbay_line_items', 'ingestion_metadata']
            missing_tables = []
            
            existing_table_names = [t['table_name'] for t in tables]
            
            for expected in expected_tables:
                if expected not in existing_table_names:
                    missing_tables.append(expected)
            
            if missing_tables:
                print("⚠️  Missing expected tables:")
                for missing in missing_tables:
                    print(f"  - {missing}")
                print("\nRun the SQL scripts to create missing tables.")
            else:
                print("✅ All expected tables are present!")
                
                # Check if any data exists
                total_rows = sum(
                    cursor.execute(f"SELECT COUNT(*) as count FROM {table['table_name']};") or 
                    cursor.fetchone()['count'] 
                    for table in tables if table['table_name'] in expected_tables
                )
                
                if total_rows == 0:
                    print("📝 Database is empty - no data has been ingested yet.")
                    print("Run the ingestion script to populate with data.")
                else:
                    print(f"📈 Database contains {total_rows:,} total rows across main tables.")
    
    except psycopg2.Error as e:
        print(f"❌ Database error: {e}")
        print("Check your database connection settings and ensure the database server is running.")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    check_database_state()
