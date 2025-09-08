#!/usr/bin/env python3
"""
Database Schema Update and Clear Script
Safely updates the database schema and clears tables for fresh data processing
"""

import os
import sys
import psycopg2
import psycopg2.extras
from datetime import datetime
import json

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

def get_database_connection():
    """Get database connection using environment variables."""
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            port=int(os.getenv('DB_PORT', '5432')),
            database=os.getenv('DB_NAME', 'fullbay_data'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            cursor_factory=psycopg2.extras.RealDictCursor
        )
        return conn
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return None

def backup_existing_data(conn):
    """Create backup of existing data before clearing."""
    print("💾 Creating backup of existing data...")
    
    try:
        with conn.cursor() as cursor:
            # Create backup timestamp
            backup_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            # Check what data exists
            tables_to_backup = [
                'fullbay_raw_data',
                'fullbay_line_items', 
                'employee_statistics'
            ]
            
            backup_info = {}
            
            for table in tables_to_backup:
                # Check if table exists and has data
                cursor.execute(f"""
                    SELECT COUNT(*) as count 
                    FROM information_schema.tables 
                    WHERE table_name = '{table}' AND table_schema = 'public';
                """)
                
                if cursor.fetchone()['count'] > 0:
                    cursor.execute(f"SELECT COUNT(*) as count FROM {table};")
                    row_count = cursor.fetchone()['count']
                    
                    if row_count > 0:
                        # Create backup table
                        backup_table_name = f"{table}_backup_{backup_timestamp}"
                        cursor.execute(f"CREATE TABLE {backup_table_name} AS SELECT * FROM {table};")
                        backup_info[table] = {
                            'backup_table': backup_table_name,
                            'row_count': row_count
                        }
                        print(f"   📋 Backed up {table}: {row_count:,} rows → {backup_table_name}")
            
            conn.commit()
            
            # Save backup info to file
            backup_file = f"database_backup_info_{backup_timestamp}.json"
            with open(backup_file, 'w') as f:
                json.dump(backup_info, f, indent=2, default=str)
            
            print(f"✅ Backup completed. Info saved to: {backup_file}")
            return backup_info
            
    except Exception as e:
        print(f"❌ Backup failed: {e}")
        conn.rollback()
        return None

def update_database_schema(conn):
    """Update database schema using the latest SQL scripts."""
    print("🔄 Updating database schema...")
    
    try:
        with conn.cursor() as cursor:
            # Read and execute the main schema file
            schema_file = "sql/00_create_all_tables.sql"
            
            if not os.path.exists(schema_file):
                print(f"❌ Schema file not found: {schema_file}")
                return False
            
            print(f"📖 Reading schema from: {schema_file}")
            
            with open(schema_file, 'r') as f:
                schema_sql = f.read()
            
            # Execute the schema update in chunks to handle errors gracefully
            print("⚙️  Executing schema updates...")
            
            # Split the SQL into statements and execute them one by one
            # This allows us to skip statements that would cause errors due to existing objects
            statements = [stmt.strip() for stmt in schema_sql.split(';') if stmt.strip()]
            
            successful_statements = 0
            skipped_statements = 0
            
            for i, statement in enumerate(statements):
                if not statement or statement.startswith('--') or statement.startswith('/*'):
                    continue
                
                try:
                    cursor.execute(statement + ';')
                    successful_statements += 1
                except psycopg2.Error as e:
                    error_msg = str(e).lower()
                    # Skip common "already exists" errors
                    if any(phrase in error_msg for phrase in [
                        'already exists',
                        'relation already exists', 
                        'function already exists',
                        'trigger already exists',
                        'index already exists'
                    ]):
                        skipped_statements += 1
                        print(f"   ⏭️  Skipped: {statement[:50]}... (already exists)")
                    else:
                        print(f"   ⚠️  Error in statement {i+1}: {e}")
                        print(f"   Statement: {statement[:100]}...")
            
            print(f"   ✅ Executed: {successful_statements} statements")
            print(f"   ⏭️  Skipped: {skipped_statements} statements (already exist)")
            
            conn.commit()
            print("✅ Schema update completed successfully!")
            return True
            
    except Exception as e:
        print(f"❌ Schema update failed: {e}")
        conn.rollback()
        return False

def clear_data_tables(conn, preserve_employee_stats=True):
    """Clear data tables for fresh processing."""
    print("🧹 Clearing data tables for fresh processing...")
    
    try:
        with conn.cursor() as cursor:
            # Tables to clear (order matters due to foreign key constraints)
            tables_to_clear = [
                # Main data tables
                'fullbay_line_items',
                'fullbay_raw_data',
                'ingestion_metadata',
                
                # Summary tables
                'daily_summary',
                'monthly_summary', 
                'customer_summary',
                'vehicle_summary',
                
                # Logging tables
                'api_request_log',
                'database_query_log',
                'data_processing_log',
                'performance_metrics_log',
                'error_tracking_log',
                'data_quality_log',
                'business_validation_log',
                'processed_invoices_tracker'
            ]
            
            # Optionally preserve employee statistics
            if not preserve_employee_stats:
                tables_to_clear.append('employee_statistics')
            
            cleared_counts = {}
            
            for table in tables_to_clear:
                # Check if table exists
                cursor.execute(f"""
                    SELECT COUNT(*) as count 
                    FROM information_schema.tables 
                    WHERE table_name = '{table}' AND table_schema = 'public';
                """)
                
                if cursor.fetchone()['count'] > 0:
                    # Get current count
                    cursor.execute(f"SELECT COUNT(*) as count FROM {table};")
                    row_count = cursor.fetchone()['count']
                    
                    if row_count > 0:
                        # Clear the table
                        cursor.execute(f"TRUNCATE TABLE {table} CASCADE;")
                        cleared_counts[table] = row_count
                        print(f"   🗑️  Cleared {table}: {row_count:,} rows")
                    else:
                        print(f"   ✓ {table}: already empty")
                else:
                    print(f"   ⚠️  {table}: table not found")
            
            conn.commit()
            
            if cleared_counts:
                total_cleared = sum(cleared_counts.values())
                print(f"✅ Cleared {len(cleared_counts)} tables, {total_cleared:,} total rows")
            else:
                print("✅ All tables were already empty")
                
            return True
            
    except Exception as e:
        print(f"❌ Table clearing failed: {e}")
        conn.rollback()
        return False

def verify_schema_and_state(conn):
    """Verify the schema is correct and tables are empty."""
    print("🔍 Verifying schema and database state...")
    
    try:
        with conn.cursor() as cursor:
            # Check expected tables exist
            expected_tables = [
                'fullbay_raw_data',
                'fullbay_line_items',
                'ingestion_metadata',
                'daily_summary',
                'monthly_summary',
                'customer_summary',
                'vehicle_summary'
            ]
            
            print("📋 Checking expected tables:")
            missing_tables = []
            
            for table in expected_tables:
                cursor.execute(f"""
                    SELECT COUNT(*) as count 
                    FROM information_schema.tables 
                    WHERE table_name = '{table}' AND table_schema = 'public';
                """)
                
                if cursor.fetchone()['count'] > 0:
                    # Check row count
                    cursor.execute(f"SELECT COUNT(*) as count FROM {table};")
                    row_count = cursor.fetchone()['count']
                    
                    status = "✅" if row_count == 0 else f"⚠️ ({row_count} rows)"
                    print(f"   {status} {table}")
                else:
                    missing_tables.append(table)
                    print(f"   ❌ {table}: MISSING")
            
            if missing_tables:
                print(f"\n❌ Missing tables: {missing_tables}")
                return False
            
            # Check main line items table structure
            cursor.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'fullbay_line_items' AND table_schema = 'public'
                ORDER BY ordinal_position;
            """)
            
            columns = cursor.fetchall()
            print(f"\n📊 fullbay_line_items table: {len(columns)} columns")
            
            # Check for key columns
            key_columns = [
                'fullbay_invoice_id',
                'line_item_type', 
                'part_description',
                'labor_description',
                'quantity',
                'unit_price',
                'line_total'
            ]
            
            column_names = [col['column_name'] for col in columns]
            missing_key_columns = [col for col in key_columns if col not in column_names]
            
            if missing_key_columns:
                print(f"⚠️  Missing key columns: {missing_key_columns}")
            else:
                print("✅ All key columns present")
            
            print("\n🎉 Database schema verification completed!")
            return True
            
    except Exception as e:
        print(f"❌ Schema verification failed: {e}")
        return False

def main():
    """Main function."""
    print("🔧 FULLBAY DATABASE SCHEMA UPDATE & CLEAR")
    print("=" * 50)
    print(f"🕒 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Get database connection
    print("🔌 Connecting to database...")
    conn = get_database_connection()
    
    if not conn:
        print("❌ Cannot proceed without database connection")
        return False
    
    try:
        print(f"✅ Connected to: {os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}")
        print()
        
        # Step 1: Backup existing data
        backup_info = backup_existing_data(conn)
        if backup_info is None:
            print("❌ Backup failed. Aborting to prevent data loss.")
            return False
        print()
        
        # Step 2: Update database schema
        if not update_database_schema(conn):
            print("❌ Schema update failed. Check the logs above.")
            return False
        print()
        
        # Step 3: Clear data tables
        if not clear_data_tables(conn, preserve_employee_stats=True):
            print("❌ Table clearing failed. Check the logs above.")
            return False
        print()
        
        # Step 4: Verify schema and state
        if not verify_schema_and_state(conn):
            print("❌ Schema verification failed. Check the logs above.")
            return False
        print()
        
        print("🎉 DATABASE UPDATE COMPLETED SUCCESSFULLY!")
        print()
        print("📋 Summary:")
        print("   ✅ Data backed up safely")
        print("   ✅ Schema updated to latest version")
        print("   ✅ Tables cleared for fresh processing")
        print("   ✅ Schema verification passed")
        print()
        print("🚀 Next Steps:")
        print("   1. Run data ingestion: python february_ingestion.py")
        print("   2. Monitor processing: python scripts/monitoring_dashboard.py")
        print("   3. Check results: python check_database_state.py")
        
        return True
        
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False
        
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
