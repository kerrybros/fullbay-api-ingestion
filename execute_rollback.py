#!/usr/bin/env python3
"""
Execute the rollback to original 75-column schema
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import os

def execute_rollback():
    """Execute the schema rollback."""
    print("ROLLBACK TO ORIGINAL 75-COLUMN SCHEMA")
    print("=" * 60)
    
    # Database connection parameters
    db_params = {
        'host': 'fullbay-postgres-db.cc74yq2i4j8n.us-east-1.rds.amazonaws.com',
        'port': 5432,
        'dbname': 'fullbay_data',
        'user': 'postgres',
        'password': '5255Tillman'
    }
    
    try:
        # Connect to database
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        print("Connected to database successfully")
        
        # Check current state
        cursor.execute("SELECT COUNT(*) as current_rows FROM fullbay_line_items")
        current_rows = cursor.fetchone()['current_rows']
        print(f"Current rows in table: {current_rows}")
        
        cursor.execute("""
            SELECT COUNT(*) as current_columns 
            FROM information_schema.columns 
            WHERE table_name = 'fullbay_line_items' 
            AND table_schema = 'public'
        """)
        current_columns = cursor.fetchone()['current_columns']
        print(f"Current columns: {current_columns}")
        
        # Read the rollback SQL
        with open('rollback_to_original_schema.sql', 'r') as f:
            rollback_sql = f.read()
        
        print("\nExecuting rollback...")
        print("This will:")
        print("1. Create a backup of current data")
        print("2. Drop the current table")
        print("3. Recreate with original 75-column structure")
        print("4. Restore data with proper column mapping")
        
        # Execute the rollback
        cursor.execute(rollback_sql)
        
        # Commit the changes
        conn.commit()
        
        print("✅ Rollback completed successfully!")
        
        # Verify the rollback
        cursor.execute("SELECT COUNT(*) as new_rows FROM fullbay_line_items")
        new_rows = cursor.fetchone()['new_rows']
        
        cursor.execute("""
            SELECT COUNT(*) as new_columns 
            FROM information_schema.columns 
            WHERE table_name = 'fullbay_line_items' 
            AND table_schema = 'public'
        """)
        new_columns = cursor.fetchone()['new_columns']
        
        print(f"\n📊 Rollback Results:")
        print(f"  Rows preserved: {new_rows} (was {current_rows})")
        print(f"  Columns: {new_columns} (was {current_columns})")
        
        # Show the new column structure
        cursor.execute("""
            SELECT column_name, data_type, ordinal_position
            FROM information_schema.columns 
            WHERE table_name = 'fullbay_line_items' 
            AND table_schema = 'public'
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        
        print(f"\n📋 New Column Structure ({len(columns)} columns):")
        print("-" * 60)
        
        for col in columns:
            print(f"{col['ordinal_position']:2d}. {col['column_name']:<30} {col['data_type']}")
        
        # Test data integrity
        cursor.execute("""
            SELECT 
                COUNT(*) as total_line_items,
                COUNT(DISTINCT fullbay_invoice_id) as unique_invoices,
                SUM(line_total_price) as total_value
            FROM fullbay_line_items
        """)
        
        stats = cursor.fetchone()
        print(f"\n✅ Data Integrity Check:")
        print(f"  Total line items: {stats['total_line_items']}")
        print(f"  Unique invoices: {stats['unique_invoices']}")
        print(f"  Total value: ${stats['total_value']:,.2f}")
        
        print(f"\n🎉 Rollback to original 75-column schema completed successfully!")
        
    except Exception as e:
        print(f"❌ Error during rollback: {e}")
        if 'conn' in locals():
            conn.rollback()
        raise
    finally:
        if 'conn' in locals():
            conn.close()
            print("Database connection closed")

if __name__ == "__main__":
    execute_rollback()
