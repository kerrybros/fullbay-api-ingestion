#!/usr/bin/env python3
"""
Get the exact database schema and export data exactly as it exists in the database
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import csv
from datetime import datetime

def get_exact_db_schema():
    """Get the exact database schema."""
    db_params = {
        'host': 'fullbay-postgres-db.cc74yq2i4j8n.us-east-1.rds.amazonaws.com',
        'port': 5432,
        'dbname': 'fullbay_data',
        'user': 'postgres',
        'password': '5255Tillman'
    }
    
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        print("EXACT DATABASE SCHEMA - fullbay_line_items TABLE")
        print("=" * 80)
        
        # Get exact column information
        cursor.execute("""
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default,
                ordinal_position
            FROM information_schema.columns 
            WHERE table_name = 'fullbay_line_items' 
            AND table_schema = 'public'
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        
        print(f"{'Position':<8} {'Column Name':<35} {'Data Type':<20} {'Nullable':<8} {'Default'}")
        print("-" * 80)
        
        for col in columns:
            default = col['column_default'] or 'NULL'
            print(f"{col['ordinal_position']:<8} {col['column_name']:<35} {col['data_type']:<20} {col['is_nullable']:<8} {default}")
        
        print(f"\nTotal columns: {len(columns)}")
        
        # Get table statistics
        cursor.execute("SELECT COUNT(*) as total_rows FROM fullbay_line_items")
        total_rows = cursor.fetchone()['total_rows']
        print(f"Total rows in table: {total_rows}")
        
        # Get April 2025 data count
        cursor.execute("""
            SELECT COUNT(*) as april_count 
            FROM fullbay_line_items 
            WHERE invoice_date >= '2025-04-01' AND invoice_date <= '2025-04-30'
        """)
        april_count = cursor.fetchone()['april_count']
        print(f"April 2025 rows: {april_count}")
        
        return columns
        
    except Exception as e:
        print(f"Error: {e}")
        return None
    finally:
        if 'conn' in locals():
            conn.close()

def export_exact_db_data():
    """Export data exactly as it exists in the database."""
    db_params = {
        'host': 'fullbay-postgres-db.cc74yq2i4j8n.us-east-1.rds.amazonaws.com',
        'port': 5432,
        'dbname': 'fullbay_data',
        'user': 'postgres',
        'password': '5255Tillman'
    }
    
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        print("\nEXPORTING EXACT DATABASE DATA")
        print("=" * 50)
        
        # Get all column names in exact order
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns 
            WHERE table_name = 'fullbay_line_items' 
            AND table_schema = 'public'
            ORDER BY ordinal_position
        """)
        
        columns = [row['column_name'] for row in cursor.fetchall()]
        
        # Build SELECT statement with all columns in exact order
        select_columns = ', '.join(columns)
        
        # Query to get April 2025 data with ALL columns
        query = f"""
        SELECT {select_columns}
        FROM fullbay_line_items 
        WHERE invoice_date >= '2025-04-01' 
        AND invoice_date <= '2025-04-30'
        ORDER BY invoice_date, fullbay_invoice_id, id
        """
        
        print("Executing query with ALL columns...")
        cursor.execute(query)
        
        rows = cursor.fetchall()
        
        if not rows:
            print("No data found for April 2025")
            return
        
        print(f"Found {len(rows)} rows for April 2025")
        
        # Create CSV filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        csv_filename = f"april_2025_exact_db_data_{timestamp}.csv"
        
        # Write to CSV with exact column order
        print(f"Writing exact database data to {csv_filename}...")
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=columns)
            
            # Write header
            writer.writeheader()
            
            # Write data rows exactly as they are
            for row in rows:
                writer.writerow(row)
        
        print(f"✅ Successfully exported {len(rows)} records to {csv_filename}")
        print(f"📁 File: {csv_filename}")
        print(f"📊 Columns exported: {len(columns)}")
        
        # Show first few column names for verification
        print(f"\nFirst 10 columns: {', '.join(columns[:10])}")
        print(f"Last 10 columns: {', '.join(columns[-10:])}")
        
    except Exception as e:
        print(f"❌ Error exporting data: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    # First get the exact schema
    columns = get_exact_db_schema()
    
    if columns:
        # Then export the exact data
        export_exact_db_data()
