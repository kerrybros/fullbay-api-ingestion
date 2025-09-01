#!/usr/bin/env python3
"""
Export database data to CSV that matches exactly to the database schema and data
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import csv
from datetime import datetime
import os

def get_database_connection():
    """Get database connection."""
    db_params = {
        'host': 'fullbay-postgres-db.cc74yq2i4j8n.us-east-1.rds.amazonaws.com',
        'port': 5432,
        'dbname': 'fullbay_data',
        'user': 'postgres',
        'password': '5255Tillman'
    }
    
    try:
        conn = psycopg2.connect(**db_params)
        return conn
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return None

def get_database_schema():
    """Get the exact database schema."""
    conn = get_database_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
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
        cursor.close()
        return columns
    except Exception as e:
        print(f"❌ Error getting schema: {e}")
        return None
    finally:
        conn.close()

def get_database_statistics():
    """Get database statistics."""
    conn = get_database_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Total rows
        cursor.execute("SELECT COUNT(*) as total_rows FROM fullbay_line_items")
        total_rows = cursor.fetchone()['total_rows']
        
        # April 2025 data
        cursor.execute("""
            SELECT COUNT(*) as april_rows 
            FROM fullbay_line_items 
            WHERE invoice_date >= '2025-04-01' AND invoice_date <= '2025-04-30'
        """)
        april_rows = cursor.fetchone()['april_rows']
        
        # Line item type breakdown
        cursor.execute("""
            SELECT line_item_type, COUNT(*) as count
            FROM fullbay_line_items 
            WHERE invoice_date >= '2025-04-01' AND invoice_date <= '2025-04-30'
            GROUP BY line_item_type
            ORDER BY count DESC
        """)
        line_item_breakdown = cursor.fetchall()
        
        # Invoice count
        cursor.execute("""
            SELECT COUNT(DISTINCT fullbay_invoice_id) as invoice_count
            FROM fullbay_line_items 
            WHERE invoice_date >= '2025-04-01' AND invoice_date <= '2025-04-30'
        """)
        invoice_count = cursor.fetchone()['invoice_count']
        
        # Customer count
        cursor.execute("""
            SELECT COUNT(DISTINCT customer_id) as customer_count
            FROM fullbay_line_items 
            WHERE invoice_date >= '2025-04-01' AND invoice_date <= '2025-04-30'
        """)
        customer_count = cursor.fetchone()['customer_count']
        
        cursor.close()
        
        return {
            'total_rows': total_rows,
            'april_rows': april_rows,
            'line_item_breakdown': line_item_breakdown,
            'invoice_count': invoice_count,
            'customer_count': customer_count
        }
    except Exception as e:
        print(f"❌ Error getting statistics: {e}")
        return None
    finally:
        conn.close()

def export_database_to_csv():
    """Export database data to CSV with exact schema match."""
    print("📊 EXPORTING DATABASE TO CSV - EXACT SCHEMA MATCH")
    print("=" * 60)
    
    # Get schema and statistics
    schema = get_database_schema()
    stats = get_database_statistics()
    
    if not schema or not stats:
        print("❌ Failed to get required data")
        return None
    
    print(f"📋 Database Schema: {len(schema)} columns")
    print(f"📊 Total Records: {stats['total_rows']:,}")
    print(f"📅 April 2025 Records: {stats['april_rows']:,}")
    
    # Create CSV filename
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_filename = f"fullbay_database_export_{timestamp}.csv"
    
    print(f"\n📄 Creating CSV: {csv_filename}")
    
    # Get all column names in exact order, excluding exported
    column_names = [col['column_name'] for col in schema if col['column_name'] != 'exported']
    
    # Connect to database and export data
    conn = get_database_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Build SELECT statement with all columns in exact order
        select_columns = ', '.join(column_names)
        
        # Query to get all data with ALL columns
        query = f"""
        SELECT {select_columns}
        FROM fullbay_line_items 
        ORDER BY invoice_date, fullbay_invoice_id, id
        """
        
        print("🔍 Executing query with ALL columns...")
        cursor.execute(query)
        
        rows = cursor.fetchall()
        
        if not rows:
            print("❌ No data found in database")
            return None
        
        print(f"✅ Found {len(rows):,} rows to export")
        
        # Write to CSV with exact column order
        print(f"💾 Writing data to {csv_filename}...")
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=column_names)
            
            # Write header
            writer.writeheader()
            
            # Write data rows exactly as they are
            for row in rows:
                writer.writerow(row)
        
        print(f"✅ Successfully exported {len(rows):,} records to {csv_filename}")
        print(f"📁 File location: {os.path.abspath(csv_filename)}")
        print(f"📊 Columns exported: {len(column_names)}")
        
        # Show column information
        print(f"\n📋 COLUMN BREAKDOWN:")
        print(f"   - Total columns: {len(column_names)}")
        print(f"   - First 5 columns: {', '.join(column_names[:5])}")
        print(f"   - Last 5 columns: {', '.join(column_names[-5:])}")
        
        # Show data statistics
        print(f"\n📈 DATA STATISTICS:")
        print(f"   - Total records: {len(rows):,}")
        print(f"   - File size: {os.path.getsize(csv_filename):,} bytes")
        
        # Show line item breakdown
        if stats['line_item_breakdown']:
            print(f"\n🏷️  LINE ITEM BREAKDOWN:")
            for item in stats['line_item_breakdown']:
                print(f"   - {item['line_item_type']}: {item['count']:,}")
        
        cursor.close()
        return csv_filename
        
    except Exception as e:
        print(f"❌ Error exporting data: {e}")
        return None
    finally:
        conn.close()

def export_april_2025_only():
    """Export only April 2025 data to CSV."""
    print("📊 EXPORTING APRIL 2025 DATA TO CSV")
    print("=" * 50)
    
    # Get schema and statistics
    schema = get_database_schema()
    stats = get_database_statistics()
    
    if not schema or not stats:
        print("❌ Failed to get required data")
        return None
    
    # Create CSV filename
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_filename = f"april_2025_database_export_{timestamp}.csv"
    
    print(f"📄 Creating CSV: {csv_filename}")
    
    # Get all column names in exact order, excluding exported
    column_names = [col['column_name'] for col in schema if col['column_name'] != 'exported']
    
    # Connect to database and export data
    conn = get_database_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Build SELECT statement with all columns in exact order
        select_columns = ', '.join(column_names)
        
        # Query to get April 2025 data with ALL columns
        query = f"""
        SELECT {select_columns}
        FROM fullbay_line_items 
        WHERE invoice_date >= '2025-04-01' AND invoice_date <= '2025-04-30'
        ORDER BY invoice_date, fullbay_invoice_id, id
        """
        
        print("🔍 Executing query for April 2025 data...")
        cursor.execute(query)
        
        rows = cursor.fetchall()
        
        if not rows:
            print("❌ No April 2025 data found")
            return None
        
        print(f"✅ Found {len(rows):,} April 2025 rows to export")
        
        # Write to CSV with exact column order
        print(f"💾 Writing April 2025 data to {csv_filename}...")
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=column_names)
            
            # Write header
            writer.writeheader()
            
            # Write data rows exactly as they are
            for row in rows:
                writer.writerow(row)
        
        print(f"✅ Successfully exported {len(rows):,} April 2025 records to {csv_filename}")
        print(f"📁 File location: {os.path.abspath(csv_filename)}")
        print(f"📊 Columns exported: {len(column_names)}")
        
        cursor.close()
        return csv_filename
        
    except Exception as e:
        print(f"❌ Error exporting April 2025 data: {e}")
        return None
    finally:
        conn.close()

if __name__ == "__main__":
    print("Choose export option:")
    print("1. Export ALL database data")
    print("2. Export April 2025 data only")
    
    choice = input("Enter choice (1 or 2): ").strip()
    
    if choice == "1":
        csv_file = export_database_to_csv()
        if csv_file:
            print(f"\n🎉 Full database export completed: {csv_file}")
        else:
            print("\n❌ Failed to export full database")
    elif choice == "2":
        csv_file = export_april_2025_only()
        if csv_file:
            print(f"\n🎉 April 2025 export completed: {csv_file}")
        else:
            print("\n❌ Failed to export April 2025 data")
    else:
        print("❌ Invalid choice. Please run again and select 1 or 2.")
