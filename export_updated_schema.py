#!/usr/bin/env python3
"""
Export April 2025 data with updated schema structure
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import csv
from datetime import datetime

def export_updated_schema():
    """Export data with updated schema."""
    print("EXPORTING WITH UPDATED SCHEMA")
    print("=" * 40)
    
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
        
        # Get all line items for April 2025 with updated schema
        cursor.execute("""
            SELECT * FROM fullbay_line_items 
            WHERE invoice_date >= '2025-04-01' 
            AND invoice_date <= '2025-04-30'
            ORDER BY invoice_date, fullbay_invoice_id, id
        """)
        
        rows = cursor.fetchall()
        
        if not rows:
            print("No data found for April 2025")
            return
        
        # Get column names
        columns = [desc[0] for desc in cursor.description]
        
        # Create filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"april_2025_updated_schema_{timestamp}.csv"
        
        print(f"Found {len(rows)} line items")
        print(f"Exporting to: {filename}")
        print(f"Columns: {len(columns)}")
        
        # Write to CSV
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=columns)
            writer.writeheader()
            
            for row in rows:
                # Convert row to dict and handle any None values
                row_dict = {}
                for col in columns:
                    value = row[col]
                    if value is None:
                        row_dict[col] = ''
                    else:
                        row_dict[col] = str(value)
                writer.writerow(row_dict)
        
        print(f"✅ Successfully exported {len(rows)} records to {filename}")
        print(f"📊 File size: {len(rows)} rows × {len(columns)} columns")
        
        # Show column names
        print(f"\n📋 COLUMN NAMES:")
        for i, col in enumerate(columns):
            print(f"   {i+1:2d}. {col}")
        
        # Show some statistics
        cursor.execute("""
            SELECT 
                COUNT(*) as total_line_items,
                COUNT(DISTINCT fullbay_invoice_id) as unique_invoices,
                COUNT(DISTINCT customer_id) as unique_customers,
                SUM(total) as total_value,
                line_item_type,
                COUNT(*) as count
            FROM fullbay_line_items 
            WHERE invoice_date >= '2025-04-01' 
            AND invoice_date <= '2025-04-30'
            GROUP BY line_item_type
            ORDER BY count DESC
        """)
        
        stats = cursor.fetchall()
        print(f"\n📈 LINE ITEM BREAKDOWN:")
        for stat in stats:
            print(f"   {stat['line_item_type']}: {stat['count']} items")
        
        cursor.execute("""
            SELECT 
                COUNT(*) as total_line_items,
                COUNT(DISTINCT fullbay_invoice_id) as unique_invoices,
                COUNT(DISTINCT customer_id) as unique_customers,
                SUM(total) as total_value
            FROM fullbay_line_items 
            WHERE invoice_date >= '2025-04-01' 
            AND invoice_date <= '2025-04-30'
        """)
        
        summary = cursor.fetchone()
        print(f"\n📋 SUMMARY:")
        print(f"   Total Line Items: {summary['total_line_items']}")
        print(f"   Unique Invoices: {summary['unique_invoices']}")
        print(f"   Unique Customers: {summary['unique_customers']}")
        print(f"   Total Value: ${summary['total_value']:,.2f}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        raise

if __name__ == "__main__":
    export_updated_schema()
