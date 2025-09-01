#!/usr/bin/env python3
"""
Export April 2025 data with final schema from database
- Changes customer_title to customer
- Removes primary_technician_number
- Creates CSV schema directly from database schema
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import csv
from datetime import datetime

def export_final_schema_from_db():
    """Export data with final schema from database."""
    print("EXPORTING FINAL SCHEMA FROM DATABASE")
    print("=" * 50)
    
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
        
        # Get exact database schema
        cursor.execute("""
            SELECT column_name, ordinal_position
            FROM information_schema.columns 
            WHERE table_name = 'fullbay_line_items' 
            AND table_schema = 'public'
            ORDER BY ordinal_position
        """)
        
        db_columns = cursor.fetchall()
        print(f"Database has {len(db_columns)} columns")
        
        # Create CSV column mapping with requested changes
        csv_columns = []
        select_columns = []
        
        for col in db_columns:
            db_col_name = col['column_name']
            
            # Apply requested changes
            if db_col_name == 'customer_title':
                csv_col_name = 'customer'
                select_col = 'customer_title as customer'
            elif db_col_name == 'primary_technician_number':
                # Skip this column entirely
                continue
            else:
                csv_col_name = db_col_name
                select_col = db_col_name
            
            csv_columns.append(csv_col_name)
            select_columns.append(select_col)
        
        print(f"CSV will have {len(csv_columns)} columns")
        
        # Build SELECT statement
        select_statement = ', '.join(select_columns)
        
        # Query April 2025 data
        query = f"""
        SELECT {select_statement}
        FROM fullbay_line_items 
        WHERE invoice_date >= '2025-04-01' 
        AND invoice_date <= '2025-04-30'
        ORDER BY invoice_date, fullbay_invoice_id, id
        """
        
        print("Executing query...")
        cursor.execute(query)
        
        rows = cursor.fetchall()
        
        if not rows:
            print("No data found for April 2025")
            return
        
        print(f"Found {len(rows)} rows for April 2025")
        
        # Create filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"april_2025_final_schema_{timestamp}.csv"
        
        print(f"Exporting to: {filename}")
        print(f"Columns: {len(csv_columns)}")
        
        # Write to CSV
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
            writer.writeheader()
            
            for row in rows:
                # Convert row to dict and handle any None values
                row_dict = {}
                for col in csv_columns:
                    value = row[col]
                    if value is None:
                        row_dict[col] = ''
                    else:
                        row_dict[col] = str(value)
                writer.writerow(row_dict)
        
        print(f"✅ Successfully exported {len(rows)} records to {filename}")
        print(f"📊 File size: {len(rows)} rows × {len(csv_columns)} columns")
        
        # Show column names
        print(f"\n📋 CSV COLUMN NAMES:")
        for i, col in enumerate(csv_columns):
            print(f"   {i+1:2d}. {col}")
        
        # Show some statistics
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
        
        stats = cursor.fetchone()
        print(f"\n📈 SUMMARY STATISTICS:")
        print(f"   Total Line Items: {stats['total_line_items']:,}")
        print(f"   Unique Invoices: {stats['unique_invoices']:,}")
        print(f"   Unique Customers: {stats['unique_customers']:,}")
        print(f"   Total Value: ${stats['total_value']:,.2f}")
        
        # Show line item breakdown
        cursor.execute("""
            SELECT 
                line_item_type,
                COUNT(*) as count,
                SUM(total) as total_value
            FROM fullbay_line_items 
            WHERE invoice_date >= '2025-04-01' 
            AND invoice_date <= '2025-04-30'
            GROUP BY line_item_type
            ORDER BY count DESC
        """)
        
        breakdown = cursor.fetchall()
        print(f"\n📦 LINE ITEM BREAKDOWN:")
        for item in breakdown:
            print(f"   {item['line_item_type']}: {item['count']} items (${item['total_value']:,.2f})")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        raise

if __name__ == "__main__":
    export_final_schema_from_db()
