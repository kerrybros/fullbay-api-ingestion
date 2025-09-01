#!/usr/bin/env python3
"""
Check current database status after April ingestion
"""

import psycopg2

def check_db_status():
    try:
        # Connect to database
        conn = psycopg2.connect(
            host='fullbay-postgres-db.cc74yq2i4j8n.us-east-1.rds.amazonaws.com',
            port=5432,
            dbname='fullbay_data',
            user='postgres',
            password='5255Tillman'
        )
        
        cursor = conn.cursor()
        
        # Check total records
        cursor.execute('SELECT COUNT(*) FROM fullbay_line_items')
        total_line_items = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM fullbay_raw_data')
        total_raw_data = cursor.fetchone()[0]
        
        print(f"📊 DATABASE STATUS AFTER APRIL INGESTION")
        print("=" * 50)
        print(f"Total line items: {total_line_items:,}")
        print(f"Total raw data records: {total_raw_data:,}")
        print()
        
        if total_line_items > 0:
            # Check date range
            cursor.execute('SELECT MIN(invoice_date), MAX(invoice_date) FROM fullbay_line_items')
            date_range = cursor.fetchone()
            print(f"📅 Date range: {date_range[0]} to {date_range[1]}")
            
            # Check unique dates
            cursor.execute('SELECT COUNT(DISTINCT invoice_date) FROM fullbay_line_items')
            unique_dates = cursor.fetchone()[0]
            print(f"📅 Unique dates processed: {unique_dates}")
            
            # Check line item breakdown
            cursor.execute('''
                SELECT line_item_type, COUNT(*) 
                FROM fullbay_line_items 
                GROUP BY line_item_type 
                ORDER BY COUNT(*) DESC
            ''')
            line_types = cursor.fetchall()
            
            print(f"\n🏷️  LINE ITEM BREAKDOWN:")
            print("-" * 30)
            for line_type, count in line_types:
                print(f"  {line_type}: {count:,}")
            
            # Check if we have April data
            cursor.execute('''
                SELECT COUNT(*) 
                FROM fullbay_line_items 
                WHERE invoice_date >= '2025-04-01' AND invoice_date <= '2025-04-30'
            ''')
            april_count = cursor.fetchone()[0]
            print(f"\n📅 April 2025 line items: {april_count:,}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_db_status()
