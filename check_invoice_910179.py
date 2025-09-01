#!/usr/bin/env python3
"""
Check if invoice number 910179 exists in the database
"""

import psycopg2

def check_invoice():
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
        
        # Check if invoice exists
        cursor.execute('SELECT COUNT(*) FROM fullbay_line_items WHERE invoice_number = %s', ('910179',))
        count = cursor.fetchone()[0]
        
        print(f"Invoice 910179: {count} line items found")
        
        if count > 0:
            # Get sample line items
            cursor.execute('''
                SELECT id, line_item_type, part_description, labor_description, line_total, quantity
                FROM fullbay_line_items 
                WHERE invoice_number = %s 
                LIMIT 10
            ''', ('910179',))
            
            rows = cursor.fetchall()
            print(f"\nSample line items for invoice 910179:")
            print("-" * 80)
            
            for row in rows:
                id_val, line_type, part_desc, labor_desc, line_total, qty = row
                description = part_desc if part_desc else labor_desc
                print(f"ID: {id_val:4} | Type: {line_type:15} | Qty: {qty:3} | Total: ${line_total:8.2f} | {description}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_invoice()
