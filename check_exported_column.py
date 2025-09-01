#!/usr/bin/env python3
"""
Check what the exported column is used for
"""

import psycopg2
from psycopg2.extras import RealDictCursor

def check_exported_column():
    """Check what the exported column is used for."""
    print("CHECKING EXPORTED COLUMN USAGE")
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
        
        # Check exported column values
        cursor.execute("""
            SELECT 
                exported,
                COUNT(*) as count,
                COUNT(DISTINCT fullbay_invoice_id) as unique_invoices
            FROM fullbay_line_items 
            WHERE invoice_date >= '2025-04-01' 
            AND invoice_date <= '2025-04-30'
            GROUP BY exported
            ORDER BY exported
        """)
        
        results = cursor.fetchall()
        print("📊 EXPORTED COLUMN VALUES:")
        for row in results:
            print(f"   exported = {row['exported']}: {row['count']} line items, {row['unique_invoices']} invoices")
        
        # Check if it's related to invoice export status
        cursor.execute("""
            SELECT 
                fullbay_invoice_id,
                invoice_number,
                exported,
                COUNT(*) as line_items
            FROM fullbay_line_items 
            WHERE invoice_date >= '2025-04-01' 
            AND invoice_date <= '2025-04-30'
            GROUP BY fullbay_invoice_id, invoice_number, exported
            ORDER BY fullbay_invoice_id
            LIMIT 10
        """)
        
        print("\n🔍 SAMPLE INVOICES WITH EXPORTED STATUS:")
        for row in cursor.fetchall():
            print(f"   Invoice {row['fullbay_invoice_id']} ({row['invoice_number']}): exported={row['exported']}, {row['line_items']} line items")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        raise

if __name__ == "__main__":
    check_exported_column()
