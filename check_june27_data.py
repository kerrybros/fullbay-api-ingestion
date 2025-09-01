#!/usr/bin/env python3
"""
Check if any June 27th data was stored in the database.
"""

import os
import sys
import json
from datetime import datetime

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

# Set default environment variables for local testing
os.environ.setdefault('DB_HOST', 'fullbay-postgres-db.cc74yq2i4j8n.us-east-1.rds.amazonaws.com')
os.environ.setdefault('DB_USER', 'postgres')
os.environ.setdefault('DB_PASSWORD', '5255Tillman')
os.environ.setdefault('DB_NAME', 'fullbay_data')
os.environ.setdefault('DB_PORT', '5432')

from config import Config
from database import DatabaseManager

def check_june27_data():
    """Check if any June 27th data was stored."""
    print("🔍 CHECKING JUNE 27TH DATA")
    print("=" * 50)
    
    try:
        # Set up database connection
        config = Config()
        db_manager = DatabaseManager(config)
        db_manager.connect()
        
        with db_manager._get_connection() as conn:
            with conn.cursor() as cursor:
                # Check raw data for June 27th
                cursor.execute("""
                    SELECT 
                        fullbay_invoice_id,
                        created_at
                    FROM fullbay_raw_data 
                    WHERE DATE(created_at) = '2025-08-28'
                    ORDER BY created_at DESC
                    LIMIT 10
                """)
                
                recent_raw = cursor.fetchall()
                print(f"📊 Recent raw data records (last 10):")
                for record in recent_raw:
                    print(f"  📄 Invoice {record['fullbay_invoice_id']} - {record['created_at']}")
                
                # Check line items for June 27th
                cursor.execute("""
                    SELECT 
                        fullbay_invoice_id,
                        invoice_date,
                        created_at
                    FROM fullbay_line_items 
                    WHERE DATE(created_at) = '2025-08-28'
                    ORDER BY created_at DESC
                    LIMIT 10
                """)
                
                recent_line_items = cursor.fetchall()
                print(f"\n📊 Recent line items (last 10):")
                for record in recent_line_items:
                    print(f"  📋 Invoice {record['fullbay_invoice_id']} - {record['invoice_date']} - {record['created_at']}")
                
                # Check for the problematic invoice specifically
                cursor.execute("""
                    SELECT 
                        fullbay_invoice_id,
                        created_at
                    FROM fullbay_raw_data 
                    WHERE fullbay_invoice_id = '17889795'
                """)
                
                problem_invoice = cursor.fetchone()
                if problem_invoice:
                    print(f"\n🔍 PROBLEMATIC INVOICE 17889795:")
                    print(f"  ✅ Found in raw_data: {problem_invoice['created_at']}")
                    
                    # Check if it has any line items
                    cursor.execute("""
                        SELECT COUNT(*) as count
                        FROM fullbay_line_items 
                        WHERE fullbay_invoice_id = '17889795'
                    """)
                    
                    line_count = cursor.fetchone()['count']
                    print(f"  📋 Line items: {line_count}")
                else:
                    print(f"\n🔍 PROBLEMATIC INVOICE 17889795:")
                    print(f"  ❌ Not found in raw_data")
                
                # Check total counts by date
                cursor.execute("""
                    SELECT 
                        DATE(created_at) as date,
                        COUNT(*) as count
                    FROM fullbay_raw_data 
                    GROUP BY DATE(created_at)
                    ORDER BY date DESC
                """)
                
                daily_raw = cursor.fetchall()
                print(f"\n📊 Daily raw data counts:")
                for record in daily_raw:
                    print(f"  📅 {record['date']}: {record['count']} records")
                
                cursor.execute("""
                    SELECT 
                        DATE(created_at) as date,
                        COUNT(*) as count
                    FROM fullbay_line_items 
                    GROUP BY DATE(created_at)
                    ORDER BY date DESC
                """)
                
                daily_line_items = cursor.fetchall()
                print(f"\n📊 Daily line items counts:")
                for record in daily_line_items:
                    print(f"  📅 {record['date']}: {record['count']} items")
        
        db_manager.close()
        
    except Exception as e:
        print(f"❌ Error checking June 27th data: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_june27_data()
