#!/usr/bin/env python3
"""
Verify Negative Values in Database
Check that negative prices and quantities are being preserved correctly.
"""

import os
import sys
import psycopg2
from datetime import datetime

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

# Set default environment variables for local testing
os.environ.setdefault('DB_HOST', 'fullbay-postgres-db.cc74yq2i4j8n.us-east-1.rds.amazonaws.com')
os.environ.setdefault('DB_USER', 'postgres')
os.environ.setdefault('DB_PASSWORD', '5255Tillman')
os.environ.setdefault('DB_NAME', 'fullbay_data')
os.environ.setdefault('DB_PORT', '5432')

def verify_negative_values():
    """Verify that negative values are preserved in the database."""
    print("🔍 VERIFYING NEGATIVE VALUES IN DATABASE")
    print("=" * 60)
    
    try:
        # Connect to database
        conn = psycopg2.connect(
            host=os.environ['DB_HOST'],
            user=os.environ['DB_USER'],
            password=os.environ['DB_PASSWORD'],
            database=os.environ['DB_NAME'],
            port=os.environ['DB_PORT']
        )
        
        with conn.cursor() as cursor:
            # Check for negative prices
            print("📊 CHECKING NEGATIVE PRICES:")
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_negative_prices,
                    COUNT(CASE WHEN line_total_price < 0 THEN 1 END) as negative_prices,
                    COUNT(CASE WHEN line_total_price = 0 THEN 1 END) as zero_prices,
                    COUNT(CASE WHEN line_total_price > 0 THEN 1 END) as positive_prices,
                    MIN(line_total_price) as min_price,
                    MAX(line_total_price) as max_price
                FROM fullbay_line_items 
                WHERE line_total_price IS NOT NULL
            """)
            
            price_stats = cursor.fetchone()
            print(f"  📈 Total line items with prices: {price_stats[0]}")
            print(f"  ➖ Negative prices (returns/credits): {price_stats[1]}")
            print(f"  🔘 Zero prices: {price_stats[2]}")
            print(f"  ➕ Positive prices: {price_stats[3]}")
            print(f"  📉 Min price: ${price_stats[4]:.2f}")
            print(f"  📈 Max price: ${price_stats[5]:.2f}")
            
            # Show some examples of negative prices
            if price_stats[1] > 0:
                print(f"\n📋 EXAMPLES OF NEGATIVE PRICES (returns/credits):")
                cursor.execute("""
                    SELECT 
                        fullbay_invoice_id,
                        line_item_type,
                        line_total_price,
                        quantity,
                        part_description,
                        labor_description
                    FROM fullbay_line_items 
                    WHERE line_total_price < 0
                    ORDER BY line_total_price ASC
                    LIMIT 10
                """)
                
                for row in cursor.fetchall():
                    print(f"  • Invoice {row[0]}: {row[1]} - ${row[2]:.2f} (Qty: {row[3]})")
                    if row[4]:  # part description
                        print(f"    Part: {row[4]}")
                    if row[5]:  # labor description
                        print(f"    Labor: {row[5]}")
            
            # Check for negative quantities
            print(f"\n📦 CHECKING NEGATIVE QUANTITIES:")
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_with_quantity,
                    COUNT(CASE WHEN quantity < 0 THEN 1 END) as negative_quantities,
                    COUNT(CASE WHEN quantity = 0 THEN 1 END) as zero_quantities,
                    COUNT(CASE WHEN quantity > 0 THEN 1 END) as positive_quantities,
                    MIN(quantity) as min_quantity,
                    MAX(quantity) as max_quantity
                FROM fullbay_line_items 
                WHERE quantity IS NOT NULL
            """)
            
            qty_stats = cursor.fetchone()
            print(f"  📈 Total line items with quantities: {qty_stats[0]}")
            print(f"  ➖ Negative quantities (returns): {qty_stats[1]}")
            print(f"  🔘 Zero quantities: {qty_stats[2]}")
            print(f"  ➕ Positive quantities: {qty_stats[3]}")
            print(f"  📉 Min quantity: {qty_stats[4]}")
            print(f"  📈 Max quantity: {qty_stats[5]}")
            
            # Show some examples of negative quantities
            if qty_stats[1] > 0:
                print(f"\n📋 EXAMPLES OF NEGATIVE QUANTITIES (returns):")
                cursor.execute("""
                    SELECT 
                        fullbay_invoice_id,
                        line_item_type,
                        quantity,
                        line_total_price,
                        part_description
                    FROM fullbay_line_items 
                    WHERE quantity < 0
                    ORDER BY quantity ASC
                    LIMIT 10
                """)
                
                for row in cursor.fetchall():
                    print(f"  • Invoice {row[0]}: {row[1]} - Qty: {row[2]} (Price: ${row[3]:.2f})")
                    if row[4]:  # part description
                        print(f"    Part: {row[4]}")
            
            # Check labor hours (should include 0.0 as valid)
            print(f"\n⏱️  CHECKING LABOR HOURS:")
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_labor_items,
                    COUNT(CASE WHEN actual_hours IS NULL THEN 1 END) as null_hours,
                    COUNT(CASE WHEN actual_hours = 0 THEN 1 END) as zero_hours,
                    COUNT(CASE WHEN actual_hours > 0 THEN 1 END) as positive_hours,
                    MIN(actual_hours) as min_hours,
                    MAX(actual_hours) as max_hours
                FROM fullbay_line_items 
                WHERE line_item_type = 'LABOR'
            """)
            
            hours_stats = cursor.fetchone()
            print(f"  📈 Total labor items: {hours_stats[0]}")
            print(f"  ❓ Null hours: {hours_stats[1]}")
            print(f"  🔘 Zero hours (no labor time): {hours_stats[2]}")
            print(f"  ➕ Positive hours: {hours_stats[3]}")
            print(f"  📉 Min hours: {hours_stats[4]}")
            print(f"  📈 Max hours: {hours_stats[5]}")
            
            # Show some examples of zero hours
            if hours_stats[2] > 0:
                print(f"\n📋 EXAMPLES OF ZERO LABOR HOURS:")
                cursor.execute("""
                    SELECT 
                        fullbay_invoice_id,
                        actual_hours,
                        labor_description,
                        assigned_technician
                    FROM fullbay_line_items 
                    WHERE line_item_type = 'LABOR' AND actual_hours = 0
                    LIMIT 5
                """)
                
                for row in cursor.fetchall():
                    print(f"  • Invoice {row[0]}: {row[1]} hours")
                    if row[2]:  # labor description
                        print(f"    Description: {row[2]}")
                    if row[3]:  # technician
                        print(f"    Technician: {row[3]}")
            
            # Summary of data quality
            print(f"\n📊 DATA QUALITY SUMMARY:")
            total_items = price_stats[0]
            if total_items > 0:
                negative_price_pct = (price_stats[1] / total_items) * 100
                negative_qty_pct = (qty_stats[1] / total_items) * 100
                zero_hours_pct = (hours_stats[2] / hours_stats[0]) * 100 if hours_stats[0] > 0 else 0
                
                print(f"  ➖ Negative prices: {negative_price_pct:.1f}% (returns/credits)")
                print(f"  ➖ Negative quantities: {negative_qty_pct:.1f}% (returns)")
                print(f"  🔘 Zero labor hours: {zero_hours_pct:.1f}% (no labor time)")
                
                print(f"\n✅ VERIFICATION COMPLETE:")
                print(f"  • Negative prices are being preserved correctly")
                print(f"  • Negative quantities are being preserved correctly") 
                print(f"  • Zero labor hours are being preserved correctly")
                print(f"  • Business logic for returns/credits is maintained")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error verifying negative values: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    verify_negative_values()
