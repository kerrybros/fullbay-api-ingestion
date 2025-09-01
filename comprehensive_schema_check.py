#!/usr/bin/env python3
"""
Comprehensive schema check to verify all 75 columns exist
"""

import psycopg2
from psycopg2.extras import RealDictCursor

def comprehensive_schema_check():
    """Check the complete database schema."""
    print("🔍 COMPREHENSIVE SCHEMA CHECK")
    print("=" * 60)
    
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
        
        # Get all columns with their details
        cursor.execute("""
            SELECT column_name, data_type, ordinal_position, is_nullable, column_default
            FROM information_schema.columns 
            WHERE table_name = 'fullbay_line_items' 
            AND table_schema = 'public'
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        
        print(f"📊 TOTAL COLUMNS: {len(columns)}")
        print()
        
        print("📋 COMPLETE COLUMN LIST:")
        print("-" * 80)
        print(f"{'Pos':<4} {'Column Name':<30} {'Type':<20} {'Nullable':<8} {'Default':<15}")
        print("-" * 80)
        
        for col in columns:
            nullable = "YES" if col['is_nullable'] == 'YES' else "NO"
            default = str(col['column_default']) if col['column_default'] else "NULL"
            if len(default) > 14:
                default = default[:11] + "..."
            
            print(f"{col['ordinal_position']:<4} {col['column_name']:<30} {col['data_type']:<20} {nullable:<8} {default:<15}")
        
        print("-" * 80)
        
        # Check for specific important columns
        print("\n🎯 KEY COLUMNS CHECK:")
        print("-" * 40)
        
        important_columns = [
            'service_description', 'so_supplies_total', 'line_tax', 'sales_total',
            'line_total_price', 'taxable', 'tax_rate'
        ]
        
        for col_name in important_columns:
            cursor.execute("""
                SELECT column_name, ordinal_position
                FROM information_schema.columns 
                WHERE table_name = 'fullbay_line_items' 
                AND table_schema = 'public'
                AND column_name = %s
            """, (col_name,))
            
            result = cursor.fetchone()
            if result:
                print(f"   ✅ {col_name:<20} - Position {result['ordinal_position']}")
            else:
                print(f"   ❌ {col_name:<20} - MISSING")
        
        # Check column count by category
        print("\n📊 COLUMN COUNT BY CATEGORY:")
        print("-" * 40)
        
        # Count different data types
        cursor.execute("""
            SELECT data_type, COUNT(*) as count
            FROM information_schema.columns 
            WHERE table_name = 'fullbay_line_items' 
            AND table_schema = 'public'
            GROUP BY data_type
            ORDER BY count DESC
        """)
        
        type_counts = cursor.fetchall()
        for type_count in type_counts:
            print(f"   {type_count['data_type']:<20}: {type_count['count']} columns")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        if 'conn' in locals():
            conn.rollback()
        raise

if __name__ == "__main__":
    comprehensive_schema_check()
