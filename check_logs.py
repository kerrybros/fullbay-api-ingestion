#!/usr/bin/env python3
"""
Log checking and monitoring script for Fullbay API ingestion.

This script provides comprehensive visibility into the ingestion process,
including database queries, log analysis, and data quality checks.
"""

import os
import sys
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

# Set default environment variables for local testing
os.environ.setdefault('DB_HOST', 'fullbay-postgres-db.cc74yq2i4j8n.us-east-1.rds.amazonaws.com')
os.environ.setdefault('DB_USER', 'postgres')
os.environ.setdefault('DB_PASSWORD', '5255Tillman')
os.environ.setdefault('DB_NAME', 'fullbay_data')
os.environ.setdefault('DB_PORT', '5432')
os.environ.setdefault('FULLBAY_API_KEY', '4b9fcc18-1f24-09fb-275b-ad1974786395')

from config import Config
from database import DatabaseManager

def check_database_logs():
    """Check database logs and recent activity."""
    print("🔍 CHECKING DATABASE LOGS")
    print("=" * 50)
    
    try:
        # Set up database connection
        config = Config()
        db_manager = DatabaseManager(config)
        db_manager.connect()
        
        with db_manager._get_connection() as conn:
            with conn.cursor() as cursor:
                # Check recent ingestion activity
                print("\n📊 RECENT INGESTION ACTIVITY (Last 24 hours):")
                cursor.execute("""
                    SELECT 
                        DATE(ingestion_timestamp) as date,
                        COUNT(*) as raw_records,
                        COUNT(DISTINCT fullbay_invoice_id) as unique_invoices
                    FROM fullbay_raw_data 
                    WHERE ingestion_timestamp >= NOW() - INTERVAL '24 hours'
                    GROUP BY DATE(ingestion_timestamp)
                    ORDER BY date DESC
                """)
                
                recent_activity = cursor.fetchall()
                if recent_activity:
                    for activity in recent_activity:
                        print(f"  📅 {activity['date']}: {activity['raw_records']} raw records, {activity['unique_invoices']} unique invoices")
                else:
                    print("  ⚠️  No recent ingestion activity found")
                
                # Check line items activity
                print("\n📋 RECENT LINE ITEMS ACTIVITY (Last 24 hours):")
                cursor.execute("""
                    SELECT 
                        DATE(ingestion_timestamp) as date,
                        COUNT(*) as line_items,
                        COUNT(DISTINCT fullbay_invoice_id) as unique_invoices,
                        SUM(line_total_price) as total_value
                    FROM fullbay_line_items 
                    WHERE ingestion_timestamp >= NOW() - INTERVAL '24 hours'
                    GROUP BY DATE(ingestion_timestamp)
                    ORDER BY date DESC
                """)
                
                line_items_activity = cursor.fetchall()
                if line_items_activity:
                    for activity in line_items_activity:
                        print(f"  📅 {activity['date']}: {activity['line_items']} line items, {activity['unique_invoices']} invoices, ${activity['total_value']:,.2f}")
                else:
                    print("  ⚠️  No recent line items activity found")
                
                # Check for processing errors
                print("\n❌ RECENT PROCESSING ERRORS:")
                cursor.execute("""
                    SELECT 
                        fullbay_invoice_id,
                        processing_errors,
                        ingestion_timestamp
                    FROM fullbay_raw_data 
                    WHERE processing_errors IS NOT NULL
                    AND ingestion_timestamp >= NOW() - INTERVAL '24 hours'
                    ORDER BY ingestion_timestamp DESC
                    LIMIT 10
                """)
                
                errors = cursor.fetchall()
                if errors:
                    for error in errors:
                        print(f"  🚨 Invoice {error['fullbay_invoice_id']} at {error['ingestion_timestamp']}: {error['processing_errors'][:100]}...")
                else:
                    print("  ✅ No processing errors found in the last 24 hours")
                
                # Data quality summary
                print("\n📈 DATA QUALITY SUMMARY:")
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_line_items,
                        COUNT(CASE WHEN invoice_number IS NULL OR invoice_number = '' THEN 1 END) as missing_invoice_numbers,
                        COUNT(CASE WHEN customer_id IS NULL OR customer_title IS NULL THEN 1 END) as missing_customer_info,
                        COUNT(CASE WHEN unit_vin IS NULL OR unit_make IS NULL THEN 1 END) as missing_unit_info,
                        COUNT(CASE WHEN line_total_price <= 0 THEN 1 END) as zero_negative_prices,
                        COUNT(CASE WHEN line_item_type = 'LABOR' AND (actual_hours IS NULL OR actual_hours <= 0) THEN 1 END) as missing_labor_hours
                    FROM fullbay_line_items
                    WHERE ingestion_timestamp >= NOW() - INTERVAL '24 hours'
                """)
                
                quality_stats = cursor.fetchone()
                if quality_stats:
                    total = quality_stats['total_line_items']
                    if total > 0:
                        print(f"  📊 Total line items (24h): {total}")
                        print(f"  ⚠️  Missing invoice numbers: {quality_stats['missing_invoice_numbers']} ({(quality_stats['missing_invoice_numbers']/total*100):.1f}%)")
                        print(f"  ⚠️  Missing customer info: {quality_stats['missing_customer_info']} ({(quality_stats['missing_customer_info']/total*100):.1f}%)")
                        print(f"  ⚠️  Missing unit info: {quality_stats['missing_unit_info']} ({(quality_stats['missing_unit_info']/total*100):.1f}%)")
                        print(f"  ⚠️  Zero/negative prices: {quality_stats['zero_negative_prices']} ({(quality_stats['zero_negative_prices']/total*100):.1f}%)")
                        print(f"  ⚠️  Missing labor hours: {quality_stats['missing_labor_hours']} ({(quality_stats['missing_labor_hours']/total*100):.1f}%)")
                    else:
                        print("  📊 No line items found in the last 24 hours")
                
        db_manager.close()
        
    except Exception as e:
        print(f"❌ Error checking database logs: {e}")

def check_file_logs():
    """Check log files if they exist."""
    print("\n📁 CHECKING LOG FILES")
    print("=" * 50)
    
    log_dir = "logs"
    if os.path.exists(log_dir):
        log_files = [f for f in os.listdir(log_dir) if f.endswith('.log')]
        
        if log_files:
            print(f"📂 Found {len(log_files)} log files:")
            for log_file in sorted(log_files, reverse=True)[:5]:  # Show last 5
                file_path = os.path.join(log_dir, log_file)
                file_size = os.path.getsize(file_path)
                file_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                
                print(f"  📄 {log_file} ({file_size:,} bytes, {file_time.strftime('%Y-%m-%d %H:%M:%S')})")
                
                # Show last few lines of the most recent log
                if log_file == sorted(log_files, reverse=True)[0]:
                    print(f"\n📋 Last 10 lines of {log_file}:")
                    try:
                        with open(file_path, 'r') as f:
                            lines = f.readlines()
                            for line in lines[-10:]:
                                print(f"    {line.rstrip()}")
                    except Exception as e:
                        print(f"    ❌ Error reading log file: {e}")
        else:
            print("📂 No log files found")
    else:
        print("📂 Logs directory does not exist")

def check_ingestion_metadata():
    """Check ingestion metadata table."""
    print("\n📊 CHECKING INGESTION METADATA")
    print("=" * 50)
    
    try:
        config = Config()
        db_manager = DatabaseManager(config)
        db_manager.connect()
        
        with db_manager._get_connection() as conn:
            with conn.cursor() as cursor:
                # Check recent executions
                cursor.execute("""
                    SELECT 
                        execution_id,
                        start_time,
                        end_time,
                        status,
                        records_processed,
                        records_inserted,
                        line_items_created,
                        error_message
                    FROM ingestion_metadata
                    ORDER BY start_time DESC
                    LIMIT 10
                """)
                
                executions = cursor.fetchall()
                if executions:
                    print("📋 Recent executions:")
                    for execution in executions:
                        duration = None
                        if execution['end_time'] and execution['start_time']:
                            duration = (execution['end_time'] - execution['start_time']).total_seconds()
                        
                        print(f"  🚀 {execution['execution_id']}")
                        print(f"     📅 {execution['start_time']}")
                        print(f"     ⏱️  Duration: {duration:.2f}s" if duration else "     ⏱️  Duration: Unknown")
                        print(f"     📊 Status: {execution['status']}")
                        print(f"     📈 Processed: {execution['records_processed']}, Inserted: {execution['records_inserted']}, Line Items: {execution['line_items_created']}")
                        if execution['error_message']:
                            print(f"     ❌ Error: {execution['error_message'][:100]}...")
                        print()
                else:
                    print("📋 No ingestion metadata found")
        
        db_manager.close()
        
    except Exception as e:
        print(f"❌ Error checking ingestion metadata: {e}")

def check_data_summary():
    """Provide a comprehensive data summary."""
    print("\n📈 COMPREHENSIVE DATA SUMMARY")
    print("=" * 50)
    
    try:
        config = Config()
        db_manager = DatabaseManager(config)
        db_manager.connect()
        
        with db_manager._get_connection() as conn:
            with conn.cursor() as cursor:
                # Overall statistics
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_raw_records,
                        COUNT(DISTINCT fullbay_invoice_id) as unique_invoices,
                        MAX(ingestion_timestamp) as last_ingestion
                    FROM fullbay_raw_data
                """)
                
                raw_stats = cursor.fetchone()
                print(f"📊 Raw Data:")
                print(f"  📄 Total records: {raw_stats['total_raw_records']:,}")
                print(f"  🧾 Unique invoices: {raw_stats['unique_invoices']:,}")
                print(f"  🕐 Last ingestion: {raw_stats['last_ingestion']}")
                
                # Line items statistics
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_line_items,
                        COUNT(DISTINCT fullbay_invoice_id) as unique_invoices,
                        SUM(line_total_price) as total_value,
                        MAX(ingestion_timestamp) as last_line_item
                    FROM fullbay_line_items
                """)
                
                line_stats = cursor.fetchone()
                print(f"\n📋 Line Items:")
                print(f"  📄 Total line items: {line_stats['total_line_items']:,}")
                print(f"  🧾 Unique invoices: {line_stats['unique_invoices']:,}")
                print(f"  💰 Total value: ${line_stats['total_value']:,.2f}")
                print(f"  🕐 Last line item: {line_stats['last_line_item']}")
                
                # Line item type breakdown
                cursor.execute("""
                    SELECT 
                        line_item_type,
                        COUNT(*) as count,
                        SUM(line_total_price) as total_value
                    FROM fullbay_line_items
                    GROUP BY line_item_type
                    ORDER BY count DESC
                """)
                
                type_breakdown = cursor.fetchall()
                print(f"\n📊 Line Item Types:")
                for item_type in type_breakdown:
                    print(f"  {item_type['line_item_type']}: {item_type['count']:,} items, ${item_type['total_value']:,.2f}")
                
                # Recent activity by date
                cursor.execute("""
                    SELECT 
                        DATE(ingestion_timestamp) as date,
                        COUNT(*) as line_items,
                        SUM(line_total_price) as daily_value
                    FROM fullbay_line_items
                    WHERE ingestion_timestamp >= NOW() - INTERVAL '7 days'
                    GROUP BY DATE(ingestion_timestamp)
                    ORDER BY date DESC
                """)
                
                daily_activity = cursor.fetchall()
                print(f"\n📅 Daily Activity (Last 7 days):")
                for day in daily_activity:
                    print(f"  📅 {day['date']}: {day['line_items']:,} line items, ${day['daily_value']:,.2f}")
        
        db_manager.close()
        
    except Exception as e:
        print(f"❌ Error checking data summary: {e}")

def main():
    """Main function to run all log checks."""
    print("🔍 FULLBAY API INGESTION LOG CHECKER")
    print("=" * 60)
    print(f"🕐 Check time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Run all checks
    check_database_logs()
    check_file_logs()
    check_ingestion_metadata()
    check_data_summary()
    
    print("\n✅ Log check completed!")

if __name__ == "__main__":
    main()
