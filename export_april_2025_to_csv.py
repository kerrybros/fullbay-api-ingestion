#!/usr/bin/env python3
"""
Export all April 2025 Fullbay data to CSV format
"""

import os
import sys
import csv
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from pathlib import Path

def export_april_2025_to_csv():
    """Export all April 2025 data to CSV."""
    print("Exporting April 2025 data to CSV...")
    print("=" * 60)
    
    # Database connection parameters
    db_params = {
        'host': 'fullbay-postgres-db.cc74yq2i4j8n.us-east-1.rds.amazonaws.com',
        'port': 5432,
        'dbname': 'fullbay_data',
        'user': 'postgres',
        'password': '5255Tillman'
    }
    
    try:
        # Connect to database
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        print("Connected to database successfully")
        
        # Query to get all April 2025 data
        query = """
        SELECT 
            -- Invoice Level Info
            fullbay_invoice_id,
            invoice_number,
            invoice_date,
            due_date,
            exported,
            
            -- Shop Info
            shop_title,
            shop_email,
            shop_physical_address,
            
            -- Customer Info
            customer_id,
            customer_title,
            customer_external_id,
            customer_main_phone,
            customer_secondary_phone,
            customer_billing_address,
            
            -- Service Order Info
            fullbay_service_order_id,
            repair_order_number,
            service_order_created,
            service_order_start_date,
            service_order_completion_date,
            
            -- Unit/Vehicle Info
            unit_id,
            unit_number,
            unit_type,
            unit_year,
            unit_make,
            unit_model,
            unit_vin,
            unit_license_plate,
            
            -- Primary Technician Info
            primary_technician,
            primary_technician_number,
            
            -- Complaint/Work Order Info
            fullbay_complaint_id,
            complaint_type,
            complaint_subtype,
            complaint_note,
            complaint_cause,
            complaint_authorized,
            
            -- Correction/Service Info
            fullbay_correction_id,
            correction_title,
            global_component,
            global_system,
            global_service,
            recommended_correction,
            actual_correction,
            correction_performed,
            
            -- Line Item Details
            line_item_type,
            fullbay_part_id,
            part_description,
            shop_part_number,
            vendor_part_number,
            part_category,
            
            -- Labor Info
            labor_description,
            labor_rate_type,
            assigned_technician,
            assigned_technician_number,
            
            -- Quantities
            quantity,
            to_be_returned_quantity,
            returned_quantity,
            
            -- Hours
            labor_hours,
            actual_hours,
            assigned_technician_portion,
            
            -- Financial Details
            unit_cost,
            unit_price,
            line_total_cost,
            line_total,
            price_overridden,
            
            -- Tax Info
            taxable,
            tax_rate,
            tax_amount,
            
            -- Classification
            inventory_item,
            core_type,
            sublet,
            
            -- QuickBooks Integration
            quickbooks_account,
            quickbooks_item,
            quickbooks_item_type,
            
            -- Service Order Totals
            so_total_parts_cost,
            so_total_parts_price,
            so_total_labor_hours,
            so_total_labor_cost,
            so_subtotal,
            so_tax_total,
            so_total_amount,
            
            -- Metadata
            ingestion_timestamp,
            ingestion_source
            
        FROM fullbay_line_items 
        WHERE invoice_date >= '2025-04-01' 
        AND invoice_date <= '2025-04-30'
        ORDER BY invoice_date, fullbay_invoice_id, line_item_type, id
        """
        
        print("Executing query...")
        cursor.execute(query)
        
        # Get all results
        rows = cursor.fetchall()
        
        if not rows:
            print("No data found for April 2025")
            return
        
        print(f"Found {len(rows)} line items for April 2025")
        
        # Create CSV filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        csv_filename = f"april_2025_fullbay_data_{timestamp}.csv"
        
        # Get column names from the first row
        fieldnames = list(rows[0].keys())
        
        # Write to CSV
        print(f"Writing data to {csv_filename}...")
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            # Write header
            writer.writeheader()
            
            # Write data rows
            for row in rows:
                # Convert any None values to empty strings for CSV
                cleaned_row = {}
                for key, value in row.items():
                    if value is None:
                        cleaned_row[key] = ''
                    else:
                        cleaned_row[key] = value
                writer.writerow(cleaned_row)
        
        print(f"✅ Successfully exported {len(rows)} records to {csv_filename}")
        
        # Generate summary statistics
        print("\n📊 April 2025 Data Summary:")
        print("-" * 40)
        
        # Count unique invoices
        unique_invoices = len(set(row['fullbay_invoice_id'] for row in rows))
        print(f"Unique Invoices: {unique_invoices}")
        
        # Count by line item type
        line_item_types = {}
        for row in rows:
            line_type = row['line_item_type'] or 'UNKNOWN'
            line_item_types[line_type] = line_item_types.get(line_type, 0) + 1
        
        print("Line Items by Type:")
        for line_type, count in sorted(line_item_types.items()):
            print(f"  {line_type}: {count}")
        
        # Total financial value
        total_value = sum(float(row['line_total'] or 0) for row in rows)
        print(f"Total Financial Value: ${total_value:,.2f}")
        
        # Date range
        dates = [row['invoice_date'] for row in rows if row['invoice_date']]
        if dates:
            min_date = min(dates)
            max_date = max(dates)
            print(f"Date Range: {min_date} to {max_date}")
        
        print(f"\n📁 File saved as: {csv_filename}")
        print(f"📏 File size: {Path(csv_filename).stat().st_size / 1024:.1f} KB")
        
    except Exception as e:
        print(f"❌ Error exporting data: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()
            print("Database connection closed")

if __name__ == "__main__":
    export_april_2025_to_csv()
