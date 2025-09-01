#!/usr/bin/env python3
"""
Count the columns in my CREATE TABLE SQL to ensure exactly 73 columns.
"""

def count_sql_columns():
    """Count columns in the SQL."""
    
    # All columns from my SQL (excluding id which is SERIAL PRIMARY KEY)
    columns = [
        # PRIMARY KEY & REFERENCE (2 columns)
        'id',  # SERIAL PRIMARY KEY
        'raw_data_id',
        
        # INVOICE LEVEL INFO (8 columns)
        'fullbay_invoice_id',
        'invoice_number',
        'invoice_date',
        'due_date',
        'exported',
        'shop_title',
        'shop_email',
        'shop_address',
        
        # CUSTOMER INFO (6 columns)
        'customer_id',
        'customer_title',
        'customer_external_id',
        'customer_main_phone',
        'customer_secondary_phone',
        'customer_billing_address',
        
        # SERVICE ORDER INFO (5 columns)
        'fullbay_service_order_id',
        'repair_order_number',
        'service_order_created',
        'service_order_start_date',
        'service_order_completion_date',
        
        # UNIT/VEHICLE INFO (8 columns)
        'unit_id',
        'unit',
        'unit_type',
        'unit_year',
        'unit_make',
        'unit_model',
        'unit_vin',
        'unit_license_plate',
        
        # PRIMARY TECHNICIAN INFO (2 columns)
        'primary_technician',
        'primary_technician_number',
        
        # COMPLAINT INFO (6 columns)
        'fullbay_complaint_id',
        'complaint_type',
        'complaint_subtype',
        'complaint_note',
        'complaint_cause',
        'complaint_authorized',
        
        # CORRECTION/SERVICE INFO (7 columns)
        'fullbay_correction_id',
        'correction_title',
        'global_component',
        'global_system',
        'global_service',
        'recommended_correction',
        'service_description',
        
        # LINE ITEM CLASSIFICATION (1 column)
        'line_item_type',
        
        # PART INFO (5 columns)
        'fullbay_part_id',
        'part_description',
        'shop_part_number',
        'vendor_part_number',
        'part_category',
        
        # LABOR INFO (4 columns)
        'labor_description',
        'labor_rate_type',
        'assigned_technician',
        'assigned_technician_number',
        
        # QUANTITIES (3 columns)
        'quantity',
        'to_be_returned_quantity',
        'returned_quantity',
        
        # HOURS (2 columns)
        'so_hours',
        'technician_portion',
        
        # FINANCIAL DETAILS (4 columns) - including price_overridden
        'unit_cost',
        'unit_price',
        'line_total_price',
        'price_overridden',
        
        # TAX CALCULATION (4 columns)
        'taxable',
        'tax_rate',
        'line_tax',
        'sales_total',
        
        # CLASSIFICATION (3 columns)
        'inventory_item',
        'core_type',
        'sublet',
        
        # SERVICE ORDER TOTALS (1 column)
        'so_supplies_total',
        
        # METADATA (2 columns)
        'ingestion_timestamp',
        'ingestion_source'
    ]
    
    print("🔍 COLUMN COUNT ANALYSIS")
    print("=" * 50)
    print(f"Total columns: {len(columns)}")
    
    # Count by category
    categories = {
        'PRIMARY KEY & REFERENCE': 2,
        'INVOICE LEVEL INFO': 8,
        'CUSTOMER INFO': 6,
        'SERVICE ORDER INFO': 5,
        'UNIT/VEHICLE INFO': 8,
        'PRIMARY TECHNICIAN INFO': 2,
        'COMPLAINT INFO': 6,
        'CORRECTION/SERVICE INFO': 7,
        'LINE ITEM CLASSIFICATION': 1,
        'PART INFO': 5,
        'LABOR INFO': 4,
        'QUANTITIES': 3,
        'HOURS': 2,
        'FINANCIAL DETAILS': 4,
        'TAX CALCULATION': 4,
        'CLASSIFICATION': 3,
        'SERVICE ORDER TOTALS': 1,
        'METADATA': 2
    }
    
    total_expected = sum(categories.values())
    print(f"Expected total from categories: {total_expected}")
    
    print("\nColumns by category:")
    for category, count in categories.items():
        print(f"  {category}: {count}")
    
    if len(columns) == 73:
        print("\n✅ SUCCESS: SQL has exactly 73 columns!")
    else:
        print(f"\n⚠️ WARNING: Expected 73 columns, got {len(columns)}")

if __name__ == "__main__":
    count_sql_columns()
