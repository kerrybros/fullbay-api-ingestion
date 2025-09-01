#!/usr/bin/env python3
"""
Verify the exact 75 columns from the original schema
"""

def verify_75_columns():
    """Verify the exact 75 columns from the original schema."""
    
    # Original 75 columns from the rollback script
    original_75_columns = [
        'id', 'raw_data_id', 'fullbay_invoice_id', 'invoice_number', 'invoice_date', 
        'due_date', 'exported', 'shop_title', 'shop_email', 'shop_address',
        'customer_id', 'customer_title', 'customer_external_id', 'customer_main_phone', 
        'customer_secondary_phone', 'customer_billing_address',
        'fullbay_service_order_id', 'repair_order_number', 'service_order_created', 
        'service_order_start_date', 'service_order_completion_date',
        'unit_id', 'unit_number', 'unit_type', 'unit_year', 'unit_make', 'unit_model', 
        'unit_vin', 'unit_license_plate',
        'primary_technician', 'primary_technician_number',
        'fullbay_complaint_id', 'complaint_type', 'complaint_subtype', 'complaint_note', 
        'complaint_cause', 'complaint_authorized',
        'fullbay_correction_id', 'correction_title', 'global_component', 'global_system', 
        'global_service', 'recommended_correction', 'actual_correction', 'correction_performed',
        'line_item_type', 'fullbay_part_id', 'part_description', 'shop_part_number', 
        'vendor_part_number', 'part_category',
        'labor_description', 'labor_rate_type', 'assigned_technician', 'assigned_technician_number',
        'quantity', 'to_be_returned_quantity', 'returned_quantity',
        'labor_hours', 'actual_hours', 'technician_portion',
        'unit_cost', 'unit_price', 'line_total_cost', 'line_total_price', 'price_overridden',
        'taxable', 'tax_rate', 'tax_amount', 'inventory_item', 'core_type', 'sublet',
        'quickbooks_account', 'quickbooks_item', 'quickbooks_item_type',
        'so_total_parts_cost', 'so_total_parts_price', 'so_total_labor_hours', 
        'so_total_labor_cost', 'so_subtotal', 'so_tax_total', 'so_total_amount',
        'ingestion_timestamp', 'ingestion_source'
    ]
    
    print("ORIGINAL 75-COLUMN SCHEMA VERIFICATION")
    print("=" * 50)
    print(f"Original schema column count: {len(original_75_columns)}")
    
    print("\nOriginal 75 columns:")
    for i, col in enumerate(original_75_columns, 1):
        print(f"{i:2d}. {col}")
    
    # Current schema from our check
    current_84_columns = [
        'id', 'raw_data_id', 'fullbay_invoice_id', 'invoice_number', 'invoice_date', 
        'due_date', 'exported', 'shop_title', 'shop_email', 'shop_address',
        'customer_id', 'customer_title', 'customer_external_id', 'customer_main_phone', 
        'customer_secondary_phone', 'customer_billing_address',
        'fullbay_service_order_id', 'repair_order_number', 'service_order_created', 
        'service_order_start_date', 'service_order_completion_date',
        'unit_id', 'unit_number', 'unit_type', 'unit_year', 'unit_make', 'unit_model', 
        'unit_vin', 'unit_license_plate',
        'primary_technician', 'primary_technician_number',
        'fullbay_complaint_id', 'complaint_type', 'complaint_subtype', 'complaint_note', 
        'complaint_cause', 'complaint_authorized',
        'fullbay_correction_id', 'correction_title', 'global_component', 'global_system', 
        'global_service', 'recommended_correction', 'actual_correction', 'correction_performed',
        'line_item_type', 'fullbay_part_id', 'part_description', 'shop_part_number', 
        'vendor_part_number', 'part_category',
        'labor_description', 'labor_rate_type', 'assigned_technician', 'assigned_technician_number',
        'quantity', 'to_be_returned_quantity', 'returned_quantity',
        'labor_hours', 'actual_hours', 'technician_portion',
        'unit_cost', 'unit_price', 'line_total_cost', 'line_total_price', 'price_overridden',
        'taxable', 'tax_rate', 'tax_amount', 'inventory_item', 'core_type', 'sublet',
        'quickbooks_account', 'quickbooks_item', 'quickbooks_item_type',
        'so_total_parts_cost', 'so_total_parts_price', 'so_total_labor_hours', 
        'so_total_labor_cost', 'so_subtotal', 'so_tax_total', 'so_total_amount',
        'ingestion_timestamp', 'ingestion_source'
    ]
    
    print(f"\nCurrent schema column count: {len(current_84_columns)}")
    
    # Check if they match
    if original_75_columns == current_84_columns:
        print("\n✅ PERFECT MATCH! Current schema matches original 75 columns exactly.")
    else:
        print("\n❌ MISMATCH! Current schema differs from original 75 columns.")
        
        # Find differences
        extra_in_current = [col for col in current_84_columns if col not in original_75_columns]
        missing_in_current = [col for col in original_75_columns if col not in current_84_columns]
        
        if extra_in_current:
            print(f"\nExtra columns in current schema ({len(extra_in_current)}):")
            for col in extra_in_current:
                print(f"  - {col}")
        
        if missing_in_current:
            print(f"\nMissing columns in current schema ({len(missing_in_current)}):")
            for col in missing_in_current:
                print(f"  - {col}")

if __name__ == "__main__":
    verify_75_columns()
