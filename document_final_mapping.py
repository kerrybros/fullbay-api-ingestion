#!/usr/bin/env python3
"""
Document the final field mapping from FullBay API to CSV schema
"""

import psycopg2
from psycopg2.extras import RealDictCursor

def document_final_mapping():
    """Document the final field mapping."""
    print("FULLBAY API TO CSV SCHEMA MAPPING DOCUMENTATION")
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
        
        # Get database schema
        cursor.execute("""
            SELECT column_name, data_type, ordinal_position
            FROM information_schema.columns 
            WHERE table_name = 'fullbay_line_items' 
            AND table_schema = 'public'
            ORDER BY ordinal_position
        """)
        
        db_columns = cursor.fetchall()
        
        print(f"📊 DATABASE SCHEMA: {len(db_columns)} columns")
        print(f"📊 CSV SCHEMA: {len(db_columns) - 1} columns (removed primary_technician_number)")
        print()
        
        # Document the mapping
        print("🔗 FIELD MAPPING BREAKDOWN:")
        print("-" * 60)
        
        # Group columns by category
        categories = {
            "PRIMARY KEYS & REFERENCES": [],
            "INVOICE LEVEL": [],
            "SHOP INFORMATION": [],
            "CUSTOMER INFORMATION": [],
            "SERVICE ORDER": [],
            "VEHICLE/UNIT": [],
            "TECHNICIAN": [],
            "COMPLAINT": [],
            "CORRECTION": [],
            "LINE ITEM DETAILS": [],
            "PARTS": [],
            "LABOR": [],
            "QUANTITIES & HOURS": [],
            "FINANCIAL": [],
            "CLASSIFICATION": [],
            "METADATA": []
        }
        
        # Categorize columns
        for col in db_columns:
            col_name = col['column_name']
            
            if col_name in ['id', 'raw_data_id', 'fullbay_invoice_id']:
                categories["PRIMARY KEYS & REFERENCES"].append(col)
            elif col_name in ['invoice_number', 'invoice_date', 'due_date']:
                categories["INVOICE LEVEL"].append(col)
            elif col_name in ['shop_title', 'shop_email', 'shop_address']:
                categories["SHOP INFORMATION"].append(col)
            elif col_name in ['customer_id', 'customer_title', 'customer_external_id', 'customer_main_phone', 'customer_secondary_phone', 'customer_billing_address']:
                categories["CUSTOMER INFORMATION"].append(col)
            elif col_name in ['fullbay_service_order_id', 'service_order', 'service_order_created', 'service_order_start_date', 'service_order_completion_date']:
                categories["SERVICE ORDER"].append(col)
            elif col_name in ['unit_id', 'unit', 'unit_type', 'unit_year', 'unit_make', 'unit_model', 'unit_vin', 'unit_license_plate']:
                categories["VEHICLE/UNIT"].append(col)
            elif col_name in ['primary_technician', 'primary_technician_number']:
                categories["TECHNICIAN"].append(col)
            elif col_name in ['soai_id', 'complaint_type', 'complaint_subtype', 'complaint_note', 'complaint_cause', 'complaint_authorized']:
                categories["COMPLAINT"].append(col)
            elif col_name in ['fullbay_correction_id', 'correction_title', 'component', 'system', 'global_service', 'recommended_correction', 'actual_correction', 'correction_performed']:
                categories["CORRECTION"].append(col)
            elif col_name in ['line_item_type']:
                categories["LINE ITEM DETAILS"].append(col)
            elif col_name in ['fullbay_part_id', 'part_description', 'shop_part_number', 'vendor_part_number', 'part_category']:
                categories["PARTS"].append(col)
            elif col_name in ['labor_description', 'labor_rate_type', 'assigned_technician', 'assigned_technician_number']:
                categories["LABOR"].append(col)
            elif col_name in ['quantity', 'to_be_returned_quantity', 'returned_quantity', 'labor_hours', 'actual_hours', 'technician_portion']:
                categories["QUANTITIES & HOURS"].append(col)
            elif col_name in ['unit_cost', 'unit_price', 'line_total_cost', 'total', 'price_overridden', 'taxable', 'tax_rate', 'tax_amount']:
                categories["FINANCIAL"].append(col)
            elif col_name in ['inventory_item', 'core_type', 'sublet']:
                categories["CLASSIFICATION"].append(col)
            elif col_name in ['ingestion_timestamp', 'ingestion_source']:
                categories["METADATA"].append(col)
        
        # Print each category
        for category, columns in categories.items():
            if columns:
                print(f"\n📋 {category}:")
                print("-" * 40)
                
                for col in columns:
                    col_name = col['column_name']
                    data_type = col['data_type']
                    position = col['ordinal_position']
                    
                    # Show CSV mapping
                    if col_name == 'customer_title':
                        csv_name = 'customer'
                        mapping_note = " (RENAMED in CSV)"
                    elif col_name == 'primary_technician_number':
                        csv_name = 'REMOVED'
                        mapping_note = " (REMOVED from CSV)"
                    else:
                        csv_name = col_name
                        mapping_note = ""
                    
                    print(f"   {position:2d}. {col_name:<30} ({data_type:<15}) → {csv_name}{mapping_note}")
        
        # Show API to Database mapping
        print(f"\n🔗 FULLBAY API TO DATABASE MAPPING:")
        print("-" * 60)
        
        api_mappings = {
            "Invoice Level": {
                "primaryKey": "fullbay_invoice_id",
                "invoiceNumber": "invoice_number", 
                "invoiceDate": "invoice_date",
                "dueDate": "due_date",
                "shopTitle": "shop_title",
                "shopEmail": "shop_email",
                "shopPhysicalAddress": "shop_address"
            },
            "Customer": {
                "Customer.customerId": "customer_id",
                "Customer.title": "customer_title (→ customer in CSV)",
                "Customer.externalId": "customer_external_id",
                "Customer.mainPhone": "customer_main_phone",
                "Customer.secondaryPhone": "customer_secondary_phone",
                "customerBillingAddress": "customer_billing_address"
            },
            "Service Order": {
                "ServiceOrder.primaryKey": "fullbay_service_order_id",
                "ServiceOrder.repairOrderNumber": "service_order",
                "ServiceOrder.created": "service_order_created",
                "ServiceOrder.startDateTime": "service_order_start_date",
                "ServiceOrder.completionDateTime": "service_order_completion_date"
            },
            "Vehicle/Unit": {
                "ServiceOrder.Unit.customerUnitId": "unit_id",
                "ServiceOrder.Unit.number": "unit",
                "ServiceOrder.Unit.type": "unit_type",
                "ServiceOrder.Unit.year": "unit_year",
                "ServiceOrder.Unit.make": "unit_make",
                "ServiceOrder.Unit.model": "unit_model",
                "ServiceOrder.Unit.vin": "unit_vin",
                "ServiceOrder.Unit.licensePlate": "unit_license_plate"
            },
            "Technician": {
                "ServiceOrder.technician": "primary_technician",
                "ServiceOrder.technicianNumber": "primary_technician_number (REMOVED from CSV)"
            },
            "Complaint": {
                "ServiceOrder.Complaints[].primaryKey": "soai_id",
                "ServiceOrder.Complaints[].type": "complaint_type",
                "ServiceOrder.Complaints[].subType": "complaint_subtype",
                "ServiceOrder.Complaints[].note": "complaint_note",
                "ServiceOrder.Complaints[].cause": "complaint_cause",
                "ServiceOrder.Complaints[].authorized": "complaint_authorized"
            },
            "Correction": {
                "ServiceOrder.Complaints[].Corrections[].primaryKey": "fullbay_correction_id",
                "ServiceOrder.Complaints[].Corrections[].title": "correction_title",
                "ServiceOrder.Complaints[].Corrections[].globalComponent": "component",
                "ServiceOrder.Complaints[].Corrections[].globalSystem": "system",
                "ServiceOrder.Complaints[].Corrections[].globalService": "global_service",
                "ServiceOrder.Complaints[].Corrections[].recommendedCorrection": "recommended_correction",
                "ServiceOrder.Complaints[].Corrections[].actualCorrection": "actual_correction",
                "ServiceOrder.Complaints[].Corrections[].correctionPerformed": "correction_performed"
            },
            "Parts": {
                "ServiceOrder.Complaints[].Corrections[].Parts[].primaryKey": "fullbay_part_id",
                "ServiceOrder.Complaints[].Corrections[].Parts[].description": "part_description",
                "ServiceOrder.Complaints[].Corrections[].Parts[].shopPartNumber": "shop_part_number",
                "ServiceOrder.Complaints[].Corrections[].Parts[].vendorPartNumber": "vendor_part_number",
                "ServiceOrder.Complaints[].Corrections[].Parts[].partCategory": "part_category",
                "ServiceOrder.Complaints[].Corrections[].Parts[].quantity": "quantity",
                "ServiceOrder.Complaints[].Corrections[].Parts[].toBeReturnedQuantity": "to_be_returned_quantity",
                "ServiceOrder.Complaints[].Corrections[].Parts[].returnedQuantity": "returned_quantity",
                "ServiceOrder.Complaints[].Corrections[].Parts[].cost": "unit_cost",
                "ServiceOrder.Complaints[].Corrections[].Parts[].sellingPrice": "unit_price",
                "ServiceOrder.Complaints[].Corrections[].Parts[].sellingPriceOverridden": "price_overridden",
                "ServiceOrder.Complaints[].Corrections[].Parts[].taxable": "taxable",
                "ServiceOrder.Complaints[].Corrections[].Parts[].inventory": "inventory_item",
                "ServiceOrder.Complaints[].Corrections[].Parts[].coreType": "core_type",
                "ServiceOrder.Complaints[].Corrections[].Parts[].sublet": "sublet"
            },
            "Labor": {
                "ServiceOrder.Complaints[].Corrections[].actualCorrection": "labor_description",
                "ServiceOrder.Complaints[].Corrections[].laborRate": "labor_rate_type",
                "ServiceOrder.Complaints[].AssignedTechnicians[].technician": "assigned_technician",
                "ServiceOrder.Complaints[].AssignedTechnicians[].technicianNumber": "assigned_technician_number",
                "ServiceOrder.Complaints[].Corrections[].laborHoursTotal": "labor_hours",
                "ServiceOrder.Complaints[].AssignedTechnicians[].actualHours": "actual_hours",
                "ServiceOrder.Complaints[].AssignedTechnicians[].portion": "technician_portion",
                "ServiceOrder.Complaints[].Corrections[].laborTotal": "total (calculated per technician)"
            }
        }
        
        for category, mappings in api_mappings.items():
            print(f"\n📋 {category}:")
            print("-" * 40)
            for api_field, db_field in mappings.items():
                print(f"   {api_field:<50} → {db_field}")
        
        # Show data transformation logic
        print(f"\n🔄 DATA TRANSFORMATION LOGIC:")
        print("-" * 60)
        
        transformations = [
            "1. **Flattening**: 1 Invoice JSON → Multiple line items (1 per part + 1 per labor)",
            "2. **Part Grouping**: Identical parts with same price are grouped together",
            "3. **Labor Distribution**: Labor distributed across assigned technicians",
            "4. **Date Parsing**: Various date formats converted to YYYY-MM-DD",
            "5. **Currency Cleaning**: Removes $ and commas from monetary values",
            "6. **Boolean Conversion**: 'Yes'/'No' strings converted to true/false",
            "7. **Null Handling**: Missing values set to appropriate defaults",
            "8. **Line Item Classification**: Parts classified as PART, SUPPLY, FREIGHT, or MISC"
        ]
        
        for transformation in transformations:
            print(f"   {transformation}")
        
        # Show final statistics
        cursor.execute("""
            SELECT 
                COUNT(*) as total_line_items,
                COUNT(DISTINCT fullbay_invoice_id) as unique_invoices,
                COUNT(DISTINCT customer_id) as unique_customers,
                SUM(total) as total_value
            FROM fullbay_line_items 
            WHERE invoice_date >= '2025-04-01' 
            AND invoice_date <= '2025-04-30'
        """)
        
        stats = cursor.fetchone()
        
        print(f"\n📈 FINAL DATA STATISTICS:")
        print("-" * 40)
        print(f"   Total Line Items: {stats['total_line_items']:,}")
        print(f"   Unique Invoices: {stats['unique_invoices']:,}")
        print(f"   Unique Customers: {stats['unique_customers']:,}")
        print(f"   Total Value: ${stats['total_value']:,.2f}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        raise

if __name__ == "__main__":
    document_final_mapping()
