#!/usr/bin/env python3
"""
Test script for validating JSON flattening logic.

This script tests the flattening functionality with sample Fullbay invoice data
to ensure the logic works correctly before deployment.
"""

import json
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

# Sample Fullbay Invoice JSON (based on your analysis document)
SAMPLE_INVOICE_JSON = {
    "primaryKey": "17506970",
    "invoiceNumber": "14282", 
    "invoiceDate": "2025-05-30",
    "dueDate": "2025-06-29",
    "exported": "0",
    "shopTitle": "KERRY BROTHERS TRUCK REPAIR",
    "shopEmail": "info@kerrybrothers.com",
    "shopPhysicalAddress": "123 Truck Lane, Detroit, MI 48202",
    "customerBillingAddress": "5001 BELLEVUE, Detroit, MI 48202",
    "subTotal": "4641.50",
    "taxTotal": "127.83", 
    "total": "4769.33",
    "Customer": {
        "customerId": 2250840,
        "title": "GAETANO #3",
        "externalId": "GTNC03",
        "mainPhone": "313-555-0123",
        "secondaryPhone": "313-555-0124"
    },
    "ServiceOrder": {
        "primaryKey": "18959643",
        "repairOrderNumber": "15448",
        "created": "2025-05-28T10:30:00",
        "startDateTime": "2025-05-28T11:00:00", 
        "completionDateTime": "2025-05-30T16:45:00",
        "technician": "Jacob Humphries",
        "technicianNumber": "TECH001",
        "partsCostTotal": "1250.75",
        "partsTotal": "2100.50",
        "laborHoursTotal": "8.55",
        "laborTotal": "2541.00",
        "Customer": {
            "customerId": 2250840,
            "title": "GAETANO #3"
        },
        "Unit": {
            "customerUnitId": "6271523",
            "number": "734",
            "type": "Truck",
            "year": "2019",
            "make": "Freightliner", 
            "model": "Cascadia",
            "vin": "3AKBHKDV0KSKC8657",
            "licensePlate": "TRK-734"
        },
        "Complaints": [
            {
                "primaryKey": 47868748,
                "type": "Engine",
                "subType": "Performance",
                "note": "CHECK ENGINE LIGHT",
                "cause": "SCR SYSTEM FAULT",
                "authorized": "Yes",
                "AssignedTechnicians": [
                    {
                        "technician": "Jacob Humphries",
                        "technicianNumber": "TECH001",
                        "actualHours": 3.42,
                        "portion": 100
                    }
                ],
                "Corrections": [
                    {
                        "primaryKey": 42557049,
                        "title": "COMPUTER CHECK",
                        "globalComponent": "ENGINE",
                        "globalSystem": "EXHAUST",
                        "globalService": "DIAGNOSIS",
                        "recommendedCorrection": "CHECK SCR SYSTEM CODES",
                        "actualCorrection": "COMPUTER CHECK. SCR CONVERSION CODES PRESENT. DEF AWNING KIT INSTALLED.",
                        "correctionPerformed": "Yes",
                        "laborHoursTotal": "3.42",
                        "laborTotal": "1650.00",
                        "laborRate": "Standard",
                        "taxable": "Yes",
                        "Parts": [
                            {
                                "primaryKey": 70166538,
                                "description": "DEF AWNING KIT",
                                "shopPartNumber": "4921911",
                                "vendorPartNumber": "DEF-AWN-001",
                                "partCategory": "ENGINE COMPONENTS",
                                "quantity": "1",
                                "cost": "206.40",
                                "sellingPrice": "412.80",
                                "sellingPriceOverridden": "No",
                                "taxable": "Yes",
                                "inventory": "Yes",
                                "coreType": "None",
                                "sublet": "No",
                                "quickBooksAccount": "Parts Income",
                                "quickBooksItem": "DEF Awning Kit",
                                "quickBooksItemType": "Inventory Part"
                            },
                            {
                                "primaryKey": 70166539,
                                "description": "GASKET, EXHAUST",
                                "shopPartNumber": "TCX/AMS013",
                                "vendorPartNumber": "GASK-EXH-013",
                                "partCategory": "GASKETS",
                                "quantity": "2",
                                "cost": "11.79",
                                "sellingPrice": "32.42",
                                "sellingPriceOverridden": "No",
                                "taxable": "Yes",
                                "inventory": "Yes",
                                "coreType": "None",
                                "sublet": "No",
                                "quickBooksAccount": "Parts Income",
                                "quickBooksItem": "Exhaust Gasket",
                                "quickBooksItemType": "Inventory Part"
                            }
                        ]
                    }
                ]
            },
            {
                "primaryKey": 47868749,
                "type": "Maintenance",
                "subType": "Oil Change", 
                "note": "LOF-PM SERVICE-REGULAR",
                "cause": "SCHEDULED MAINTENANCE",
                "authorized": "Yes",
                "AssignedTechnicians": [
                    {
                        "technician": "Thomas Germain",
                        "technicianNumber": "TECH002", 
                        "actualHours": 2.13,
                        "portion": 100
                    }
                ],
                "Corrections": [
                    {
                        "primaryKey": 42940071,
                        "title": "LOF-PM SERVICE",
                        "globalComponent": "ENGINE",
                        "globalSystem": "LUBRICATION",
                        "globalService": "MAINTENANCE",
                        "recommendedCorrection": "PERFORM LOF SERVICE",
                        "actualCorrection": "LOF-PM SERVICE-REGULAR. CHANGED OIL AND FILTER.",
                        "correctionPerformed": "Yes",
                        "laborHoursTotal": "2.13",
                        "laborTotal": "210.00", 
                        "laborRate": "Standard",
                        "taxable": "Yes",
                        "Parts": [
                            {
                                "primaryKey": 70166540,
                                "description": "OIL - 1540",
                                "shopPartNumber": "1540",
                                "vendorPartNumber": "OIL-15W40",
                                "partCategory": "FLUIDS",
                                "quantity": "4",
                                "cost": "6.48",
                                "sellingPrice": "12.96",
                                "sellingPriceOverridden": "No",
                                "taxable": "Yes",
                                "inventory": "Yes",
                                "coreType": "None",
                                "sublet": "No",
                                "quickBooksAccount": "Parts Income",
                                "quickBooksItem": "Engine Oil",
                                "quickBooksItemType": "Inventory Part"
                            },
                            {
                                "primaryKey": 70166541,
                                "description": "FILTER, OIL",
                                "shopPartNumber": "P7505",
                                "vendorPartNumber": "FILT-OIL-7505", 
                                "partCategory": "FILTERS",
                                "quantity": "1",
                                "cost": "28.89",
                                "sellingPrice": "57.79",
                                "sellingPriceOverridden": "No",
                                "taxable": "Yes",
                                "inventory": "Yes",
                                "coreType": "None",
                                "sublet": "No",
                                "quickBooksAccount": "Parts Income",
                                "quickBooksItem": "Oil Filter",
                                "quickBooksItemType": "Inventory Part"
                            }
                        ]
                    }
                ]
            }
        ]
    }
}


# Simplified flattening logic for testing (extracted from DatabaseManager)
def flatten_invoice_to_line_items(record: Dict[str, Any], raw_data_id: int) -> List[Dict[str, Any]]:
    """
    Simplified version of invoice flattening logic for testing.
    """
    line_items = []
    
    # Extract common context
    invoice_context = extract_invoice_context(record)
    
    # Get service order data
    service_order = record.get('ServiceOrder', {})
    complaints = service_order.get('Complaints', [])
    
    # Process each complaint
    for complaint in complaints:
        complaint_context = extract_complaint_context(complaint, invoice_context, service_order)
        
        corrections = complaint.get('Corrections', [])
        
        # Process each correction within the complaint
        for correction in corrections:
            correction_context = extract_correction_context(correction, complaint_context)
            
            # Process parts for this correction
            parts = correction.get('Parts', [])
            if parts:
                parts_line_items = process_parts(parts, correction_context)
                line_items.extend(parts_line_items)
            
            # Process labor for this correction
            labor_line_items = process_labor(correction, complaint, correction_context)
            line_items.extend(labor_line_items)
    
    # Add raw_data_id to all line items
    for item in line_items:
        item['raw_data_id'] = raw_data_id
    
    return line_items


def extract_invoice_context(record: Dict[str, Any]) -> Dict[str, Any]:
    """Extract invoice-level context."""
    service_order = record.get('ServiceOrder', {})
    customer = record.get('Customer', {})
    so_customer = service_order.get('Customer', {})
    unit = service_order.get('Unit', {})
    
    # Use service order customer if available, otherwise use invoice customer
    customer_data = so_customer if so_customer else customer
    
    return {
        'fullbay_invoice_id': record.get('primaryKey'),
        'invoice_number': record.get('invoiceNumber'),
        'invoice_date': record.get('invoiceDate'),
        'due_date': record.get('dueDate'),
        'exported': record.get('exported') == '1',
        'shop_title': record.get('shopTitle'),
        'shop_email': record.get('shopEmail'),
        'shop_address': record.get('shopPhysicalAddress'),
        'customer_id': customer_data.get('customerId'),
        'customer_title': customer_data.get('title'),
        'customer_external_id': customer_data.get('externalId'),
        'customer_main_phone': customer_data.get('mainPhone'),
        'customer_secondary_phone': customer_data.get('secondaryPhone'),
        'customer_billing_address': record.get('customerBillingAddress'),
        'fullbay_service_order_id': service_order.get('primaryKey'),
        'repair_order_number': service_order.get('repairOrderNumber'),
        'service_order_created': service_order.get('created'),
        'service_order_start_date': service_order.get('startDateTime'),
        'service_order_completion_date': service_order.get('completionDateTime'),
        'unit_id': unit.get('customerUnitId'),
        'unit_number': unit.get('number'),
        'unit_type': unit.get('type'),
        'unit_year': unit.get('year'),
        'unit_make': unit.get('make'),
        'unit_model': unit.get('model'),
        'unit_vin': unit.get('vin'),
        'unit_license_plate': unit.get('licensePlate'),
        'primary_technician': service_order.get('technician'),
        'primary_technician_number': service_order.get('technicianNumber'),
        'so_total_parts_cost': parse_decimal(service_order.get('partsCostTotal')),
        'so_total_parts_price': parse_decimal(service_order.get('partsTotal')),
        'so_total_labor_hours': parse_decimal(service_order.get('laborHoursTotal')),
        'so_total_labor_cost': parse_decimal(service_order.get('laborTotal')),
        'so_subtotal': parse_decimal(record.get('subTotal')),
        'so_tax_total': parse_decimal(record.get('taxTotal')),
        'so_total_amount': parse_decimal(record.get('total')),
    }


def extract_complaint_context(complaint: Dict[str, Any], invoice_context: Dict[str, Any], service_order: Dict[str, Any]) -> Dict[str, Any]:
    """Extract complaint-level context."""
    context = invoice_context.copy()
    context.update({
        'fullbay_complaint_id': complaint.get('primaryKey'),
        'complaint_type': complaint.get('type'),
        'complaint_subtype': complaint.get('subType'),
        'complaint_note': complaint.get('note'),
        'complaint_cause': complaint.get('cause'),
        'complaint_authorized': complaint.get('authorized') == 'Yes',
    })
    return context


def extract_correction_context(correction: Dict[str, Any], complaint_context: Dict[str, Any]) -> Dict[str, Any]:
    """Extract correction-level context."""
    context = complaint_context.copy()
    context.update({
        'fullbay_correction_id': correction.get('primaryKey'),
        'correction_title': correction.get('title'),
        'global_component': correction.get('globalComponent'),
        'global_system': correction.get('globalSystem'),
        'global_service': correction.get('globalService'),
        'recommended_correction': correction.get('recommendedCorrection'),
        'actual_correction': correction.get('actualCorrection'),
        'correction_performed': correction.get('correctionPerformed'),
    })
    return context


def process_parts(parts: List[Dict[str, Any]], context: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Process parts for a correction, grouping identical parts correctly."""
    if not parts:
        return []
    
    # Group parts by part_number + unit_price
    parts_groups = {}
    
    for part in parts:
        shop_part_number = part.get('shopPartNumber', '')
        selling_price = parse_decimal(part.get('sellingPrice'))
        
        # Create grouping key
        group_key = f"{shop_part_number}|{selling_price}"
        
        if group_key not in parts_groups:
            parts_groups[group_key] = {
                'parts': [],
                'total_quantity': 0,
                'total_cost': 0,
                'total_price': 0
            }
        
        parts_groups[group_key]['parts'].append(part)
        parts_groups[group_key]['total_quantity'] += parse_decimal(part.get('quantity', 0))
        parts_groups[group_key]['total_cost'] += parse_decimal(part.get('cost', 0)) * parse_decimal(part.get('quantity', 0))
        parts_groups[group_key]['total_price'] += parse_decimal(part.get('sellingPrice', 0)) * parse_decimal(part.get('quantity', 0))
    
    # Create line items from grouped parts
    line_items = []
    
    for group_key, group_data in parts_groups.items():
        # Use the first part in the group as the template
        template_part = group_data['parts'][0]
        
        line_item = context.copy()
        line_item.update({
            'line_item_type': classify_part_type(template_part),
            'fullbay_part_id': template_part.get('primaryKey'),
            'part_description': template_part.get('description'),
            'shop_part_number': template_part.get('shopPartNumber'),
            'vendor_part_number': template_part.get('vendorPartNumber'),
            'part_category': template_part.get('partCategory'),
            'quantity': group_data['total_quantity'],
            'unit_cost': parse_decimal(template_part.get('cost')),
            'unit_price': parse_decimal(template_part.get('sellingPrice')),
            'line_total_cost': group_data['total_cost'],
            'line_total_price': group_data['total_price'],
            'price_overridden': template_part.get('sellingPriceOverridden') == 'Yes',
            'taxable': template_part.get('taxable') == 'Yes',
            'inventory_item': template_part.get('inventory') == 'Yes',
            'core_type': template_part.get('coreType'),
            'sublet': template_part.get('sublet') == 'Yes',
            'quickbooks_account': template_part.get('quickBooksAccount'),
            'quickbooks_item': template_part.get('quickBooksItem'),
            'quickbooks_item_type': template_part.get('quickBooksItemType'),
        })
        
        line_items.append(line_item)
    
    return line_items


def process_labor(correction: Dict[str, Any], complaint: Dict[str, Any], context: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Process labor for a correction, creating separate rows per technician."""
    line_items = []
    
    # Get assigned technicians from the complaint
    assigned_technicians = complaint.get('AssignedTechnicians', [])
    
    if not assigned_technicians:
        # No specific technician assignments, but there's labor - create one labor row
        if parse_decimal(correction.get('laborHoursTotal', 0)) > 0:
            line_item = context.copy()
            line_item.update({
                'line_item_type': 'LABOR',
                'labor_description': correction.get('actualCorrection') or correction.get('recommendedCorrection'),
                'labor_rate_type': correction.get('laborRate'),
                'assigned_technician': context.get('primary_technician'),
                'assigned_technician_number': context.get('primary_technician_number'),
                'labor_hours': parse_decimal(correction.get('laborHoursTotal')),
                'actual_hours': parse_decimal(correction.get('laborHoursTotal')),
                'technician_portion': 100,
                'line_total_price': parse_decimal(correction.get('laborTotal')),
                'taxable': correction.get('taxable') != 'No',
            })
            line_items.append(line_item)
    else:
        # Create separate labor row for each technician
        for tech_assignment in assigned_technicians:
            line_item = context.copy()
            line_item.update({
                'line_item_type': 'LABOR',
                'labor_description': correction.get('actualCorrection') or correction.get('recommendedCorrection'),
                'labor_rate_type': correction.get('laborRate'),
                'assigned_technician': tech_assignment.get('technician'),
                'assigned_technician_number': tech_assignment.get('technicianNumber'),
                'labor_hours': parse_decimal(correction.get('laborHoursTotal')),
                'actual_hours': parse_decimal(tech_assignment.get('actualHours')),
                'technician_portion': tech_assignment.get('portion', 100),
                'line_total_price': calculate_technician_labor_cost(
                    correction.get('laborTotal'),
                    tech_assignment.get('portion', 100)
                ),
                'taxable': correction.get('taxable') != 'No',
            })
            line_items.append(line_item)
    
    return line_items


def classify_part_type(part: Dict[str, Any]) -> str:
    """Classify a part into the appropriate line item type."""
    description = (part.get('description', '') or '').upper()
    part_category = (part.get('partCategory', '') or '').upper()
    shop_part_number = (part.get('shopPartNumber', '') or '').upper()
    
    # Classify based on description or category
    if 'FREIGHT' in description or 'FREIGHT' in shop_part_number:
        return 'FREIGHT'
    elif any(supply_word in description for supply_word in ['GREASE', 'OIL', 'FLUID', 'WASHER FLUID']):
        return 'SUPPLY'
    elif any(supply_word in part_category for supply_word in ['FLUIDS', 'GREASE', 'SHOP SUPPLIES']):
        return 'SUPPLY'
    else:
        return 'PART'
        

def calculate_technician_labor_cost(total_labor_cost: Any, portion: int) -> float:
    """Calculate a technician's portion of labor cost."""
    total_cost = parse_decimal(total_labor_cost) or 0
    portion_decimal = (portion or 100) / 100.0
    return total_cost * portion_decimal


def parse_decimal(value: Any) -> float:
    """Parse decimal/numeric values."""
    if value is None:
        return 0.0
    try:
        if isinstance(value, (int, float)):
            return float(value)
        elif isinstance(value, str):
            # Remove currency symbols and commas
            cleaned = value.replace("$", "").replace(",", "").strip()
            if cleaned:
                return float(cleaned)
        return 0.0
    except (ValueError, TypeError):
        return 0.0


def test_flattening_logic():
    """
    Test the invoice flattening logic with sample data.
    """
    print("🧪 Testing JSON Flattening Logic")
    print("=" * 50)
    
    try:
        # Test the flattening logic directly
        print(f"📋 Input: Invoice #{SAMPLE_INVOICE_JSON['invoiceNumber']} "
              f"({SAMPLE_INVOICE_JSON['primaryKey']})")
        print(f"📊 Customer: {SAMPLE_INVOICE_JSON['Customer']['title']}")
        print(f"🚛 Unit: {SAMPLE_INVOICE_JSON['ServiceOrder']['Unit']['year']} "
              f"{SAMPLE_INVOICE_JSON['ServiceOrder']['Unit']['make']} "
              f"{SAMPLE_INVOICE_JSON['ServiceOrder']['Unit']['model']}")
        
        # Flatten the invoice
        raw_data_id = 999  # Mock ID for testing
        line_items = flatten_invoice_to_line_items(SAMPLE_INVOICE_JSON, raw_data_id)
        
        print(f"\n✅ Flattening Result: {len(line_items)} line items created")
        print("-" * 50)
        
        # Analyze the results
        analyze_flattening_results(line_items)
        
        # Show sample line items
        show_sample_line_items(line_items)
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def analyze_flattening_results(line_items):
    """
    Analyze and report on the flattening results.
    """
    print("\n📊 FLATTENING ANALYSIS:")
    print("-" * 30)
    
    # Count by type
    type_counts = {}
    total_parts_value = 0
    total_labor_value = 0
    corrections_processed = set()
    technicians = set()
    
    for item in line_items:
        item_type = item.get('line_item_type', 'UNKNOWN')
        type_counts[item_type] = type_counts.get(item_type, 0) + 1
        
        # Track financial totals
        line_total = item.get('line_total_price', 0) or 0
        if item_type == 'PART' or item_type == 'SUPPLY':
            total_parts_value += line_total
        elif item_type == 'LABOR':
            total_labor_value += line_total
            
        # Track corrections and technicians
        if item.get('fullbay_correction_id'):
            corrections_processed.add(item.get('fullbay_correction_id'))
        if item.get('assigned_technician'):
            technicians.add(item.get('assigned_technician'))
    
    # Report counts
    print(f"📦 Parts/Supplies: {type_counts.get('PART', 0) + type_counts.get('SUPPLY', 0)} items")
    print(f"👨‍🔧 Labor: {type_counts.get('LABOR', 0)} items")  
    print(f"🔧 Corrections: {len(corrections_processed)} processed")
    print(f"👥 Technicians: {len(technicians)} involved")
    
    # Financial validation
    print(f"\n💰 FINANCIAL BREAKDOWN:")
    print(f"   Parts/Supplies: ${total_parts_value:,.2f}")
    print(f"   Labor: ${total_labor_value:,.2f}")
    print(f"   Total Line Items: ${total_parts_value + total_labor_value:,.2f}")
    
    # Expected totals from original invoice
    expected_parts = float(SAMPLE_INVOICE_JSON['ServiceOrder']['partsTotal'].replace('$', '').replace(',', ''))
    expected_labor = float(SAMPLE_INVOICE_JSON['ServiceOrder']['laborTotal'].replace('$', '').replace(',', ''))
    expected_total = expected_parts + expected_labor
    
    print(f"\n🎯 VALIDATION CHECK:")
    print(f"   Expected Parts: ${expected_parts:,.2f}")
    print(f"   Expected Labor: ${expected_labor:,.2f}")
    print(f"   Expected Total: ${expected_total:,.2f}")
    
    # Validation status
    parts_match = abs(total_parts_value - expected_parts) < 0.01
    labor_match = abs(total_labor_value - expected_labor) < 0.01
    
    print(f"   Parts Match: {'✅' if parts_match else '❌'}")
    print(f"   Labor Match: {'✅' if labor_match else '❌'}")
    
    if not parts_match or not labor_match:
        print("\n⚠️  VALIDATION ISSUES DETECTED - Review flattening logic!")


def show_sample_line_items(line_items):
    """
    Display sample line items to verify structure.
    """
    print(f"\n📋 SAMPLE LINE ITEMS:")
    print("-" * 50)
    
    # Show first few items of each type
    shown_types = set()
    for item in line_items:
        item_type = item.get('line_item_type')
        if item_type not in shown_types:
            shown_types.add(item_type)
            print(f"\n🏷️  {item_type} Example:")
            
            if item_type == 'PART' or item_type == 'SUPPLY':
                print(f"   Part: {item.get('part_description', 'N/A')}")
                print(f"   Part #: {item.get('shop_part_number', 'N/A')}")
                print(f"   Qty: {item.get('quantity', 0)}")
                print(f"   Unit Price: ${item.get('unit_price', 0):,.2f}")
                print(f"   Line Total: ${item.get('line_total_price', 0):,.2f}")
                
            elif item_type == 'LABOR':
                print(f"   Description: {item.get('labor_description', 'N/A')}")
                print(f"   Technician: {item.get('assigned_technician', 'N/A')}")
                print(f"   Hours: {item.get('actual_hours', 0)}")
                print(f"   Line Total: ${item.get('line_total_price', 0):,.2f}")
            
            print(f"   Invoice: {item.get('fullbay_invoice_id')}")
            print(f"   RO#: {item.get('repair_order_number')}")
            print(f"   Correction: {item.get('fullbay_correction_id')}")


if __name__ == "__main__":
    print("🚀 Starting Fullbay JSON Flattening Test")
    print("=" * 60)
    
    success = test_flattening_logic()
    
    if success:
        print("\n✅ Test completed successfully!")
        print("\n🔄 Next steps:")
        print("   1. Review validation results above")
        print("   2. Test with real Fullbay API data")  
        print("   3. Set up monitoring and reporting")
        print("   4. Deploy to AWS environment")
    else:
        print("\n❌ Test failed - fix issues before proceeding")
        sys.exit(1)