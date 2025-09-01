#!/usr/bin/env python3
"""
Process Multiple FullBay Invoices - Standalone Version

This script processes a JSON file containing multiple FullBay invoices
and flattens them into a single Excel-compatible CSV file.

Usage:
    python scripts/process_multiple_invoices_standalone.py input_file.json [output_file.csv]
"""

import json
import sys
import csv
from datetime import datetime
from typing import Dict, Any, List, Optional
from decimal import Decimal, InvalidOperation


class StandaloneInvoiceProcessor:
    """
    Standalone processor for multiple invoices without database dependencies.
    """
    
    def __init__(self):
        """Initialize the processor."""
        pass
        
    def load_json_file(self, file_path: str) -> Dict[str, Any]:
        """Load JSON file containing multiple invoices."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            print(f"✅ Loaded JSON file: {file_path}")
            return data
        except Exception as e:
            print(f"❌ Error loading JSON file: {e}")
            return {}
    
    def parse_decimal(self, value) -> Decimal:
        """Safely parse decimal values."""
        if value is None or value == "":
            return Decimal('0')
        try:
            return Decimal(str(value))
        except (InvalidOperation, ValueError):
            return Decimal('0')
    
    def extract_invoice_context(self, invoice: Dict[str, Any]) -> Dict[str, Any]:
        """Extract invoice-level context."""
        return {
            'fullbay_invoice_id': invoice.get('primaryKey'),
            'invoice_number': invoice.get('invoiceNumber'),
            'invoice_date': invoice.get('invoiceDate'),
            'due_date': invoice.get('dueDate'),
            'exported': invoice.get('exported'),
            'shop_title': invoice.get('shopTitle'),
            'shop_email': invoice.get('shopEmail'),
            'shop_address': invoice.get('shopPhysicalAddress'),
            'customer_id': invoice.get('Customer', {}).get('customerId'),
            'customer_title': invoice.get('Customer', {}).get('title'),
            'customer_external_id': invoice.get('Customer', {}).get('externalId'),
            'customer_main_phone': invoice.get('Customer', {}).get('mainPhone'),
            'customer_secondary_phone': invoice.get('Customer', {}).get('secondaryPhone'),
            'customer_billing_address': invoice.get('customerBillingAddress'),
            'misc_charge_total': self.parse_decimal(invoice.get('miscChargeTotal')),
            'service_call_total': self.parse_decimal(invoice.get('serviceCallTotal')),
            'mileage_total': self.parse_decimal(invoice.get('mileageTotal')),
            'mileage_cost_total': self.parse_decimal(invoice.get('mileageCostTotal')),
            'parts_total': self.parse_decimal(invoice.get('partsTotal')),
            'labor_hours_total': self.parse_decimal(invoice.get('laborHoursTotal')),
            'labor_total': self.parse_decimal(invoice.get('laborTotal')),
            'sublet_labor_total': self.parse_decimal(invoice.get('subletLaborTotal')),
            'supplies_total': self.parse_decimal(invoice.get('suppliesTotal')),
            'subtotal': self.parse_decimal(invoice.get('subTotal')),
            'tax_total': self.parse_decimal(invoice.get('taxTotal')),
            'total_amount': self.parse_decimal(invoice.get('total')),
            'balance': self.parse_decimal(invoice.get('balance')),
            'quickbooks_id': invoice.get('quickBooksId'),
            'promise_to_pay_date': invoice.get('promiseToPayDate'),
            'created_by_technician': invoice.get('createdByTechnician'),
            'created_by_technician_number': invoice.get('createdByTechnicianNumber'),
            'created': invoice.get('created'),
        }
    
    def extract_service_order_context(self, service_order: Dict[str, Any], invoice_context: Dict[str, Any]) -> Dict[str, Any]:
        """Extract service order context."""
        context = invoice_context.copy()
        
        # Service order details
        context.update({
            'fullbay_service_order_id': service_order.get('primaryKey'),
            'repair_order_number': service_order.get('repairOrderNumber'),
            'primary_technician': service_order.get('technician'),
            'primary_technician_number': service_order.get('technicianNumber'),
            'parts_manager': service_order.get('partsManager'),
            'parts_manager_number': service_order.get('partsManagerNumber'),
            'customer_authorized_on_hours_only': service_order.get('customerAuthorizedOnHoursOnly'),
            'description': service_order.get('description'),
            'submitter_contact': service_order.get('submitterContact'),
            'submitter_contact_email': service_order.get('submitterContactEmail'),
            'submitter_contact_phone': service_order.get('submitterContactPhone'),
            'submitter_contact_cell_phone': service_order.get('submitterContactCellPhone'),
            'authorizer_contact': service_order.get('authorizerContact'),
            'authorizer_contact_email': service_order.get('authorizerContactEmail'),
            'authorizer_contact_phone': service_order.get('authorizerContactPhone'),
            'authorizer_contact_cell_phone': service_order.get('authorizerContactCellPhone'),
            'billing_customer': service_order.get('billingCustomer'),
            'unit_access_method': service_order.get('unitAccessMethod'),
            'unit_available_date_time': service_order.get('unitAvailableDateTime'),
            'unit_must_be_accessed_at_available_date_time': service_order.get('unitMustBeAccessedAtAvailableDateTime'),
            'unit_return_date_time': service_order.get('unitReturnDateTime'),
            'unit_return_asap': service_order.get('unitReturnAsap'),
            'location_information': service_order.get('locationInformation'),
            'authorization_number': service_order.get('authorizationNumber'),
            'po_number': service_order.get('poNumber'),
            'parts_po_number': service_order.get('partsPoNumber'),
            'hot': service_order.get('hot'),
            'follow_in_use_schedule': service_order.get('followInUseSchedule'),
            'unscheduled': service_order.get('unscheduled'),
            'customer_threshold': self.parse_decimal(service_order.get('customerThreshold')),
            'pre_authorized': service_order.get('preAuthorized'),
            'so_labor_hours_total': self.parse_decimal(service_order.get('laborHoursTotal')),
            'so_actual_hours_total': self.parse_decimal(service_order.get('actualHoursTotal')),
            'so_labor_total': self.parse_decimal(service_order.get('laborTotal')),
            'so_sublet_labor_total': self.parse_decimal(service_order.get('subletLaborTotal')),
            'so_parts_cost_total': self.parse_decimal(service_order.get('partsCostTotal')),
            'so_parts_total': self.parse_decimal(service_order.get('partsTotal')),
            'so_mileage_total': self.parse_decimal(service_order.get('mileageTotal')),
            'so_mileage_cost_total': self.parse_decimal(service_order.get('mileageCostTotal')),
            'override_supplies_total': service_order.get('overrideSuppliesTotal'),
            'overriden_supplies_total': self.parse_decimal(service_order.get('overridenSuppliesTotal')),
            'so_service_call_total': self.parse_decimal(service_order.get('serviceCallTotal')),
            'all_parts_priced_date_time': service_order.get('allPartsPricedDateTime'),
            'service_order_start_date': service_order.get('startDateTime'),
            'service_order_completion_date': service_order.get('completionDateTime'),
            'service_order_created_by_technician': service_order.get('createdByTechnician'),
            'service_order_created_by_technician_number': service_order.get('createdByTechnicianNumber'),
            'service_order_created': service_order.get('created'),
        })
        
        # Unit/Vehicle details
        unit = service_order.get('Unit', {})
        context.update({
            'unit_id': unit.get('customerUnitId'),
            'unit_number': unit.get('number'),
            'unit_nickname': unit.get('nickname'),
            'unit_type': unit.get('type'),
            'unit_subtype': unit.get('subType'),
            'unit_year': unit.get('year'),
            'unit_make': unit.get('make'),
            'unit_model': unit.get('model'),
            'unit_vin': unit.get('vin'),
            'unit_license_plate': unit.get('licensePlate'),
        })
        
        return context
    
    def extract_complaint_context(self, complaint: Dict[str, Any], service_order_context: Dict[str, Any]) -> Dict[str, Any]:
        """Extract complaint context."""
        context = service_order_context.copy()
        
        context.update({
            'fullbay_complaint_id': complaint.get('primaryKey'),
            'complaint_mileage_rate': complaint.get('mileageRate'),
            'complaint_labor_rate': complaint.get('laborRate'),
            'complaint_type': complaint.get('type'),
            'complaint_subtype': complaint.get('subType'),
            'complaint_authorized': complaint.get('authorized'),
            'complaint_severity': complaint.get('severity'),
            'complaint_note': complaint.get('note'),
            'complaint_cause': complaint.get('cause'),
            'complaint_cause_type': complaint.get('causeType'),
            'complaint_labor_hours_total': self.parse_decimal(complaint.get('laborHoursTotal')),
            'complaint_actual_hours_total': self.parse_decimal(complaint.get('actualHoursTotal')),
            'complaint_labor_taxable': complaint.get('laborTaxable'),
            'complaint_labor_total': self.parse_decimal(complaint.get('laborTotal')),
            'complaint_parts_cost_total': self.parse_decimal(complaint.get('partsCostTotal')),
            'complaint_parts_total': self.parse_decimal(complaint.get('partsTotal')),
            'complaint_mileage_taxable': complaint.get('mileageTaxable'),
            'complaint_mileage_total': self.parse_decimal(complaint.get('mileageTotal')),
            'complaint_mileage_cost_total': self.parse_decimal(complaint.get('mileageCostTotal')),
            'complaint_service_call_taxable': complaint.get('serviceCallTaxable'),
            'complaint_service_call_total': self.parse_decimal(complaint.get('serviceCallTotal')),
            'complaint_sublet': complaint.get('sublet'),
            'complaint_part_category': complaint.get('partCategory'),
            'complaint_quickbooks_account': complaint.get('quickBooksAccount'),
            'complaint_quickbooks_item': complaint.get('quickBooksItem'),
            'complaint_quickbooks_item_type': complaint.get('quickBooksItemType'),
            'complaint_created': complaint.get('created'),
        })
        
        return context
    
    def extract_correction_context(self, correction: Dict[str, Any], complaint_context: Dict[str, Any]) -> Dict[str, Any]:
        """Extract correction context."""
        context = complaint_context.copy()
        
        context.update({
            'fullbay_correction_id': correction.get('primaryKey'),
            'correction_global_component': correction.get('globalComponent'),
            'correction_global_system': correction.get('globalSystem'),
            'correction_global_service': correction.get('globalService'),
            'correction_unit_service': correction.get('unitService'),
            'correction_labor_rate': correction.get('laborRate'),
            'correction_title': correction.get('title'),
            'correction_recommended_correction': correction.get('recommendedCorrection'),
            'correction_actual_correction': correction.get('actualCorrection'),
            'correction_performed': correction.get('correctionPerformed'),
            'correction_pre_authorized': correction.get('preAuthorized'),
            'correction_pre_paid': correction.get('prePaid'),
            'correction_labor_hours_total': self.parse_decimal(correction.get('laborHoursTotal')),
            'correction_labor_total': self.parse_decimal(correction.get('laborTotal')),
            'correction_taxable': correction.get('taxable'),
            'correction_parts_cost_total': self.parse_decimal(correction.get('partsCostTotal')),
            'correction_parts_total': self.parse_decimal(correction.get('partsTotal')),
            'correction_created': correction.get('created'),
        })
        
        return context
    
    def classify_part_type(self, part: Dict[str, Any]) -> str:
        """Classify part type based on description and category."""
        description = (part.get('description', '') or '').upper()
        category = (part.get('partCategory', '') or '').upper()
        
        if 'FREIGHT' in description or 'FREIGHT' in category:
            return 'FREIGHT'
        elif any(word in description for word in ['OIL', 'GREASE', 'ANTI-FREEZE', 'DEF', 'WASHER FLUID', 'FREON']):
            return 'SUPPLY'
        elif any(word in description for word in ['SHOP SUPPLY', 'SHOP SUPPLIES']):
            return 'SUPPLY'
        else:
            return 'PART'
    
    def process_parts(self, parts: List[Dict[str, Any]], context: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process parts and group by description and price."""
        if not parts:
            return []
        
        # Group parts by description and price
        parts_groups = {}
        for part in parts:
            description = part.get('description', '')
            price = self.parse_decimal(part.get('sellingPrice'))
            group_key = f"{description}_{price}"
            
            if group_key not in parts_groups:
                parts_groups[group_key] = {
                    'parts': [],
                    'total_quantity': Decimal('0'),
                    'total_cost': Decimal('0'),
                    'total_price': Decimal('0')
                }
            
            parts_groups[group_key]['parts'].append(part)
            quantity = self.parse_decimal(part.get('quantity', 1))
            parts_groups[group_key]['total_quantity'] += quantity
            parts_groups[group_key]['total_cost'] += self.parse_decimal(part.get('cost', 0)) * quantity
            parts_groups[group_key]['total_price'] += price * quantity
        
        # Create line items from grouped parts
        line_items = []
        for group_key, group_data in parts_groups.items():
            template_part = group_data['parts'][0]
            
            line_item = context.copy()
            line_item.update({
                'line_item_type': self.classify_part_type(template_part),
                'fullbay_part_id': template_part.get('primaryKey'),
                'part_description': template_part.get('description'),
                'shop_part_number': template_part.get('shopPartNumber'),
                'vendor_part_number': template_part.get('vendorPartNumber'),
                'part_category': template_part.get('partCategory'),
                'quantity': float(group_data['total_quantity']),
                'unit_cost': float(self.parse_decimal(template_part.get('cost'))),
                'unit_price': float(self.parse_decimal(template_part.get('sellingPrice'))),
                'line_total_cost': float(group_data['total_cost']),
                'line_total_price': float(group_data['total_price']),
                'price_overridden': template_part.get('sellingPriceOverridden') == 'Yes',
                'taxable': template_part.get('taxable') == 'Yes',
                'inventory_item': template_part.get('inventory') == 'Yes',
                'core_type': template_part.get('coreType'),
                'sublet': template_part.get('sublet') == 'Yes',
                'quickbooks_account': template_part.get('quickBooksAccount'),
                'quickbooks_item': template_part.get('quickBooksItem'),
                'quickbooks_item_type': template_part.get('quickBooksItemType'),
                'part_created': template_part.get('created'),
            })
            
            line_items.append(line_item)
        
        return line_items
    
    def process_labor(self, correction: Dict[str, Any], complaint: Dict[str, Any], context: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process labor and split by technician."""
        line_items = []
        
        # Get assigned technicians
        assigned_technicians = complaint.get('AssignedTechnicians', [])
        
        if not assigned_technicians:
            # No technicians assigned, create one line item with primary technician
            line_item = context.copy()
            line_item.update({
                'line_item_type': 'LABOR',
                'technician': context.get('primary_technician'),
                'technician_number': context.get('primary_technician_number'),
                'actual_hours': float(context.get('complaint_actual_hours_total', 0)),
                'portion': 100.0,
                'labor_rate': context.get('complaint_labor_rate'),
                'labor_hours': float(context.get('complaint_labor_hours_total', 0)),
                'labor_total': float(context.get('complaint_labor_total', 0)),
                'line_total_price': float(context.get('complaint_labor_total', 0)),
                'line_total_cost': 0.0,  # Cost not typically tracked for labor
            })
            line_items.append(line_item)
        else:
            # Process each technician separately
            for tech in assigned_technicians:
                line_item = context.copy()
                line_item.update({
                    'line_item_type': 'LABOR',
                    'technician': tech.get('technician'),
                    'technician_number': tech.get('technicianNumber'),
                    'actual_hours': float(self.parse_decimal(tech.get('actualHours'))),
                    'portion': float(self.parse_decimal(tech.get('portion'))),
                    'labor_rate': context.get('complaint_labor_rate'),
                    'labor_hours': float(context.get('complaint_labor_hours_total', 0)),
                    'labor_total': float(context.get('complaint_labor_total', 0)),
                    'line_total_price': float(self.parse_decimal(tech.get('actualHours')) * self.parse_decimal(context.get('complaint_labor_total', 0)) / self.parse_decimal(context.get('complaint_actual_hours_total', 1))),
                    'line_total_cost': 0.0,  # Cost not typically tracked for labor
                })
                line_items.append(line_item)
        
        return line_items
    
    def process_invoice_level_charges(self, invoice: Dict[str, Any], context: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process invoice-level charges like misc charges, supplies, etc."""
        line_items = []
        
        # Process misc charges
        misc_total = self.parse_decimal(invoice.get('miscChargeTotal'))
        if misc_total > 0:
            line_item = context.copy()
            line_item.update({
                'line_item_type': 'MISC',
                'part_description': 'Miscellaneous Charges',
                'quantity': 1.0,
                'unit_price': float(misc_total),
                'line_total_price': float(misc_total),
                'line_total_cost': 0.0,
            })
            line_items.append(line_item)
        
        # Process supplies
        supplies_total = self.parse_decimal(invoice.get('suppliesTotal'))
        if supplies_total > 0:
            line_item = context.copy()
            line_item.update({
                'line_item_type': 'SUPPLY',
                'part_description': 'Shop Supplies',
                'quantity': 1.0,
                'unit_price': float(supplies_total),
                'line_total_price': float(supplies_total),
                'line_total_cost': 0.0,
            })
            line_items.append(line_item)
        
        # Process service call charges
        service_call_total = self.parse_decimal(invoice.get('serviceCallTotal'))
        if service_call_total > 0:
            line_item = context.copy()
            line_item.update({
                'line_item_type': 'SERVICE_CALL',
                'part_description': 'Service Call Charge',
                'quantity': 1.0,
                'unit_price': float(service_call_total),
                'line_total_price': float(service_call_total),
                'line_total_cost': 0.0,
            })
            line_items.append(line_item)
        
        # Process mileage charges
        mileage_total = self.parse_decimal(invoice.get('mileageTotal'))
        if mileage_total > 0:
            line_item = context.copy()
            line_item.update({
                'line_item_type': 'MILEAGE',
                'part_description': 'Mileage Charges',
                'quantity': 1.0,
                'unit_price': float(mileage_total),
                'line_total_price': float(mileage_total),
                'line_total_cost': 0.0,
            })
            line_items.append(line_item)
        
        return line_items
    
    def flatten_invoice(self, invoice: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Flatten a single invoice into line items."""
        line_items = []
        
        try:
            # Extract invoice context
            invoice_context = self.extract_invoice_context(invoice)
            
            # Get service order data
            service_order = invoice.get('ServiceOrder', {})
            if not service_order:
                print(f"  ⚠️  No service order found for invoice {invoice.get('invoiceNumber')}")
                return line_items
            
            service_order_context = self.extract_service_order_context(service_order, invoice_context)
            
            # Get complaints
            complaints = service_order.get('Complaints', [])
            
            # Process each complaint
            for complaint in complaints:
                complaint_context = self.extract_complaint_context(complaint, service_order_context)
                
                corrections = complaint.get('Corrections', [])
                
                # Process each correction within the complaint
                for correction in corrections:
                    correction_context = self.extract_correction_context(correction, complaint_context)
                    
                    # Process parts for this correction
                    parts = correction.get('Parts', [])
                    if parts:
                        parts_line_items = self.process_parts(parts, correction_context)
                        line_items.extend(parts_line_items)
                    
                    # Process labor for this correction
                    labor_line_items = self.process_labor(correction, complaint, correction_context)
                    line_items.extend(labor_line_items)
            
            # Process invoice-level charges
            misc_charges = self.process_invoice_level_charges(invoice, invoice_context)
            line_items.extend(misc_charges)
            
        except Exception as e:
            print(f"  ❌ Error flattening invoice: {e}")
        
        return line_items
    
    def process_invoices(self, json_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process all invoices in the JSON data."""
        all_line_items = []
        
        # Extract invoices from resultSet
        result_set = json_data.get('resultSet', [])
        total_invoices = len(result_set)
        
        print(f"📊 Processing {total_invoices} invoices...")
        
        for i, invoice in enumerate(result_set, 1):
            try:
                print(f"Processing invoice {i}/{total_invoices}: {invoice.get('invoiceNumber', 'Unknown')}")
                
                # Flatten the invoice
                line_items = self.flatten_invoice(invoice)
                
                if line_items:
                    all_line_items.extend(line_items)
                    print(f"  ✅ Generated {len(line_items)} line items")
                else:
                    print(f"  ⚠️  No line items generated for this invoice")
                    
            except Exception as e:
                print(f"  ❌ Error processing invoice {i}: {e}")
                continue
        
        print(f"🎉 Total line items generated: {len(all_line_items)}")
        return all_line_items
    
    def save_to_csv(self, line_items: List[Dict[str, Any]], output_file: str):
        """Save flattened data to CSV file."""
        if not line_items:
            print("❌ No line items to save")
            return
        
        try:
            # Get all unique column names
            all_columns = set()
            for item in line_items:
                all_columns.update(item.keys())
            
            # Sort columns for consistent output
            columns = sorted(list(all_columns))
            
            with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=columns)
                writer.writeheader()
                writer.writerows(line_items)
            
            print(f"✅ Saved {len(line_items)} line items to: {output_file}")
            print(f"📋 Columns: {len(columns)}")
            
        except Exception as e:
            print(f"❌ Error saving CSV file: {e}")
    
    def generate_summary(self, line_items: List[Dict[str, Any]], json_data: Dict[str, Any]):
        """Generate a summary of the processing results."""
        if not line_items:
            print("❌ No data to summarize")
            return
        
        # Calculate totals
        total_revenue = sum(float(item.get('line_total_price', 0) or 0) for item in line_items)
        total_cost = sum(float(item.get('line_total_cost', 0) or 0) for item in line_items)
        total_profit = total_revenue - total_cost
        
        # Count by type
        type_counts = {}
        for item in line_items:
            item_type = item.get('line_item_type', 'UNKNOWN')
            type_counts[item_type] = type_counts.get(item_type, 0) + 1
        
        # Unique counts
        unique_invoices = len(set(item.get('fullbay_invoice_id') for item in line_items))
        unique_customers = len(set(item.get('customer_id') for item in line_items))
        unique_vehicles = len(set(item.get('unit_vin') for item in line_items if item.get('unit_vin')))
        
        print("\n" + "="*60)
        print("📊 PROCESSING SUMMARY")
        print("="*60)
        print(f"📄 Input: {json_data.get('resultCount', 0)} invoices")
        print(f"📋 Output: {len(line_items)} line items")
        print(f"💰 Total Revenue: ${total_revenue:,.2f}")
        print(f"💸 Total Cost: ${total_cost:,.2f}")
        print(f"💵 Total Profit: ${total_profit:,.2f}")
        print(f"📈 Profit Margin: {(total_profit/total_revenue*100):.1f}%" if total_revenue > 0 else "N/A")
        print(f"🏢 Unique Invoices: {unique_invoices}")
        print(f"👥 Unique Customers: {unique_customers}")
        print(f"🚛 Unique Vehicles: {unique_vehicles}")
        
        print("\n📊 Line Items by Type:")
        for item_type, count in sorted(type_counts.items()):
            print(f"  {item_type}: {count}")
        
        print("="*60)


def main():
    """Main function to process multiple invoices."""
    if len(sys.argv) < 2:
        print("Usage: python scripts/process_multiple_invoices_standalone.py input_file.json [output_file.csv]")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else f"flattened_invoices_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    print("🚀 FullBay Multiple Invoice Processor - Standalone")
    print("="*60)
    
    # Initialize processor
    processor = StandaloneInvoiceProcessor()
    
    # Load JSON data
    json_data = processor.load_json_file(input_file)
    if not json_data:
        sys.exit(1)
    
    # Process invoices
    line_items = processor.process_invoices(json_data)
    
    if not line_items:
        print("❌ No line items generated. Exiting.")
        sys.exit(1)
    
    # Save to CSV
    processor.save_to_csv(line_items, output_file)
    
    # Generate summary
    processor.generate_summary(line_items, json_data)
    
    print(f"\n🎉 Processing complete! Open {output_file} in Excel to view the data.")


if __name__ == "__main__":
    main()
