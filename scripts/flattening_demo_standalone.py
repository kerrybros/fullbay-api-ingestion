#!/usr/bin/env python3
"""
FullBay Invoice Flattening Demo - Standalone Version

This script demonstrates the complete flattening logic for FullBay invoices.
It processes sample JSON and outputs the results in Excel-compatible format
to ensure no data loss and proper column mapping.

Usage:
    python scripts/flattening_demo_standalone.py [sample_json_file.json]
    
If no file is provided, it will use the built-in sample data.
"""

import json
import sys
import csv
from datetime import datetime
from typing import Dict, Any, List, Optional
import os


class StandaloneFlatteningDemo:
    """
    Standalone demo class for testing and demonstrating the flattening logic.
    """
    
    def __init__(self):
        """Initialize the demo with sample configuration."""
        
        # Sample data for demonstration
        self.sample_invoice = {
            "primaryKey": "17506970",
            "invoiceNumber": "14282",
            "invoiceDate": "2024-01-15",
            "dueDate": "2024-02-15",
            "exported": "0",
            "shopTitle": "Kerry Brothers Truck Repair",
            "shopEmail": "info@kerrybrothers.com",
            "shopPhysicalAddress": "123 Main St, Anytown, USA",
            "subTotal": "2500.00",
            "taxTotal": "200.00",
            "total": "2700.00",
            "customerBillingAddress": "456 Business Ave, Customer City, USA",
            "Customer": {
                "customerId": 2250840,
                "title": "GAETANO #3",
                "externalId": "CUST001",
                "mainPhone": "555-123-4567",
                "secondaryPhone": "555-987-6543"
            },
            "ServiceOrder": {
                "primaryKey": "15448",
                "repairOrderNumber": "RO-2024-001",
                "created": "2024-01-15T08:00:00Z",
                "startDateTime": "2024-01-15T09:00:00Z",
                "completionDateTime": "2024-01-15T17:00:00Z",
                "technician": "Jacob Humphries",
                "technicianNumber": "TECH001",
                "partsCostTotal": "1200.00",
                "partsTotal": "1800.00",
                "laborHoursTotal": "5.55",
                "laborTotal": "700.00",
                "Customer": {
                    "customerId": 2250840,
                    "title": "GAETANO #3"
                },
                "Unit": {
                    "customerUnitId": "UNIT001",
                    "number": "TRUCK-001",
                    "type": "Tractor",
                    "year": "2019",
                    "make": "Freightliner",
                    "model": "Cascadia",
                    "vin": "3AKBHKDV0KSKC8657",
                    "licensePlate": "ABC123"
                },
                "Complaints": [
                    {
                        "primaryKey": "COMP001",
                        "type": "Mechanical",
                        "subType": "Engine",
                        "note": "Engine running rough",
                        "cause": "DEF system issues",
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
                                "primaryKey": "42557049",
                                "title": "Computer Check",
                                "globalComponent": "Engine",
                                "globalSystem": "DEF System",
                                "globalService": "Diagnostic",
                                "recommendedCorrection": "Check DEF system and replace faulty components",
                                "actualCorrection": "Replaced DEF sensor and cleaned system",
                                "correctionPerformed": "Yes",
                                "laborRate": "Standard",
                                "laborHoursTotal": "3.42",
                                "laborTotal": "1650.00",
                                "taxable": "Yes",
                                "Parts": [
                                    {
                                        "primaryKey": "PART001",
                                        "description": "DEF AWNING KIT",
                                        "shopPartNumber": "4921911",
                                        "vendorPartNumber": "VENDOR001",
                                        "partCategory": "Engine Parts",
                                        "quantity": "1",
                                        "cost": "350.00",
                                        "sellingPrice": "412.80",
                                        "sellingPriceOverridden": "No",
                                        "taxable": "Yes",
                                        "inventory": "Yes",
                                        "coreType": "None",
                                        "sublet": "No",
                                        "toBeReturnedQuantity": "0",
                                        "returnedQuantity": "0",
                                        "quickBooksAccount": "Parts Sales",
                                        "quickBooksItem": "DEF Kit",
                                        "quickBooksItemType": "Inventory"
                                    },
                                    {
                                        "primaryKey": "PART002",
                                        "description": "GASKET, EXHAUST",
                                        "shopPartNumber": "TCX/AMS013",
                                        "vendorPartNumber": "VENDOR002",
                                        "partCategory": "Gaskets",
                                        "quantity": "2",
                                        "cost": "8.50",
                                        "sellingPrice": "32.42",
                                        "sellingPriceOverridden": "No",
                                        "taxable": "Yes",
                                        "inventory": "Yes",
                                        "coreType": "None",
                                        "sublet": "No",
                                        "toBeReturnedQuantity": "0",
                                        "returnedQuantity": "0",
                                        "quickBooksAccount": "Parts Sales",
                                        "quickBooksItem": "Gasket",
                                        "quickBooksItemType": "Inventory"
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        "primaryKey": "COMP002",
                        "type": "Maintenance",
                        "subType": "Preventive",
                        "note": "Regular maintenance service",
                        "cause": "Scheduled maintenance",
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
                                "primaryKey": "42940071",
                                "title": "LOF-PM SERVICE-REGULAR",
                                "globalComponent": "Engine",
                                "globalSystem": "Lubrication",
                                "globalService": "Maintenance",
                                "recommendedCorrection": "Oil change and filter replacement",
                                "actualCorrection": "Changed oil and replaced filters",
                                "correctionPerformed": "Yes",
                                "laborRate": "Standard",
                                "laborHoursTotal": "2.13",
                                "laborTotal": "210.00",
                                "taxable": "Yes",
                                "Parts": [
                                    {
                                        "primaryKey": "PART003",
                                        "description": "OIL - 1540",
                                        "shopPartNumber": "1540",
                                        "vendorPartNumber": "VENDOR003",
                                        "partCategory": "Fluids",
                                        "quantity": "40",
                                        "cost": "4.50",
                                        "sellingPrice": "6.48",
                                        "sellingPriceOverridden": "No",
                                        "taxable": "Yes",
                                        "inventory": "Yes",
                                        "coreType": "None",
                                        "sublet": "No",
                                        "toBeReturnedQuantity": "0",
                                        "returnedQuantity": "0",
                                        "quickBooksAccount": "Parts Sales",
                                        "quickBooksItem": "Oil",
                                        "quickBooksItemType": "Inventory"
                                    },
                                    {
                                        "primaryKey": "PART004",
                                        "description": "FILTER, OIL",
                                        "shopPartNumber": "P7505",
                                        "vendorPartNumber": "VENDOR004",
                                        "partCategory": "Filters",
                                        "quantity": "1",
                                        "cost": "45.00",
                                        "sellingPrice": "57.79",
                                        "sellingPriceOverridden": "No",
                                        "taxable": "Yes",
                                        "inventory": "Yes",
                                        "coreType": "None",
                                        "sublet": "No",
                                        "toBeReturnedQuantity": "0",
                                        "returnedQuantity": "0",
                                        "quickBooksAccount": "Parts Sales",
                                        "quickBooksItem": "Oil Filter",
                                        "quickBooksItemType": "Inventory"
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        }
    
    def load_sample_json(self, file_path: str) -> Dict[str, Any]:
        """Load sample JSON from file."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading JSON file: {e}")
            return self.sample_invoice
    
    def _parse_decimal(self, value: Any) -> Optional[float]:
        """Parse decimal values safely."""
        if value is None:
            return None
        
        try:
            if isinstance(value, (int, float)):
                return float(value)
            elif isinstance(value, str):
                cleaned = value.replace("$", "").replace(",", "").strip()
                if cleaned:
                    return float(cleaned)
            return None
        except (ValueError, TypeError):
            return None
    
    def _parse_date(self, date_str: Optional[str]) -> Optional[str]:
        """Parse date string."""
        if not date_str:
            return None
        return date_str
    
    def _parse_datetime(self, datetime_str: Optional[str]) -> Optional[str]:
        """Parse datetime string."""
        if not datetime_str:
            return None
        return datetime_str
    
    def _classify_part_type(self, part: Dict[str, Any]) -> str:
        """Classify a part into the appropriate line item type."""
        description = (part.get('description', '') or '').upper()
        shop_part_number = (part.get('shopPartNumber', '') or '').upper()
        
        # Only classify FREIGHT based on part number or description
        if 'FREIGHT' in description or 'FREIGHT' in shop_part_number:
            return 'FREIGHT'
        else:
            return 'PART'
    
    def _extract_invoice_context(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Extract invoice-level context that will be repeated on every line item."""
        service_order = record.get('ServiceOrder', {})
        customer = record.get('Customer', {})
        so_customer = service_order.get('Customer', {})
        unit = service_order.get('Unit', {})
        
        # Use service order customer if available, otherwise use invoice customer
        customer_data = so_customer if so_customer else customer
        
        context = {
            # Invoice level
            'invoice_id': record.get('primaryKey'),
            'invoice_number': record.get('invoiceNumber'),
            'invoice_date': self._parse_date(record.get('invoiceDate')),
            'due_date': self._parse_date(record.get('dueDate')),
            'exported': record.get('exported') == '1',
            
            # Shop info
            'shop_title': record.get('shopTitle'),
            'shop_address': record.get('shopPhysicalAddress'),
            
            # Customer info
            'customer_id': customer_data.get('customerId'),
            'customer_title': customer_data.get('title'),
            'customer_external_id': customer_data.get('externalId'),
            'customer_main_phone': customer_data.get('mainPhone'),
            'customer_secondary_phone': customer_data.get('secondaryPhone'),
            'customer_billing_address': record.get('customerBillingAddress'),
            
                         # Service order info
             'service_order_primary_key': service_order.get('primaryKey'),
                         'service_order_number': service_order.get('repairOrderNumber'),
            'service_order_created': self._parse_datetime(service_order.get('created')),
            'service_order_start_date': self._parse_datetime(service_order.get('startDateTime')),
            'service_order_completion_date': self._parse_datetime(service_order.get('completionDateTime')),
            
            # Unit/Vehicle info
            'unit_id': unit.get('customerUnitId'),
            'unit_number': unit.get('number'),
            'unit_type': unit.get('type'),
            'unit_year': unit.get('year'),
            'unit_make': unit.get('make'),
            'unit_model': unit.get('model'),
            'unit_vin': unit.get('vin'),
            'unit_license_plate': unit.get('licensePlate'),
            
            # Primary technician
            'primary_technician': service_order.get('technician'),
            'primary_technician_number': service_order.get('technicianNumber'),
            
            
        }
        
        return context
    
    def _extract_complaint_context(self, complaint: Dict[str, Any], invoice_context: Dict[str, Any]) -> Dict[str, Any]:
        """Extract complaint-level context."""
        context = invoice_context.copy()
        
        # Add complaint-specific data
        context.update({
            'SOAI_ID': complaint.get('primaryKey'),
            'complaint_type': complaint.get('type'),
            'complaint_subtype': complaint.get('subType'),
            'complaint_note': complaint.get('note'),
            'complaint_cause': complaint.get('cause'),
            'complaint_authorized': complaint.get('authorized') == 'Yes',
        })
        
        return context
    
    def _extract_correction_context(self, correction: Dict[str, Any], complaint_context: Dict[str, Any]) -> Dict[str, Any]:
        """Extract correction-level context."""
        context = complaint_context.copy()
        
        # Add correction-specific data
        context.update({
                         'fullbay_correction_id': correction.get('primaryKey'),
             'correction_title': correction.get('title'),
             'component': correction.get('globalComponent'),
             'system': correction.get('globalSystem'),
             'global_service': correction.get('globalService'),
            'recommended_correction': correction.get('recommendedCorrection'),
            'actual_correction': correction.get('actualCorrection'),
            'correction_performed': correction.get('correctionPerformed'),
        })
        
        return context
    
    def _process_parts(self, parts: List[Dict[str, Any]], context: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process parts for a correction, grouping identical parts correctly."""
        if not parts:
            return []
        
        # Filter out parts with zero effective quantity
        valid_parts = []
        for part in parts:
            quantity = self._parse_decimal(part.get('quantity', 0)) or 0
            to_be_returned = self._parse_decimal(part.get('toBeReturnedQuantity', 0)) or 0
            returned = self._parse_decimal(part.get('returnedQuantity', 0)) or 0
            core_type = part.get('coreType', '')
            
            # Special handling for dirty cores
            if core_type == 'Dirty':
                # For dirty cores, only include if returnedQuantity > 0
                if returned > 0:
                    # Use returnedQuantity as the effective quantity
                    effective_quantity = returned
                    # Mark this as a dirty core for special pricing
                    part['_is_dirty_core'] = True
                else:
                    # Omit dirty cores with no returned quantity
                    continue
            else:
                # Regular parts - calculate effective quantity based on return logic
                if returned > 0:
                    # If returned quantity > 0, subtract returned from quantity
                    effective_quantity = quantity - returned
                elif to_be_returned > 0:
                    # If returned is 0 but to_be_returned > 0, subtract to_be_returned from quantity
                    effective_quantity = quantity - to_be_returned
                else:
                    # If both are 0, use original quantity
                    effective_quantity = quantity
            
            # Only include parts with positive effective quantity
            if effective_quantity > 0:
                valid_parts.append(part)
        
        if not valid_parts:
            return []
        
        # Group parts by part_number + unit_price + service_description
        parts_groups = {}
        
        for part in valid_parts:
            shop_part_number = part.get('shopPartNumber', '')
            selling_price = self._parse_decimal(part.get('sellingPrice'))
            
            # Get service description from context
            service_description = context.get('actual_correction') or context.get('recommended_correction') or ''
            
            # Calculate effective quantity for this part
            quantity = self._parse_decimal(part.get('quantity', 0)) or 0
            to_be_returned = self._parse_decimal(part.get('toBeReturnedQuantity', 0)) or 0
            returned = self._parse_decimal(part.get('returnedQuantity', 0)) or 0
            core_type = part.get('coreType', '')
            
            # Special handling for dirty cores
            if core_type == 'Dirty':
                # For dirty cores, use returnedQuantity as effective quantity
                effective_quantity = returned
                # Make selling price negative for dirty cores (core refund)
                adjusted_selling_price = -selling_price if selling_price else 0
            else:
                # Regular parts - calculate effective quantity based on return logic
                if returned > 0:
                    effective_quantity = quantity - returned
                elif to_be_returned > 0:
                    effective_quantity = quantity - to_be_returned
                else:
                    effective_quantity = quantity
                adjusted_selling_price = selling_price
            
            # Create grouping key (include cost and service description for separate grouping)
            unit_cost = self._parse_decimal(part.get('cost', 0)) or 0
            group_key = f"{shop_part_number}|{adjusted_selling_price}|{unit_cost}|{service_description}"
            
            if group_key not in parts_groups:
                parts_groups[group_key] = {
                    'parts': [],
                    'total_quantity': 0,
                    'total_cost': 0,
                    'total_price': 0
                }
            
            parts_groups[group_key]['parts'].append(part)
            parts_groups[group_key]['total_quantity'] += effective_quantity
            parts_groups[group_key]['total_cost'] += self._parse_decimal(part.get('cost', 0)) * effective_quantity
            parts_groups[group_key]['total_price'] += adjusted_selling_price * effective_quantity
        
        # Create line items from grouped parts
        line_items = []
        
        for group_key, group_data in parts_groups.items():
            # Use the first part in the group as the template
            template_part = group_data['parts'][0]
            
            line_item = context.copy()
            
            # Check if this is a dirty core group
            is_dirty_core = any(p.get('_is_dirty_core', False) for p in group_data['parts'])
            
            # Calculate adjusted unit price for dirty cores
            original_unit_price = self._parse_decimal(template_part.get('sellingPrice'))
            if is_dirty_core:
                unit_price = -original_unit_price if original_unit_price else 0
            else:
                unit_price = original_unit_price
            
            line_item.update({
                'line_item_type': self._classify_part_type(template_part),
                'fullbay_part_id': template_part.get('primaryKey'),
                'part_description': template_part.get('description'),
                'shop_part_number': template_part.get('shopPartNumber'),
                'vendor_part_number': template_part.get('vendorPartNumber'),
                'part_category': template_part.get('partCategory'),
                
                # Calculate effective quantities and totals
                'quantity': group_data['total_quantity'],
                'to_be_returned_quantity': sum(self._parse_decimal(p.get('toBeReturnedQuantity', 0)) for p in group_data['parts']),
                'returned_quantity': sum(self._parse_decimal(p.get('returnedQuantity', 0)) for p in group_data['parts']),
                
                # Financial details based on effective quantity
                'unit_cost': self._parse_decimal(template_part.get('cost')),
                'unit_price': unit_price,
                'total_cost': group_data['total_cost'],
                'total': group_data['total_price'],
                'price_overridden': template_part.get('sellingPriceOverridden') == 'Yes',
                
                # Classification
                'taxable': template_part.get('taxable') == 'Yes',
                'inventory_item': template_part.get('inventory') == 'Yes',
                'core_type': template_part.get('coreType'),
                'sublet': template_part.get('sublet') == 'Yes',
                
                # Service description for parts
                'service_description': context.get('actual_correction') or context.get('recommended_correction'),
                
            })
            
            line_items.append(line_item)
        
        return line_items
    
    def _process_labor(self, correction: Dict[str, Any], complaint: Dict[str, Any], context: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process labor for a correction, creating separate rows per technician."""
        line_items = []
        
        # Get assigned technicians from the complaint
        assigned_technicians = complaint.get('AssignedTechnicians', [])
        total_labor_hours = self._parse_decimal(correction.get('laborHoursTotal', 0))
        
        if not assigned_technicians:
            # No specific technician assignments, but there's labor - create one labor row
            if total_labor_hours > 0:
                line_item = context.copy()
                line_item.update({
                    'line_item_type': 'LABOR',
                    'service_description': correction.get('actualCorrection') or correction.get('recommendedCorrection'),
                    'labor_rate_type': correction.get('laborRate'),
                    'assigned_technician': context.get('primary_technician'),
                    'assigned_technician_number': context.get('primary_technician_number'),
                    
                    'labor_hours': total_labor_hours,
                    'actual_hours': total_labor_hours,  # Use total if no individual hours
                    'technician_portion': 100,
                    
                    'total': self._parse_decimal(correction.get('laborTotal')),
                    'taxable': correction.get('taxable') != 'No',
                })
                line_items.append(line_item)
        else:
            # Calculate total portion to ensure proper distribution
            total_portion = sum(tech.get('portion', 100) for tech in assigned_technicians)
            
            # Create separate labor row for each technician
            for tech_assignment in assigned_technicians:
                portion = tech_assignment.get('portion', 100)
                
                # Calculate this technician's portion of the total labor hours
                technician_labor_hours = total_labor_hours * (portion / total_portion) if total_portion > 0 else 0
                
                line_item = context.copy()
                line_item.update({
                    'line_item_type': 'LABOR',
                    'service_description': correction.get('actualCorrection') or correction.get('recommendedCorrection'),
                    'labor_rate_type': correction.get('laborRate'),
                    'assigned_technician': tech_assignment.get('technician'),
                    'assigned_technician_number': tech_assignment.get('technicianNumber'),
                    
                    'labor_hours': technician_labor_hours,  # This tech's portion of total hours
                    'actual_hours': technician_labor_hours,  # Same as labor_hours for consistency
                    'technician_portion': portion,
                    
                    # Calculate this technician's portion of the labor cost
                    'total': self._calculate_technician_labor_cost(
                        correction.get('laborTotal'),
                        portion
                    ),
                    'taxable': correction.get('taxable') != 'No',
                })
                line_items.append(line_item)
        
        return line_items
    
    def _calculate_technician_labor_cost(self, total_labor_cost: Any, portion: int) -> float:
        """Calculate a technician's portion of labor cost."""
        total_cost = self._parse_decimal(total_labor_cost) or 0
        return total_cost * (portion / 100)
    
    def _process_invoice_level_charges(self, invoice_data: Dict[str, Any], context: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process invoice-level charges like misc charges and supplies."""
        line_items = []
        
        # Process misc charges
        misc_charge_total = self._parse_decimal(invoice_data.get('miscChargeTotal'))
        if misc_charge_total and misc_charge_total > 0:
            line_item = context.copy()
            line_item.update({
                                 'line_item_type': 'MISC',
                 'service_description': 'Miscellaneous Charges',
                'total': misc_charge_total,
                'quantity': 1,
                'unit_price': misc_charge_total,
                'taxable': True,  # Misc charges are typically taxable
            })
            line_items.append(line_item)
        
        # Process supplies
        supplies_total = self._parse_decimal(invoice_data.get('suppliesTotal'))
        if supplies_total and supplies_total > 0:
            line_item = context.copy()
            line_item.update({
                'line_item_type': 'SHOP SUPPLIES',
                'service_description': 'Shop Supplies',
                'total': supplies_total,
                'quantity': 1,
                'unit_price': supplies_total,
                'taxable': True,  # Supplies are typically taxable
            })
            line_items.append(line_item)
        
        # Process service call charges
        service_call_total = self._parse_decimal(invoice_data.get('serviceCallTotal'))
        if service_call_total and service_call_total > 0:
            line_item = context.copy()
            line_item.update({
                                 'line_item_type': 'SERVICE_CALL',
                 'service_description': 'Service Call',
                'total': service_call_total,
                'quantity': 1,
                'unit_price': service_call_total,
                'taxable': True,
            })
            line_items.append(line_item)
        
        # Process mileage charges
        mileage_total = self._parse_decimal(invoice_data.get('mileageTotal'))
        if mileage_total and mileage_total > 0:
            line_item = context.copy()
            line_item.update({
                                 'line_item_type': 'MILEAGE',
                 'service_description': 'Mileage',
                'total': mileage_total,
                'quantity': 1,
                'unit_price': mileage_total,
                'taxable': True,
            })
            line_items.append(line_item)
        
        return line_items
    
    def flatten_invoice(self, invoice_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Flatten invoice using the same logic as the production system."""
        line_items = []
        
        try:
            # Extract common context that will be repeated on every row
            invoice_context = self._extract_invoice_context(invoice_data)
            
            # Get service order data
            service_order = invoice_data.get('ServiceOrder', {})
            complaints = service_order.get('Complaints', [])
            
            print(f"Processing invoice {invoice_context['invoice_id']} with {len(complaints)} complaints")
            
            # Process each complaint
            for complaint in complaints:
                complaint_context = self._extract_complaint_context(complaint, invoice_context)
                
                corrections = complaint.get('Corrections', [])
                
                # Process each correction within the complaint
                for correction in corrections:
                    correction_context = self._extract_correction_context(correction, complaint_context)
                    
                    # Process parts for this correction
                    parts = correction.get('Parts', [])
                    if parts:
                        parts_line_items = self._process_parts(parts, correction_context)
                        line_items.extend(parts_line_items)
                    
                    # Process labor for this correction
                    labor_line_items = self._process_labor(correction, complaint, correction_context)
                    line_items.extend(labor_line_items)
            
            # Process invoice-level charges (misc charges, supplies, etc.)
            misc_charges = self._process_invoice_level_charges(invoice_data, invoice_context)
            line_items.extend(misc_charges)
            
            print(f"Flattened invoice into {len(line_items)} line items")
            return line_items
            
        except Exception as e:
            print(f"Error flattening invoice {invoice_data.get('primaryKey', 'unknown')}: {e}")
            return []
    
    def get_all_columns(self) -> List[str]:
        """Get all possible columns for the flattened data."""
        return [
            # Invoice Level
            'invoice_id', 'invoice_number', 'invoice_date', 'due_date', 'exported',
            'shop_title', 'shop_address',
            
            # Customer Level
            'customer_id', 'customer_title', 'customer_external_id', 'customer_main_phone',
            'customer_secondary_phone', 'customer_billing_address',
            
                         # Service Order Level
             'service_order_primary_key', 'service_order_number', 'service_order_created',
            'service_order_start_date', 'service_order_completion_date',
            
            # Unit/Vehicle Level
            'unit_id', 'unit_number', 'unit_type', 'unit_year', 'unit_make', 'unit_model',
            'unit_vin', 'unit_license_plate',
            
                         # Primary Technician
             'primary_technician', 'primary_technician_number',
             
             # Complaint Level
            'SOAI_ID', 'complaint_type', 'complaint_subtype', 'complaint_note',
            'complaint_cause', 'complaint_authorized',
            
                         # Correction Level
             'fullbay_correction_id', 'correction_title', 'component', 'system',
             'global_service', 'recommended_correction', 'actual_correction', 'correction_performed',
            
                         # Line Item Level
             'line_item_type', 'fullbay_part_id', 'part_description', 'shop_part_number',
             'vendor_part_number', 'part_category', 'quantity', 'to_be_returned_quantity',
             'returned_quantity', 'unit_cost', 'unit_price', 'total_cost',
             'total', 'price_overridden', 'taxable', 'inventory_item',
             'core_type', 'sublet',
            
                                     # Labor Specific
             'service_description', 'labor_rate_type', 'assigned_technician', 'assigned_technician_number',
             'labor_hours', 'actual_hours', 'technician_portion',
            
            # Processing Metadata
            'processing_timestamp'
        ]
    
    def cleanup_old_files(self, pattern: str = "flattened_invoice_*.csv"):
        """Clean up old flattened CSV files to keep workspace clean."""
        import glob
        import os
        
        try:
            # Find all files matching the pattern
            files_to_remove = glob.glob(pattern)
            
            if files_to_remove:
                print(f"Cleaning up {len(files_to_remove)} old flattened files...")
                for file_path in files_to_remove:
                    try:
                        os.remove(file_path)
                        print(f"  Removed: {file_path}")
                    except Exception as e:
                        print(f"  Could not remove {file_path}: {e}")
                print("Cleanup completed!")
            else:
                print("No old files to clean up.")
                
        except Exception as e:
            print(f"Error during cleanup: {e}")

    def export_to_csv(self, line_items: List[Dict[str, Any]], output_file: str = "flattened_invoice.csv"):
        """Export flattened data to CSV (Excel-compatible)."""
        if not line_items:
            print("No line items to export")
            return
        
        # Get all possible columns
        all_columns = self.get_all_columns()
        
        # Add processing timestamp
        timestamp = datetime.now().isoformat()
        for item in line_items:
            item['processing_timestamp'] = timestamp
        
        try:
            with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=all_columns)
                writer.writeheader()
                
                for item in line_items:
                    # Ensure all columns are present (fill missing with None)
                    row = {col: item.get(col) for col in all_columns}
                    writer.writerow(row)
            
            print(f"Exported {len(line_items)} line items to {output_file}")
            
        except Exception as e:
            print(f"Error exporting to CSV: {e}")
    
    def print_summary(self, line_items: List[Dict[str, Any]], original_invoice: Dict[str, Any]):
        """Print a summary of the flattening results."""
        print("\n" + "="*80)
        print("FLATTENING SUMMARY")
        print("="*80)
        
        # Original invoice info
        print(f"Original Invoice ID: {original_invoice.get('primaryKey', 'Unknown')}")
        print(f"Invoice Number: {original_invoice.get('invoiceNumber', 'Unknown')}")
        print(f"Customer: {original_invoice.get('Customer', {}).get('title', 'Unknown')}")
        
        # Service order info
        service_order = original_invoice.get('ServiceOrder', {})
        complaints = service_order.get('Complaints', [])
        print(f"Service Order ID: {service_order.get('primaryKey', 'Unknown')}")
        print(f"Number of Complaints: {len(complaints)}")
        
        # Count corrections and parts
        total_corrections = 0
        total_parts = 0
        total_labor_records = 0
        
        for complaint in complaints:
            corrections = complaint.get('Corrections', [])
            total_corrections += len(corrections)
            
            for correction in corrections:
                parts = correction.get('Parts', [])
                total_parts += len(parts)
                
                # Count labor records
                assigned_techs = complaint.get('AssignedTechnicians', [])
                if assigned_techs:
                    total_labor_records += len(assigned_techs)
                elif self._parse_decimal(correction.get('laborHoursTotal', 0)) > 0:
                    total_labor_records += 1
        
        print(f"Total Corrections: {total_corrections}")
        print(f"Total Parts in Original: {total_parts}")
        print(f"Total Labor Records Expected: {total_labor_records}")
        
        # Flattened results
        print(f"\nFlattened Line Items: {len(line_items)}")
        
        # Breakdown by type
        type_counts = {}
        for item in line_items:
            item_type = item.get('line_item_type', 'UNKNOWN')
            type_counts[item_type] = type_counts.get(item_type, 0) + 1
        
        print("\nLine Item Breakdown:")
        for item_type, count in type_counts.items():
            print(f"  {item_type}: {count}")
        
        # Financial summary
        total_revenue = sum(item.get('total', 0) or 0 for item in line_items)
        total_cost = sum(item.get('total_cost', 0) or 0 for item in line_items)
        
        print(f"\nFinancial Summary:")
        print(f"  Total Revenue: ${total_revenue:,.2f}")
        print(f"  Total Cost: ${total_cost:,.2f}")
        print(f"  Gross Profit: ${total_revenue - total_cost:,.2f}")
        
        # Data quality check
        print(f"\nData Quality Check:")
        missing_critical = 0
        for item in line_items:
            if not item.get('invoice_id'):
                missing_critical += 1
        
        if missing_critical == 0:
            print("  ✅ All line items have critical invoice ID")
        else:
            print(f"  ⚠️ {missing_critical} line items missing critical data")
        
        print("="*80)
    
    def run_demo(self, json_file: Optional[str] = None):
        """Run the complete flattening demo."""
        print("FullBay Invoice Flattening Demo - Standalone Version")
        print("="*60)
        
        # Load invoice data
        if json_file and os.path.exists(json_file):
            print(f"Loading invoice from: {json_file}")
            invoice_data = self.load_sample_json(json_file)
        else:
            print("Using built-in sample invoice")
            invoice_data = self.sample_invoice
        
        # Flatten the invoice
        print("\nFlattening invoice...")
        line_items = self.flatten_invoice(invoice_data)
        
        if not line_items:
            print("❌ No line items generated - check for errors")
            return
        
        # Print summary
        self.print_summary(line_items, invoice_data)
        
        # Clean up old files before creating new ones
        self.cleanup_old_files()
        
        # Export to CSV
        output_file = f"flattened_invoice_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        self.export_to_csv(line_items, output_file)
        
        # Show first few line items as preview
        print(f"\nPreview of first 3 line items:")
        print("-" * 80)
        
        for i, item in enumerate(line_items[:3]):
            print(f"\nLine Item {i+1}:")
            print(f"  Type: {item.get('line_item_type')}")
            print(f"  Description: {item.get('part_description') or item.get('service_description')}")
            print(f"  Quantity: {item.get('quantity') or item.get('actual_hours')}")
            print(f"  Unit Price: ${item.get('unit_price') or item.get('total'):,.2f}")
            print(f"  Total: ${item.get('total'):,.2f}")
            print(f"  Technician: {item.get('assigned_technician') or 'N/A'}")
        
        print(f"\n✅ Demo completed successfully!")
        print(f"📊 Check the CSV file: {output_file}")
        print(f"📋 Total line items generated: {len(line_items)}")


def main():
    """Main function to run the demo."""
    json_file = sys.argv[1] if len(sys.argv) > 1 else None
    
    demo = StandaloneFlatteningDemo()
    demo.run_demo(json_file)


if __name__ == "__main__":
    main()
