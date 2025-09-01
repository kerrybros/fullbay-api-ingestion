#!/usr/bin/env python3
"""
FullBay Invoice Flattening Demo

This script demonstrates the complete flattening logic for FullBay invoices.
It processes sample JSON and outputs the results in Excel-compatible format
to ensure no data loss and proper column mapping.

Usage:
    python scripts/flattening_demo.py [sample_json_file.json]
    
If no file is provided, it will use the built-in sample data.
"""

import json
import sys
import csv
from datetime import datetime
from typing import Dict, Any, List, Optional
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from database import DatabaseManager
from config import Config


class FlatteningDemo:
    """
    Demo class for testing and demonstrating the flattening logic.
    """
    
    def __init__(self):
        """Initialize the demo with sample configuration."""
        self.config = Config()
        self.db_manager = DatabaseManager(self.config)
        
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
    
    def flatten_invoice(self, invoice_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Flatten invoice using the same logic as the production system.
        
        Args:
            invoice_data: Raw FullBay invoice JSON
            
        Returns:
            List of flattened line items
        """
        try:
            # Use the same flattening logic as the database manager
            line_items = self.db_manager._flatten_invoice_to_line_items(invoice_data, 1)
            return line_items
        except Exception as e:
            print(f"Error during flattening: {e}")
            return []
    
    def get_all_columns(self) -> List[str]:
        """Get all possible columns for the flattened data."""
        return [
            # Invoice Level
            'fullbay_invoice_id', 'invoice_number', 'invoice_date', 'due_date', 'exported',
            'shop_title', 'shop_email', 'shop_address',
            
            # Customer Level
            'customer_id', 'customer_title', 'customer_external_id', 'customer_main_phone',
            'customer_secondary_phone', 'customer_billing_address',
            
            # Service Order Level
            'fullbay_service_order_id', 'repair_order_number', 'service_order_created',
            'service_order_start_date', 'service_order_completion_date',
            
            # Unit/Vehicle Level
            'unit_id', 'unit_number', 'unit_type', 'unit_year', 'unit_make', 'unit_model',
            'unit_vin', 'unit_license_plate',
            
            # Primary Technician
            'primary_technician', 'primary_technician_number',
            
            # Service Order Totals
            'so_total_parts_cost', 'so_total_parts_price', 'so_total_labor_hours',
            'so_total_labor_cost', 'so_subtotal', 'so_tax_total', 'so_total_amount',
            
            # Complaint Level
            'fullbay_complaint_id', 'complaint_type', 'complaint_subtype', 'complaint_note',
            'complaint_cause', 'complaint_authorized',
            
            # Correction Level
            'fullbay_correction_id', 'correction_title', 'global_component', 'global_system',
            'global_service', 'recommended_correction', 'actual_correction', 'correction_performed',
            
            # Line Item Level
            'line_item_type', 'fullbay_part_id', 'part_description', 'shop_part_number',
            'vendor_part_number', 'part_category', 'quantity', 'to_be_returned_quantity',
            'returned_quantity', 'unit_cost', 'unit_price', 'line_total_cost',
            'line_total_price', 'price_overridden', 'taxable', 'inventory_item',
            'core_type', 'sublet', 'quickbooks_account', 'quickbooks_item', 'quickbooks_item_type',
            
            # Labor Specific
            'labor_description', 'labor_rate_type', 'assigned_technician', 'assigned_technician_number',
            'labor_hours', 'actual_hours', 'technician_portion',
            
            # Processing Metadata
            'raw_data_id', 'processing_timestamp'
        ]
    
    def export_to_csv(self, line_items: List[Dict[str, Any]], output_file: str = "flattened_invoice.csv"):
        """
        Export flattened data to CSV (Excel-compatible).
        
        Args:
            line_items: List of flattened line items
            output_file: Output CSV file path
        """
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
        total_revenue = sum(item.get('line_total_price', 0) or 0 for item in line_items)
        total_cost = sum(item.get('line_total_cost', 0) or 0 for item in line_items)
        
        print(f"\nFinancial Summary:")
        print(f"  Total Revenue: ${total_revenue:,.2f}")
        print(f"  Total Cost: ${total_cost:,.2f}")
        print(f"  Gross Profit: ${total_revenue - total_cost:,.2f}")
        
        # Data quality check
        print(f"\nData Quality Check:")
        missing_critical = 0
        for item in line_items:
            if not item.get('fullbay_invoice_id'):
                missing_critical += 1
        
        if missing_critical == 0:
            print("  ✅ All line items have critical invoice ID")
        else:
            print(f"  ⚠️ {missing_critical} line items missing critical data")
        
        print("="*80)
    
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
    
    def run_demo(self, json_file: Optional[str] = None):
        """
        Run the complete flattening demo.
        
        Args:
            json_file: Optional path to JSON file to process
        """
        print("FullBay Invoice Flattening Demo")
        print("="*50)
        
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
        
        # Export to CSV
        output_file = f"flattened_invoice_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        self.export_to_csv(line_items, output_file)
        
        # Show first few line items as preview
        print(f"\nPreview of first 3 line items:")
        print("-" * 80)
        
        for i, item in enumerate(line_items[:3]):
            print(f"\nLine Item {i+1}:")
            print(f"  Type: {item.get('line_item_type')}")
            print(f"  Description: {item.get('part_description') or item.get('labor_description')}")
            print(f"  Quantity: {item.get('quantity') or item.get('actual_hours')}")
            print(f"  Unit Price: ${item.get('unit_price') or item.get('line_total_price'):,.2f}")
            print(f"  Total: ${item.get('line_total_price'):,.2f}")
            print(f"  Technician: {item.get('assigned_technician') or 'N/A'}")
        
        print(f"\n✅ Demo completed successfully!")
        print(f"📊 Check the CSV file: {output_file}")
        print(f"📋 Total line items generated: {len(line_items)}")


def main():
    """Main function to run the demo."""
    json_file = sys.argv[1] if len(sys.argv) > 1 else None
    
    demo = FlatteningDemo()
    demo.run_demo(json_file)


if __name__ == "__main__":
    main()
