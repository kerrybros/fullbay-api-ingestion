#!/usr/bin/env python3
"""
Test Script for Real FullBay API Response

This script tests the flattening logic with the actual API response format
that includes the resultSet wrapper structure.

Usage:
    python scripts/test_real_api_response.py [api_response_file.json]
    
If no file is provided, it will use the built-in sample API response.
"""

import json
import sys
import csv
from datetime import datetime
from typing import Dict, Any, List, Optional
import os

# Import the flattening logic
from flattening_demo_standalone import StandaloneFlatteningDemo


class RealApiResponseTester:
    """
    Test class for handling real FullBay API responses.
    """
    
    def __init__(self):
        """Initialize with sample API response."""
        self.sample_api_response = {
            "status": "SUCCESS",
            "startRecordNumber": 1,
            "endRecordNumber": 1,
            "resultCount": 1,
            "totalCount": 1,
            "pageSize": 1,
            "currentPage": 1,
            "totalPages": 1,
            "resultSet": [
                {
                    "primaryKey": "17556190",
                    "exported": "1",
                    "invoiceDate": "2025-06-03",
                    "dueDate": "2025-06-18",
                    "invoiceNumber": "14550",
                    "customerTitle": "A.M. DELIVERY",
                    "customerBillingEmployee": "TONY BROWN",
                    "customerBillingEmail": "AMDELIVERYINC@GMAIL.COM",
                    "customerBillingAddress": "18290 New Jersey Drive, Southfield, MI 48075, US",
                    "Customer": {
                        "customerId": 3305045,
                        "title": "A.M. DELIVERY",
                        "externalId": None,
                        "mainPhone": "(313) 475-1876",
                        "secondaryPhone": ""
                    },
                    "shopTitle": "Kerry Brothers Truck Repair - Detroit",
                    "shopEmail": "det@kerrybros.com",
                    "shopPhysicalAddress": "Facility ID F132285 5255 Tillman St, Detroit, MI 48208, US",
                    "miscChargeTotal": 4.73,
                    "serviceCallTotal": 0,
                    "mileageTotal": 0,
                    "mileageCostTotal": 0,
                    "partsTotal": 0,
                    "laborHoursTotal": 1,
                    "laborTotal": 150,
                    "subletLaborTotal": None,
                    "suppliesTotal": 7.5,
                    "subTotal": 162.23,
                    "TaxInformation": {
                        "taxTitle": "Michigan Non-Exempt",
                        "taxRate": "6",
                        "taxTotal": 0.45,
                        "TaxLineInformation": [
                            {
                                "taxTitle": "Non-Exempt",
                                "taxRate": 6,
                                "taxTotal": "0.45"
                            }
                        ]
                    },
                    "taxTitle": "Michigan Non-Exempt",
                    "taxRate": "6",
                    "taxTotal": 0.45,
                    "total": 162.68,
                    "balance": 0,
                    "quickBooksId": "200813",
                    "promiseToPayDate": "",
                    "createdByTechnician": "AMY MCKELVEY",
                    "createdByTechnicianNumber": "",
                    "created": "2025-06-03 12:31:27",
                    "ServiceOrder": {
                        "primaryKey": "19259697",
                        "repairOrderNumber": "16025",
                        "technician": "Rachel Kerry",
                        "technicianNumber": "",
                        "partsManager": "",
                        "partsManagerNumber": "",
                        "customerAuthorizedOnHoursOnly": "No",
                        "Customer": {
                            "customerId": 3305045,
                            "title": "A.M. DELIVERY",
                            "externalId": None,
                            "mainPhone": "(313) 475-1876",
                            "secondaryPhone": ""
                        },
                        "Unit": {
                            "customerUnitId": "7246973",
                            "number": "FLATBED",
                            "nickname": "",
                            "type": "Truck",
                            "subType": None,
                            "year": "2012",
                            "make": "Hino",
                            "model": "Conventional Type Truck",
                            "vin": "5PVNJ8JN7C4S50893",
                            "licensePlate": ""
                        },
                        "description": "",
                        "submitterContact": "TONY BROWN",
                        "submitterContactEmail": "AMDELIVERYINC@GMAIL.COM",
                        "submitterContactPhone": "(313) 475-1876",
                        "submitterContactCellPhone": None,
                        "authorizerContact": "TONY BROWN",
                        "authorizerContactEmail": "AMDELIVERYINC@GMAIL.COM",
                        "authorizerContactPhone": "(313) 475-1876",
                        "authorizerContactCellPhone": None,
                        "billingCustomer": "A.M. DELIVERY",
                        "BillingAddress": {
                            "title": "Southfield, MI",
                            "line1": "18290 New Jersey Drive",
                            "line2": "",
                            "city": "Southfield",
                            "state": "MI",
                            "country": "US",
                            "postalCode": "48075"
                        },
                        "RemitToAddress": {
                            "title": None,
                            "line1": None,
                            "line2": None,
                            "city": None,
                            "state": None,
                            "country": None,
                            "postalCode": None
                        },
                        "ShipToAddress": {
                            "title": "Southfield, MI",
                            "line1": "18290 New Jersey Drive",
                            "line2": "",
                            "city": "Southfield",
                            "state": "MI",
                            "country": "US",
                            "postalCode": "48075"
                        },
                        "unitAccessMethod": None,
                        "unitAvailableDateTime": "2025-05-30 12:30:00",
                        "unitMustBeAccessedAtAvailableDateTime": "No",
                        "unitReturnDateTime": None,
                        "unitReturnAsap": "Yes",
                        "UnitPickUpAddress": {
                            "title": None,
                            "line1": None,
                            "line2": None,
                            "city": None,
                            "state": None,
                            "country": None,
                            "postalCode": None
                        },
                        "locationInformation": "",
                        "authorizationNumber": "",
                        "poNumber": "",
                        "partsPoNumber": "28631268",
                        "hot": "No",
                        "followInUseSchedule": "No",
                        "unscheduled": "No",
                        "customerThreshold": 2197.18,
                        "preAuthorized": "Yes",
                        "laborHoursTotal": 1,
                        "actualHoursTotal": 1.2622222222222221,
                        "laborTotal": 150,
                        "subletLaborTotal": None,
                        "partsCostTotal": 0,
                        "partsTotal": 0,
                        "mileageTotal": 0,
                        "mileageCostTotal": 0,
                        "overrideSuppliesTotal": "No",
                        "overridenSuppliesTotal": 0,
                        "serviceCallTotal": 0,
                        "allPartsPricedDateTime": "",
                        "startDateTime": "2025-05-31 11:45:03",
                        "completionDateTime": "2025-06-03 11:02:34",
                        "createdByTechnician": "Bill McLaury",
                        "createdByTechnicianNumber": "",
                        "created": "2025-05-30 12:26:29",
                        "MiscCharges": [
                            {
                                "primaryKey": "4559925",
                                "description": "3% CREDIT CARD SURCHARGE",
                                "quantity": 1,
                                "rate": 4.73,
                                "amount": "4.73",
                                "taxable": "No",
                                "quickBooksItem": "DISCOUNT",
                                "quickBooksItemType": "misc"
                            }
                        ],
                        "Complaints": [
                            {
                                "primaryKey": 48627604,
                                "mileageRate": "Standard",
                                "laborRate": "Standard",
                                "type": "Complaint",
                                "subType": "",
                                "authorized": "Yes",
                                "severity": " - ",
                                "note": "CHK ENGINE LIGHT- LOW POWER",
                                "cause": ".",
                                "causeType": None,
                                "laborHoursTotal": 1,
                                "actualHoursTotal": 1.2527777777777778,
                                "laborTaxable": "",
                                "laborTotal": 150,
                                "partsCostTotal": 0,
                                "partsTotal": 0,
                                "mileageTaxable": "",
                                "mileageTotal": 0,
                                "mileageCostTotal": 0,
                                "serviceCallTaxable": "",
                                "serviceCallTotal": 0,
                                "sublet": "No",
                                "partCategory": None,
                                "quickBooksAccount": None,
                                "quickBooksItem": None,
                                "quickBooksItemType": None,
                                "created": "2025-05-30 12:26:30",
                                "AssignedTechnicians": [
                                    {
                                        "actualHours": 1.2527777777777778,
                                        "portion": 100,
                                        "primaryKey": "50323843",
                                        "quickBooksLaborItem": None,
                                        "technician": "Jacob Humphries",
                                        "technicianNumber": ""
                                    }
                                ],
                                "Corrections": [
                                    {
                                        "primaryKey": "43244317",
                                        "globalComponent": "CHASSIS MAINTENANCE",
                                        "globalSystem": "MAINTENANCE",
                                        "globalService": None,
                                        "unitService": None,
                                        "laborRate": "Standard",
                                        "title": "",
                                        "recommendedCorrection": "UNIT HAD DPF FAULT CODES AND DIFF PRESSURE CODES. NEED TO PULL DPF FOR CLEANING. HAS EXTREMELY HIGH DIFF PRESSURE\n6/2 - CUSTOMER HAS DENIED SERVICES AT THIS TIME. WE WILL RELEASE UNIT AS IS.",
                                        "actualCorrection": "UNIT HAD DPF FAULT CODES AND DIFF PRESSURE CODES. NEED TO PULL DPF FOR CLEANING. HAS EXTREMELY HIGH DIFF PRESSURE\n6/2 - CUSTOMER HAS DENIED SERVICES AT THIS TIME. WE WILL RELEASE UNIT AS IS.",
                                        "correctionPerformed": "Performed",
                                        "preAuthorized": "No",
                                        "prePaid": "No",
                                        "laborHoursTotal": 1,
                                        "laborTotal": 150,
                                        "taxable": "No",
                                        "partsCostTotal": 0,
                                        "partsTotal": 0,
                                        "created": "2025-05-30 12:26:30",
                                        "Parts": []
                                    }
                                ]
                            },
                            {
                                "primaryKey": 48627603,
                                "mileageRate": "Standard",
                                "laborRate": "Standard",
                                "type": "Complaint",
                                "subType": "PM",
                                "authorized": "Yes",
                                "severity": " - ",
                                "note": "QUALITY CONTROL",
                                "cause": "Customer request",
                                "causeType": None,
                                "laborHoursTotal": 0,
                                "actualHoursTotal": 0.009444444444444445,
                                "laborTaxable": "",
                                "laborTotal": 0,
                                "partsCostTotal": 0,
                                "partsTotal": 0,
                                "mileageTaxable": "",
                                "mileageTotal": 0,
                                "mileageCostTotal": 0,
                                "serviceCallTaxable": "",
                                "serviceCallTotal": 0,
                                "sublet": "No",
                                "partCategory": None,
                                "quickBooksAccount": None,
                                "quickBooksItem": None,
                                "quickBooksItemType": None,
                                "created": "2025-05-30 12:26:29",
                                "AssignedTechnicians": [
                                    {
                                        "actualHours": 0.009444444444444445,
                                        "portion": 100,
                                        "primaryKey": "50402587",
                                        "quickBooksLaborItem": None,
                                        "technician": "Jacob Humphries",
                                        "technicianNumber": ""
                                    }
                                ],
                                "Corrections": [
                                    {
                                        "primaryKey": "43244316",
                                        "globalComponent": "CHASSIS MAINTENANCE",
                                        "globalSystem": "MAINTENANCE",
                                        "globalService": "QUALITY CONTROL",
                                        "unitService": "QUALITY CONTROL",
                                        "laborRate": "Standard",
                                        "title": "QUALITY CONTROL",
                                        "recommendedCorrection": "QUALITY CONTROL INSPECTION",
                                        "actualCorrection": "QUALITY CONTROL INSPECTION",
                                        "correctionPerformed": "Performed",
                                        "preAuthorized": "Yes",
                                        "prePaid": "No",
                                        "laborHoursTotal": 0,
                                        "laborTotal": 0,
                                        "taxable": "No",
                                        "partsCostTotal": 0,
                                        "partsTotal": 0,
                                        "created": "2025-05-30 12:26:30",
                                        "Parts": []
                                    }
                                ]
                            }
                        ]
                    }
                }
            ]
        }
        
        self.flattening_demo = StandaloneFlatteningDemo()
    
    def load_api_response(self, file_path: str) -> Dict[str, Any]:
        """Load API response from file."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading API response file: {e}")
            return self.sample_api_response
    
    def extract_invoices_from_response(self, api_response: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract invoice objects from the API response."""
        if not isinstance(api_response, dict):
            print("❌ Invalid API response format")
            return []
        
        # Check if this is a valid API response
        if 'resultSet' not in api_response:
            print("❌ API response missing 'resultSet' field")
            return []
        
        result_set = api_response.get('resultSet', [])
        if not isinstance(result_set, list):
            print("❌ 'resultSet' is not an array")
            return []
        
        print(f"📊 Found {len(result_set)} invoices in API response")
        
        # Validate each invoice in the result set
        valid_invoices = []
        for i, invoice in enumerate(result_set):
            if not isinstance(invoice, dict):
                print(f"⚠️ Invoice {i+1} is not a valid object, skipping")
                continue
            
            if 'primaryKey' not in invoice:
                print(f"⚠️ Invoice {i+1} missing primaryKey, skipping")
                continue
            
            valid_invoices.append(invoice)
            print(f"✅ Invoice {i+1}: {invoice.get('primaryKey')} - {invoice.get('invoiceNumber', 'No Number')}")
        
        return valid_invoices
    
    def process_api_response(self, api_response: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process the entire API response and flatten all invoices."""
        print("="*80)
        print("PROCESSING FULLBAY API RESPONSE")
        print("="*80)
        
        # Extract API metadata
        status = api_response.get('status', 'UNKNOWN')
        total_count = api_response.get('totalCount', 0)
        result_count = api_response.get('resultCount', 0)
        current_page = api_response.get('currentPage', 1)
        total_pages = api_response.get('totalPages', 1)
        
        print(f"API Status: {status}")
        print(f"Total Records: {total_count}")
        print(f"Records in Response: {result_count}")
        print(f"Page {current_page} of {total_pages}")
        print()
        
        # Extract invoices from result set
        invoices = self.extract_invoices_from_response(api_response)
        
        if not invoices:
            print("❌ No valid invoices found in API response")
            return []
        
        # Process each invoice
        all_line_items = []
        
        for i, invoice in enumerate(invoices):
            print(f"\n{'='*60}")
            print(f"PROCESSING INVOICE {i+1} OF {len(invoices)}")
            print(f"{'='*60}")
            
            try:
                # Flatten this invoice
                line_items = self.flattening_demo.flatten_invoice(invoice)
                
                if line_items:
                    all_line_items.extend(line_items)
                    print(f"✅ Invoice {invoice.get('primaryKey')} flattened into {len(line_items)} line items")
                else:
                    print(f"⚠️ Invoice {invoice.get('primaryKey')} produced no line items")
                    
            except Exception as e:
                print(f"❌ Error processing invoice {invoice.get('primaryKey')}: {e}")
                continue
        
        print(f"\n{'='*80}")
        print(f"TOTAL RESULTS: {len(all_line_items)} line items from {len(invoices)} invoices")
        print(f"{'='*80}")
        
        return all_line_items
    
    def export_results(self, line_items: List[Dict[str, Any]], output_file: str = None):
        """Export flattened results to CSV."""
        if not line_items:
            print("No line items to export")
            return
        
        # Clean up old files before creating new ones
        self.flattening_demo.cleanup_old_files("real_api_flattened_*.csv")
        
        if not output_file:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = f"real_api_flattened_{timestamp}.csv"
        
        self.flattening_demo.export_to_csv(line_items, output_file)
        return output_file
    
    def print_summary(self, line_items: List[Dict[str, Any]], api_response: Dict[str, Any]):
        """Print a comprehensive summary of the processing results."""
        print("\n" + "="*80)
        print("COMPREHENSIVE PROCESSING SUMMARY")
        print("="*80)
        
        # API Response Summary
        total_count = api_response.get('totalCount', 0)
        result_count = api_response.get('resultCount', 0)
        status = api_response.get('status', 'UNKNOWN')
        
        print(f"API Response Summary:")
        print(f"  Status: {status}")
        print(f"  Total Records Available: {total_count}")
        print(f"  Records in This Response: {result_count}")
        
        # Extract invoices for detailed analysis
        invoices = self.extract_invoices_from_response(api_response)
        
        print(f"\nInvoice Processing Summary:")
        print(f"  Valid Invoices Found: {len(invoices)}")
        
        # Analyze each invoice
        total_complaints = 0
        total_corrections = 0
        total_parts = 0
        total_labor_records = 0
        
        for invoice in invoices:
            service_order = invoice.get('ServiceOrder', {})
            complaints = service_order.get('Complaints', [])
            total_complaints += len(complaints)
            
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
                    elif self.flattening_demo._parse_decimal(correction.get('laborHoursTotal', 0)) > 0:
                        total_labor_records += 1
        
        print(f"  Total Complaints: {total_complaints}")
        print(f"  Total Corrections: {total_corrections}")
        print(f"  Total Parts in Original: {total_parts}")
        print(f"  Total Labor Records Expected: {total_labor_records}")
        
        # Flattened Results Summary
        print(f"\nFlattened Results Summary:")
        print(f"  Total Line Items Generated: {len(line_items)}")
        
        # Breakdown by type
        type_counts = {}
        for item in line_items:
            item_type = item.get('line_item_type', 'UNKNOWN')
            type_counts[item_type] = type_counts.get(item_type, 0) + 1
        
        print(f"\nLine Item Breakdown:")
        for item_type, count in type_counts.items():
            print(f"  {item_type}: {count}")
        
        # Financial Summary
        total_revenue = sum(item.get('line_total_price', 0) or 0 for item in line_items)
        total_cost = sum(item.get('line_total_cost', 0) or 0 for item in line_items)
        
        print(f"\nFinancial Summary:")
        print(f"  Total Revenue: ${total_revenue:,.2f}")
        print(f"  Total Cost: ${total_cost:,.2f}")
        print(f"  Gross Profit: ${total_revenue - total_cost:,.2f}")
        
        # Data Quality Check
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
    
    def run_test(self, api_response_file: Optional[str] = None):
        """Run the complete test with real API response format."""
        print("FullBay Real API Response Flattening Test")
        print("="*60)
        
        # Load API response
        if api_response_file and os.path.exists(api_response_file):
            print(f"Loading API response from: {api_response_file}")
            api_response = self.load_api_response(api_response_file)
        else:
            print("Using built-in sample API response")
            api_response = self.sample_api_response
        
        # Process the API response
        line_items = self.process_api_response(api_response)
        
        if not line_items:
            print("❌ No line items generated - check for errors")
            return
        
        # Print comprehensive summary
        self.print_summary(line_items, api_response)
        
        # Export results
        output_file = self.export_results(line_items)
        
        # Show sample of results
        print(f"\nSample of first 3 line items:")
        print("-" * 80)
        
        for i, item in enumerate(line_items[:3]):
            print(f"\nLine Item {i+1}:")
            print(f"  Invoice: {item.get('invoice_id')} - {item.get('invoice_number')}")
            print(f"  Customer: {item.get('customer_title')}")
            print(f"  Type: {item.get('line_item_type')}")
            print(f"  Description: {item.get('part_description') or item.get('service_description')}")
            print(f"  Quantity: {item.get('quantity') or item.get('actual_hours')}")
            print(f"  Total: ${item.get('total'):,.2f}")
            print(f"  Technician: {item.get('assigned_technician') or 'N/A'}")
        
        print(f"\n✅ Test completed successfully!")
        print(f"📊 Check the CSV file: {output_file}")
        print(f"📋 Total line items generated: {len(line_items)}")


def main():
    """Main function to run the test."""
    api_response_file = sys.argv[1] if len(sys.argv) > 1 else None
    
    tester = RealApiResponseTester()
    tester.run_test(api_response_file)


if __name__ == "__main__":
    main()
