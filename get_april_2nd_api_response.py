#!/usr/bin/env python3
"""
Fetch and display the raw API response for April 2nd, 2025
"""

import sys
import os
import json
from datetime import datetime

# Add src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from fullbay_client import FullbayClient
from config import Config

def get_april_2nd_response():
    try:
        # Set environment variables
        os.environ['FULLBAY_API_KEY'] = '4b9fcc18-1f24-09fb-275b-ad1974786395'
        
        # Initialize configuration and client
        config = Config()
        client = FullbayClient(config)
        
        print("🔍 Fetching API response for April 2nd, 2025...")
        print("=" * 60)
        
        # Fetch data for April 2nd
        date_str = "2025-04-02"
        invoices = client.fetch_invoices_for_date(date_str)
        
        print(f"✅ Retrieved {len(invoices)} invoices for {date_str}")
        print(f"📊 Total records: {len(invoices)}")
        print()
        
        # Save to JSON file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"april_2nd_2025_api_response_{timestamp}.json"
        
        with open(filename, 'w') as f:
            json.dump(invoices, f, indent=2)
        
        print(f"💾 Full API response saved to: {filename}")
        print()
        
        # Display summary of each invoice
        print("📋 INVOICE SUMMARY:")
        print("-" * 60)
        
        for i, invoice in enumerate(invoices, 1):
            invoice_id = invoice.get('id', 'N/A')
            invoice_number = invoice.get('invoiceNumber', 'N/A')
            customer = invoice.get('customer', {}).get('title', 'N/A') if invoice.get('customer') else 'N/A'
            unit = invoice.get('unit', {}).get('unit', 'N/A') if invoice.get('unit') else 'N/A'
            complaints_count = len(invoice.get('complaints', []))
            
            print(f"{i:2}. Invoice #{invoice_number} (ID: {invoice_id})")
            print(f"    Customer: {customer}")
            print(f"    Unit: {unit}")
            print(f"    Complaints: {complaints_count}")
            print()
        
        # Show sample of first invoice structure
        if invoices:
            print("🔍 SAMPLE INVOICE STRUCTURE (First invoice):")
            print("-" * 60)
            first_invoice = invoices[0]
            
            # Show top-level keys
            print("Top-level keys:")
            for key in sorted(first_invoice.keys()):
                value = first_invoice[key]
                if isinstance(value, list):
                    print(f"  {key}: [{len(value)} items]")
                elif isinstance(value, dict):
                    print(f"  {key}: {{...}}")
                else:
                    print(f"  {key}: {value}")
            
            print()
            print(f"📁 Complete data available in: {filename}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        raise

if __name__ == "__main__":
    get_april_2nd_response()
