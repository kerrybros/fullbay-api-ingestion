#!/usr/bin/env python3
"""
Process Multiple FullBay Invoices

This script processes a JSON file containing multiple FullBay invoices
and flattens them into a single Excel-compatible CSV file.

Usage:
    python scripts/process_multiple_invoices.py input_file.json [output_file.csv]
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


class MultipleInvoiceProcessor:
    """
    Process multiple invoices from a JSON file and flatten them.
    """
    
    def __init__(self):
        """Initialize the processor."""
        self.config = Config()
        self.db_manager = DatabaseManager(self.config)
        
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
                
                # Use the database manager's flattening logic
                line_items = self.db_manager._flatten_invoice_to_line_items(invoice, i)
                
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
        print("Usage: python scripts/process_multiple_invoices.py input_file.json [output_file.csv]")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else f"flattened_invoices_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    print("🚀 FullBay Multiple Invoice Processor")
    print("="*50)
    
    # Initialize processor
    processor = MultipleInvoiceProcessor()
    
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
