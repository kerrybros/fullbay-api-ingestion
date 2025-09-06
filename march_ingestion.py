#!/usr/bin/env python3
"""
March 2025 Fullbay Data Ingestion Script

This script pulls March 2025 data day by day from the Fullbay API
and processes it into the line_items table.
"""

import os
import sys
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any

# Add src directory to path
sys.path.append('src')

from config import Config
from fullbay_client import FullbayClient
from database import DatabaseManager

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('march_ingestion.log')
    ]
)
logger = logging.getLogger(__name__)

def generate_march_dates() -> List[datetime]:
    """Generate all dates in March 2025."""
    dates = []
    start_date = datetime(2025, 3, 1, tzinfo=timezone.utc)
    end_date = datetime(2025, 3, 31, tzinfo=timezone.utc)
    
    current_date = start_date
    while current_date <= end_date:
        dates.append(current_date)
        current_date += timedelta(days=1)
    
    return dates

def process_march_data():
    """Process March 2025 data day by day."""
    logger.info("Starting March 2025 Fullbay data ingestion")
    
    try:
        # Initialize components
        config = Config()
        fullbay_client = FullbayClient(config)
        db_manager = DatabaseManager(config)
        
        # Connect to database
        db_manager.connect()
        logger.info("Database connection established")
        
        # Test API connection
        api_status = fullbay_client.get_api_status()
        logger.info(f"API Status: {api_status}")
        
        if api_status.get('status') != 'connected':
            raise Exception(f"API connection failed: {api_status}")
        
        # Generate March dates
        march_dates = generate_march_dates()
        logger.info(f"Processing {len(march_dates)} days in March 2025")
        
        total_invoices = 0
        total_line_items = 0
        successful_days = 0
        failed_days = 0
        
        # Process each day
        for i, date in enumerate(march_dates, 1):
            date_str = date.strftime('%Y-%m-%d')
            logger.info(f"Processing day {i}/{len(march_dates)}: {date_str}")
            
            try:
                # Fetch invoices for this date
                logger.info(f"Fetching invoices for {date_str}...")
                logger.info(f"This may take up to 1000 seconds (16+ minutes) - please be patient...")
                invoices = fullbay_client.fetch_invoices_for_date(date)
                
                if not invoices:
                    logger.info(f"No invoices found for {date_str}")
                    continue
                
                logger.info(f"Found {len(invoices)} invoices for {date_str}")
                
                # Insert raw data and process into line items
                logger.info(f"Inserting and processing data for {date_str}...")
                records_inserted = db_manager.insert_records(invoices)
                
                logger.info(f"Successfully processed {records_inserted} records for {date_str}")
                
                total_invoices += len(invoices)
                total_line_items += records_inserted
                successful_days += 1
                
                # Small delay to be respectful to the API
                import time
                time.sleep(2)  # Increased delay to be more respectful
                
            except Exception as e:
                logger.error(f"Failed to process {date_str}: {e}")
                failed_days += 1
                continue
        
        # Final summary
        logger.info("="*60)
        logger.info("MARCH 2025 INGESTION COMPLETED")
        logger.info("="*60)
        logger.info(f"Successful days: {successful_days}")
        logger.info(f"Failed days: {failed_days}")
        logger.info(f"Total invoices processed: {total_invoices:,}")
        logger.info(f"Total line items created: {total_line_items:,}")
        logger.info("="*60)
        
        # Verify final state
        with db_manager._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM fullbay_raw_data")
                raw_count = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM fullbay_line_items")
                line_count = cursor.fetchone()[0]
                
                cursor.execute("SELECT MIN(invoice_date), MAX(invoice_date) FROM fullbay_line_items WHERE invoice_date IS NOT NULL")
                date_range = cursor.fetchone()
                
                logger.info(f"Final database state:")
                logger.info(f"   Raw data records: {raw_count:,}")
                logger.info(f"   Line items records: {line_count:,}")
                logger.info(f"   Date range: {date_range[0]} to {date_range[1]}")
        
        return True
        
    except Exception as e:
        logger.error(f"Fatal error during March ingestion: {e}")
        return False
        
    finally:
        if 'db_manager' in locals():
            db_manager.close()
            logger.info("Database connection closed")

def main():
    """Main function."""
    print("March 2025 Fullbay Data Ingestion")
    print("="*50)
    
    # Check if user wants to proceed
    response = input("This will pull March 2025 data day by day. Continue? (y/n): ")
    if response.lower() != 'y':
        print("Operation cancelled")
        return
    
    success = process_march_data()
    
    if success:
        print("\nMarch 2025 ingestion completed successfully!")
    else:
        print("\nMarch 2025 ingestion failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
