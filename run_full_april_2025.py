#!/usr/bin/env python3
"""
Fullbay API Ingestion for Entire Month of April 2025

This script processes all of April 2025 with the updated mapping functionality:
- Return quantity logic (effective quantity = quantity - returned_qty)
- Always create SHOP SUPPLIES line items (even when 0)
- MISC charges support (when they exist in data)
- Updated column names (line_total instead of line_total_price)
"""

import logging
import sys
import os
from datetime import datetime, timedelta
import time

# Add src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from fullbay_client import FullbayClient
from database import DatabaseManager
from config import Config

def setup_logging():
    """Set up logging configuration."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"logs/full_april_2025_{timestamp}.log"
    
    # Create logs directory if it doesn't exist
    os.makedirs("logs", exist_ok=True)
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logger = logging.getLogger("full_april_2025")
    logger.info(f"Logging configured - Log file: {log_filename}")
    
    return logger, log_filename

def test_api_connection(client, logger):
    """Test Fullbay API connection."""
    try:
        logger.info("Testing Fullbay API connection...")
        status = client.get_api_status()
        logger.info(f"✅ API connection successful")
        logger.info(f"   Status: {status}")
        return True
    except Exception as e:
        logger.error(f"❌ API connection failed: {e}")
        return False

def process_day(date_str, client, db_manager, logger):
    """Process a single day of data."""
    logger.info(f"\n{'='*50}")
    logger.info(f"Day: {date_str}")
    logger.info(f"{'='*50}")
    
    try:
        # Retrieve data for the date
        logger.info(f"Processing {date_str}...")
        logger.info(f"Retrieving data for {date_str}...")
        
        invoices = client.fetch_invoices_for_date(date_str)
        logger.info(f"Retrieved {len(invoices)} invoices for {date_str}")
        
        if not invoices:
            logger.info(f"{date_str}: No data available")
            return 0, 0
        
        # Persist data to database
        logger.info(f"Persisting data for {date_str}...")
        line_items_created = db_manager.insert_records(invoices)
        
        logger.info(f"{date_str}: {len(invoices)} records inserted, {line_items_created} line items created")
        return len(invoices), line_items_created
        
    except Exception as e:
        logger.error(f"❌ Error processing {date_str}: {e}")
        return 0, 0

def generate_april_dates():
    """Generate all dates for April 2025."""
    dates = []
    start_date = datetime(2025, 4, 1)
    end_date = datetime(2025, 4, 30)
    
    current_date = start_date
    while current_date <= end_date:
        dates.append(current_date.strftime("%Y-%m-%d"))
        current_date += timedelta(days=1)
    
    return dates

def main():
    """Main execution function."""
    logger, log_filename = setup_logging()
    
    logger.info("Fullbay API Ingestion for Entire Month of April 2025")
    logger.info("Processing with updated mapping functionality")
    logger.info("=" * 60)
    
    try:
        # Initialize configuration and clients
        logger.info("Initializing configuration and clients...")
        config = Config()
        logger.info(f"Config loaded - Environment: {config.environment}")
        logger.info(f"API Key configured: {bool(config.fullbay_api_key)}")
        
        client = FullbayClient(config)
        logger.info("FullbayClient initialized")
        
        # Test API connection
        if not test_api_connection(client, logger):
            logger.error("❌ API connection test failed - exiting")
            return
        
        # Initialize database
        logger.info("Initializing database...")
        db_manager = DatabaseManager(config)
        db_manager.connect()
        
        # Generate all April dates
        dates_to_process = generate_april_dates()
        logger.info(f"Generated {len(dates_to_process)} dates for April 2025")
        
        total_records = 0
        total_line_items = 0
        start_time = datetime.now()
        
        logger.info(f"\nProcessing {len(dates_to_process)} days of data...")
        
        # Process each day
        for i, date_str in enumerate(dates_to_process, 1):
            logger.info(f"\nDay {i}/{len(dates_to_process)}: {date_str}")
            logger.info("=" * 50)
            
            records_processed, line_items_created = process_day(date_str, client, db_manager, logger)
            
            total_records += records_processed
            total_line_items += line_items_created
            
            # Wait between requests (except for the last one)
            if i < len(dates_to_process):
                logger.info("Waiting 2 seconds before next request...")
                time.sleep(2)
        
        # Final summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info(f"\n{'='*60}")
        logger.info("FULL MONTH OF APRIL 2025 INGESTION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total days processed: {len(dates_to_process)}")
        logger.info(f"Total records processed: {total_records}")
        logger.info(f"Total line items created: {total_line_items}")
        logger.info(f"Total processing time: {duration:.2f} seconds")
        logger.info(f"Average time per day: {duration/len(dates_to_process):.2f} seconds")
        
        # Save detailed results
        results = {
            "execution_date": datetime.now().isoformat(),
            "month": "April 2025",
            "days_processed": dates_to_process,
            "total_records": total_records,
            "total_line_items": total_line_items,
            "processing_duration_seconds": duration,
            "log_file": log_filename
        }
        
        results_filename = f"full_april_2025_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        import json
        with open(results_filename, 'w') as f:
            json.dump(results, f, indent=2)
        
        logger.info(f"Detailed results saved to: {results_filename}")
        
        if total_records > 0:
            logger.info("✅ SUCCESS: Full month of April processed successfully!")
            logger.info("Ready to generate comprehensive CSV export")
        else:
            logger.warning("⚠️  No data was processed - check API availability")
        
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        raise
    finally:
        # Clean up
        if 'db_manager' in locals():
            db_manager.close()

if __name__ == "__main__":
    main()
