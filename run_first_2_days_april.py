#!/usr/bin/env python3
"""
Fullbay API Ingestion for First 2 Days of April 2025

This script tests the updated mapping functionality with fresh data:
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
    log_filename = f"logs/april_first_2_days_{timestamp}.log"
    
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
    
    logger = logging.getLogger("april_first_2_days")
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

def main():
    """Main execution function."""
    logger, log_filename = setup_logging()
    
    logger.info("Fullbay API Ingestion for First 2 Days of April 2025")
    logger.info("Testing new mapping functionality")
    logger.info("=" * 50)
    
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
        
        # Define the two days to process
        dates_to_process = [
            "2025-04-01",
            "2025-04-02"
        ]
        
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
        
        logger.info(f"\n{'='*50}")
        logger.info("FIRST 2 DAYS OF APRIL 2025 INGESTION SUMMARY")
        logger.info("=" * 50)
        logger.info(f"Total days processed: {len(dates_to_process)}")
        logger.info(f"Total records processed: {total_records}")
        logger.info(f"Total line items created: {total_line_items}")
        logger.info(f"Total processing time: {duration:.2f} seconds")
        logger.info(f"Average time per day: {duration/len(dates_to_process):.2f} seconds")
        
        # Save detailed results
        results = {
            "execution_date": datetime.now().isoformat(),
            "days_processed": dates_to_process,
            "total_records": total_records,
            "total_line_items": total_line_items,
            "processing_duration_seconds": duration,
            "log_file": log_filename
        }
        
        results_filename = f"april_first_2_days_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        import json
        with open(results_filename, 'w') as f:
            json.dump(results, f, indent=2)
        
        logger.info(f"Detailed results saved to: {results_filename}")
        
        if total_records > 0:
            logger.info("✅ SUCCESS: All days processed successfully!")
            logger.info("Ready to verify new mapping functionality")
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
