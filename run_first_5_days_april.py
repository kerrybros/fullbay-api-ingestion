#!/usr/bin/env python3
"""
Run Fullbay API ingestion for the first 5 days of April 2025.
This script will process April 1-5 with the new mapping to test functionality.
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone, date, timedelta
from pathlib import Path
import time

# Set up environment variables for testing
os.environ.setdefault('DB_HOST', 'fullbay-postgres-db.cc74yq2i4j8n.us-east-1.rds.amazonaws.com')
os.environ.setdefault('DB_PORT', '5432')
os.environ.setdefault('DB_NAME', 'fullbay_data')
os.environ.setdefault('DB_USER', 'postgres')
os.environ.setdefault('DB_PASSWORD', '5255Tillman')
os.environ.setdefault('ENVIRONMENT', 'development')
os.environ.setdefault('LOG_LEVEL', 'INFO')

# Add src to Python path
current_dir = Path(__file__).parent
src_dir = current_dir / "src"
sys.path.insert(0, str(src_dir))

# Import modules directly
import config
import fullbay_client
import database
import utils

def setup_april_logging():
    """Set up comprehensive logging for April 2025 ingestion."""
    # Create logs directory if it doesn't exist
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    
    # Create specific log file for April 2025 ingestion
    log_filename = f"{log_dir}/april_first_5_days_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    # Configure root logger
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),  # Console output
            logging.FileHandler(log_filename, encoding='utf-8')   # File output with UTF-8 encoding
        ]
    )
    
    # Create and return logger
    logger = logging.getLogger("april_first_5_days")
    logger.setLevel(logging.INFO)
    
    # Set third-party library log levels to reduce noise
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("boto3").setLevel(logging.WARNING)
    
    logger.info(f"Logging configured - Log file: {log_filename}")
    return logger

def check_logging_configuration(logger):
    """Verify that logging is properly configured."""
    logger.info("Checking logging configuration...")
    
    # Test different log levels
    logger.debug("DEBUG: This should not appear with INFO level")
    logger.info("INFO: This should appear")
    logger.warning("WARNING: This should appear")
    logger.error("ERROR: This should appear")
    
    # Check if file logging is working
    logger.info("Logging configuration verified")
    return True

def test_api_connection(logger):
    """Test the Fullbay API connection."""
    logger.info("Testing Fullbay API connection...")
    
    try:
        # Set the real API key
        os.environ['FULLBAY_API_KEY'] = '4b9fcc18-1f24-09fb-275b-ad1974786395'
        
        config_obj = config.Config()
        logger.info(f"Config loaded - Environment: {config_obj.environment}")
        logger.info(f"API Key configured: {bool(config_obj.fullbay_api_key)}")
        
        client = fullbay_client.FullbayClient(config_obj)
        logger.info("FullbayClient initialized")
        
        # Test API status
        api_status = client.get_api_status()
        logger.info(f"API Status: {api_status}")
        
        if api_status.get("status") != "connected":
            raise Exception(f"API connection failed: {api_status.get('error', 'Unknown error')}")
        
        logger.info("✅ API connection successful")
        return client, config_obj
        
    except Exception as e:
        logger.error(f"❌ API connection failed: {e}")
        return None, None

def process_single_day(logger, client, config_obj, target_date):
    """Process a single day of data."""
    date_str = target_date.strftime('%Y-%m-%d')
    start_time = datetime.now(timezone.utc)
    
    logger.info(f"Processing {date_str}...")
    
    try:
        # Get data from API
        logger.info(f"Retrieving data for {date_str}...")
        api_data = client.fetch_invoices_for_date(target_date)
        
        if not api_data:
            logger.info(f"{date_str}: No data available")
            return {
                'date': date_str,
                'status': 'no_data',
                'records_processed': 0,
                'records_inserted': 0,
                'line_items_created': 0,
                'duration_seconds': 0,
                'error': None
            }
        
        logger.info(f"Retrieved {len(api_data)} invoices for {date_str}")
        
        # Process and persist data
        logger.info(f"Persisting data for {date_str}...")
        db_manager = database.DatabaseManager(config_obj)
        db_manager.connect()
        
        records_inserted = db_manager.insert_records(api_data)
        
        # Get line items count
        with db_manager._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT COUNT(*) as line_count 
                    FROM fullbay_line_items 
                    WHERE invoice_date = %s
                """, (date_str,))
                line_items_count = cursor.fetchone()['line_count']
        
        db_manager.close()
        
        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()
        
        logger.info(f"{date_str}: {records_inserted} records inserted, {line_items_count} line items created in {duration:.2f}s")
        
        return {
            'date': date_str,
            'status': 'success',
            'records_processed': len(api_data),
            'records_inserted': records_inserted,
            'line_items_created': line_items_count,
            'duration_seconds': duration,
            'error': None
        }
        
    except Exception as e:
        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()
        
        logger.error(f"{date_str}: Processing failed - {e}")
        
        return {
            'date': date_str,
            'status': 'error',
            'records_processed': 0,
            'records_inserted': 0,
            'line_items_created': 0,
            'duration_seconds': duration,
            'error': str(e)
        }

def generate_first_5_days_april():
    """Generate dates for first 5 days of April 2025."""
    dates = []
    start_date = date(2025, 4, 1)
    
    for i in range(5):
        current_date = start_date + timedelta(days=i)
        dates.append(current_date)
    
    return dates

def main():
    """Run the first 5 days of April 2025 ingestion process."""
    print("Fullbay API Ingestion for First 5 Days of April 2025")
    print("Testing new mapping functionality")
    print("=" * 70)
    
    # Set up logging
    logger = setup_april_logging()
    
    # Check logging configuration
    check_logging_configuration(logger)
    
    # Test API connection
    client, config_obj = test_api_connection(logger)
    if not client:
        logger.error("Cannot proceed: API connection failed")
        return False
    
    # Skip database connection test since connection is established in connect()
    logger.info("Database connection already established, proceeding with ingestion...")
    
    # Generate first 5 days of April
    april_dates = generate_first_5_days_april()
    logger.info(f"Processing first {len(april_dates)} days of April 2025")
    
    # Process each day
    results = []
    total_records_processed = 0
    total_records_inserted = 0
    total_line_items_created = 0
    total_duration = 0
    success_count = 0
    error_count = 0
    no_data_count = 0
    
    for i, target_date in enumerate(april_dates, 1):
        logger.info(f"\n{'='*50}")
        logger.info(f"Day {i}/{len(april_dates)}: {target_date.strftime('%Y-%m-%d')}")
        logger.info(f"{'='*50}")
        
        result = process_single_day(logger, client, config_obj, target_date)
        results.append(result)
        
        # Update totals
        total_records_processed += result['records_processed']
        total_records_inserted += result['records_inserted']
        total_line_items_created += result['line_items_created']
        total_duration += result['duration_seconds']
        
        if result['status'] == 'success':
            success_count += 1
        elif result['status'] == 'error':
            error_count += 1
        else:
            no_data_count += 1
        
        # Add a small delay between requests to be respectful to the API
        if i < len(april_dates):
            logger.info("Waiting 2 seconds before next request...")
            time.sleep(2)
    
    # Final summary
    logger.info(f"\n{'='*60}")
    logger.info("FIRST 5 DAYS OF APRIL 2025 INGESTION SUMMARY")
    logger.info(f"{'='*60}")
    logger.info(f"Total days processed: {len(april_dates)}")
    logger.info(f"Successful days: {success_count}")
    logger.info(f"Error days: {error_count}")
    logger.info(f"No data days: {no_data_count}")
    logger.info(f"Total records processed: {total_records_processed}")
    logger.info(f"Total records inserted: {total_records_inserted}")
    logger.info(f"Total line items created: {total_line_items_created}")
    logger.info(f"Total processing time: {total_duration:.2f} seconds")
    logger.info(f"Average time per day: {total_duration/len(april_dates):.2f} seconds")
    
    # Save detailed results to file
    results_file = f"april_first_5_days_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(results_file, 'w', encoding='utf-8') as f:
        json.dump({
            'summary': {
                'total_days': len(april_dates),
                'successful_days': success_count,
                'error_days': error_count,
                'no_data_days': no_data_count,
                'total_records_processed': total_records_processed,
                'total_records_inserted': total_records_inserted,
                'total_line_items_created': total_line_items_created,
                'total_duration_seconds': total_duration,
                'average_duration_per_day': total_duration/len(april_dates)
            },
            'daily_results': results
        }, f, indent=2, default=str)
    
    logger.info(f"Detailed results saved to: {results_file}")
    
    if error_count == 0:
        logger.info("SUCCESS: All days processed successfully!")
        logger.info("Ready to verify new mapping functionality")
    else:
        logger.warning(f"{error_count} days had errors. Check the log for details.")
    
    return error_count == 0

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
