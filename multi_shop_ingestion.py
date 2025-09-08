#!/usr/bin/env python3
"""
Multi-Shop Fullbay Data Ingestion Script

This script handles data ingestion for multiple shops with different API keys.
Supports shop selection and date range specification.
"""

import os
import sys
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional

# Load environment variables from local config file if it exists
def load_local_env():
    """Load environment variables from local_config.env if it exists."""
    env_file = "local_config.env"
    if os.path.exists(env_file):
        print("🔧 Loading environment variables from local_config.env...")
        try:
            with open(env_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        key = key.strip()
                        value = value.strip()
                        
                        # Remove quotes if present
                        if value.startswith('"') and value.endswith('"'):
                            value = value[1:-1]
                        elif value.startswith("'") and value.endswith("'"):
                            value = value[1:-1]
                        
                        # Don't load placeholder values
                        if not value.endswith('_here'):
                            os.environ[key] = value
            print("✅ Environment variables loaded from local config")
        except Exception as e:
            print(f"⚠️  Error loading local config: {e}")

# Load local environment variables first
load_local_env()

# Add src directory to path
sys.path.append('src')

from config import Config
from fullbay_client import FullbayClient
from database import DatabaseManager
from multi_shop_config import MultiShopConfigManager

def setup_logging(shop_id: str, date_range: str) -> logging.Logger:
    """Set up logging for the ingestion process."""
    log_filename = f"multi_shop_ingestion_{shop_id}_{date_range}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_filename)
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info(f"Logging to: {log_filename}")
    return logger

def generate_date_range(start_date: datetime, end_date: datetime) -> List[datetime]:
    """Generate list of dates in the specified range."""
    dates = []
    current_date = start_date
    
    while current_date <= end_date:
        dates.append(current_date)
        current_date += timedelta(days=1)
    
    return dates

def parse_date_input(date_str: str) -> datetime:
    """Parse date string in various formats."""
    formats = [
        '%Y-%m-%d',
        '%m/%d/%Y',
        '%m-%d-%Y',
        '%Y/%m/%d'
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    
    raise ValueError(f"Unable to parse date: {date_str}")

def get_date_range_input() -> tuple[datetime, datetime]:
    """Get date range from user input."""
    print("\n📅 DATE RANGE SELECTION")
    print("=" * 30)
    print("Enter dates in format: YYYY-MM-DD (e.g., 2025-01-01)")
    print()
    
    while True:
        try:
            start_str = input("Start date: ").strip()
            start_date = parse_date_input(start_str)
            break
        except ValueError as e:
            print(f"❌ {e}. Please try again.")
    
    while True:
        try:
            end_str = input("End date: ").strip()
            end_date = parse_date_input(end_str)
            
            if end_date < start_date:
                print("❌ End date must be after start date. Please try again.")
                continue
            
            break
        except ValueError as e:
            print(f"❌ {e}. Please try again.")
    
    # Confirm date range
    days_count = (end_date - start_date).days + 1
    print(f"\n📊 Selected range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print(f"📊 Total days: {days_count}")
    
    if days_count > 31:
        confirm = input(f"⚠️  This is {days_count} days. Continue? (y/n): ").strip().lower()
        if confirm != 'y':
            print("❌ Operation cancelled")
            sys.exit(0)
    
    return start_date, end_date

def process_shop_data(shop_id: str, start_date: datetime, end_date: datetime) -> bool:
    """Process data for a specific shop and date range."""
    date_range_str = f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
    logger = setup_logging(shop_id, date_range_str)
    
    logger.info(f"🚀 Starting {shop_id} shop data ingestion")
    logger.info(f"📅 Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    try:
        # Initialize components with shop-specific configuration
        config = Config(shop_id=shop_id)
        logger.info(f"📋 Configuration: {config}")
        
        fullbay_client = FullbayClient(config)
        db_manager = DatabaseManager(config)
        
        # Connect to database
        db_manager.connect()
        logger.info("✅ Database connection established")
        
        # Test API connection
        logger.info("🔍 Testing API connection...")
        try:
            token = fullbay_client._generate_token(start_date.strftime('%Y-%m-%d'))
            logger.info("✅ API token generation successful")
        except Exception as e:
            raise Exception(f"API connection test failed: {e}")
        
        # Generate date list
        dates = generate_date_range(start_date, end_date)
        logger.info(f"📅 Processing {len(dates)} days for {config.shop_name}")
        
        total_invoices = 0
        total_line_items = 0
        successful_days = 0
        failed_days = 0
        
        # Process each day
        for i, date in enumerate(dates, 1):
            date_str = date.strftime('%Y-%m-%d')
            logger.info(f"\n📅 Processing day {i}/{len(dates)}: {date_str}")
            
            try:
                # Fetch invoices for this date
                logger.info(f"🔍 Fetching invoices for {date_str}...")
                logger.info(f"⏳ This may take up to 1000 seconds (16+ minutes) - please be patient...")
                invoices = fullbay_client.fetch_invoices_for_date(date)
                
                if not invoices:
                    logger.info(f"ℹ️  No invoices found for {date_str}")
                    successful_days += 1
                    continue
                
                logger.info(f"📊 Found {len(invoices)} invoices for {date_str}")
                
                # Insert raw data and process into line items
                logger.info(f"💾 Inserting and processing data for {date_str}...")
                records_inserted = db_manager.insert_records(invoices)
                
                logger.info(f"✅ Successfully processed {records_inserted} records for {date_str}")
                
                total_invoices += len(invoices)
                total_line_items += records_inserted
                successful_days += 1
                
                # Small delay to be respectful to the API
                import time
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"❌ Failed to process {date_str}: {e}")
                failed_days += 1
                continue
        
        # Final summary
        logger.info("\n" + "="*60)
        logger.info(f"🎉 {config.shop_name.upper()} INGESTION COMPLETED")
        logger.info("="*60)
        logger.info(f"✅ Successful days: {successful_days}")
        logger.info(f"❌ Failed days: {failed_days}")
        logger.info(f"📊 Total invoices processed: {total_invoices:,}")
        logger.info(f"📊 Total line items created: {total_line_items:,}")
        logger.info("="*60)
        
        return successful_days > 0
        
    except Exception as e:
        logger.error(f"💥 Fatal error during {shop_id} ingestion: {e}")
        return False
        
    finally:
        if 'db_manager' in locals():
            db_manager.close()
            logger.info("🔌 Database connection closed")

def add_new_shop():
    """Interactive function to add a new shop configuration."""
    print("\n🏪 ADD NEW SHOP")
    print("=" * 30)
    
    shop_id = input("Shop ID (e.g., CHI, NYC, ATL): ").strip().upper()
    if not shop_id:
        print("❌ Shop ID cannot be empty")
        return
    
    shop_name = input("Shop Name (e.g., Chicago, New York): ").strip()
    if not shop_name:
        print("❌ Shop name cannot be empty")
        return
    
    api_key = input("Fullbay API Key: ").strip()
    if not api_key:
        print("❌ API key cannot be empty")
        return
    
    # Add to configuration
    manager = MultiShopConfigManager()
    success = manager.add_shop_to_config_file(shop_id, shop_name, api_key)
    
    if success:
        print(f"✅ Added shop: {shop_name} ({shop_id})")
    else:
        print(f"❌ Failed to add shop")

def main():
    """Main function."""
    print("🏪 MULTI-SHOP FULLBAY DATA INGESTION")
    print("=" * 50)
    
    # Initialize shop manager
    manager = MultiShopConfigManager()
    
    if not manager.get_shop_ids():
        print("❌ No shops configured!")
        print("\nWould you like to add a shop? (y/n): ", end="")
        if input().strip().lower() == 'y':
            add_new_shop()
            manager = MultiShopConfigManager()  # Reload
        
        if not manager.get_shop_ids():
            print("❌ No shops available. Exiting.")
            sys.exit(1)
    
    # Show available options
    print("\nSelect an option:")
    print("1. Run ingestion for a shop")
    print("2. Add new shop")
    print("3. List all shops")
    print("0. Exit")
    
    while True:
        try:
            choice = input("\nEnter your choice: ").strip()
            
            if choice == '0':
                print("👋 Goodbye!")
                sys.exit(0)
            elif choice == '1':
                break
            elif choice == '2':
                add_new_shop()
                manager = MultiShopConfigManager()  # Reload
                continue
            elif choice == '3':
                manager.list_shops()
                continue
            else:
                print("❌ Invalid choice. Please try again.")
        except KeyboardInterrupt:
            print("\n👋 Goodbye!")
            sys.exit(0)
    
    # Select shop
    shop_id = manager.interactive_shop_selection()
    if not shop_id:
        sys.exit(0)
    
    # Validate shop configuration
    is_valid, error_msg = manager.validate_shop_config(shop_id)
    if not is_valid:
        print(f"❌ Shop configuration error: {error_msg}")
        sys.exit(1)
    
    # Get date range
    start_date, end_date = get_date_range_input()
    
    # Confirm operation
    shop_config = manager.get_shop_config(shop_id)
    days_count = (end_date - start_date).days + 1
    
    print(f"\n📋 INGESTION SUMMARY")
    print("=" * 30)
    print(f"Shop: {shop_config.shop_name} ({shop_id})")
    print(f"Date Range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print(f"Total Days: {days_count}")
    print(f"Estimated Time: {days_count * 8}-{days_count * 16} minutes")
    print()
    
    confirm = input("Continue with ingestion? (y/n): ").strip().lower()
    if confirm != 'y':
        print("❌ Operation cancelled")
        sys.exit(0)
    
    # Run ingestion
    success = process_shop_data(shop_id, start_date, end_date)
    
    if success:
        print(f"\n🎉 {shop_config.shop_name} ingestion completed successfully!")
        print("You can check results with: python check_database_state.py")
    else:
        print(f"\n❌ {shop_config.shop_name} ingestion failed!")
        print("Check the log file for detailed error information.")
        sys.exit(1)

if __name__ == "__main__":
    main()
