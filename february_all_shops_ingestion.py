#!/usr/bin/env python3
"""
February 2025 Multi-Shop Data Ingestion Script

Ingests February 2025 data for all configured shops (DET, OKPK, WIX, BUS).
Processes each shop sequentially with comprehensive logging and error handling.
"""

import os
import sys
import logging
import asyncio
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Tuple

# Add src directory to path
sys.path.append('src')

from config import Config
from database import DatabaseManager
from fullbay_client import FullbayClient
from utils import setup_logging
from multi_shop_config import MultiShopConfigManager

# Configure logging
logger = logging.getLogger(__name__)

def load_local_env():
    """Load environment variables from local_config.env."""
    env_file = "local_config.env"
    if os.path.exists(env_file):
        with open(env_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key] = value
        logger.info("✅ Environment variables loaded from local config")
    else:
        logger.error("❌ local_config.env not found!")
        raise FileNotFoundError("local_config.env is required")

def get_february_date_range() -> Tuple[date, date]:
    """Get the date range for February 2025."""
    start_date = date(2025, 2, 1)
    end_date = date(2025, 2, 28)  # February 2025 has 28 days
    return start_date, end_date

async def ingest_shop_data(shop_id: str, shop_name: str, start_date: date, end_date: date) -> Dict[str, Any]:
    """
    Ingest February data for a single shop.
    
    Args:
        shop_id: Shop identifier (e.g., 'DET', 'OKPK')
        shop_name: Human-readable shop name
        start_date: Start date for ingestion
        end_date: End date for ingestion
        
    Returns:
        Dictionary with ingestion results
    """
    logger.info(f"🏪 Starting ingestion for {shop_name} ({shop_id})")
    logger.info(f"📅 Date range: {start_date} to {end_date}")
    
    results = {
        'shop_id': shop_id,
        'shop_name': shop_name,
        'start_date': start_date,
        'end_date': end_date,
        'total_days': (end_date - start_date).days + 1,
        'days_processed': 0,
        'days_with_data': 0,
        'total_invoices': 0,
        'total_line_items': 0,
        'errors': [],
        'success': False
    }
    
    try:
        # Initialize configuration and clients
        config = Config(shop_id=shop_id)
        client = FullbayClient(config)
        db_manager = DatabaseManager(config)
        
        # Connect to database
        db_manager.connect()
        logger.info(f"🔌 Connected to database and API for {shop_name}")
        
        # Process each day in February
        current_date = start_date
        daily_results = []
        
        while current_date <= end_date:
            try:
                logger.info(f"📅 Processing {current_date} for {shop_name}...")
                
                # Fetch invoices for this date
                target_datetime = datetime.combine(current_date, datetime.min.time())
                invoices = client.fetch_invoices_for_date(target_datetime)
                
                day_result = {
                    'date': current_date,
                    'invoice_count': len(invoices) if invoices else 0,
                    'line_items_created': 0,
                    'success': True,
                    'error': None
                }
                
                if invoices:
                    logger.info(f"   📊 Found {len(invoices)} invoices for {current_date}")
                    results['days_with_data'] += 1
                    
                    # Process invoices using the correct DatabaseManager method
                    try:
                        # Insert and process all invoices for this date
                        line_items_created = db_manager.insert_records(invoices)
                        logger.info(f"   💾 Successfully processed {len(invoices)} invoices → {line_items_created} line items")
                        
                    except Exception as e:
                        error_msg = f"Failed to process invoices for {current_date}: {str(e)}"
                        logger.error(f"   ❌ {error_msg}")
                        results['errors'].append(error_msg)
                        line_items_created = 0
                    
                    day_result['line_items_created'] = line_items_created
                    results['total_invoices'] += len(invoices)
                    results['total_line_items'] += line_items_created
                    
                    logger.info(f"   ✅ {current_date}: {len(invoices)} invoices → {line_items_created} line items")
                else:
                    logger.info(f"   📭 No invoices found for {current_date}")
                
                daily_results.append(day_result)
                results['days_processed'] += 1
                
            except Exception as e:
                error_msg = f"Failed to process {current_date} for {shop_name}: {str(e)}"
                logger.error(f"   ❌ {error_msg}")
                results['errors'].append(error_msg)
                
                day_result = {
                    'date': current_date,
                    'invoice_count': 0,
                    'line_items_created': 0,
                    'success': False,
                    'error': str(e)
                }
                daily_results.append(day_result)
            
            # Move to next day
            current_date += timedelta(days=1)
        
        # Close database connection
        db_manager.close()
        
        results['daily_results'] = daily_results
        results['success'] = len(results['errors']) == 0
        
        # Log summary for this shop
        logger.info(f"🎉 {shop_name} ({shop_id}) Summary:")
        logger.info(f"   📊 Total invoices: {results['total_invoices']:,}")
        logger.info(f"   📋 Total line items: {results['total_line_items']:,}")
        logger.info(f"   📅 Days with data: {results['days_with_data']}/{results['total_days']}")
        logger.info(f"   ❌ Errors: {len(results['errors'])}")
        
        return results
        
    except Exception as e:
        error_msg = f"Critical error for {shop_name} ({shop_id}): {str(e)}"
        logger.error(f"💥 {error_msg}")
        results['errors'].append(error_msg)
        results['success'] = False
        return results

async def main():
    """Main ingestion function for all shops."""
    print("🚀 FEBRUARY 2025 MULTI-SHOP DATA INGESTION")
    print("=" * 60)
    
    # Setup logging
    log_filename = f"logs/february_all_shops_2025_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    os.makedirs("logs", exist_ok=True)
    setup_logging(log_level="INFO", log_file=log_filename)
    
    start_time = datetime.now()
    logger.info("🚀 Starting February 2025 multi-shop ingestion")
    
    try:
        # Load environment variables
        load_local_env()
        
        # Get February date range
        start_date, end_date = get_february_date_range()
        logger.info(f"📅 Ingesting data from {start_date} to {end_date}")
        
        # Initialize multi-shop manager
        shop_manager = MultiShopConfigManager()
        available_shops = shop_manager.get_all_shops()
        
        if not available_shops:
            logger.error("❌ No shops configured!")
            return False
        
        logger.info(f"🏪 Found {len(available_shops)} shops to process")
        
        # Process each shop
        all_results = []
        overall_success = True
        
        for shop_id, shop_config in available_shops.items():
            print(f"\n{'='*20} {shop_config.shop_name} ({shop_id}) {'='*20}")
            
            shop_results = await ingest_shop_data(
                shop_id, 
                shop_config.shop_name, 
                start_date, 
                end_date
            )
            
            all_results.append(shop_results)
            
            if not shop_results['success']:
                overall_success = False
            
            # Brief pause between shops
            await asyncio.sleep(2)
        
        # Print final summary
        print(f"\n{'='*60}")
        print("🎉 FEBRUARY 2025 INGESTION COMPLETE")
        print("=" * 60)
        
        total_invoices = 0
        total_line_items = 0
        total_errors = 0
        
        for result in all_results:
            status = "✅" if result['success'] else "❌"
            print(f"{status} {result['shop_name']} ({result['shop_id']}):")
            print(f"   📊 Invoices: {result['total_invoices']:,}")
            print(f"   📋 Line Items: {result['total_line_items']:,}")
            print(f"   📅 Days with data: {result['days_with_data']}/{result['total_days']}")
            print(f"   ❌ Errors: {len(result['errors'])}")
            
            total_invoices += result['total_invoices']
            total_line_items += result['total_line_items']
            total_errors += len(result['errors'])
            print()
        
        print("📊 OVERALL SUMMARY:")
        print(f"   🏪 Shops processed: {len(all_results)}")
        print(f"   📊 Total invoices: {total_invoices:,}")
        print(f"   📋 Total line items: {total_line_items:,}")
        print(f"   ❌ Total errors: {total_errors}")
        print(f"   ⏱️  Duration: {datetime.now() - start_time}")
        
        success_count = sum(1 for r in all_results if r['success'])
        print(f"   🎯 Success rate: {success_count}/{len(all_results)} shops")
        
        logger.info("🎉 February 2025 multi-shop ingestion completed")
        logger.info(f"📊 Final totals: {total_invoices:,} invoices, {total_line_items:,} line items")
        
        return overall_success
        
    except Exception as e:
        logger.error(f"💥 Critical error in main ingestion: {str(e)}")
        print(f"💥 Critical error: {str(e)}")
        return False

if __name__ == "__main__":
    success = asyncio.run(main())
    exit_code = 0 if success else 1
    print(f"\n🏁 Ingestion {'completed successfully' if success else 'completed with errors'}")
    sys.exit(exit_code)
