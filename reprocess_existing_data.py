#!/usr/bin/env python3
"""
Reprocess Existing Raw Data Script

This script reprocesses the raw JSON data already stored in the database
to capture the labor line items that failed to insert due to the labor_hours field issue.
"""

import os
import sys
import logging
from datetime import datetime
from typing import List, Dict, Any

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
from database import DatabaseManager

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('reprocess_existing_data.log')
    ]
)
logger = logging.getLogger(__name__)

def get_existing_raw_data(db_manager: DatabaseManager) -> List[Dict[str, Any]]:
    """Get all existing raw data from the database."""
    try:
        with db_manager._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT id, fullbay_invoice_id, raw_json_data, ingestion_timestamp
                    FROM fullbay_raw_data
                    ORDER BY ingestion_timestamp DESC
                """)
                
                raw_records = cursor.fetchall()
                logger.info(f"Found {len(raw_records)} raw data records to reprocess")
                
                # Convert to the format expected by insert_records
                records = []
                for record in raw_records:
                    raw_data = record['raw_json_data']
                    raw_data['_db_id'] = record['id']  # Store the database ID
                    records.append(raw_data)
                
                return records
                
    except Exception as e:
        logger.error(f"Error retrieving raw data: {e}")
        return []

def clear_existing_line_items(db_manager: DatabaseManager):
    """Clear existing line items to avoid duplicates."""
    try:
        with db_manager._get_connection() as conn:
            with conn.cursor() as cursor:
                # Get count before clearing
                cursor.execute("SELECT COUNT(*) as count FROM fullbay_line_items")
                before_count = cursor.fetchone()['count']
                
                logger.info(f"Clearing {before_count:,} existing line items before reprocessing...")
                
                # Clear the line items table
                cursor.execute("TRUNCATE TABLE fullbay_line_items CASCADE")
                conn.commit()
                
                logger.info("✅ Existing line items cleared successfully")
                
    except Exception as e:
        logger.error(f"Error clearing existing line items: {e}")
        raise

def reprocess_raw_data():
    """Reprocess all existing raw data to capture missing labor line items."""
    logger.info("🚀 Starting reprocessing of existing raw data")
    
    try:
        # Initialize database manager (no shop-specific config needed for reprocessing)
        config = Config()
        db_manager = DatabaseManager(config)
        
        # Connect to database
        db_manager.connect()
        logger.info("✅ Database connection established")
        
        # Get existing raw data
        logger.info("📖 Retrieving existing raw data...")
        raw_records = get_existing_raw_data(db_manager)
        
        if not raw_records:
            logger.warning("❌ No raw data found to reprocess")
            return False
        
        # Clear existing line items to avoid duplicates
        logger.info("🧹 Clearing existing line items...")
        clear_existing_line_items(db_manager)
        
        # Reprocess all raw data
        logger.info(f"⚙️  Reprocessing {len(raw_records):,} raw records...")
        
        total_line_items = 0
        processed_invoices = 0
        failed_invoices = 0
        
        # Process in batches to avoid memory issues
        batch_size = 100
        for i in range(0, len(raw_records), batch_size):
            batch = raw_records[i:i+batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(raw_records) + batch_size - 1) // batch_size
            
            logger.info(f"📦 Processing batch {batch_num}/{total_batches} ({len(batch)} records)...")
            
            try:
                # Process this batch
                records_inserted = db_manager.insert_records(batch)
                
                total_line_items += records_inserted
                processed_invoices += len(batch)
                
                logger.info(f"✅ Batch {batch_num}: Created {records_inserted} line items from {len(batch)} invoices")
                
            except Exception as e:
                logger.error(f"❌ Batch {batch_num} failed: {e}")
                failed_invoices += len(batch)
                continue
        
        # Final summary
        logger.info("\n" + "="*60)
        logger.info("🎉 REPROCESSING COMPLETED")
        logger.info("="*60)
        logger.info(f"📊 Total raw records: {len(raw_records):,}")
        logger.info(f"✅ Successfully processed: {processed_invoices:,}")
        logger.info(f"❌ Failed to process: {failed_invoices:,}")
        logger.info(f"📊 Total line items created: {total_line_items:,}")
        logger.info("="*60)
        
        # Verify final state
        with db_manager._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) as count FROM fullbay_line_items")
                final_count = cursor.fetchone()['count']
                
                cursor.execute("SELECT COUNT(*) as count FROM fullbay_raw_data")
                raw_count = cursor.fetchone()['count']
                
                logger.info(f"📊 Final database state:")
                logger.info(f"   Raw data records: {raw_count:,}")
                logger.info(f"   Line items records: {final_count:,}")
                logger.info(f"   Average line items per invoice: {final_count/raw_count:.1f}")
        
        return True
        
    except Exception as e:
        logger.error(f"💥 Fatal error during reprocessing: {e}")
        return False
        
    finally:
        if 'db_manager' in locals():
            db_manager.close()
            logger.info("🔌 Database connection closed")

def main():
    """Main function."""
    print("🔄 REPROCESS EXISTING RAW DATA")
    print("="*50)
    print("This will reprocess all existing raw JSON data to capture")
    print("the labor line items that failed due to the labor_hours field issue.")
    print()
    print("⚠️  WARNING: This will:")
    print("   1. Clear all existing line items")
    print("   2. Reprocess all raw data with the fixed labor_hours field")
    print("   3. Recreate all line items including the missing labor items")
    print()
    
    # Check if user wants to proceed
    response = input("Continue with reprocessing? (y/n): ")
    if response.lower() != 'y':
        print("❌ Operation cancelled")
        return
    
    success = reprocess_raw_data()
    
    if success:
        print("\n🎉 Reprocessing completed successfully!")
        print("All labor line items should now be properly captured in the database.")
        print("You can verify with: python check_database_state.py")
    else:
        print("\n❌ Reprocessing failed!")
        print("Check the reprocess_existing_data.log file for detailed error information.")
        sys.exit(1)

if __name__ == "__main__":
    main()

