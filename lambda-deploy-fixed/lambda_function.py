"""
Main Lambda function handler for Fullbay API data ingestion.

This function is triggered by EventBridge (CloudWatch Events) on a configurable schedule
to pull data from the Fullbay API and persist it to RDS.
"""

import json
import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional

from config import Config
from fullbay_client import FullbayClient
from database import DatabaseManager
from utils import setup_logging, handle_errors

# Initialize logging
logger = setup_logging()


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda function handler.
    
    Args:
        event: Lambda event data (from EventBridge trigger)
        context: Lambda runtime context
        
    Returns:
        Dict containing execution status and metadata
    """
    start_time = datetime.now(timezone.utc)
    execution_id = context.aws_request_id if context else "local-test"
    
    logger.info(f"🚀 Starting Fullbay API ingestion - Execution ID: {execution_id}")
    
    try:
        # Load configuration
        config = Config()
        logger.info(f"📊 Configuration loaded - Environment: {config.environment}")
        
        # Initialize clients
        fullbay_client = FullbayClient(config)
        db_manager = DatabaseManager(config)
        
        # Test API connection first
        logger.info("🔗 Testing Fullbay API connection...")
        api_status = fullbay_client.get_api_status()
        logger.info(f"📡 API Status: {api_status}")
        
        if api_status.get("status") != "connected":
            raise Exception(f"Fullbay API connection failed: {api_status.get('error', 'Unknown error')}")
        
        # Connect to database
        logger.info("💾 Connecting to database...")
        db_manager.connect()
        logger.info("✅ Database connection established")
        
        # Determine target date (yesterday by default)
        target_date = get_target_date_from_event(event)
        logger.info(f"📅 Target date for invoice fetch: {target_date.strftime('%Y-%m-%d')}")
        
        # Retrieve data from Fullbay API
        logger.info("📡 Fetching invoices from Fullbay API...")
        api_data = fullbay_client.fetch_invoices_for_date(target_date)
        
        if not api_data:
            logger.warning(f"⚠️ No invoices found for date {target_date.strftime('%Y-%m-%d')}")
            return create_response(
                "SUCCESS", 
                f"No invoices found for {target_date.strftime('%Y-%m-%d')}", 
                execution_id, 
                start_time,
                records_processed=0,
                records_inserted=0,
                target_date=target_date.strftime('%Y-%m-%d')
            )
        
        logger.info(f"📦 Retrieved {len(api_data)} invoices from Fullbay API")
        
        # Persist data to database
        logger.info("💾 Persisting invoices to database...")
        records_inserted = db_manager.insert_records(api_data)
        
        logger.info(f"✅ Successfully inserted {records_inserted} line items from {len(api_data)} invoices")
        
        # Log execution metadata
        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()
        
        # Log to database
        db_manager.log_execution_metadata(
            execution_id=execution_id,
            start_time=start_time,
            status="SUCCESS",
            end_time=end_time,
            records_processed=len(api_data),
            records_inserted=records_inserted,
            api_endpoint="getInvoices.php"
        )
        
        response = create_response(
            "SUCCESS", 
            f"Processed {len(api_data)} invoices, created {records_inserted} line items", 
            execution_id, 
            start_time,
            records_processed=len(api_data),
            records_inserted=records_inserted,
            duration_seconds=duration,
            target_date=target_date.strftime('%Y-%m-%d')
        )
        
        logger.info(f"🎉 Ingestion completed successfully in {duration:.2f} seconds")
        return response
        
    except Exception as e:
        logger.error(f"❌ Ingestion failed: {str(e)}", exc_info=True)
        
        # Attempt to close database connection if it exists
        try:
            if 'db_manager' in locals():
                db_manager.close()
        except Exception as cleanup_error:
            logger.error(f"Error during cleanup: {cleanup_error}")
        
        # Log error to database if possible
        try:
            if 'db_manager' in locals() and 'execution_id' in locals():
                db_manager.log_execution_metadata(
                    execution_id=execution_id,
                    start_time=start_time,
                    status="ERROR",
                    end_time=datetime.now(timezone.utc),
                    error_message=str(e),
                    api_endpoint="getInvoices.php"
                )
        except Exception as log_error:
            logger.error(f"Failed to log error metadata: {log_error}")
        
        return create_response(
            "ERROR", 
            f"Ingestion failed: {str(e)}", 
            execution_id, 
            start_time,
            target_date=target_date.strftime('%Y-%m-%d') if 'target_date' in locals() else None
        )


def get_target_date_from_event(event: Dict[str, Any]) -> datetime:
    """
    Determine the target date for invoice fetching from the event.
    
    Args:
        event: Lambda event data
        
    Returns:
        Target date for invoice fetching (defaults to yesterday)
    """
    # Check if event specifies a target date
    if event and isinstance(event, dict):
        # Check for explicit date in event
        if 'target_date' in event:
            try:
                target_date = datetime.strptime(event['target_date'], '%Y-%m-%d')
                return target_date.replace(tzinfo=timezone.utc)
            except ValueError as e:
                logger.warning(f"Invalid target_date in event: {e}, using yesterday")
        
        # Check for days_back parameter
        if 'days_back' in event:
            try:
                days_back = int(event['days_back'])
                target_date = datetime.now(timezone.utc) - timedelta(days=days_back)
                return target_date
            except ValueError as e:
                logger.warning(f"Invalid days_back in event: {e}, using yesterday")
    
    # Default to yesterday
    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    return yesterday


def create_response(
    status: str, 
    message: str, 
    execution_id: str, 
    start_time: datetime,
    **kwargs
) -> Dict[str, Any]:
    """
    Create standardized response for Lambda function.
    
    Args:
        status: Response status (SUCCESS, ERROR, etc.)
        message: Response message
        execution_id: Unique execution identifier
        start_time: Execution start time
        **kwargs: Additional response data
        
    Returns:
        Standardized response dictionary
    """
    end_time = datetime.now(timezone.utc)
    duration = (end_time - start_time).total_seconds()
    
    response = {
        "statusCode": 200 if status == "SUCCESS" else 500,
        "body": json.dumps({
            "status": status,
            "message": message,
            "execution_id": execution_id,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": duration,
            **kwargs
        })
    }
    
    return response


# For local testing
if __name__ == "__main__":
    # Mock Lambda context for local testing
    class MockContext:
        aws_request_id = "local-test-123"
        function_name = "fullbay-ingestion-local"
        
    # Mock event (normally from EventBridge)
    test_event = {
        "source": "aws.events",
        "detail-type": "Scheduled Event",
        "detail": {}
    }
    
    result = lambda_handler(test_event, MockContext())
    print("Local test result:")
    print(json.dumps(result, indent=2))
