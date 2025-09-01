"""
Main Lambda function handler for Fullbay API data ingestion.

This function is triggered by EventBridge (CloudWatch Events) on a configurable schedule
to pull data from the Fullbay API and persist it to RDS.
"""

import json
import logging
import os
from datetime import datetime, timezone
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
    
    logger.info(f"Starting Fullbay API ingestion - Execution ID: {execution_id}")
    
    try:
        # Load configuration
        config = Config()
        logger.info(f"Configuration loaded - Environment: {config.environment}")
        
        # Initialize clients
        fullbay_client = FullbayClient(config)
        db_manager = DatabaseManager(config)
        
        # Connect to database
        db_manager.connect()
        logger.info("Database connection established")
        
        # Retrieve data from Fullbay API
        logger.info("Fetching data from Fullbay API...")
        api_data = fullbay_client.fetch_data()
        
        if not api_data:
            logger.warning("No data retrieved from Fullbay API")
            return create_response("SUCCESS", "No data to process", execution_id, start_time)
        
        logger.info(f"Retrieved {len(api_data)} records from Fullbay API")
        
        # Persist data to database
        logger.info("Persisting data to database...")
        records_inserted = db_manager.insert_records(api_data)
        
        logger.info(f"Successfully inserted {records_inserted} records")
        
        # Clean up
        db_manager.close()
        
        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()
        
        response = create_response(
            "SUCCESS", 
            f"Processed {records_inserted} records", 
            execution_id, 
            start_time,
            records_processed=records_inserted,
            duration_seconds=duration
        )
        
        logger.info(f"Ingestion completed successfully in {duration:.2f} seconds")
        return response
        
    except Exception as e:
        logger.error(f"Ingestion failed: {str(e)}", exc_info=True)
        
        # Attempt to close database connection if it exists
        try:
            if 'db_manager' in locals():
                db_manager.close()
        except Exception as cleanup_error:
            logger.error(f"Error during cleanup: {cleanup_error}")
        
        return create_response(
            "ERROR", 
            f"Ingestion failed: {str(e)}", 
            execution_id, 
            start_time,
            error=str(e)
        )


def create_response(
    status: str, 
    message: str, 
    execution_id: str, 
    start_time: datetime,
    **kwargs
) -> Dict[str, Any]:
    """
    Create standardized response object.
    
    Args:
        status: SUCCESS or ERROR
        message: Human-readable status message
        execution_id: Unique execution identifier
        start_time: Function start timestamp
        **kwargs: Additional metadata to include
        
    Returns:
        Standardized response dictionary
    """
    response = {
        "statusCode": 200 if status == "SUCCESS" else 500,
        "body": json.dumps({
            "status": status,
            "message": message,
            "execution_id": execution_id,
            "timestamp": start_time.isoformat(),
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
