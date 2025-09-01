"""
Lambda function handler for Fullbay API integration testing without database.

This version tests the Fullbay API integration without requiring database connectivity.
"""

import json
import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional

from fullbay_client import FullbayClient
from config import Config

# Initialize logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda function handler for Fullbay API integration testing.
    
    Args:
        event: Lambda event data
        context: Lambda runtime context
        
    Returns:
        Dict containing test results
    """
    start_time = datetime.now(timezone.utc)
    execution_id = context.aws_request_id if context else "test-execution"
    
    logger.info(f"🧪 Starting Fullbay API integration test - Execution ID: {execution_id}")
    
    try:
        # Load configuration
        config = Config()
        logger.info(f"📊 Configuration loaded - Environment: {config.environment}")
        
        # Initialize Fullbay client
        fullbay_client = FullbayClient(config)
        
        # Test API connection
        logger.info("🔗 Testing Fullbay API connection...")
        api_status = fullbay_client.get_api_status()
        logger.info(f"📡 API Status: {api_status}")
        
        if api_status.get("status") != "connected":
            raise Exception(f"Fullbay API connection failed: {api_status.get('error', 'Unknown error')}")
        
        # Determine target date (yesterday by default)
        target_date = get_target_date_from_event(event)
        logger.info(f"📅 Target date for invoice fetch: {target_date.strftime('%Y-%m-%d')}")
        
        # Test token generation
        date_str = target_date.strftime('%Y-%m-%d')
        token = fullbay_client._generate_token(date_str)
        logger.info(f"🔑 Generated token for {date_str}: {token[:10]}...")
        
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
                target_date=target_date.strftime('%Y-%m-%d'),
                api_status=api_status,
                token_generated=True
            )
        
        logger.info(f"📦 Retrieved {len(api_data)} invoices from Fullbay API")
        
        # Show sample invoice structure
        sample_invoice_keys = []
        if api_data:
            sample_invoice = api_data[0]
            sample_invoice_keys = list(sample_invoice.keys())[:10]
            logger.info(f"📋 Sample invoice keys: {sample_invoice_keys}...")
        
        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()
        
        response = create_response(
            "SUCCESS", 
            f"Successfully retrieved {len(api_data)} invoices", 
            execution_id, 
            start_time,
            records_processed=len(api_data),
            records_inserted=0,  # No database insertion in test mode
            duration_seconds=duration,
            target_date=target_date.strftime('%Y-%m-%d'),
            api_status=api_status,
            token_generated=True,
            sample_invoice_keys=sample_invoice_keys
        )
        
        logger.info(f"🎉 Fullbay API integration test completed successfully in {duration:.2f} seconds")
        return response
        
    except Exception as e:
        logger.error(f"❌ Fullbay API integration test failed: {str(e)}", exc_info=True)
        
        return create_response(
            "ERROR", 
            f"Fullbay API integration test failed: {str(e)}", 
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
