"""
Utility functions for logging and monitoring.
"""

import logging
import os
import sys
from datetime import datetime
from typing import Optional
import json

def setup_logging(
    log_level: str = "INFO",
    log_file: Optional[str] = None,
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
) -> logging.Logger:
    """
    Set up comprehensive logging configuration.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional file path for file logging
        log_format: Log message format
        
    Returns:
        Configured logger
    """
    # Create logs directory if it doesn't exist
    if log_file and not os.path.exists(os.path.dirname(log_file)):
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout),  # Console output
            *([logging.FileHandler(log_file)] if log_file else [])
        ]
    )
    
    # Create and return logger
    logger = logging.getLogger("fullbay_ingestion")
    logger.setLevel(getattr(logging, log_level.upper()))
    
    return logger

def log_ingestion_summary(
    logger: logging.Logger,
    start_time: datetime,
    end_time: datetime,
    records_processed: int,
    records_inserted: int,
    line_items_created: int,
    errors_count: int,
    target_date: str,
    execution_id: str
):
    """
    Log a comprehensive ingestion summary.
    
    Args:
        logger: Logger instance
        start_time: When ingestion started
        end_time: When ingestion completed
        records_processed: Number of records processed
        records_inserted: Number of records inserted
        line_items_created: Number of line items created
        errors_count: Number of errors encountered
        target_date: Target date for ingestion
        execution_id: Unique execution identifier
    """
    duration = (end_time - start_time).total_seconds()
    
    summary = {
        "execution_id": execution_id,
        "target_date": target_date,
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "duration_seconds": duration,
        "records_processed": records_processed,
        "records_inserted": records_inserted,
        "line_items_created": line_items_created,
        "errors_count": errors_count,
        "success_rate": f"{(records_inserted/records_processed*100):.1f}%" if records_processed > 0 else "0%",
        "status": "SUCCESS" if errors_count == 0 else "PARTIAL_SUCCESS" if records_inserted > 0 else "FAILED"
    }
    
    logger.info("=== INGESTION SUMMARY ===")
    logger.info(json.dumps(summary, indent=2))
    logger.info("=========================")

def log_data_quality_report(
    logger: logging.Logger,
    data_quality_metrics: dict
):
    """
    Log data quality metrics.
    
    Args:
        logger: Logger instance
        data_quality_metrics: Dictionary containing data quality metrics
    """
    logger.info("=== DATA QUALITY REPORT ===")
    logger.info(json.dumps(data_quality_metrics, indent=2))
    logger.info("===========================")

def log_api_request(
    logger: logging.Logger,
    method: str,
    url: str,
    params: dict,
    headers: dict,
    response_status: int,
    response_time: float,
    response_size: int
):
    """
    Log API request details.
    
    Args:
        logger: Logger instance
        method: HTTP method
        url: Request URL
        params: Request parameters
        headers: Request headers
        response_status: HTTP response status
        response_time: Response time in seconds
        response_size: Response size in bytes
    """
    # Mask sensitive data
    masked_params = params.copy()
    if 'key' in masked_params:
        masked_params['key'] = f"{masked_params['key'][:8]}...{masked_params['key'][-4:]}"
    if 'token' in masked_params:
        masked_params['token'] = f"{masked_params['token'][:8]}..."
    
    request_log = {
        "method": method,
        "url": url,
        "params": masked_params,
        "headers": {k: v for k, v in headers.items() if k.lower() not in ['authorization', 'cookie']},
        "response_status": response_status,
        "response_time_seconds": response_time,
        "response_size_bytes": response_size
    }
    
    logger.info("=== API REQUEST LOG ===")
    logger.info(json.dumps(request_log, indent=2))
    logger.info("=======================")

def log_database_operation(
    logger: logging.Logger,
    operation: str,
    table: str,
    records_count: int,
    duration: float,
    success: bool,
    error_message: Optional[str] = None
):
    """
    Log database operation details.
    
    Args:
        logger: Logger instance
        operation: Database operation (INSERT, UPDATE, DELETE, etc.)
        table: Table name
        records_count: Number of records affected
        duration: Operation duration in seconds
        success: Whether operation was successful
        error_message: Error message if operation failed
    """
    db_log = {
        "operation": operation,
        "table": table,
        "records_count": records_count,
        "duration_seconds": duration,
        "success": success,
        "error_message": error_message
    }
    
    logger.info("=== DATABASE OPERATION ===")
    logger.info(json.dumps(db_log, indent=2))
    logger.info("==========================")

def create_execution_logger(execution_id: str, target_date: str) -> logging.Logger:
    """
    Create a logger specifically for an execution run.
    
    Args:
        execution_id: Unique execution identifier
        target_date: Target date for the execution
        
    Returns:
        Logger instance
    """
    # Create execution-specific log file
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    
    log_filename = f"{log_dir}/ingestion_{target_date}_{execution_id}.log"
    
    # Create logger
    logger = logging.getLogger(f"fullbay_ingestion_{execution_id}")
    logger.setLevel(logging.INFO)
    
    # Create file handler
    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(logging.INFO)
    
    # Create formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(formatter)
    
    # Add handler to logger
    logger.addHandler(file_handler)
    
    return logger