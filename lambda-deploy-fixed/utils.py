"""
Utility functions for logging, error handling, and common operations.
"""

import logging
import os
import sys
from typing import Any, Dict
from functools import wraps
from datetime import datetime, timezone


def setup_logging(level: str = None) -> logging.Logger:
    """
    Set up logging configuration for the Lambda function.
    
    Args:
        level: Logging level override
        
    Returns:
        Configured logger
    """
    # Get log level from environment or use default
    log_level = (level or os.getenv("LOG_LEVEL", "INFO")).upper()
    
    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    
    # Configure specific loggers
    logger = logging.getLogger("fullbay_ingestion")
    
    # Set third-party library log levels
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("boto3").setLevel(logging.WARNING)
    
    return logger


def handle_errors(func):
    """
    Decorator for consistent error handling and logging.
    
    Args:
        func: Function to wrap
        
    Returns:
        Wrapped function with error handling
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger = logging.getLogger("fullbay_ingestion")
            logger.error(f"Error in {func.__name__}: {str(e)}", exc_info=True)
            raise
    
    return wrapper


def format_timestamp(dt: datetime = None) -> str:
    """
    Format datetime for consistent logging and database storage.
    
    Args:
        dt: Datetime to format (uses current time if None)
        
    Returns:
        ISO formatted timestamp string
    """
    if dt is None:
        dt = datetime.now(timezone.utc)
    return dt.isoformat()


def sanitize_for_logging(data: Any, max_length: int = 1000) -> str:
    """
    Sanitize data for safe logging (remove sensitive info, truncate if needed).
    
    Args:
        data: Data to sanitize
        max_length: Maximum length of output string
        
    Returns:
        Sanitized string representation
    """
    if isinstance(data, dict):
        # Remove sensitive fields
        sensitive_fields = ["password", "token", "key", "secret", "auth"]
        sanitized = {}
        
        for key, value in data.items():
            if any(sensitive in key.lower() for sensitive in sensitive_fields):
                sanitized[key] = "***REDACTED***"
            else:
                sanitized[key] = value
        
        result = str(sanitized)
    else:
        result = str(data)
    
    # Truncate if too long
    if len(result) > max_length:
        result = result[:max_length] + "...[TRUNCATED]"
    
    return result


def validate_environment_variables(required_vars: list) -> Dict[str, str]:
    """
    Validate that required environment variables are present.
    
    Args:
        required_vars: List of required environment variable names
        
    Returns:
        Dict of environment variables and their values
        
    Raises:
        ValueError: If required variables are missing
    """
    missing_vars = []
    env_vars = {}
    
    for var in required_vars:
        value = os.getenv(var)
        if not value:
            missing_vars.append(var)
        else:
            env_vars[var] = value
    
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {missing_vars}")
    
    return env_vars


def chunk_list(data: list, chunk_size: int) -> list:
    """
    Split a list into chunks of specified size.
    
    Args:
        data: List to chunk
        chunk_size: Size of each chunk
        
    Returns:
        List of chunks
    """
    return [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]


def retry_on_failure(max_retries: int = 3, delay: float = 1.0):
    """
    Decorator for retrying functions on failure.
    
    Args:
        max_retries: Maximum number of retry attempts
        delay: Delay between retries in seconds
        
    Returns:
        Decorator function
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            import time
            
            logger = logging.getLogger("fullbay_ingestion")
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries:
                        logger.warning(
                            f"Attempt {attempt + 1} failed for {func.__name__}: {e}. "
                            f"Retrying in {delay} seconds..."
                        )
                        time.sleep(delay)
                    else:
                        logger.error(f"All {max_retries + 1} attempts failed for {func.__name__}")
            
            raise last_exception
        
        return wrapper
    return decorator


class MetricsCollector:
    """
    Simple metrics collector for tracking function performance and usage.
    """
    
    def __init__(self):
        self.metrics = {}
        self.start_times = {}
    
    def start_timer(self, metric_name: str):
        """Start timing a metric."""
        self.start_times[metric_name] = datetime.now(timezone.utc)
    
    def end_timer(self, metric_name: str):
        """End timing a metric and record the duration."""
        if metric_name in self.start_times:
            duration = (datetime.now(timezone.utc) - self.start_times[metric_name]).total_seconds()
            self.record_metric(f"{metric_name}_duration_seconds", duration)
            del self.start_times[metric_name]
    
    def record_metric(self, metric_name: str, value: Any):
        """Record a metric value."""
        if metric_name not in self.metrics:
            self.metrics[metric_name] = []
        self.metrics[metric_name].append({
            "value": value,
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
    
    def increment_counter(self, counter_name: str):
        """Increment a counter metric."""
        current_value = self.get_metric_value(counter_name, 0)
        self.record_metric(counter_name, current_value + 1)
    
    def get_metric_value(self, metric_name: str, default: Any = None):
        """Get the latest value of a metric."""
        if metric_name in self.metrics and self.metrics[metric_name]:
            return self.metrics[metric_name][-1]["value"]
        return default
    
    def get_all_metrics(self) -> Dict[str, Any]:
        """Get all recorded metrics."""
        return self.metrics.copy()
    
    def clear_metrics(self):
        """Clear all recorded metrics."""
        self.metrics.clear()
        self.start_times.clear()


# Global metrics collector instance
metrics = MetricsCollector()