#!/usr/bin/env python3
"""
CloudWatch Monitor for Fullbay API Ingestion
Simple monitoring and logging integration.
"""

import os
import json
import time
from datetime import datetime
from typing import Optional, Dict, Any

try:
    import boto3
    CLOUDWATCH_AVAILABLE = True
except ImportError:
    CLOUDWATCH_AVAILABLE = False
    print("⚠️  boto3 not available - CloudWatch monitoring disabled")

class CloudWatchMonitor:
    """Simple CloudWatch monitoring for Fullbay API ingestion."""
    
    def __init__(self, region: str = 'us-east-1', enabled: bool = True):
        """Initialize CloudWatch monitor."""
        self.enabled = enabled and CLOUDWATCH_AVAILABLE
        self.region = region
        self.namespace = 'FullbayAPI/Ingestion'
        self.log_group = '/aws/fullbay-api-ingestion'
        
        if self.enabled:
            try:
                self.cloudwatch = boto3.client('cloudwatch', region_name=region)
                self.logs = boto3.client('logs', region_name=region)
                self._setup_log_group()
                print(f"✅ CloudWatch monitoring enabled for region: {region}")
            except Exception as e:
                print(f"⚠️  CloudWatch initialization failed: {e}")
                self.enabled = False
        else:
            print("⚠️  CloudWatch monitoring disabled")
    
    def _setup_log_group(self):
        """Create CloudWatch log group if it doesn't exist."""
        if not self.enabled:
            return
            
        try:
            self.logs.create_log_group(logGroupName=self.log_group)
        except self.logs.exceptions.ResourceAlreadyExistsException:
            pass
        except Exception as e:
            print(f"⚠️  Could not create log group: {e}")
    
    def log_metric(self, metric_name: str, value: float, unit: str = 'Count', 
                   dimensions: Optional[list] = None) -> bool:
        """Send a custom metric to CloudWatch."""
        if not self.enabled:
            return False
            
        try:
            metric_data = {
                'MetricName': metric_name,
                'Value': value,
                'Unit': unit,
                'Timestamp': datetime.utcnow()
            }
            
            if dimensions:
                metric_data['Dimensions'] = dimensions
            
            self.cloudwatch.put_metric_data(
                Namespace=self.namespace,
                MetricData=[metric_data]
            )
            return True
            
        except Exception as e:
            print(f"❌ Failed to send metric {metric_name}: {e}")
            return False
    
    def log_event(self, message: str, level: str = 'INFO', 
                  extra_data: Optional[Dict[str, Any]] = None) -> bool:
        """Send a log event to CloudWatch Logs."""
        if not self.enabled:
            return False
            
        try:
            log_event = {
                'timestamp': datetime.utcnow().isoformat(),
                'level': level,
                'message': message
            }
            
            if extra_data:
                log_event['data'] = extra_data
            
            # Create log stream if it doesn't exist
            log_stream_name = f"ingestion-{datetime.now().strftime('%Y-%m-%d')}"
            try:
                self.logs.create_log_stream(
                    logGroupName=self.log_group,
                    logStreamName=log_stream_name
                )
            except self.logs.exceptions.ResourceAlreadyExistsException:
                pass
            except Exception as e:
                print(f"⚠️  Could not create log stream: {e}")
                return False
            
            self.logs.put_log_events(
                logGroupName=self.log_group,
                logStreamName=log_stream_name,
                logEvents=[{
                    'timestamp': int(datetime.utcnow().timestamp() * 1000),
                    'message': json.dumps(log_event)
                }]
            )
            return True
            
        except Exception as e:
            print(f"❌ Failed to log event: {e}")
            return False
    
    def monitor_api_performance(self, fetch_time: float, invoice_count: int, 
                               success: bool = True) -> None:
        """Monitor API performance metrics."""
        if not self.enabled:
            return
            
        # API response time
        self.log_metric('APIResponseTime', fetch_time, 'Seconds', [
            {'Name': 'Operation', 'Value': 'FetchInvoices'},
            {'Name': 'Success', 'Value': str(success)}
        ])
        
        # Invoices processed
        self.log_metric('InvoicesProcessed', invoice_count, 'Count', [
            {'Name': 'Operation', 'Value': 'FetchInvoices'},
            {'Name': 'Success', 'Value': str(success)}
        ])
        
        # Invoices per second
        if fetch_time > 0:
            invoices_per_second = invoice_count / fetch_time
            self.log_metric('InvoicesPerSecond', invoices_per_second, 'Count/Second')
    
    def monitor_processing_performance(self, process_time: float, 
                                      line_items_created: int, 
                                      success: bool = True) -> None:
        """Monitor data processing performance metrics."""
        if not self.enabled:
            return
            
        # Processing time
        self.log_metric('ProcessingTime', process_time, 'Seconds', [
            {'Name': 'Operation', 'Value': 'ProcessInvoices'},
            {'Name': 'Success', 'Value': str(success)}
        ])
        
        # Line items created
        self.log_metric('LineItemsCreated', line_items_created, 'Count', [
            {'Name': 'Operation', 'Value': 'ProcessInvoices'},
            {'Name': 'Success', 'Value': str(success)}
        ])
        
        # Line items per second
        if process_time > 0:
            line_items_per_second = line_items_created / process_time
            self.log_metric('LineItemsPerSecond', line_items_per_second, 'Count/Second')
    
    def monitor_data_quality(self, total_items: int, missing_unit_info: int,
                            zero_prices: int, missing_labor_hours: int) -> None:
        """Monitor data quality metrics."""
        if not self.enabled or total_items <= 0:
            return
            
        # Data quality percentages
        missing_unit_pct = (missing_unit_info / total_items) * 100
        zero_prices_pct = (zero_prices / total_items) * 100
        missing_labor_pct = (missing_labor_hours / total_items) * 100
        
        self.log_metric('DataQuality_MissingUnitInfo', missing_unit_pct, 'Percent')
        self.log_metric('DataQuality_ZeroPrices', zero_prices_pct, 'Percent')  # Updated metric name
        self.log_metric('DataQuality_MissingLaborHours', missing_labor_pct, 'Percent')
    
    def monitor_errors(self, error_count: int, error_type: str) -> None:
        """Monitor error metrics."""
        if not self.enabled:
            return
            
        self.log_metric('Errors', error_count, 'Count', [
            {'Name': 'ErrorType', 'Value': error_type}
        ])
    
    def log_ingestion_start(self, target_date: str) -> None:
        """Log ingestion start event."""
        self.log_event(
            f"Starting ingestion for {target_date}",
            'INFO',
            {'target_date': target_date, 'event': 'ingestion_start'}
        )
    
    def log_ingestion_complete(self, target_date: str, invoice_count: int, 
                              line_items_created: int, total_time: float) -> None:
        """Log ingestion completion event."""
        self.log_event(
            f"Completed ingestion for {target_date}",
            'INFO',
            {
                'target_date': target_date,
                'event': 'ingestion_complete',
                'invoice_count': invoice_count,
                'line_items_created': line_items_created,
                'total_time': total_time
            }
        )
    
    def log_ingestion_error(self, target_date: str, error: str, 
                           error_details: Optional[Dict[str, Any]] = None) -> None:
        """Log ingestion error event."""
        self.log_event(
            f"Error during ingestion for {target_date}: {error}",
            'ERROR',
            {
                'target_date': target_date,
                'event': 'ingestion_error',
                'error': error,
                'error_details': error_details or {}
            }
        )

# Global monitor instance
_global_monitor = None

def get_monitor() -> CloudWatchMonitor:
    """Get the global CloudWatch monitor instance."""
    global _global_monitor
    if _global_monitor is None:
        _global_monitor = CloudWatchMonitor()
    return _global_monitor

def set_monitor(monitor: CloudWatchMonitor) -> None:
    """Set the global CloudWatch monitor instance."""
    global _global_monitor
    _global_monitor = monitor
