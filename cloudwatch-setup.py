#!/usr/bin/env python3
"""
CloudWatch Setup for Fullbay API Ingestion
Sets up monitoring, logging, and alerting for the system.
"""

import os
import sys
import json
import boto3
from datetime import datetime, timedelta

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

# Set default environment variables for local testing
os.environ.setdefault('AWS_REGION', 'us-east-1')
os.environ.setdefault('DB_HOST', 'fullbay-postgres-db.cc74yq2i4j8n.us-east-1.rds.amazonaws.com')
os.environ.setdefault('DB_USER', 'postgres')
os.environ.setdefault('DB_PASSWORD', '5255Tillman')
os.environ.setdefault('DB_NAME', 'fullbay_data')
os.environ.setdefault('DB_PORT', '5432')
os.environ.setdefault('FULLBAY_API_KEY', '4b9fcc18-1f24-09fb-275b-ad1974786395')

from config import Config
from fullbay_client import FullbayClient
from database import DatabaseManager

class CloudWatchMonitor:
    """CloudWatch monitoring and logging for Fullbay API ingestion."""
    
    def __init__(self, region='us-east-1'):
        """Initialize CloudWatch client."""
        self.region = region
        self.cloudwatch = boto3.client('cloudwatch', region_name=region)
        self.logs = boto3.client('logs', region_name=region)
        
        # Define metric namespaces
        self.namespace = 'FullbayAPI/Ingestion'
        self.log_group = '/aws/fullbay-api-ingestion'
        
        # Initialize log group
        self._setup_log_group()
    
    def _setup_log_group(self):
        """Create CloudWatch log group if it doesn't exist."""
        try:
            self.logs.create_log_group(logGroupName=self.log_group)
            print(f"✅ Created CloudWatch log group: {self.log_group}")
        except self.logs.exceptions.ResourceAlreadyExistsException:
            print(f"✅ CloudWatch log group already exists: {self.log_group}")
        except Exception as e:
            print(f"⚠️  Could not create log group: {e}")
    
    def log_metric(self, metric_name, value, unit='Count', dimensions=None):
        """Send a custom metric to CloudWatch."""
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
            print(f"📊 Sent metric: {metric_name} = {value} {unit}")
            
        except Exception as e:
            print(f"❌ Failed to send metric {metric_name}: {e}")
    
    def log_event(self, message, level='INFO', extra_data=None):
        """Send a log event to CloudWatch Logs."""
        try:
            log_event = {
                'timestamp': datetime.utcnow().isoformat(),
                'level': level,
                'message': message
            }
            
            if extra_data:
                log_event['data'] = extra_data
            
            self.logs.put_log_events(
                logGroupName=self.log_group,
                logStreamName=f"ingestion-{datetime.now().strftime('%Y-%m-%d')}",
                logEvents=[{
                    'timestamp': int(datetime.utcnow().timestamp() * 1000),
                    'message': json.dumps(log_event)
                }]
            )
            print(f"📝 Logged {level}: {message}")
            
        except Exception as e:
            print(f"❌ Failed to log event: {e}")
    
    def monitor_api_performance(self, fetch_time, invoice_count, success=True):
        """Monitor API performance metrics."""
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
    
    def monitor_processing_performance(self, process_time, line_items_created, success=True):
        """Monitor data processing performance metrics."""
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
    
    def monitor_data_quality(self, total_items, missing_unit_info, zero_negative_prices, missing_labor_hours):
        """Monitor data quality metrics."""
        # Data quality percentages
        if total_items > 0:
            missing_unit_pct = (missing_unit_info / total_items) * 100
            zero_negative_pct = (zero_negative_prices / total_items) * 100
            missing_labor_pct = (missing_labor_hours / total_items) * 100
            
            self.log_metric('DataQuality_MissingUnitInfo', missing_unit_pct, 'Percent')
            self.log_metric('DataQuality_ZeroNegativePrices', zero_negative_pct, 'Percent')
            self.log_metric('DataQuality_MissingLaborHours', missing_labor_pct, 'Percent')
    
    def monitor_errors(self, error_count, error_type):
        """Monitor error metrics."""
        self.log_metric('Errors', error_count, 'Count', [
            {'Name': 'ErrorType', 'Value': error_type}
        ])
    
    def create_dashboard(self):
        """Create a CloudWatch dashboard for monitoring."""
        dashboard_body = {
            "widgets": [
                {
                    "type": "metric",
                    "x": 0,
                    "y": 0,
                    "width": 12,
                    "height": 6,
                    "properties": {
                        "metrics": [
                            [self.namespace, "InvoicesProcessed"],
                            [self.namespace, "LineItemsCreated"]
                        ],
                        "view": "timeSeries",
                        "stacked": False,
                        "region": self.region,
                        "title": "Processing Volume",
                        "period": 300
                    }
                },
                {
                    "type": "metric",
                    "x": 12,
                    "y": 0,
                    "width": 12,
                    "height": 6,
                    "properties": {
                        "metrics": [
                            [self.namespace, "APIResponseTime"],
                            [self.namespace, "ProcessingTime"]
                        ],
                        "view": "timeSeries",
                        "stacked": False,
                        "region": self.region,
                        "title": "Performance Metrics",
                        "period": 300
                    }
                },
                {
                    "type": "metric",
                    "x": 0,
                    "y": 6,
                    "width": 12,
                    "height": 6,
                    "properties": {
                        "metrics": [
                            [self.namespace, "DataQuality_MissingUnitInfo"],
                            [self.namespace, "DataQuality_ZeroNegativePrices"],
                            [self.namespace, "DataQuality_MissingLaborHours"]
                        ],
                        "view": "timeSeries",
                        "stacked": False,
                        "region": self.region,
                        "title": "Data Quality Metrics",
                        "period": 300
                    }
                },
                {
                    "type": "metric",
                    "x": 12,
                    "y": 6,
                    "width": 12,
                    "height": 6,
                    "properties": {
                        "metrics": [
                            [self.namespace, "Errors"]
                        ],
                        "view": "timeSeries",
                        "stacked": False,
                        "region": self.region,
                        "title": "Error Count",
                        "period": 300
                    }
                }
            ]
        }
        
        try:
            self.cloudwatch.put_dashboard(
                DashboardName='FullbayAPI-Ingestion',
                DashboardBody=json.dumps(dashboard_body)
            )
            print(f"✅ Created CloudWatch dashboard: FullbayAPI-Ingestion")
        except Exception as e:
            print(f"❌ Failed to create dashboard: {e}")

def test_cloudwatch_setup():
    """Test CloudWatch setup and functionality."""
    print("🔧 TESTING CLOUDWATCH SETUP")
    print("=" * 50)
    
    try:
        # Initialize CloudWatch monitor
        monitor = CloudWatchMonitor()
        
        # Test basic functionality
        print("\n📊 Testing metric logging...")
        monitor.log_metric('TestMetric', 42, 'Count')
        
        print("\n📝 Testing event logging...")
        monitor.log_event('Test event from Fullbay API ingestion', 'INFO', {'test': True})
        
        print("\n⚡ Testing performance monitoring...")
        monitor.monitor_api_performance(5.2, 25, True)
        monitor.monitor_processing_performance(12.8, 150, True)
        
        print("\n📈 Testing data quality monitoring...")
        monitor.monitor_data_quality(1000, 89, 128, 45)
        
        print("\n❌ Testing error monitoring...")
        monitor.monitor_errors(3, 'API_Timeout')
        
        print("\n📊 Creating dashboard...")
        monitor.create_dashboard()
        
        print("\n✅ CloudWatch setup test completed successfully!")
        
    except Exception as e:
        print(f"❌ CloudWatch setup test failed: {e}")
        import traceback
        traceback.print_exc()

def create_iam_policy():
    """Create IAM policy for CloudWatch access."""
    policy_document = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "cloudwatch:PutMetricData",
                    "cloudwatch:GetMetricData",
                    "cloudwatch:GetMetricStatistics",
                    "cloudwatch:ListMetrics",
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                    "logs:DescribeLogGroups",
                    "logs:DescribeLogStreams",
                    "logs:GetLogEvents"
                ],
                "Resource": "*"
            },
            {
                "Effect": "Allow",
                "Action": [
                    "cloudwatch:PutDashboard",
                    "cloudwatch:GetDashboard",
                    "cloudwatch:ListDashboards",
                    "cloudwatch:DeleteDashboards"
                ],
                "Resource": "*"
            }
        ]
    }
    
    print("🔐 IAM POLICY FOR CLOUDWATCH ACCESS")
    print("=" * 50)
    print("Copy this policy to your IAM user/role:")
    print(json.dumps(policy_document, indent=2))
    
    return policy_document

if __name__ == "__main__":
    print("🚀 CLOUDWATCH SETUP FOR FULLBAY API INGESTION")
    print("=" * 60)
    
    # Create IAM policy
    create_iam_policy()
    
    # Test CloudWatch setup
    test_cloudwatch_setup()
