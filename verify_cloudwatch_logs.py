#!/usr/bin/env python3
"""
Verify CloudWatch Logs and Metrics
Check that our monitoring is actually writing to CloudWatch.
"""

import boto3
import time
from datetime import datetime, timedelta

def verify_cloudwatch_logs():
    """Verify that logs are being written to CloudWatch."""
    print("🔍 VERIFYING CLOUDWATCH LOGS")
    print("=" * 50)
    
    try:
        logs_client = boto3.client('logs', region_name='us-east-1')
        cloudwatch_client = boto3.client('cloudwatch', region_name='us-east-1')
        
        log_group = '/aws/fullbay-api-ingestion'
        namespace = 'FullbayAPI/Ingestion'
        
        # Check if log group exists
        try:
            response = logs_client.describe_log_groups(logGroupNamePrefix=log_group)
            if response['logGroups']:
                print(f"✅ Log group found: {log_group}")
                
                # Get recent log streams
                today = datetime.now().strftime('%Y-%m-%d')
                log_stream_name = f"ingestion-{today}"
                
                try:
                    stream_response = logs_client.describe_log_streams(
                        logGroupName=log_group,
                        logStreamNamePrefix=log_stream_name
                    )
                    
                    if stream_response['logStreams']:
                        print(f"✅ Log stream found: {log_stream_name}")
                        
                        # Get recent log events
                        for stream in stream_response['logStreams']:
                            events_response = logs_client.get_log_events(
                                logGroupName=log_group,
                                logStreamName=stream['logStreamName'],
                                startTime=int((datetime.now() - timedelta(hours=1)).timestamp() * 1000),
                                endTime=int(datetime.now().timestamp() * 1000)
                            )
                            
                            if events_response['events']:
                                print(f"📝 Found {len(events_response['events'])} recent log events")
                                print("📄 Recent log messages:")
                                for event in events_response['events'][-3:]:  # Show last 3 events
                                    print(f"   {event['message']}")
                            else:
                                print("⚠️  No recent log events found")
                    else:
                        print(f"⚠️  No log streams found for today")
                        
                except Exception as e:
                    print(f"❌ Error checking log streams: {e}")
            else:
                print(f"❌ Log group not found: {log_group}")
                
        except Exception as e:
            print(f"❌ Error checking log group: {e}")
            
    except Exception as e:
        print(f"❌ Error connecting to CloudWatch: {e}")

def verify_cloudwatch_metrics():
    """Verify that metrics are being written to CloudWatch."""
    print("\n📊 VERIFYING CLOUDWATCH METRICS")
    print("=" * 50)
    
    try:
        cloudwatch_client = boto3.client('cloudwatch', region_name='us-east-1')
        namespace = 'FullbayAPI/Ingestion'
        
        # List metrics in our namespace
        try:
            response = cloudwatch_client.list_metrics(Namespace=namespace)
            
            if response['Metrics']:
                print(f"✅ Found {len(response['Metrics'])} metrics in namespace: {namespace}")
                print("📈 Available metrics:")
                
                # Group metrics by name
                metric_names = set()
                for metric in response['Metrics']:
                    metric_names.add(metric['MetricName'])
                
                for name in sorted(metric_names):
                    print(f"   • {name}")
                    
                # Get recent metric data for a few key metrics
                print("\n📊 Recent metric data (last hour):")
                end_time = datetime.utcnow()
                start_time = end_time - timedelta(hours=1)
                
                key_metrics = ['APIResponseTime', 'InvoicesProcessed', 'LineItemsCreated', 'Errors']
                
                for metric_name in key_metrics:
                    try:
                        metric_response = cloudwatch_client.get_metric_statistics(
                            Namespace=namespace,
                            MetricName=metric_name,
                            StartTime=start_time,
                            EndTime=end_time,
                            Period=300,  # 5-minute periods
                            Statistics=['Sum', 'Average', 'Maximum']
                        )
                        
                        if metric_response['Datapoints']:
                            latest = max(metric_response['Datapoints'], key=lambda x: x['Timestamp'])
                            print(f"   • {metric_name}: {latest}")
                        else:
                            print(f"   • {metric_name}: No recent data")
                            
                    except Exception as e:
                        print(f"   • {metric_name}: Error retrieving data - {e}")
                        
            else:
                print(f"⚠️  No metrics found in namespace: {namespace}")
                
        except Exception as e:
            print(f"❌ Error listing metrics: {e}")
            
    except Exception as e:
        print(f"❌ Error connecting to CloudWatch: {e}")

if __name__ == "__main__":
    print("🔍 CLOUDWATCH VERIFICATION")
    print("=" * 80)
    print(f"🕐 Verification time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    verify_cloudwatch_logs()
    verify_cloudwatch_metrics()
    
    print(f"\n✅ Verification completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
