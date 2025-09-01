# CloudWatch Setup Guide for Fullbay API Ingestion

## 🔧 **Step-by-Step Setup Instructions**

### **1. IAM Permissions Setup**

#### **Option A: Add to Existing IAM User/Role**
1. Go to AWS IAM Console
2. Find your user/role (currently: `cursor-user`)
3. Add the following policy:

```json
{
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
```

#### **Option B: Create New IAM Role (Recommended for Production)**
1. Create a new IAM role with the above policy
2. Attach it to your Lambda function or EC2 instance
3. Use role-based authentication instead of user credentials

### **2. Test CloudWatch Setup**

Run the setup script to test permissions:

```bash
python cloudwatch-setup.py
```

### **3. Integration with Existing Code**

#### **Add CloudWatch to Database Manager**

Update `src/database.py` to include CloudWatch monitoring:

```python
# Add to imports
from cloudwatch_monitor import CloudWatchMonitor

class DatabaseManager:
    def __init__(self, config):
        # ... existing code ...
        self.cloudwatch = CloudWatchMonitor()
    
    def insert_records(self, invoices):
        start_time = time.time()
        try:
            # ... existing processing code ...
            
            # Monitor performance
            process_time = time.time() - start_time
            self.cloudwatch.monitor_processing_performance(
                process_time, line_items_created, True
            )
            
            # Monitor data quality
            self.cloudwatch.monitor_data_quality(
                total_items, missing_unit_info, zero_negative_prices, missing_labor_hours
            )
            
            return line_items_created
            
        except Exception as e:
            # Monitor errors
            self.cloudwatch.monitor_errors(1, type(e).__name__)
            raise
```

#### **Add CloudWatch to Fullbay Client**

Update `src/fullbay_client.py`:

```python
from cloudwatch_monitor import CloudWatchMonitor

class FullbayClient:
    def __init__(self, config):
        # ... existing code ...
        self.cloudwatch = CloudWatchMonitor()
    
    def fetch_invoices_for_date(self, target_date):
        start_time = time.time()
        try:
            # ... existing API call code ...
            
            # Monitor API performance
            fetch_time = time.time() - start_time
            self.cloudwatch.monitor_api_performance(
                fetch_time, len(invoices), True
            )
            
            return invoices
            
        except Exception as e:
            # Monitor errors
            self.cloudwatch.monitor_errors(1, 'API_Error')
            raise
```

### **4. CloudWatch Dashboard**

The setup script will create a dashboard with:

- **Processing Volume**: Invoices and line items processed
- **Performance Metrics**: API response time and processing time
- **Data Quality Metrics**: Missing data percentages
- **Error Count**: Error tracking by type

### **5. CloudWatch Alarms**

#### **Create Performance Alarms**

```python
def create_alarms(self):
    """Create CloudWatch alarms for monitoring."""
    
    # API Response Time Alarm
    self.cloudwatch.put_metric_alarm(
        AlarmName='FullbayAPI-HighResponseTime',
        MetricName='APIResponseTime',
        Namespace=self.namespace,
        Statistic='Average',
        Period=300,
        EvaluationPeriods=2,
        Threshold=60,  # 60 seconds
        ComparisonOperator='GreaterThanThreshold',
        AlarmDescription='API response time is too high'
    )
    
    # Error Rate Alarm
    self.cloudwatch.put_metric_alarm(
        AlarmName='FullbayAPI-HighErrorRate',
        MetricName='Errors',
        Namespace=self.namespace,
        Statistic='Sum',
        Period=300,
        EvaluationPeriods=1,
        Threshold=5,  # 5 errors
        ComparisonOperator='GreaterThanThreshold',
        AlarmDescription='Too many errors occurring'
    )
```

### **6. Logging Configuration**

#### **Structured Logging**

```python
def log_structured_event(self, event_type, data):
    """Log structured events to CloudWatch."""
    log_data = {
        'event_type': event_type,
        'timestamp': datetime.utcnow().isoformat(),
        'data': data
    }
    
    self.cloudwatch.log_event(
        f"Fullbay API {event_type}",
        'INFO',
        log_data
    )
```

### **7. Production Deployment**

#### **Environment Variables**

Add to your environment:

```bash
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
```

#### **Lambda Function Integration**

For Lambda deployment, add to `template.yaml`:

```yaml
Resources:
  FullbayIngestionFunction:
    Type: AWS::Serverless::Function
    Properties:
      # ... existing properties ...
      Environment:
        Variables:
          AWS_REGION: us-east-1
      Policies:
        - CloudWatchPutMetricPolicy:
            MetricNamespace: FullbayAPI/Ingestion
        - CloudWatchLogsFullAccess
```

### **8. Monitoring Best Practices**

#### **Key Metrics to Monitor**

1. **API Performance**
   - Response time (target: < 30 seconds)
   - Success rate (target: > 95%)
   - Invoices per second

2. **Processing Performance**
   - Processing time (target: < 60 seconds)
   - Line items per second
   - Database connection health

3. **Data Quality**
   - Missing unit info (target: < 15%)
   - Zero/negative prices (target: < 10%)
   - Missing labor hours (target: < 10%)

4. **Error Tracking**
   - API errors
   - Database errors
   - Processing errors

#### **Alerting Strategy**

- **Critical**: API failures, database errors
- **Warning**: High response times, data quality issues
- **Info**: Processing volume, performance trends

### **9. Troubleshooting**

#### **Common Issues**

1. **Access Denied Errors**
   - Verify IAM permissions
   - Check AWS credentials
   - Ensure region is correct

2. **Metric Not Appearing**
   - Check namespace spelling
   - Verify metric name format
   - Wait 5-10 minutes for metrics to appear

3. **Log Group Issues**
   - Ensure log group exists
   - Check log stream naming
   - Verify timestamp format

#### **Testing Commands**

```bash
# Test basic functionality
python cloudwatch-setup.py

# Test with real data
python test_june25_real_api.py

# Check CloudWatch console
# Go to: https://console.aws.amazon.com/cloudwatch/
```

### **10. Cost Optimization**

#### **CloudWatch Pricing**

- **Custom Metrics**: $0.30 per metric per month
- **Logs**: $0.50 per GB ingested
- **Dashboards**: $3.00 per dashboard per month

#### **Optimization Tips**

1. **Batch Metrics**: Send multiple metrics in one call
2. **Log Retention**: Set appropriate log retention periods
3. **Metric Resolution**: Use standard resolution (1 minute) instead of high resolution
4. **Dashboard Cleanup**: Remove unused dashboards

### **11. Security Considerations**

1. **IAM Least Privilege**: Only grant necessary permissions
2. **Encryption**: Enable CloudWatch Logs encryption
3. **Access Control**: Use IAM roles instead of access keys
4. **Audit Logging**: Monitor CloudWatch API calls

---

## 🎯 **Next Steps**

1. **Set up IAM permissions** using the provided policy
2. **Test the setup** with `python cloudwatch-setup.py`
3. **Integrate monitoring** into your existing code
4. **Create alarms** for critical metrics
5. **Set up dashboards** for operational visibility
6. **Configure alerting** (email, SNS, etc.)

This setup will give you comprehensive monitoring and alerting for your Fullbay API ingestion system!
