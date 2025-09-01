# Fullbay API Data Ingestion Lambda

A Python-based AWS Lambda function that pulls data from the Fullbay API on a configurable schedule and persists it to an AWS RDS PostgreSQL database.

## 🏗️ Architecture

```
EventBridge (Schedule) → Lambda Function → Fullbay API → RDS PostgreSQL
                             ↓
                       AWS Secrets Manager
                             ↓  
                       CloudWatch Logs
```

## 📋 Features

- **Scheduled Data Ingestion**: Configurable schedule (default: daily)
- **Secure Credential Management**: Uses AWS Secrets Manager for API keys and database passwords
- **Error Handling & Logging**: Comprehensive error handling with CloudWatch integration
- **Database Connection Pooling**: Efficient connection management for RDS
- **Monitoring & Alerting**: CloudWatch alarms for errors and performance
- **Extensible Design**: Structured for future enhancements (deduplication, etc.)

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
- AWS CLI configured with appropriate permissions
- AWS SAM CLI installed
- PostgreSQL RDS instance (or local PostgreSQL for development)
- Fullbay API access credentials

### 1. Clone and Setup

```bash
git clone <your-repo>
cd fullbay-api-ingestion

# Set up local development environment
chmod +x scripts/setup-local.sh
./scripts/setup-local.sh
```

### 2. Configure Environment

```bash
# Copy environment template
cp .env.example .env

# Edit with your values
nano .env
```

Required environment variables:
```bash
# Database
DB_HOST=your-rds-endpoint.region.rds.amazonaws.com
DB_USER=fullbay_user
DB_PASSWORD=your_secure_password  # For local dev only

# API
FULLBAY_API_KEY=your_fullbay_api_key  # For local dev only

# AWS (for production)
SECRETS_MANAGER_SECRET_NAME=fullbay-api-credentials
AWS_REGION=us-east-1
```

### 3. Test Locally

```bash
# Activate virtual environment
source venv/bin/activate

# Run unit tests
pytest tests/unit/

# Test the function locally
python scripts/test-local.py
```

## 🏭 Production Deployment

### 1. Set up AWS Resources

#### Create RDS Database
```bash
# Create parameter group
aws rds create-db-parameter-group \
    --db-parameter-group-name fullbay-postgres \
    --db-parameter-group-family postgres15 \
    --description "Parameters for Fullbay ingestion database"

# Create subnet group (replace with your subnet IDs)
aws rds create-db-subnet-group \
    --db-subnet-group-name fullbay-subnet-group \
    --db-subnet-group-description "Subnet group for Fullbay database" \
    --subnet-ids subnet-12345678 subnet-87654321

# Create RDS instance
aws rds create-db-instance \
    --db-instance-identifier fullbay-data-prod \
    --db-instance-class db.t3.micro \
    --engine postgres \
    --engine-version 15.4 \
    --master-username fullbay_admin \
    --master-user-password "YourSecurePassword123!" \
    --allocated-storage 20 \
    --db-subnet-group-name fullbay-subnet-group \
    --vpc-security-group-ids sg-12345678 \
    --db-parameter-group-name fullbay-postgres \
    --backup-retention-period 7 \
    --storage-encrypted
```

#### Create Secrets Manager Secret
```bash
aws secretsmanager create-secret \
    --name fullbay-api-credentials \
    --description "Fullbay API and database credentials" \
    --secret-string '{
        "fullbay_api_key": "your_actual_api_key_here",
        "db_password": "YourSecurePassword123!"
    }'
```

### 2. Configure VPC and Security Groups

Create security group for Lambda:
```bash
aws ec2 create-security-group \
    --group-name fullbay-lambda-sg \
    --description "Security group for Fullbay Lambda function" \
    --vpc-id vpc-12345678

# Allow outbound HTTPS (for API calls)
aws ec2 authorize-security-group-egress \
    --group-id sg-lambda123 \
    --protocol tcp \
    --port 443 \
    --cidr 0.0.0.0/0

# Allow outbound PostgreSQL (for RDS)
aws ec2 authorize-security-group-egress \
    --group-id sg-lambda123 \
    --protocol tcp \
    --port 5432 \
    --source-group sg-rds123
```

### 3. Deploy with SAM

```bash
# Deploy to development
./scripts/deploy.sh development

# Deploy to production (with guided mode)
./scripts/deploy.sh production
```

During guided deployment, you'll need to provide:
- VPC Subnet IDs (where Lambda will run)
- Security Group IDs
- RDS endpoint
- Secrets Manager secret name

### 4. Verify Deployment

```bash
# Check function status
aws lambda get-function --function-name fullbay-api-ingestion-production

# Test manually
aws lambda invoke \
    --function-name fullbay-api-ingestion-production \
    --payload '{}' \
    response.json

# Check logs
aws logs tail /aws/lambda/fullbay-api-ingestion-production --follow
```

## 🔧 Configuration

### Environment Variables

| Variable | Description | Required | Default |
|----------|-------------|----------|---------|
| `ENVIRONMENT` | Deployment environment | No | `development` |
| `AWS_REGION` | AWS region | No | `us-east-1` |
| `DB_HOST` | RDS endpoint | Yes | - |
| `DB_PORT` | Database port | No | `5432` |
| `DB_NAME` | Database name | No | `fullbay_data` |
| `DB_USER` | Database username | Yes | - |
| `FULLBAY_API_BASE_URL` | API base URL | No | `https://api.fullbay.com` |
| `FULLBAY_API_VERSION` | API version | No | `v1` |
| `SECRETS_MANAGER_SECRET_NAME` | Secret name | Production | - |
| `SCHEDULE_EXPRESSION` | EventBridge schedule | No | `rate(1 day)` |
| `LOG_LEVEL` | Logging level | No | `INFO` |

### Schedule Expressions

Common schedule patterns:
- Daily at midnight UTC: `rate(1 day)` or `cron(0 0 * * ? *)`
- Every 6 hours: `rate(6 hours)`
- Weekdays at 9 AM UTC: `cron(0 9 ? * MON-FRI *)`
- Every 15 minutes: `rate(15 minutes)`

### Database Schema

The function creates these tables automatically:

**fullbay_work_orders**
```sql
- id (SERIAL PRIMARY KEY)
- fullbay_id (VARCHAR, UNIQUE)
- work_order_number (VARCHAR)
- customer_id (VARCHAR)
- customer_name (TEXT)
- vehicle_info (JSONB)
- status (VARCHAR)
- created_at (TIMESTAMP WITH TIME ZONE)
- updated_at (TIMESTAMP WITH TIME ZONE)
- total_amount (DECIMAL)
- labor_amount (DECIMAL)
- parts_amount (DECIMAL)
- tax_amount (DECIMAL)
- raw_data (JSONB)
- ingestion_timestamp (TIMESTAMP WITH TIME ZONE)
- ingestion_source (VARCHAR)
```

**ingestion_metadata**
```sql
- id (SERIAL PRIMARY KEY)
- execution_id (VARCHAR, UNIQUE)
- start_time (TIMESTAMP WITH TIME ZONE)
- end_time (TIMESTAMP WITH TIME ZONE)
- status (VARCHAR)
- records_processed (INTEGER)
- records_inserted (INTEGER)
- error_message (TEXT)
- api_endpoint (VARCHAR)
```

## 🧪 Testing

### Unit Tests
```bash
# Run all unit tests
pytest tests/unit/

# Run with coverage
pytest tests/unit/ --cov=src --cov-report=html

# Run specific test file
pytest tests/unit/test_config.py -v
```

### Integration Tests
```bash
# Set up test environment
export RUN_INTEGRATION_TESTS=true

# Run integration tests
pytest tests/integration/ -v
```

### Local Testing
```bash
# Test with real API/DB connections
python scripts/test-local.py
```

## 📊 Monitoring

### CloudWatch Metrics

The function automatically creates these alarms:
- **Error Alarm**: Triggers on any function errors
- **Duration Alarm**: Triggers if function runs longer than 4 minutes

### Custom Metrics

You can add custom metrics in your code:
```python
from src.utils import metrics

# Record custom metrics
metrics.record_metric("api_response_time", response_time)
metrics.increment_counter("records_processed")
```

### Logs

Check CloudWatch logs:
```bash
# View recent logs
aws logs tail /aws/lambda/fullbay-api-ingestion-production

# Filter for errors
aws logs filter-log-events \
    --log-group-name /aws/lambda/fullbay-api-ingestion-production \
    --filter-pattern "ERROR"
```

## 🔒 Security

### IAM Permissions

The function requires these permissions (automatically granted by SAM template):

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "secretsmanager:GetSecretValue"
            ],
            "Resource": "arn:aws:secretsmanager:region:account:secret:fullbay-api-credentials*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": "arn:aws:logs:*:*:*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "ec2:CreateNetworkInterface",
                "ec2:DescribeNetworkInterfaces",
                "ec2:DeleteNetworkInterface"
            ],
            "Resource": "*"
        }
    ]
}
```

### Security Best Practices

- ✅ API keys stored in Secrets Manager (never in code)
- ✅ Database passwords in Secrets Manager
- ✅ VPC configuration for database access
- ✅ Encrypted RDS storage
- ✅ Least privilege IAM permissions
- ✅ CloudWatch logging for audit trail

## 🚨 Troubleshooting

### Common Issues

**1. Database Connection Timeout**
```
Error: Failed to connect to database: timeout
```
- Check VPC configuration and security groups
- Verify RDS is in the same VPC as Lambda subnets
- Ensure security groups allow PostgreSQL traffic (port 5432)

**2. API Authentication Failed**
```
Error: Failed to fetch data from Fullbay API: 401 Unauthorized
```
- Verify API key in Secrets Manager is correct
- Check Fullbay API key hasn't expired
- Confirm API endpoint and version are correct

**3. Lambda Timeout**
```
Error: Task timed out after 300.00 seconds
```
- Increase Lambda timeout in template.yaml (max 15 minutes)
- Check for large data volumes or slow API responses
- Consider implementing pagination limits

**4. Out of Memory**
```
Error: Runtime.OutOfMemory
```
- Increase Lambda memory in template.yaml
- Check for memory leaks in data processing
- Consider processing data in smaller batches

### Debug Mode

Enable debug logging:
```bash
# Set environment variable
aws lambda update-function-configuration \
    --function-name fullbay-api-ingestion-production \
    --environment Variables='{LOG_LEVEL=DEBUG}'
```

## 🛠️ Development

### Project Structure
```
fullbay-api-ingestion/
├── src/                          # Source code
│   ├── lambda_function.py        # Main Lambda handler
│   ├── config.py                 # Configuration management
│   ├── fullbay_client.py         # API client
│   ├── database.py               # Database operations
│   └── utils.py                  # Utilities and helpers
├── tests/                        # Test suite
│   ├── unit/                     # Unit tests
│   ├── integration/              # Integration tests
│   └── conftest.py               # Test configuration
├── scripts/                      # Deployment and utility scripts
│   ├── deploy.sh                 # Deployment script
│   ├── setup-local.sh            # Local setup
│   └── test-local.py             # Local testing
├── config/                       # Environment configurations
│   └── environments/             # Environment-specific configs
├── template.yaml                 # SAM template
├── requirements.txt              # Python dependencies
└── README.md                     # This file
```

### Adding New Features

1. **New API Endpoints**: Extend `FullbayClient.fetch_data()`
2. **Data Transformations**: Add methods to `DatabaseManager._process_record()`
3. **Deduplication**: Implement in `DatabaseManager.insert_records()`
4. **New Schedules**: Update EventBridge rules in `template.yaml`

### Code Style

This project uses:
- **Black** for code formatting
- **Flake8** for linting
- **isort** for import sorting
- **mypy** for type checking

```bash
# Format code
black src/ tests/

# Check linting
flake8 src/ tests/

# Sort imports
isort src/ tests/

# Type checking
mypy src/
```

## 📚 API Documentation

### Fullbay API Integration

The function currently integrates with these Fullbay API endpoints:
- `GET /v1/work-orders` - Retrieve work orders

To add new endpoints, extend the `FullbayClient` class:

```python
def fetch_customers(self):
    return self.fetch_data("customers")

def fetch_parts(self):
    return self.fetch_data("parts")
```

## 🔄 Future Enhancements

The codebase is structured to easily add these features:

- **Deduplication**: Check existing records before insertion
- **Incremental Loading**: Track last successful run timestamp
- **Data Validation**: Enhanced validation rules for incoming data
- **Retry Logic**: Exponential backoff for failed API calls
- **Multi-Environment**: Different configurations per environment
- **Metrics Dashboard**: Custom CloudWatch dashboard
- **Dead Letter Queue**: Handle failed messages

## 📞 Support

For issues and questions:
1. Check the troubleshooting section above
2. Review CloudWatch logs for error details
3. Verify AWS resource configurations
4. Test API connectivity with the local test script

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.