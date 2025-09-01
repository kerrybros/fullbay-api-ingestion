# Lambda Deployment Guide

This guide explains how to prepare and deploy the Fullbay API ingestion function to AWS Lambda.

## Prerequisites

- Python 3.9+ installed
- AWS CLI configured with appropriate permissions
- Access to AWS Lambda, IAM, and RDS services

## Deployment Package Preparation

### Option 1: Using the Deployment Script (Recommended)

#### Windows (PowerShell)
```powershell
# Navigate to project root
cd fullbay-api-ingestion

# Run the deployment script
.\scripts\deploy-lambda.ps1
```

#### Linux/macOS (Bash)
```bash
# Navigate to project root
cd fullbay-api-ingestion

# Make script executable
chmod +x scripts/deploy-lambda.sh

# Run the deployment script
./scripts/deploy-lambda.sh
```

### Option 2: Manual Steps

1. **Install dependencies locally:**
   ```bash
   pip install -r requirements-lambda.txt -t .
   ```

2. **Create deployment package:**
   ```bash
   zip -r lambda_function.zip . -x "*.git*" "*.DS_Store" "tests/*" "docs/*" "scripts/*" "*.pyc" "__pycache__/*"
   ```

## Package Contents

The deployment package includes:
- ✅ Core Python dependencies (`requests`, `psycopg2-binary`, `boto3`)
- ✅ Source code (`src/` directory)
- ✅ Configuration files (`config/` directory)
- ❌ Development dependencies (excluded)
- ❌ Test files (excluded)
- ❌ Documentation (excluded)

## Package Size Considerations

- **Target size:** < 50MB (Lambda direct upload limit)
- **If > 50MB:** Consider using Lambda Layers for dependencies
- **Current estimated size:** ~15-25MB

## Environment Variables

Ensure these environment variables are configured in your Lambda function:

### Required
- `FULLBAY_API_URL` - Fullbay API endpoint
- `FULLBAY_API_KEY` - API authentication key
- `DB_HOST` - RDS instance endpoint
- `DB_NAME` - Database name
- `DB_USER` - Database username
- `DB_PASSWORD` - Database password
- `ENVIRONMENT` - `production` or `development`

### Optional
- `LOG_LEVEL` - Logging level (default: INFO)
- `DB_PORT` - Database port (default: 5432)
- `DB_SSL_MODE` - SSL mode (default: require)

## Lambda Configuration

### Runtime Settings
- **Runtime:** Python 3.9 or 3.10
- **Handler:** `lambda_function.lambda_handler`
- **Timeout:** 5 minutes (300 seconds)
- **Memory:** 512 MB (adjust based on data volume)

### IAM Permissions
The Lambda execution role needs:
- `AWSLambdaBasicExecutionRole`
- RDS access permissions
- VPC access (if RDS is in private subnet)

### VPC Configuration (if needed)
- **VPC:** Same VPC as RDS instance
- **Subnets:** Private subnets with NAT Gateway
- **Security Groups:** Access to RDS security group

## Deployment

### Via AWS Console
1. Create new Lambda function
2. Upload `lambda_function.zip`
3. Configure environment variables
4. Set up EventBridge trigger for scheduling

### Via AWS CLI
```bash
aws lambda create-function \
  --function-name fullbay-api-ingestion \
  --runtime python3.9 \
  --role arn:aws:iam::ACCOUNT:role/lambda-execution-role \
  --handler lambda_function.lambda_handler \
  --zip-file fileb://lambda_function.zip \
  --timeout 300 \
  --memory-size 512
```

### Via SAM/CloudFormation
Use the provided `template.yaml` for infrastructure as code deployment.

## Testing

### Local Testing
```bash
python -m src.lambda_function
```

### Lambda Testing
1. Use AWS Console test feature
2. Create test event with EventBridge format
3. Monitor CloudWatch logs for execution details

## Monitoring

### CloudWatch Metrics
- Invocation count
- Duration
- Error rate
- Throttles

### CloudWatch Logs
- Execution logs with detailed information
- Error tracking and debugging
- Performance monitoring

## Troubleshooting

### Common Issues
1. **Package too large:** Use Lambda Layers
2. **Database connection timeout:** Check VPC configuration
3. **Permission denied:** Verify IAM roles
4. **Import errors:** Check dependency installation

### Debug Steps
1. Check CloudWatch logs
2. Verify environment variables
3. Test database connectivity
4. Validate API credentials

## Security Considerations

- Store sensitive data in AWS Secrets Manager
- Use IAM roles with minimal required permissions
- Enable VPC for database access
- Use SSL/TLS for all connections
- Rotate API keys regularly
