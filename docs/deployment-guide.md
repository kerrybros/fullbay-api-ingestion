# Deployment Guide

Comprehensive step-by-step guide for deploying the Fullbay API Ingestion Lambda function to AWS.

## 📋 Prerequisites Checklist

Before deploying, ensure you have:

- [ ] AWS CLI installed and configured
- [ ] AWS SAM CLI installed
- [ ] Python 3.11+ installed
- [ ] Valid Fullbay API credentials
- [ ] AWS account with appropriate permissions
- [ ] VPC with private subnets (for RDS access)

## 🏗️ Infrastructure Setup

### 1. VPC and Networking

If you don't have an existing VPC, create one:

```bash
# Create VPC
aws ec2 create-vpc --cidr-block 10.0.0.0/16 --tag-specifications 'ResourceType=vpc,Tags=[{Key=Name,Value=fullbay-vpc}]'

# Create private subnets (replace vpc-id with your VPC ID)
aws ec2 create-subnet --vpc-id vpc-12345678 --cidr-block 10.0.1.0/24 --availability-zone us-east-1a
aws ec2 create-subnet --vpc-id vpc-12345678 --cidr-block 10.0.2.0/24 --availability-zone us-east-1b

# Create internet gateway and NAT gateway for outbound access
aws ec2 create-internet-gateway
aws ec2 attach-internet-gateway --internet-gateway-id igw-12345678 --vpc-id vpc-12345678

# Create NAT gateway (in public subnet)
aws ec2 create-nat-gateway --subnet-id subnet-public123 --allocation-id eipalloc-12345678
```

### 2. Security Groups

Create security groups for Lambda and RDS:

```bash
# Lambda security group
aws ec2 create-security-group \
    --group-name fullbay-lambda-sg \
    --description "Security group for Fullbay Lambda function" \
    --vpc-id vpc-12345678

# RDS security group
aws ec2 create-security-group \
    --group-name fullbay-rds-sg \
    --description "Security group for Fullbay RDS instance" \
    --vpc-id vpc-12345678

# Allow Lambda to access RDS
aws ec2 authorize-security-group-ingress \
    --group-id sg-rds123 \
    --protocol tcp \
    --port 5432 \
    --source-group sg-lambda123

# Allow Lambda outbound HTTPS
aws ec2 authorize-security-group-egress \
    --group-id sg-lambda123 \
    --protocol tcp \
    --port 443 \
    --cidr 0.0.0.0/0
```

### 3. RDS PostgreSQL Instance

Create the database instance:

```bash
# Create DB subnet group
aws rds create-db-subnet-group \
    --db-subnet-group-name fullbay-subnet-group \
    --db-subnet-group-description "Subnet group for Fullbay database" \
    --subnet-ids subnet-12345678 subnet-87654321

# Create parameter group
aws rds create-db-parameter-group \
    --db-parameter-group-name fullbay-postgres15 \
    --db-parameter-group-family postgres15 \
    --description "Parameters for Fullbay PostgreSQL database"

# Create RDS instance
aws rds create-db-instance \
    --db-instance-identifier fullbay-data-prod \
    --db-instance-class db.t3.micro \
    --engine postgres \
    --engine-version 15.4 \
    --master-username fullbay_admin \
    --master-user-password "$(openssl rand -base64 32)" \
    --allocated-storage 20 \
    --storage-type gp2 \
    --db-subnet-group-name fullbay-subnet-group \
    --vpc-security-group-ids sg-rds123 \
    --db-parameter-group-name fullbay-postgres15 \
    --backup-retention-period 7 \
    --storage-encrypted \
    --monitoring-interval 60 \
    --enable-performance-insights \
    --deletion-protection
```

Wait for the RDS instance to be available:
```bash
aws rds wait db-instance-available --db-instance-identifier fullbay-data-prod
```

### 4. Secrets Manager

Store sensitive credentials in AWS Secrets Manager:

```bash
# Create the secret
aws secretsmanager create-secret \
    --name fullbay-api-credentials \
    --description "Credentials for Fullbay API and database access" \
    --secret-string '{
        "fullbay_api_key": "YOUR_ACTUAL_FULLBAY_API_KEY",
        "db_password": "YOUR_ACTUAL_DB_PASSWORD"
    }'

# Verify the secret was created
aws secretsmanager describe-secret --secret-id fullbay-api-credentials
```

## 🚀 Application Deployment

### 1. Environment Configuration

Create environment-specific parameter files:

**config/parameters-development.json**
```json
[
    {
        "ParameterKey": "Environment",
        "ParameterValue": "development"
    },
    {
        "ParameterKey": "ScheduleExpression", 
        "ParameterValue": "rate(5 minutes)"
    },
    {
        "ParameterKey": "DBHost",
        "ParameterValue": "fullbay-data-dev.cluster-xyz.us-east-1.rds.amazonaws.com"
    },
    {
        "ParameterKey": "DBName",
        "ParameterValue": "fullbay_data_dev"
    },
    {
        "ParameterKey": "DBUser", 
        "ParameterValue": "fullbay_user"
    },
    {
        "ParameterKey": "SecretsManagerSecretName",
        "ParameterValue": "fullbay-api-credentials-dev"
    },
    {
        "ParameterKey": "VpcSubnetIds",
        "ParameterValue": "subnet-12345678,subnet-87654321"
    },
    {
        "ParameterKey": "VpcSecurityGroupIds",
        "ParameterValue": "sg-lambda123"
    }
]
```

**config/parameters-production.json**
```json
[
    {
        "ParameterKey": "Environment",
        "ParameterValue": "production"
    },
    {
        "ParameterKey": "ScheduleExpression",
        "ParameterValue": "rate(1 day)"
    },
    {
        "ParameterKey": "DBHost",
        "ParameterValue": "fullbay-data-prod.cluster-abc.us-east-1.rds.amazonaws.com"
    },
    {
        "ParameterKey": "DBName",
        "ParameterValue": "fullbay_data"
    },
    {
        "ParameterKey": "DBUser",
        "ParameterValue": "fullbay_admin"
    },
    {
        "ParameterKey": "SecretsManagerSecretName",
        "ParameterValue": "fullbay-api-credentials"
    },
    {
        "ParameterKey": "VpcSubnetIds",
        "ParameterValue": "subnet-12345678,subnet-87654321"
    },
    {
        "ParameterKey": "VpcSecurityGroupIds",
        "ParameterValue": "sg-lambda123"
    }
]
```

### 2. Build and Deploy

Deploy to development first:

```bash
# Make deployment script executable
chmod +x scripts/deploy.sh

# Deploy to development
./scripts/deploy.sh development

# Monitor the deployment
aws cloudformation describe-stack-events \
    --stack-name fullbay-api-ingestion-development \
    --query 'StackEvents[?ResourceStatus!=`CREATE_COMPLETE`]'
```

Deploy to production:

```bash
# Deploy to production (with confirmation prompts)
./scripts/deploy.sh production
```

### 3. Verify Deployment

Check that all resources were created successfully:

```bash
# Check CloudFormation stack
aws cloudformation describe-stacks \
    --stack-name fullbay-api-ingestion-production \
    --query 'Stacks[0].StackStatus'

# Check Lambda function
aws lambda get-function \
    --function-name fullbay-api-ingestion-production

# Check EventBridge rule
aws events describe-rule \
    --name fullbay-api-ingestion-production-ScheduledTrigger

# Check CloudWatch log group
aws logs describe-log-groups \
    --log-group-name-prefix "/aws/lambda/fullbay-api-ingestion"
```

## 🧪 Testing the Deployment

### 1. Manual Invocation

Test the Lambda function manually:

```bash
# Invoke the function
aws lambda invoke \
    --function-name fullbay-api-ingestion-production \
    --invocation-type RequestResponse \
    --payload '{"test": true}' \
    response.json

# Check the response
cat response.json | jq .

# Check logs
aws logs tail /aws/lambda/fullbay-api-ingestion-production --follow
```

### 2. Database Verification

Connect to your RDS instance and verify data:

```bash
# Connect using psql (replace with your RDS endpoint)
psql -h fullbay-data-prod.cluster-abc.us-east-1.rds.amazonaws.com \
     -U fullbay_admin \
     -d fullbay_data

# Check tables were created
\dt

# Check for data
SELECT COUNT(*) FROM fullbay_work_orders;
SELECT * FROM ingestion_metadata ORDER BY created_at DESC LIMIT 5;
```

### 3. Monitoring Setup

Verify monitoring is working:

```bash
# Check CloudWatch alarms
aws cloudwatch describe-alarms \
    --alarm-names fullbay-ingestion-errors-production fullbay-ingestion-duration-production

# Check metrics
aws cloudwatch get-metric-statistics \
    --namespace AWS/Lambda \
    --metric-name Duration \
    --dimensions Name=FunctionName,Value=fullbay-api-ingestion-production \
    --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S) \
    --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
    --period 300 \
    --statistics Average
```

## 🔄 Updating the Application

### 1. Code Updates

For code changes:

```bash
# Update the code in src/
# Run tests
pytest tests/

# Deploy the update
./scripts/deploy.sh production
```

### 2. Configuration Updates

Update Lambda environment variables:

```bash
aws lambda update-function-configuration \
    --function-name fullbay-api-ingestion-production \
    --environment Variables='{
        "ENVIRONMENT":"production",
        "DB_HOST":"new-endpoint.amazonaws.com",
        "LOG_LEVEL":"DEBUG"
    }'
```

### 3. Schedule Updates

Modify the EventBridge rule:

```bash
aws events put-rule \
    --name fullbay-api-ingestion-schedule \
    --schedule-expression "rate(6 hours)" \
    --description "Updated schedule for Fullbay ingestion"
```

## 🚨 Rollback Strategy

### 1. CloudFormation Rollback

If deployment fails, CloudFormation will automatically rollback:

```bash
# Check rollback status
aws cloudformation describe-stack-events \
    --stack-name fullbay-api-ingestion-production

# Manual rollback if needed
aws cloudformation cancel-update-stack \
    --stack-name fullbay-api-ingestion-production
```

### 2. Function-Level Rollback

Rollback to a previous version:

```bash
# List function versions
aws lambda list-versions-by-function \
    --function-name fullbay-api-ingestion-production

# Update alias to previous version
aws lambda update-alias \
    --function-name fullbay-api-ingestion-production \
    --name LIVE \
    --function-version 2
```

## 📊 Post-Deployment Checklist

After successful deployment, verify:

- [ ] Lambda function is created and configured correctly
- [ ] EventBridge rule is scheduled and enabled
- [ ] RDS connectivity from Lambda works
- [ ] Secrets Manager integration functions
- [ ] CloudWatch logs are being generated
- [ ] CloudWatch alarms are configured
- [ ] Manual invocation succeeds
- [ ] Database tables are created
- [ ] Data is being ingested correctly
- [ ] Error handling works as expected

## 🔧 Troubleshooting Deployment Issues

### Common Deployment Problems

**1. VPC Configuration Errors**
```
Error: The function could not be created because the specified subnet 
does not exist or is not available
```
Solution: Verify subnet IDs and ensure they're in the correct VPC

**2. Security Group Issues**
```
Error: Lambda function execution failed due to timeout
```
Solution: Check security group rules allow outbound HTTPS and PostgreSQL

**3. IAM Permission Errors**
```
Error: User is not authorized to perform lambda:CreateFunction
```
Solution: Review and add required IAM permissions

**4. Secrets Manager Access Issues**
```
Error: AccessDenied when calling GetSecretValue
```
Solution: Verify secret name and IAM permissions

### Deployment Logs

Monitor deployment progress:

```bash
# Watch CloudFormation events
aws cloudformation describe-stack-events \
    --stack-name fullbay-api-ingestion-production \
    --query 'StackEvents[*].[Timestamp,ResourceStatus,ResourceType,LogicalResourceId]' \
    --output table

# SAM deployment logs
sam deploy --debug
```

## 🗑️ Cleanup

To remove all resources:

```bash
# Delete CloudFormation stack
aws cloudformation delete-stack \
    --stack-name fullbay-api-ingestion-production

# Delete RDS instance (remove deletion protection first)
aws rds modify-db-instance \
    --db-instance-identifier fullbay-data-prod \
    --no-deletion-protection

aws rds delete-db-instance \
    --db-instance-identifier fullbay-data-prod \
    --skip-final-snapshot

# Delete secrets
aws secretsmanager delete-secret \
    --secret-id fullbay-api-credentials \
    --force-delete-without-recovery

# Clean up security groups and VPC resources
aws ec2 delete-security-group --group-id sg-lambda123
aws ec2 delete-security-group --group-id sg-rds123
```

This completes the comprehensive deployment guide for the Fullbay API Ingestion Lambda function.