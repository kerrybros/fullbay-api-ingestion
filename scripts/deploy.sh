#!/bin/bash

# Deployment script for Fullbay API Ingestion Lambda
# Usage: ./scripts/deploy.sh [environment] [aws_profile]

set -e

# Configuration
ENVIRONMENT=${1:-development}
AWS_PROFILE=${2:-default}
STACK_NAME="fullbay-api-ingestion-${ENVIRONMENT}"
REGION=${AWS_REGION:-us-east-1}

echo "🚀 Deploying Fullbay API Ingestion Lambda"
echo "Environment: ${ENVIRONMENT}"
echo "AWS Profile: ${AWS_PROFILE}"
echo "Region: ${REGION}"
echo "Stack Name: ${STACK_NAME}"

# Validate environment
if [[ ! "$ENVIRONMENT" =~ ^(development|staging|production)$ ]]; then
    echo "❌ Error: Environment must be development, staging, or production"
    exit 1
fi

# Check if AWS CLI is configured
if ! aws sts get-caller-identity --profile "$AWS_PROFILE" &>/dev/null; then
    echo "❌ Error: AWS CLI not configured or invalid profile: $AWS_PROFILE"
    exit 1
fi

# Check if SAM CLI is installed
if ! command -v sam &>/dev/null; then
    echo "❌ Error: AWS SAM CLI is not installed"
    echo "Install it from: https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html"
    exit 1
fi

# Build the application
echo "📦 Building SAM application..."
sam build --profile "$AWS_PROFILE"

# Deploy based on environment
if [ "$ENVIRONMENT" = "production" ]; then
    echo "🔐 Production deployment - guided mode with confirmation"
    sam deploy \
        --stack-name "$STACK_NAME" \
        --region "$REGION" \
        --profile "$AWS_PROFILE" \
        --parameter-overrides Environment="$ENVIRONMENT" \
        --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM \
        --confirm-changeset \
        --guided
else
    echo "🔧 Development/Staging deployment - using parameter file"
    
    # Create parameter file if it doesn't exist
    PARAM_FILE="config/parameters-${ENVIRONMENT}.json"
    if [ ! -f "$PARAM_FILE" ]; then
        echo "📝 Creating parameter file: $PARAM_FILE"
        cat > "$PARAM_FILE" << EOF
[
  {
    "ParameterKey": "Environment",
    "ParameterValue": "${ENVIRONMENT}"
  },
  {
    "ParameterKey": "ScheduleExpression",
    "ParameterValue": "rate(1 day)"
  }
]
EOF
        echo "⚠️  Please edit $PARAM_FILE with your specific values before deploying"
        exit 1
    fi
    
    sam deploy \
        --stack-name "$STACK_NAME" \
        --region "$REGION" \
        --profile "$AWS_PROFILE" \
        --parameter-overrides file://"$PARAM_FILE" \
        --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM \
        --no-confirm-changeset
fi

# Get deployment outputs
echo "📋 Deployment Outputs:"
aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --region "$REGION" \
    --profile "$AWS_PROFILE" \
    --query 'Stacks[0].Outputs[*].[OutputKey,OutputValue]' \
    --output table

echo "✅ Deployment completed successfully!"
echo ""
echo "Next steps:"
echo "1. Update your Secrets Manager secret with API keys and database credentials"
echo "2. Test the function manually: aws lambda invoke --function-name fullbay-api-ingestion-${ENVIRONMENT} test-output.json"
echo "3. Check CloudWatch logs for any issues"