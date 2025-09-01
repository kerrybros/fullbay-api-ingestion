#!/bin/bash

# Lambda Deployment Script
# This script prepares a deployment package for AWS Lambda

set -e  # Exit on any error

echo "🚀 Preparing Lambda deployment package..."

# Create temporary directory for deployment
DEPLOY_DIR="lambda-deploy"
rm -rf $DEPLOY_DIR
mkdir -p $DEPLOY_DIR

echo "📦 Installing Lambda dependencies..."
pip install -r requirements-lambda.txt -t $DEPLOY_DIR

echo "📁 Copying source code..."
cp -r src/* $DEPLOY_DIR/

echo "📋 Copying configuration files..."
cp -r config $DEPLOY_DIR/

echo "🗜️ Creating deployment package..."
cd $DEPLOY_DIR
zip -r ../lambda_function.zip . -x "*.pyc" "__pycache__/*" "*.git*" "*.DS_Store" "*.pytest_cache/*"
cd ..

echo "🧹 Cleaning up temporary files..."
rm -rf $DEPLOY_DIR

echo "✅ Lambda deployment package created: lambda_function.zip"
echo "📊 Package size: $(du -h lambda_function.zip | cut -f1)"
