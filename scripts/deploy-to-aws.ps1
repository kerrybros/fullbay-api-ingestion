# AWS Lambda Deployment Script
# This script deploys the Fullbay API ingestion function to AWS Lambda

param(
    [string]$FunctionName = "fullbay-api-ingestion",
    [string]$Runtime = "python3.9",
    [string]$Handler = "lambda_function.lambda_handler",
    [int]$Timeout = 300,
    [int]$MemorySize = 512,
    [string]$RoleName = "api-ingestion-lambda-role-7ram0h3c",
    [string]$Region = "us-east-1",
    [string]$AccountId = "956394424497"
)

# Set error action preference
$ErrorActionPreference = "Stop"

Write-Host "🚀 Starting AWS Lambda deployment..." -ForegroundColor Green

# Check if AWS CLI is installed
try {
    $awsVersion = aws --version
    Write-Host "✅ AWS CLI found: $awsVersion" -ForegroundColor Green
} catch {
    Write-Host "❌ AWS CLI not found. Please install AWS CLI first." -ForegroundColor Red
    exit 1
}

# Check AWS credentials
try {
    $callerIdentity = aws sts get-caller-identity --output json | ConvertFrom-Json
    Write-Host "✅ AWS credentials verified" -ForegroundColor Green
    Write-Host "   Account: $($callerIdentity.Account)" -ForegroundColor Cyan
    Write-Host "   User: $($callerIdentity.Arn)" -ForegroundColor Cyan
} catch {
    Write-Host "❌ AWS credentials not configured. Please run 'aws configure' first." -ForegroundColor Red
    exit 1
}

# Check if deployment package exists
if (-not (Test-Path "lambda_function.zip")) {
    Write-Host "❌ lambda_function.zip not found. Please run the deployment package script first." -ForegroundColor Red
    exit 1
}

$packageSize = (Get-Item "lambda_function.zip").Length
$packageSizeMB = [math]::Round($packageSize / 1MB, 2)
Write-Host "📦 Deployment package size: $packageSizeMB MB" -ForegroundColor Cyan

# Construct role ARN
$roleArn = "arn:aws:iam::$AccountId`:role/$RoleName"

Write-Host "🔧 Creating Lambda function..." -ForegroundColor Yellow
Write-Host "   Function Name: $FunctionName" -ForegroundColor Cyan
Write-Host "   Runtime: $Runtime" -ForegroundColor Cyan
Write-Host "   Handler: $Handler" -ForegroundColor Cyan
Write-Host "   Role: $roleArn" -ForegroundColor Cyan

# Create the Lambda function
try {
    $createResult = aws lambda create-function `
        --function-name $FunctionName `
        --runtime $Runtime `
        --role $roleArn `
        --handler $Handler `
        --zip-file fileb://lambda_function.zip `
        --timeout $Timeout `
        --memory-size $MemorySize `
        --description "Fullbay API data ingestion function" `
        --output json | ConvertFrom-Json
    
    Write-Host "✅ Lambda function created successfully!" -ForegroundColor Green
    Write-Host "   Function ARN: $($createResult.FunctionArn)" -ForegroundColor Cyan
    Write-Host "   State: $($createResult.State)" -ForegroundColor Cyan
    
} catch {
    Write-Host "❌ Failed to create Lambda function. Error: $_" -ForegroundColor Red
    
    # Check if function already exists
    try {
        $existingFunction = aws lambda get-function --function-name $FunctionName --output json | ConvertFrom-Json
        Write-Host "ℹ️  Function already exists. Updating code..." -ForegroundColor Yellow
        
        # Update function code
        $updateResult = aws lambda update-function-code `
            --function-name $FunctionName `
            --zip-file fileb://lambda_function.zip `
            --output json | ConvertFrom-Json
        
        Write-Host "✅ Function code updated successfully!" -ForegroundColor Green
        
    } catch {
        Write-Host "❌ Failed to update existing function. Please check AWS permissions." -ForegroundColor Red
        exit 1
    }
}

# Set basic environment variables
Write-Host "🔧 Setting environment variables..." -ForegroundColor Yellow

try {
    aws lambda update-function-configuration `
        --function-name $FunctionName `
        --environment Variables='{
            "ENVIRONMENT":"production",
            "LOG_LEVEL":"INFO",
            "DB_PORT":"5432",
            "DB_SSL_MODE":"require"
        }' --output json | Out-Null
    
    Write-Host "✅ Basic environment variables set" -ForegroundColor Green
    
} catch {
    Write-Host "⚠️  Warning: Could not set environment variables. Error: $_" -ForegroundColor Yellow
}

# Test the function
Write-Host "🧪 Testing Lambda function..." -ForegroundColor Yellow

try {
    $testEvent = @{
        source = "aws.events"
        "detail-type" = "Scheduled Event"
        detail = @{}
    } | ConvertTo-Json
    
    $testEvent | Out-File -FilePath "test-event.json" -Encoding UTF8
    
    $invokeResult = aws lambda invoke `
        --function-name $FunctionName `
        --payload file://test-event.json `
        response.json `
        --output json | ConvertFrom-Json
    
    Write-Host "✅ Function invoked successfully!" -ForegroundColor Green
    Write-Host "   Status Code: $($invokeResult.StatusCode)" -ForegroundColor Cyan
    
    # Read and display response
    if (Test-Path "response.json") {
        $response = Get-Content "response.json" | ConvertFrom-Json
        Write-Host "📄 Response:" -ForegroundColor Cyan
        Write-Host ($response | ConvertTo-Json -Depth 3) -ForegroundColor White
    }
    
} catch {
    Write-Host "⚠️  Warning: Could not test function. Error: $_" -ForegroundColor Yellow
}

# Clean up test files
if (Test-Path "test-event.json") { Remove-Item "test-event.json" }
if (Test-Path "response.json") { Remove-Item "response.json" }

Write-Host "`n🎉 Deployment completed!" -ForegroundColor Green
Write-Host "`n📋 Next Steps:" -ForegroundColor Yellow
Write-Host "1. Set required environment variables (API keys, database credentials)" -ForegroundColor White
Write-Host "2. Configure EventBridge trigger for scheduling" -ForegroundColor White
Write-Host "3. Set up CloudWatch monitoring" -ForegroundColor White
Write-Host "4. Test with real data" -ForegroundColor White

Write-Host "`n🔗 Useful Commands:" -ForegroundColor Yellow
Write-Host "• View function: aws lambda get-function --function-name $FunctionName" -ForegroundColor White
Write-Host "• View logs: aws logs describe-log-groups --log-group-name-prefix '/aws/lambda/$FunctionName'" -ForegroundColor White
Write-Host "• Update code: aws lambda update-function-code --function-name $FunctionName --zip-file fileb://lambda_function.zip" -ForegroundColor White
