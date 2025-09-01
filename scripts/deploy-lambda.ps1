# Lambda Deployment Script for Windows
# This script prepares a deployment package for AWS Lambda

param(
    [switch]$Clean
)

# Set error action preference
$ErrorActionPreference = "Stop"

Write-Host "🚀 Preparing Lambda deployment package..." -ForegroundColor Green

# Create temporary directory for deployment
$DEPLOY_DIR = "lambda-deploy"
if (Test-Path $DEPLOY_DIR) {
    Remove-Item -Recurse -Force $DEPLOY_DIR
}
New-Item -ItemType Directory -Path $DEPLOY_DIR | Out-Null

Write-Host "📦 Installing Lambda dependencies..." -ForegroundColor Yellow
pip install -r requirements-lambda.txt -t $DEPLOY_DIR

Write-Host "📁 Copying source code..." -ForegroundColor Yellow
Copy-Item -Path "src\*" -Destination $DEPLOY_DIR -Recurse -Force

Write-Host "📋 Copying configuration files..." -ForegroundColor Yellow
Copy-Item -Path "config" -Destination $DEPLOY_DIR -Recurse -Force

Write-Host "🗜️ Creating deployment package..." -ForegroundColor Yellow
Set-Location $DEPLOY_DIR
Compress-Archive -Path "*" -DestinationPath "..\lambda_function.zip" -Force
Set-Location ..

Write-Host "🧹 Cleaning up temporary files..." -ForegroundColor Yellow
Remove-Item -Recurse -Force $DEPLOY_DIR

$packageSize = (Get-Item "lambda_function.zip").Length
$packageSizeMB = [math]::Round($packageSize / 1MB, 2)

Write-Host "✅ Lambda deployment package created: lambda_function.zip" -ForegroundColor Green
Write-Host "📊 Package size: $packageSizeMB MB" -ForegroundColor Cyan

if ($packageSizeMB -gt 50) {
    Write-Host "⚠️  Warning: Package size exceeds 50MB. Consider using Lambda Layers for dependencies." -ForegroundColor Yellow
}
