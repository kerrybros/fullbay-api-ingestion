# Set Environment Variables Script
# This script sets the required environment variables for the Lambda function

param(
    [string]$FunctionName = "fullbay-api-ingestion",
    [Parameter(Mandatory=$true)]
    [string]$FullbayApiUrl,
    [Parameter(Mandatory=$true)]
    [string]$FullbayApiKey,
    [Parameter(Mandatory=$true)]
    [string]$DbHost,
    [Parameter(Mandatory=$true)]
    [string]$DbName,
    [Parameter(Mandatory=$true)]
    [string]$DbUser,
    [Parameter(Mandatory=$true)]
    [string]$DbPassword,
    [string]$Environment = "production",
    [string]$LogLevel = "INFO",
    [string]$DbPort = "5432",
    [string]$DbSslMode = "require"
)

Write-Host "🔧 Setting environment variables for Lambda function..." -ForegroundColor Green
Write-Host "   Function: $FunctionName" -ForegroundColor Cyan

# Create environment variables JSON
$envVars = @{
    "FULLBAY_API_URL" = $FullbayApiUrl
    "FULLBAY_API_KEY" = $FullbayApiKey
    "DB_HOST" = $DbHost
    "DB_NAME" = $DbName
    "DB_USER" = $DbUser
    "DB_PASSWORD" = $DbPassword
    "ENVIRONMENT" = $Environment
    "LOG_LEVEL" = $LogLevel
    "DB_PORT" = $DbPort
    "DB_SSL_MODE" = $DbSslMode
}

$envJson = $envVars | ConvertTo-Json -Compress

try {
    aws lambda update-function-configuration `
        --function-name $FunctionName `
        --environment Variables=$envJson `
        --output json | Out-Null
    
    Write-Host "✅ Environment variables set successfully!" -ForegroundColor Green
    Write-Host "`n📋 Environment Variables Set:" -ForegroundColor Yellow
    $envVars.GetEnumerator() | ForEach-Object {
        if ($_.Key -like "*PASSWORD*" -or $_.Key -like "*KEY*") {
            Write-Host "   $($_.Key): [HIDDEN]" -ForegroundColor White
        } else {
            Write-Host "   $($_.Key): $($_.Value)" -ForegroundColor White
        }
    }
    
} catch {
    Write-Host "❌ Failed to set environment variables. Error: $_" -ForegroundColor Red
    exit 1
}

Write-Host "`n🎉 Environment variables configured successfully!" -ForegroundColor Green
