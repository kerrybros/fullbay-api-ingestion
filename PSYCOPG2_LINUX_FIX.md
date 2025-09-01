# psycopg2 Linux Compatibility Fix

## Problem
You're developing on Windows but AWS Lambda runs on Linux. The psycopg2 package installed locally is Windows-compatible and won't work in Lambda.

## Root Cause
- **Local Environment**: Windows x64 with Windows psycopg2 binaries
- **Lambda Environment**: Amazon Linux 2 with Linux psycopg2 binaries
- **Incompatibility**: Windows and Linux binaries are not interchangeable

## Solutions

### Option 1: Use Lambda Layers (Recommended)
Create a Lambda layer with Linux-compatible psycopg2:

```bash
# Run the layer creation script
python create_lambda_layer.py
```

This creates `psycopg2-layer.zip` that you can attach to your Lambda function.

### Option 2: Docker-based Build (Most Reliable)
Use Docker to build Linux-compatible packages:

```bash
# Run the Docker build script
python build_lambda_package_fixed.py
```

This requires Docker Desktop to be installed.

### Option 3: Manual Wheel Download
Download pre-built Linux wheels manually:

```bash
# Run the simple build script
python build_lambda_simple.py
```

## Error Scenarios and Fixes

### 1. Import Error in Lambda
**Error**: `ModuleNotFoundError: No module named 'psycopg2'`
**Fix**: Ensure psycopg2 is included in the deployment package

### 2. Binary Compatibility Error
**Error**: `ImportError: /lib64/libc.so.6: version 'GLIBC_2.17' not found`
**Fix**: Use Linux-compatible psycopg2 binaries

### 3. Connection Error
**Error**: `psycopg2.OperationalError: could not connect to server`
**Fix**: Check database connection settings and VPC configuration

### 4. Permission Error
**Error**: `psycopg2.OperationalError: permission denied for database`
**Fix**: Verify database user permissions and connection string

## Testing the Fix

### Local Testing
```bash
# Test database connection locally
python verify_database.py
```

### Lambda Testing
```bash
# Test Lambda function locally
python -c "from src.lambda_function import lambda_handler; print(lambda_handler({}, None))"
```

## Environment Variables Required

Make sure these are set in your Lambda function:

```bash
DB_HOST=your-rds-endpoint
DB_PORT=5432
DB_NAME=your_database_name
DB_USER=your_username
DB_PASSWORD=your_password
ENVIRONMENT=production
```

## Deployment Steps

1. **Build the package**:
   ```bash
   python build_lambda_package_fixed.py
   ```

2. **Upload to Lambda**:
   - Go to AWS Lambda console
   - Upload the generated zip file
   - Set environment variables
   - Configure handler as `lambda_function.lambda_handler`

3. **Test the function**:
   - Use the test event in Lambda console
   - Check CloudWatch logs for any errors

## Verification Checklist

- [ ] Lambda package contains Linux psycopg2
- [ ] Database connection works in Lambda
- [ ] Environment variables are set correctly
- [ ] VPC configuration allows database access
- [ ] Security groups allow Lambda to connect to RDS
- [ ] IAM roles have necessary permissions

## Troubleshooting

### If Lambda still fails:
1. Check CloudWatch logs for specific error messages
2. Verify database is accessible from Lambda's VPC
3. Ensure security groups allow traffic on port 5432
4. Test database connection manually from Lambda's VPC

### If build fails:
1. Try the Docker method if available
2. Use the Lambda layer approach
3. Download pre-built wheels manually
4. Check Python version compatibility (use 3.11)

## Alternative: Use Lambda Layers

If building packages is problematic, use Lambda layers:

1. Create the layer: `python create_lambda_layer.py`
2. Upload `psycopg2-layer.zip` as a Lambda layer
3. Attach the layer to your function
4. Deploy only your code (without psycopg2)

This approach separates dependencies from code and is often more reliable.
