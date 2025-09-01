#!/usr/bin/env python3
"""
Build Lambda deployment package WITHOUT psycopg2 (to be used with Lambda layer).
"""

import os
import shutil
import subprocess
import sys
from pathlib import Path

def build_lambda_package():
    """Build Lambda package without psycopg2 (uses layer instead)."""
    print("🔨 Building Lambda package (without psycopg2)...")
    
    build_dir = "lambda-build-no-psycopg2"
    if os.path.exists(build_dir):
        shutil.rmtree(build_dir)
    os.makedirs(build_dir)
    
    try:
        # Copy source files
        print("📁 Copying source files...")
        src_dir = "src"
        for item in os.listdir(src_dir):
            source_path = os.path.join(src_dir, item)
            dest_path = os.path.join(build_dir, item)
            
            if os.path.isfile(source_path):
                shutil.copy2(source_path, dest_path)
            elif os.path.isdir(source_path):
                shutil.copytree(source_path, dest_path)
        
        # Install only non-psycopg2 dependencies
        print("📦 Installing dependencies (excluding psycopg2)...")
        
        # Create temporary requirements without psycopg2
        temp_req = "temp_requirements_no_psycopg2.txt"
        with open(temp_req, 'w') as f:
            f.write("requests>=2.31.0\n")
            f.write("boto3>=1.28.0\n")
            f.write("botocore>=1.31.0\n")
        
        subprocess.run([
            sys.executable, "-m", "pip", "install",
            "-r", temp_req,
            "-t", build_dir,
            "--no-user"
        ], check=True)
        
        # Clean up temp file
        os.remove(temp_req)
        
        # Create package
        package_name = "lambda-deployment-no-psycopg2.zip"
        if os.path.exists(package_name):
            os.remove(package_name)
        
        subprocess.run([
            "powershell", "-Command",
            f"Compress-Archive -Path '{build_dir}\\*' -DestinationPath '{package_name}' -Force"
        ], check=True)
        
        # Clean up
        shutil.rmtree(build_dir)
        
        package_size = os.path.getsize(package_name) / (1024 * 1024)
        print(f"✅ Package created: {package_name} ({package_size:.1f} MB)")
        
        return package_name
        
    except Exception as e:
        print(f"❌ Build failed: {e}")
        if os.path.exists(build_dir):
            shutil.rmtree(build_dir)
        raise

if __name__ == "__main__":
    try:
        package_name = build_lambda_package()
        print(f"\n🎉 Lambda package built: {package_name}")
        print("\n📋 Deployment instructions:")
        print("1. Upload psycopg2-layer-linux.zip as a Lambda layer")
        print("2. Upload this package as your Lambda function code")
        print("3. Attach the psycopg2 layer to your function")
        print("4. Set handler to: lambda_function.lambda_handler")
        print("5. Configure environment variables for database")
        
    except Exception as e:
        print(f"❌ Failed: {e}")
        sys.exit(1)
