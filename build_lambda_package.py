#!/usr/bin/env python3
"""
Script to build a proper Lambda deployment package with Linux-compatible dependencies.
"""

import os
import shutil
import subprocess
import sys
from pathlib import Path

def build_lambda_package():
    """Build Lambda deployment package with proper dependencies."""
    
    print("🔨 Building Lambda deployment package...")
    
    # Create temporary build directory
    build_dir = "lambda-build"
    if os.path.exists(build_dir):
        shutil.rmtree(build_dir)
    os.makedirs(build_dir)
    
    try:
        # Copy Lambda function files
        print("📁 Copying Lambda function files...")
        lambda_source_dir = "lambda-deploy-fixed"
        
        for item in os.listdir(lambda_source_dir):
            source_path = os.path.join(lambda_source_dir, item)
            dest_path = os.path.join(build_dir, item)
            
            if os.path.isfile(source_path):
                shutil.copy2(source_path, dest_path)
            elif os.path.isdir(source_path):
                if item != "psycopg2":  # Skip the Windows psycopg2
                    shutil.copytree(source_path, dest_path)
        
        # Install dependencies in the build directory
        print("📦 Installing dependencies...")
        requirements_file = os.path.join(lambda_source_dir, "requirements.txt")
        
        if os.path.exists(requirements_file):
            subprocess.run([
                sys.executable, "-m", "pip", "install",
                "-r", requirements_file,
                "-t", build_dir,
                "--platform", "manylinux2014_x86_64",
                "--only-binary=all"
            ], check=True)
        else:
            print("⚠️ No requirements.txt found, installing basic dependencies...")
            subprocess.run([
                sys.executable, "-m", "pip", "install",
                "psycopg2-binary==2.9.9",
                "requests==2.31.0",
                "urllib3==2.0.7",
                "-t", build_dir,
                "--platform", "manylinux2014_x86_64",
                "--only-binary=all"
            ], check=True)
        
        # Create deployment package
        print("📦 Creating deployment package...")
        package_name = "lambda-deployment-fixed.zip"
        
        if os.path.exists(package_name):
            os.remove(package_name)
        
        # Use PowerShell to create zip (since we're on Windows)
        subprocess.run([
            "powershell", "-Command",
            f"Compress-Archive -Path '{build_dir}\\*' -DestinationPath '{package_name}' -Force"
        ], check=True)
        
        # Get package size
        package_size = os.path.getsize(package_name) / (1024 * 1024)  # MB
        print(f"✅ Deployment package created: {package_name} ({package_size:.1f} MB)")
        
        # Clean up build directory
        shutil.rmtree(build_dir)
        print("🧹 Build directory cleaned up")
        
        return package_name
        
    except Exception as e:
        print(f"❌ Error building package: {e}")
        # Clean up on error
        if os.path.exists(build_dir):
            shutil.rmtree(build_dir)
        raise

if __name__ == "__main__":
    try:
        package_name = build_lambda_package()
        print(f"🎉 Lambda package built successfully: {package_name}")
    except Exception as e:
        print(f"❌ Failed to build Lambda package: {e}")
        sys.exit(1)
