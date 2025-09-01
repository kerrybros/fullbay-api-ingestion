#!/usr/bin/env python3
"""
Script to build Lambda deployment package with psycopg2-binary.
"""

import os
import shutil
import subprocess
import sys
import zipfile

def build_lambda_package():
    """Build Lambda deployment package with psycopg2-binary."""
    
    print("🔨 Building Lambda deployment package with psycopg2...")
    
    # Create temporary build directory
    build_dir = "lambda-build-psycopg2"
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
                if item not in ["psycopg2", "psycopg2_binary"]:  # Skip existing psycopg2
                    shutil.copytree(source_path, dest_path)
        
        # Install psycopg2-binary
        print("📦 Installing psycopg2-binary...")
        try:
            subprocess.run([
                sys.executable, "-m", "pip", "install",
                "psycopg2-binary==2.9.9",
                "-t", build_dir,
                "--no-deps"
            ], check=True, capture_output=True)
            print("✅ psycopg2-binary installed successfully")
        except subprocess.CalledProcessError as e:
            print(f"⚠️ psycopg2-binary installation failed: {e}")
            print("Trying alternative approach...")
            
            # Try installing without --no-deps
            subprocess.run([
                sys.executable, "-m", "pip", "install",
                "psycopg2-binary==2.9.9",
                "-t", build_dir
            ], check=True, capture_output=True)
            print("✅ psycopg2-binary installed with dependencies")
        
        # Create deployment package
        print("📦 Creating deployment package...")
        package_name = "lambda-deployment-with-psycopg2.zip"
        
        if os.path.exists(package_name):
            os.remove(package_name)
        
        # Create zip file
        with zipfile.ZipFile(package_name, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, dirs, files in os.walk(build_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, build_dir)
                    zipf.write(file_path, arcname)
        
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
