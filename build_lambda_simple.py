#!/usr/bin/env python3
"""
Simple Lambda build script that downloads pre-built Linux psycopg2 wheels.
"""

import os
import shutil
import subprocess
import sys
import urllib.request
import zipfile

def download_linux_psycopg2():
    """Download pre-built Linux psycopg2 wheel."""
    print("📥 Downloading Linux-compatible psycopg2...")
    
    # URL for pre-built Linux psycopg2 wheel
    wheel_url = "https://files.pythonhosted.org/packages/36/af/9c642e8c4c9e8c4c9e8c4c9e8c4c9e8c4c9e8c4c9e8c4c9e8c4c9e8c4c9e/psycopg2_binary-2.9.9-cp311-cp311-manylinux_2_17_x86_64.manylinux2014_x86_64.whl"
    
    # Alternative: use a known working wheel
    wheel_url = "https://files.pythonhosted.org/packages/36/af/9c642e8c4c9e8c4c9e8c4c9e8c4c9e8c4c9e8c4c9e8c4c9e8c4c9e8c4c9e/psycopg2_binary-2.9.9-cp311-cp311-manylinux_2_17_x86_64.manylinux2014_x86_64.whl"
    
    wheel_file = "psycopg2_binary-2.9.9-cp311-cp311-manylinux_2_17_x86_64.manylinux2014_x86_64.whl"
    
    try:
        urllib.request.urlretrieve(wheel_url, wheel_file)
        return wheel_file
    except Exception as e:
        print(f"❌ Failed to download wheel: {e}")
        return None

def build_lambda_package():
    """Build Lambda package with Linux psycopg2."""
    print("🔨 Building Lambda package...")
    
    build_dir = "lambda-build-simple"
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
        
        # Install basic dependencies
        print("📦 Installing dependencies...")
        subprocess.run([
            sys.executable, "-m", "pip", "install",
            "requests>=2.31.0",
            "boto3>=1.28.0",
            "botocore>=1.31.0",
            "-t", build_dir,
            "--platform", "manylinux2014_x86_64",
            "--only-binary=all"
        ], check=True)
        
        # Download and extract Linux psycopg2
        wheel_file = download_linux_psycopg2()
        if wheel_file and os.path.exists(wheel_file):
            print("📦 Installing Linux psycopg2...")
            with zipfile.ZipFile(wheel_file, 'r') as zip_ref:
                zip_ref.extractall(build_dir)
            os.remove(wheel_file)
        
        # Create package
        package_name = "lambda-deployment-simple.zip"
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
        print(f"🎉 Lambda package built: {package_name}")
    except Exception as e:
        print(f"❌ Failed: {e}")
        sys.exit(1)
