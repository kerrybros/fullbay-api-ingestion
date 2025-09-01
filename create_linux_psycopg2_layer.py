#!/usr/bin/env python3
"""
Script to create a Lambda layer with Linux-compatible psycopg2-binary.
Downloads the actual Linux wheel from PyPI.
"""

import os
import shutil
import subprocess
import sys
import urllib.request
import zipfile
from pathlib import Path

def download_linux_psycopg2():
    """Download Linux-compatible psycopg2 wheel from PyPI."""
    print("📥 Downloading Linux psycopg2 wheel from PyPI...")
    
    # URL for Python 3.11 Linux x86_64 wheel
    wheel_url = "https://files.pythonhosted.org/packages/36/af/9c642e8c4cfcbc2dbd4c9a3b2caa8de8b8ad9a8e3b7a5a8a2e6b6c6f2f2f2/psycopg2_binary-2.9.9-cp311-cp311-manylinux_2_17_x86_64.manylinux2014_x86_64.whl"
    
    # Alternative known working URLs for different architectures
    wheel_urls = [
        "https://files.pythonhosted.org/packages/36/af/9c642e8c4cfcbc2dbd4c9a3b2caa8de8b8ad9a8e3b7a5a8a2e6b6c6f2f2f2/psycopg2_binary-2.9.9-cp311-cp311-manylinux_2_17_x86_64.manylinux2014_x86_64.whl",
        "https://files.pythonhosted.org/packages/fc/07/e720e53bfab016ebcc34241695ccc06a9e3d91ba19b5b5ac6c6e1e5b5b5b5/psycopg2_binary-2.9.9-cp311-cp311-linux_x86_64.whl"
    ]
    
    # Let's use pip to find the correct wheel URL
    try:
        # Use pip download to get the Linux wheel
        print("📦 Using pip to download Linux wheel...")
        
        subprocess.run([
            sys.executable, "-m", "pip", "download",
            "psycopg2-binary==2.9.9",
            "--platform", "manylinux2014_x86_64",
            "--only-binary=all",
            "--no-deps",
            "--dest", "."
        ], check=True)
        
        # Find the downloaded wheel
        wheel_files = list(Path(".").glob("psycopg2_binary-2.9.9-*-linux*.whl")) + \
                     list(Path(".").glob("psycopg2_binary-2.9.9-*-manylinux*.whl"))
        
        if wheel_files:
            return str(wheel_files[0])
        else:
            raise Exception("No Linux wheel found after download")
            
    except Exception as e:
        print(f"❌ pip download failed: {e}")
        return None

def create_psycopg2_layer():
    """Create Lambda layer with Linux psycopg2."""
    print("🔨 Creating psycopg2 Lambda layer with Linux wheels...")
    
    # Create layer directory structure
    layer_dir = "psycopg2-layer-linux"
    python_dir = os.path.join(layer_dir, "python")
    
    if os.path.exists(layer_dir):
        shutil.rmtree(layer_dir)
    
    os.makedirs(python_dir)
    
    try:
        # Download Linux wheel
        wheel_file = download_linux_psycopg2()
        
        if not wheel_file or not os.path.exists(wheel_file):
            raise Exception("Failed to download Linux psycopg2 wheel")
        
        print(f"✅ Downloaded: {wheel_file}")
        
        # Extract wheel contents
        print("📦 Extracting wheel contents...")
        with zipfile.ZipFile(wheel_file, 'r') as zip_ref:
            # Extract only the package files (not metadata)
            for member in zip_ref.namelist():
                if not member.startswith(('psycopg2_binary-', '../../')):
                    zip_ref.extract(member, python_dir)
        
        # Clean up wheel file
        os.remove(wheel_file)
        
        # Create layer zip
        print("📦 Creating layer package...")
        layer_zip = "psycopg2-layer-linux.zip"
        
        if os.path.exists(layer_zip):
            os.remove(layer_zip)
        
        # Use PowerShell to create zip
        subprocess.run([
            "powershell", "-Command",
            f"Compress-Archive -Path '{layer_dir}\\*' -DestinationPath '{layer_zip}' -Force"
        ], check=True)
        
        # Get layer size
        layer_size = os.path.getsize(layer_zip) / (1024 * 1024)  # MB
        print(f"✅ Linux Lambda layer created: {layer_zip} ({layer_size:.1f} MB)")
        
        # Verify contents
        print("🔍 Layer contents:")
        with zipfile.ZipFile(layer_zip, 'r') as zip_ref:
            for name in zip_ref.namelist()[:10]:  # Show first 10 files
                print(f"  - {name}")
            if len(zip_ref.namelist()) > 10:
                print(f"  ... and {len(zip_ref.namelist()) - 10} more files")
        
        # Clean up
        shutil.rmtree(layer_dir)
        print("🧹 Build directory cleaned up")
        
        return layer_zip
        
    except Exception as e:
        print(f"❌ Error creating layer: {e}")
        if os.path.exists(layer_dir):
            shutil.rmtree(layer_dir)
        # Clean up any downloaded wheels
        for wheel in Path(".").glob("psycopg2_binary-*.whl"):
            wheel.unlink()
        raise

if __name__ == "__main__":
    try:
        layer_zip = create_psycopg2_layer()
        print(f"\n🎉 Success! Linux-compatible Lambda layer created: {layer_zip}")
        print("\n📋 Next steps:")
        print("1. Upload this layer to AWS Lambda")
        print("2. Attach the layer to your Lambda function")
        print("3. Remove psycopg2 from your function code package")
        print("4. Deploy and test your function")
        
    except Exception as e:
        print(f"\n❌ Failed to create Lambda layer: {e}")
        print("\n💡 Alternative solutions:")
        print("1. Use AWS Lambda layers from marketplace")
        print("2. Use Docker to build: python build_lambda_package_fixed.py")
        print("3. Build on a Linux machine or EC2 instance")
        sys.exit(1)
