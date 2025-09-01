#!/usr/bin/env python3
"""
Script to create a Lambda layer with psycopg2-binary for Linux compatibility.
"""

import os
import shutil
import subprocess
import sys

def create_psycopg2_layer():
    """Create a Lambda layer with psycopg2-binary."""
    
    print("🔨 Creating psycopg2 Lambda layer...")
    
    # Create layer directory structure
    layer_dir = "psycopg2-layer"
    python_dir = os.path.join(layer_dir, "python")
    
    if os.path.exists(layer_dir):
        shutil.rmtree(layer_dir)
    
    os.makedirs(python_dir)
    
    try:
        # Install psycopg2-binary for Linux
        print("📦 Installing psycopg2-binary for Linux...")
        
        # Use pip without --user flag to avoid conflicts
        env = os.environ.copy()
        env.pop('PIP_USER', None)  # Remove PIP_USER if set
        
        subprocess.run([
            sys.executable, "-m", "pip", "install",
            "psycopg2-binary==2.9.9",
            "-t", python_dir,
            "--platform", "manylinux2014_x86_64",
            "--only-binary=all",
            "--no-deps",
            "--upgrade"
        ], check=True, env=env)
        
        print("✅ psycopg2-binary installed successfully")
        
        # Create layer zip
        print("📦 Creating layer package...")
        layer_zip = "psycopg2-layer.zip"
        
        if os.path.exists(layer_zip):
            os.remove(layer_zip)
        
        # Use PowerShell to create zip
        subprocess.run([
            "powershell", "-Command",
            f"Compress-Archive -Path '{layer_dir}\\*' -DestinationPath '{layer_zip}' -Force"
        ], check=True)
        
        # Get layer size
        layer_size = os.path.getsize(layer_zip) / (1024 * 1024)  # MB
        print(f"✅ Lambda layer created: {layer_zip} ({layer_size:.1f} MB)")
        
        # Clean up
        shutil.rmtree(layer_dir)
        print("🧹 Layer directory cleaned up")
        
        return layer_zip
        
    except Exception as e:
        print(f"❌ Error creating layer: {e}")
        if os.path.exists(layer_dir):
            shutil.rmtree(layer_dir)
        raise

def create_minimal_layer():
    """Create a minimal layer by downloading wheel directly."""
    print("🔨 Creating minimal psycopg2 layer...")
    
    layer_dir = "psycopg2-layer-minimal"
    python_dir = os.path.join(layer_dir, "python")
    
    if os.path.exists(layer_dir):
        shutil.rmtree(layer_dir)
    
    os.makedirs(python_dir)
    
    try:
        # Try a simpler approach - just install the package
        print("📦 Installing psycopg2-binary...")
        
        # Create a temporary requirements file
        temp_req = "temp_requirements.txt"
        with open(temp_req, 'w') as f:
            f.write("psycopg2-binary==2.9.9\n")
        
        # Install using requirements file
        subprocess.run([
            sys.executable, "-m", "pip", "install",
            "-r", temp_req,
            "-t", python_dir,
            "--no-user"
        ], check=True)
        
        # Clean up temp file
        os.remove(temp_req)
        
        # Create layer zip
        layer_zip = "psycopg2-layer-minimal.zip"
        if os.path.exists(layer_zip):
            os.remove(layer_zip)
        
        subprocess.run([
            "powershell", "-Command",
            f"Compress-Archive -Path '{layer_dir}\\*' -DestinationPath '{layer_zip}' -Force"
        ], check=True)
        
        layer_size = os.path.getsize(layer_zip) / (1024 * 1024)
        print(f"✅ Minimal layer created: {layer_zip} ({layer_size:.1f} MB)")
        
        shutil.rmtree(layer_dir)
        return layer_zip
        
    except Exception as e:
        print(f"❌ Minimal layer failed: {e}")
        if os.path.exists(layer_dir):
            shutil.rmtree(layer_dir)
        raise

if __name__ == "__main__":
    try:
        # Try the main method first
        try:
            layer_zip = create_psycopg2_layer()
            print(f"🎉 Lambda layer created successfully: {layer_zip}")
        except Exception as e:
            print(f"⚠️ Main method failed: {e}")
            print("🔄 Trying minimal approach...")
            layer_zip = create_minimal_layer()
            print(f"🎉 Minimal layer created successfully: {layer_zip}")
            
    except Exception as e:
        print(f"❌ Failed to create Lambda layer: {e}")
        print("\n💡 Alternative solutions:")
        print("1. Use Docker: python build_lambda_package_fixed.py")
        print("2. Manual download: python build_lambda_simple.py")
        print("3. Use AWS Lambda layers from AWS marketplace")
        sys.exit(1)
