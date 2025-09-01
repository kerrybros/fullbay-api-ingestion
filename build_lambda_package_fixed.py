#!/usr/bin/env python3
"""
Script to build a proper Lambda deployment package with Linux-compatible dependencies.
Uses Docker to ensure Linux compatibility when building on Windows.
"""

import os
import shutil
import subprocess
import sys
from pathlib import Path

def check_docker():
    """Check if Docker is available."""
    try:
        result = subprocess.run(['docker', '--version'], capture_output=True, text=True)
        return result.returncode == 0
    except FileNotFoundError:
        return False

def build_with_docker():
    """Build Lambda package using Docker for Linux compatibility."""
    print("🐳 Building Lambda package using Docker...")
    
    # Create Dockerfile for building
    dockerfile_content = """
FROM public.ecr.aws/lambda/python:3.11

# Copy requirements and install dependencies
COPY requirements-lambda.txt .
RUN pip install -r requirements-lambda.txt -t /var/task

# Copy source code
COPY src/ /var/task/

# Set the CMD to your handler
CMD ["lambda_function.lambda_handler"]
"""
    
    with open('Dockerfile.lambda', 'w') as f:
        f.write(dockerfile_content)
    
    try:
        # Build Docker image
        print("🔨 Building Docker image...")
        subprocess.run([
            'docker', 'build', '-f', 'Dockerfile.lambda', 
            '-t', 'lambda-builder', '.'
        ], check=True)
        
        # Create container and copy files
        print("📦 Extracting files from container...")
        container_id = subprocess.check_output([
            'docker', 'create', 'lambda-builder'
        ], text=True).strip()
        
        # Create build directory
        build_dir = "lambda-build-docker"
        if os.path.exists(build_dir):
            shutil.rmtree(build_dir)
        os.makedirs(build_dir)
        
        # Copy files from container
        subprocess.run([
            'docker', 'cp', f'{container_id}:/var/task/.', build_dir
        ], check=True)
        
        # Remove container
        subprocess.run(['docker', 'rm', container_id], check=True)
        
        # Create zip package
        package_name = "lambda-deployment-docker.zip"
        if os.path.exists(package_name):
            os.remove(package_name)
        
        subprocess.run([
            "powershell", "-Command",
            f"Compress-Archive -Path '{build_dir}\\*' -DestinationPath '{package_name}' -Force"
        ], check=True)
        
        # Clean up
        shutil.rmtree(build_dir)
        os.remove('Dockerfile.lambda')
        
        package_size = os.path.getsize(package_name) / (1024 * 1024)
        print(f"✅ Docker-built package created: {package_name} ({package_size:.1f} MB)")
        
        return package_name
        
    except Exception as e:
        print(f"❌ Docker build failed: {e}")
        # Clean up
        if os.path.exists('Dockerfile.lambda'):
            os.remove('Dockerfile.lambda')
        if os.path.exists('lambda-build-docker'):
            shutil.rmtree('lambda-build-docker')
        raise

def build_with_pip_platform():
    """Build Lambda package using pip with platform specification."""
    print("📦 Building Lambda package with platform-specific pip...")
    
    # Create build directory
    build_dir = "lambda-build-pip"
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
        
        # Install dependencies with platform specification
        print("📦 Installing Linux-compatible dependencies...")
        subprocess.run([
            sys.executable, "-m", "pip", "install",
            "-r", "requirements-lambda.txt",
            "-t", build_dir,
            "--platform", "manylinux2014_x86_64",
            "--only-binary=all",
            "--no-deps"
        ], check=True)
        
        # Install dependencies without platform restriction for compatibility
        print("📦 Installing additional dependencies...")
        subprocess.run([
            sys.executable, "-m", "pip", "install",
            "requests>=2.31.0",
            "boto3>=1.28.0",
            "botocore>=1.31.0",
            "-t", build_dir,
            "--platform", "manylinux2014_x86_64",
            "--only-binary=all"
        ], check=True)
        
        # Create package
        package_name = "lambda-deployment-pip.zip"
        if os.path.exists(package_name):
            os.remove(package_name)
        
        subprocess.run([
            "powershell", "-Command",
            f"Compress-Archive -Path '{build_dir}\\*' -DestinationPath '{package_name}' -Force"
        ], check=True)
        
        # Clean up
        shutil.rmtree(build_dir)
        
        package_size = os.path.getsize(package_name) / (1024 * 1024)
        print(f"✅ Pip-built package created: {package_name} ({package_size:.1f} MB)")
        
        return package_name
        
    except Exception as e:
        print(f"❌ Pip build failed: {e}")
        if os.path.exists(build_dir):
            shutil.rmtree(build_dir)
        raise

def build_lambda_package():
    """Main build function that tries different methods."""
    
    print("🔨 Building Lambda deployment package...")
    
    # Try Docker first (most reliable)
    if check_docker():
        try:
            return build_with_docker()
        except Exception as e:
            print(f"⚠️ Docker build failed, trying pip method: {e}")
    
    # Fall back to pip with platform specification
    try:
        return build_with_pip_platform()
    except Exception as e:
        print(f"❌ All build methods failed: {e}")
        raise

if __name__ == "__main__":
    try:
        package_name = build_lambda_package()
        print(f"🎉 Lambda package built successfully: {package_name}")
        print("\n📋 Next steps:")
        print("1. Upload the package to AWS Lambda")
        print("2. Set environment variables for database connection")
        print("3. Configure EventBridge trigger if needed")
    except Exception as e:
        print(f"❌ Failed to build Lambda package: {e}")
        sys.exit(1)
