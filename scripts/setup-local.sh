#!/bin/bash

# Local development setup script
# Sets up the development environment and installs dependencies

set -e

echo "🛠️  Setting up Fullbay API Ingestion local development environment"

# Check Python version
python_version=$(python3 --version 2>&1 | awk '{print $2}' | cut -d. -f1,2)
required_version="3.11"

if [ "$(printf '%s\n' "$required_version" "$python_version" | sort -V | head -n1)" != "$required_version" ]; then
    echo "❌ Error: Python 3.11 or higher is required. Found: $python_version"
    exit 1
fi

echo "✅ Python version: $python_version"

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "📦 Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "🔌 Activating virtual environment..."
source venv/bin/activate

# Upgrade pip
echo "⬆️  Upgrading pip..."
pip install --upgrade pip

# Install dependencies
echo "📚 Installing dependencies..."
pip install -r requirements.txt

# Install development dependencies
echo "🔧 Installing development dependencies..."
pip install -r requirements-dev.txt 2>/dev/null || echo "No requirements-dev.txt found, skipping..."

# Create .env file if it doesn't exist
if [ ! -f ".env" ]; then
    echo "📝 Creating .env file from template..."
    cp .env.example .env
    echo "⚠️  Please edit .env file with your actual configuration values"
fi

# Set up pre-commit hooks (if available)
if command -v pre-commit &>/dev/null; then
    echo "🪝 Setting up pre-commit hooks..."
    pre-commit install
fi

# Create necessary directories
echo "📁 Creating project directories..."
mkdir -p logs
mkdir -p tests/unit
mkdir -p tests/integration
mkdir -p config/environments

echo "✅ Local development environment setup complete!"
echo ""
echo "Next steps:"
echo "1. Edit .env file with your configuration values"
echo "2. Run tests: pytest"
echo "3. Run local test: python -m src.lambda_function"
echo "4. Format code: black src/ tests/"
echo "5. Run linting: flake8 src/ tests/"