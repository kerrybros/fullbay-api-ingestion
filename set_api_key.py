#!/usr/bin/env python3
"""
Simple script to set the Fullbay API key environment variable.

This script helps set the API key for the current session.
"""

import os
import sys

def set_api_key():
    """Set the Fullbay API key."""
    print("🔑 Fullbay API Key Setup")
    print("="*30)
    
    # Get API key from user
    api_key = input("Enter your Fullbay API key: ").strip()
    
    if not api_key:
        print("❌ No API key provided")
        return False
    
    # Set environment variable
    os.environ['FULLBAY_API_KEY'] = api_key
    
    # Verify it was set
    if os.getenv('FULLBAY_API_KEY') == api_key:
        print(f"✅ API key set successfully: {api_key[:8]}...{api_key[-8:]}")
        return True
    else:
        print("❌ Failed to set API key")
        return False

def main():
    """Main function."""
    if set_api_key():
        print("\n🚀 API key is now configured for this session!")
        print("You can now run:")
        print("  python test_fullbay_connection.py  # Test the connection")
        print("  python february_ingestion.py      # Start February ingestion")
    else:
        print("\n❌ Failed to set API key")
        sys.exit(1)

if __name__ == "__main__":
    main()
