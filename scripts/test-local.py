#!/usr/bin/env python3
"""
Local testing script for the Fullbay API Ingestion Lambda function.
Allows testing the function without deploying to AWS.
"""

import os
import sys
import json
from datetime import datetime
from pathlib import Path

# Add src directory to Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("⚠️  python-dotenv not installed. Install it with: pip install python-dotenv")

from lambda_function import lambda_handler


class MockLambdaContext:
    """Mock Lambda context for local testing."""
    
    def __init__(self):
        self.aws_request_id = f"local-test-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        self.function_name = "fullbay-api-ingestion-local"
        self.function_version = "1.0"
        self.invoked_function_arn = f"arn:aws:lambda:us-east-1:123456789012:function:{self.function_name}"
        self.memory_limit_in_mb = "512"
        self.remaining_time_in_millis = 300000
        self.log_group_name = f"/aws/lambda/{self.function_name}"
        self.log_stream_name = f"2024/01/01/[1.0]{self.aws_request_id}"


def create_test_event(event_type="scheduled"):
    """Create a test event for the Lambda function."""
    
    if event_type == "scheduled":
        return {
            "version": "0",
            "id": "test-event-id",
            "detail-type": "Scheduled Event",
            "source": "aws.events",
            "account": "123456789012",
            "time": datetime.now().isoformat(),
            "region": "us-east-1",
            "detail": {}
        }
    elif event_type == "manual":
        return {
            "test": True,
            "trigger": "manual",
            "timestamp": datetime.now().isoformat()
        }
    else:
        raise ValueError(f"Unknown event type: {event_type}")


def main():
    """Main function for local testing."""
    print("🧪 Fullbay API Ingestion Local Test")
    print("=" * 50)
    
    # Check environment variables
    required_env_vars = ["DB_HOST", "DB_USER", "FULLBAY_API_KEY"]
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"❌ Missing required environment variables: {missing_vars}")
        print("Please check your .env file")
        return 1
    
    print("✅ Environment variables loaded")
    print(f"Environment: {os.getenv('ENVIRONMENT', 'development')}")
    print(f"Database Host: {os.getenv('DB_HOST', 'not set')}")
    print(f"API Base URL: {os.getenv('FULLBAY_API_BASE_URL', 'https://api.fullbay.com')}")
    print()
    
    # Create mock context and event
    context = MockLambdaContext()
    event = create_test_event("scheduled")
    
    print(f"Request ID: {context.aws_request_id}")
    print(f"Function Name: {context.function_name}")
    print()
    
    # Run the Lambda function
    try:
        print("🚀 Invoking Lambda function...")
        result = lambda_handler(event, context)
        
        print("📊 Function Result:")
        print(json.dumps(result, indent=2))
        
        # Parse the result
        body = json.loads(result.get("body", "{}"))
        status = body.get("status", "UNKNOWN")
        
        if status == "SUCCESS":
            print("\n✅ Function executed successfully!")
            records_processed = body.get("records_processed", 0)
            duration = body.get("duration_seconds", 0)
            print(f"Records processed: {records_processed}")
            print(f"Duration: {duration:.2f} seconds")
        else:
            print(f"\n❌ Function failed with status: {status}")
            print(f"Error: {body.get('message', 'Unknown error')}")
            return 1
            
    except Exception as e:
        print(f"\n💥 Function execution failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())