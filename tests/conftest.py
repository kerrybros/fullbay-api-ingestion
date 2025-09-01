"""
Pytest configuration and shared fixtures.
"""

import os
import pytest
from unittest.mock import patch
import logging

# Disable logging during tests to reduce noise
logging.disable(logging.CRITICAL)


@pytest.fixture(autouse=True)
def clean_environment():
    """Clean environment variables for each test."""
    # Store original environment
    original_env = os.environ.copy()
    
    # Clear all environment variables that might affect tests
    test_env_vars = [
        "ENVIRONMENT", "AWS_REGION", "DB_HOST", "DB_PORT", "DB_NAME", 
        "DB_USER", "DB_PASSWORD", "FULLBAY_API_KEY", "FULLBAY_API_BASE_URL",
        "SECRETS_MANAGER_SECRET_NAME", "SCHEDULE_EXPRESSION", "LOG_LEVEL"
    ]
    
    for var in test_env_vars:
        if var in os.environ:
            del os.environ[var]
    
    yield
    
    # Restore original environment
    os.environ.clear()
    os.environ.update(original_env)


@pytest.fixture
def sample_fullbay_records():
    """Sample Fullbay API records for testing."""
    return [
        {
            "id": "wo_123456789",
            "work_order_number": "WO-2024-001",
            "customer_id": "cust_abc123",
            "customer_name": "ABC Transportation",
            "status": "completed",
            "created_at": "2024-01-01T10:00:00Z",
            "updated_at": "2024-01-01T14:30:00Z",
            "total_amount": 1250.75,
            "labor_amount": 800.00,
            "parts_amount": 400.75,
            "tax_amount": 50.00,
            "vehicle": {
                "make": "Freightliner",
                "model": "Cascadia",
                "year": 2019,
                "vin": "1FUJGHDV8KLSK1234",
                "license_plate": "ABC-123"
            },
            "line_items": [
                {
                    "type": "labor",
                    "description": "Engine diagnostics",
                    "quantity": 4,
                    "rate": 150.00,
                    "amount": 600.00
                },
                {
                    "type": "part",
                    "description": "Oil filter",
                    "part_number": "OF-12345",
                    "quantity": 1,
                    "cost": 25.50,
                    "amount": 25.50
                }
            ]
        },
        {
            "id": "wo_987654321",
            "work_order_number": "WO-2024-002",
            "customer_id": "cust_xyz789",
            "customer_name": "XYZ Logistics",
            "status": "in_progress",
            "created_at": "2024-01-02T08:15:00Z",
            "updated_at": "2024-01-02T08:15:00Z",
            "total_amount": 0.00,
            "labor_amount": 0.00,
            "parts_amount": 0.00,
            "tax_amount": 0.00,
            "vehicle": {
                "make": "Peterbilt",
                "model": "579",
                "year": 2020,
                "vin": "1XP5DB9X6LD123456"
            }
        }
    ]


@pytest.fixture
def mock_aws_secrets():
    """Mock AWS Secrets Manager response."""
    return {
        "fullbay_api_key": "test-secret-api-key",
        "db_password": "test-secret-password"
    }


@pytest.fixture
def mock_lambda_context():
    """Mock Lambda context for testing."""
    class MockContext:
        aws_request_id = "test-request-123"
        function_name = "fullbay-ingestion-test"
        function_version = "1.0"
        invoked_function_arn = "arn:aws:lambda:us-east-1:123456789012:function:fullbay-ingestion-test"
        memory_limit_in_mb = "512"
        remaining_time_in_millis = 300000
        log_group_name = "/aws/lambda/fullbay-ingestion-test"
        log_stream_name = "2024/01/01/[1.0]test-request-123"
    
    return MockContext()


@pytest.fixture
def mock_eventbridge_event():
    """Mock EventBridge scheduled event."""
    return {
        "version": "0",
        "id": "12345678-1234-1234-1234-123456789012",
        "detail-type": "Scheduled Event",
        "source": "aws.events",
        "account": "123456789012",
        "time": "2024-01-01T12:00:00Z",
        "region": "us-east-1",
        "detail": {}
    }