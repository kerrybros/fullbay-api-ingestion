"""
Integration tests for end-to-end functionality.
These tests require actual AWS resources and should be run in a test environment.
"""

import os
import pytest
from unittest.mock import patch, MagicMock

# Skip integration tests if not in test environment
pytestmark = pytest.mark.skipif(
    os.getenv("RUN_INTEGRATION_TESTS") != "true",
    reason="Integration tests disabled. Set RUN_INTEGRATION_TESTS=true to enable."
)


class TestEndToEndIntegration:
    """End-to-end integration tests."""
    
    @pytest.mark.integration
    def test_full_ingestion_workflow(self, mock_lambda_context, mock_eventbridge_event):
        """Test complete ingestion workflow with mocked external services."""
        # This test would typically require:
        # 1. Mock Fullbay API responses
        # 2. Test database (could use testcontainers)
        # 3. Mock AWS Secrets Manager
        
        # For demonstration purposes, we'll show the structure
        with patch('src.fullbay_client.FullbayClient') as mock_client_class, \
             patch('src.database.DatabaseManager') as mock_db_class, \
             patch('boto3.client') as mock_boto_client:
            
            # Configure mocks
            mock_client = MagicMock()
            mock_client.fetch_data.return_value = [
                {"id": "test-123", "status": "completed", "_ingestion_timestamp": "2024-01-01T12:00:00Z"}
            ]
            mock_client_class.return_value = mock_client
            
            mock_db = MagicMock()
            mock_db.insert_records.return_value = 1
            mock_db_class.return_value = mock_db
            
            # Mock secrets
            mock_secrets_client = MagicMock()
            mock_secrets_client.get_secret_value.return_value = {
                "SecretString": '{"fullbay_api_key": "test-key", "db_password": "test-pass"}'
            }
            mock_boto_client.return_value = mock_secrets_client
            
            # Set environment variables
            with patch.dict(os.environ, {
                "ENVIRONMENT": "test",
                "DB_HOST": "test-host",
                "DB_USER": "test-user",
                "SECRETS_MANAGER_SECRET_NAME": "test-secret"
            }):
                from src.lambda_function import lambda_handler
                
                result = lambda_handler(mock_eventbridge_event, mock_lambda_context)
                
                # Verify result
                assert result["statusCode"] == 200
                
                # Verify interactions
                mock_client.fetch_data.assert_called_once()
                mock_db.connect.assert_called_once()
                mock_db.insert_records.assert_called_once()
                mock_db.close.assert_called_once()
    
    @pytest.mark.integration
    def test_api_connection_failure(self, mock_lambda_context, mock_eventbridge_event):
        """Test handling of API connection failure."""
        with patch('src.fullbay_client.FullbayClient') as mock_client_class:
            mock_client = MagicMock()
            mock_client.fetch_data.side_effect = Exception("API connection failed")
            mock_client_class.return_value = mock_client
            
            with patch.dict(os.environ, {
                "ENVIRONMENT": "test",
                "DB_HOST": "test-host",
                "DB_USER": "test-user",
                "FULLBAY_API_KEY": "test-key"
            }):
                from src.lambda_function import lambda_handler
                
                result = lambda_handler(mock_eventbridge_event, mock_lambda_context)
                
                assert result["statusCode"] == 500
                body = eval(result["body"])  # In real tests, use json.loads
                assert body["status"] == "ERROR"
                assert "API connection failed" in body["message"]
    
    @pytest.mark.integration  
    def test_database_connection_failure(self, mock_lambda_context, mock_eventbridge_event):
        """Test handling of database connection failure."""
        with patch('src.database.DatabaseManager') as mock_db_class:
            mock_db = MagicMock()
            mock_db.connect.side_effect = Exception("Database connection failed")
            mock_db_class.return_value = mock_db
            
            with patch.dict(os.environ, {
                "ENVIRONMENT": "test",
                "DB_HOST": "test-host",
                "DB_USER": "test-user",
                "FULLBAY_API_KEY": "test-key"
            }):
                from src.lambda_function import lambda_handler
                
                result = lambda_handler(mock_eventbridge_event, mock_lambda_context)
                
                assert result["statusCode"] == 500
                body = eval(result["body"])  # In real tests, use json.loads
                assert body["status"] == "ERROR"
                assert "Database connection failed" in body["message"]