"""
Unit tests for configuration management.
"""

import os
import pytest
from unittest.mock import patch, MagicMock
from src.config import Config


class TestConfig:
    """Test cases for Config class."""
    
    def test_config_initialization_with_defaults(self):
        """Test config initialization with default values."""
        with patch.dict(os.environ, {
            "DB_HOST": "test-host",
            "DB_USER": "test-user",
            "FULLBAY_API_KEY": "test-key"
        }, clear=True):
            config = Config()
            
            assert config.environment == "development"
            assert config.aws_region == "us-east-1"
            assert config.db_host == "test-host"
            assert config.db_user == "test-user"
            assert config.db_port == 5432
            assert config.db_name == "fullbay_data"
    
    def test_config_with_environment_variables(self):
        """Test config with custom environment variables."""
        with patch.dict(os.environ, {
            "ENVIRONMENT": "production",
            "AWS_REGION": "us-west-2",
            "DB_HOST": "prod-host",
            "DB_PORT": "3306",
            "DB_NAME": "prod_db",
            "DB_USER": "prod-user",
            "FULLBAY_API_KEY": "prod-key",
            "FULLBAY_API_BASE_URL": "https://api.prod.fullbay.com",
            "SCHEDULE_EXPRESSION": "rate(6 hours)"
        }, clear=True):
            config = Config()
            
            assert config.environment == "production"
            assert config.aws_region == "us-west-2"
            assert config.db_host == "prod-host"
            assert config.db_port == 3306
            assert config.db_name == "prod_db"
            assert config.db_user == "prod-user"
            assert config.fullbay_api_base_url == "https://api.prod.fullbay.com"
            assert config.schedule_expression == "rate(6 hours)"
    
    def test_config_missing_required_vars(self):
        """Test config validation with missing required variables."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError) as exc_info:
                Config()
            
            assert "Missing required environment variables" in str(exc_info.value)
            assert "DB_HOST" in str(exc_info.value)
            assert "DB_USER" in str(exc_info.value)
    
    @patch('boto3.client')
    def test_load_secrets_from_secrets_manager(self, mock_boto_client):
        """Test loading secrets from AWS Secrets Manager."""
        # Mock secrets manager response
        mock_client = MagicMock()
        mock_client.get_secret_value.return_value = {
            "SecretString": '{"fullbay_api_key": "secret-key", "db_password": "secret-pass"}'
        }
        mock_boto_client.return_value = mock_client
        
        with patch.dict(os.environ, {
            "DB_HOST": "test-host",
            "DB_USER": "test-user",
            "SECRETS_MANAGER_SECRET_NAME": "test-secret",
            "ENVIRONMENT": "production"
        }, clear=True):
            config = Config()
            
            assert config.fullbay_api_key == "secret-key"
            assert config.db_password == "secret-pass"
            mock_client.get_secret_value.assert_called_once_with(SecretId="test-secret")
    
    def test_db_connection_string(self):
        """Test database connection string generation."""
        with patch.dict(os.environ, {
            "DB_HOST": "test-host",
            "DB_PORT": "5432",
            "DB_NAME": "test_db",
            "DB_USER": "test-user",
            "DB_PASSWORD": "test-pass",
            "FULLBAY_API_KEY": "test-key"
        }, clear=True):
            config = Config()
            
            expected = "postgresql://test-user:test-pass@test-host:5432/test_db"
            assert config.db_connection_string == expected
    
    def test_fullbay_headers(self):
        """Test Fullbay API headers generation."""
        with patch.dict(os.environ, {
            "DB_HOST": "test-host",
            "DB_USER": "test-user",
            "FULLBAY_API_KEY": "test-api-key",
            "ENVIRONMENT": "test-env"
        }, clear=True):
            config = Config()
            headers = config.get_fullbay_headers()
            
            assert headers["Authorization"] == "Bearer test-api-key"
            assert headers["Content-Type"] == "application/json"
            assert "FullbayIngestion/1.0.0" in headers["User-Agent"]
            assert "test-env" in headers["User-Agent"]