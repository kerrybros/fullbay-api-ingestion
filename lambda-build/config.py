"""
Configuration management for the Fullbay API ingestion Lambda.

Handles environment variables, AWS Secrets Manager integration,
and provides centralized configuration access.
"""

import os
import json
import boto3
from typing import Dict, Any, Optional
from botocore.exceptions import ClientError


class Config:
    """
    Configuration manager that loads settings from environment variables
    and AWS Secrets Manager.
    """
    
    def __init__(self):
        self.environment = os.getenv("ENVIRONMENT", "development")
        self.aws_region = os.getenv("AWS_REGION", "us-east-1")
        
        # API Configuration
        self.fullbay_api_base_url = os.getenv("FULLBAY_API_BASE_URL", "https://api.fullbay.com")
        self.fullbay_api_version = os.getenv("FULLBAY_API_VERSION", "v1")
        
        # Database Configuration
        self.db_host = os.getenv("DB_HOST")
        self.db_port = int(os.getenv("DB_PORT", "5432"))
        self.db_name = os.getenv("DB_NAME", "fullbay_data")
        self.db_user = os.getenv("DB_USER")
        
        # AWS Secrets
        self.secrets_manager_secret_name = os.getenv("SECRETS_MANAGER_SECRET_NAME")
        
        # Scheduling (for future use)
        self.schedule_expression = os.getenv("SCHEDULE_EXPRESSION", "rate(1 day)")
        
        # Load secrets from AWS Secrets Manager
        self._secrets = self._load_secrets()
        
        # Validate required configuration
        self._validate_config()
    
    def _load_secrets(self) -> Dict[str, Any]:
        """
        Load secrets from AWS Secrets Manager.
        
        Returns:
            Dict containing secret values
        """
        if not self.secrets_manager_secret_name:
            return {}
            
        try:
            secrets_client = boto3.client("secretsmanager", region_name=self.aws_region)
            response = secrets_client.get_secret_value(SecretId=self.secrets_manager_secret_name)
            return json.loads(response["SecretString"])
        except ClientError as e:
            if self.environment == "development":
                # In development, allow running without secrets manager
                return {}
            raise Exception(f"Failed to load secrets: {e}")
        except Exception as e:
            raise Exception(f"Error loading secrets: {e}")
    
    def _validate_config(self):
        """
        Validate that required configuration is present.
        
        Raises:
            ValueError: If required configuration is missing
        """
        required_env_vars = ["DB_HOST", "DB_USER"]
        missing_vars = [var for var in required_env_vars if not os.getenv(var)]
        
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {missing_vars}")
        
        if not self.fullbay_api_key and self.environment != "development":
            raise ValueError("Fullbay API key is required")
    
    @property
    def fullbay_api_key(self) -> Optional[str]:
        """Get Fullbay API key from secrets or environment."""
        return self._secrets.get("fullbay_api_key") or os.getenv("FULLBAY_API_KEY")
    
    @property
    def db_password(self) -> Optional[str]:
        """Get database password from secrets or environment."""
        return self._secrets.get("db_password") or os.getenv("DB_PASSWORD")
    
    @property
    def db_connection_string(self) -> str:
        """
        Generate database connection string.
        
        Returns:
            PostgreSQL connection string
        """
        return (
            f"postgresql://{self.db_user}:{self.db_password}@"
            f"{self.db_host}:{self.db_port}/{self.db_name}"
        )
    
    def get_fullbay_headers(self) -> Dict[str, str]:
        """
        Get headers for Fullbay API requests.
        
        Returns:
            Dict containing required API headers
        """
        return {
            "Authorization": f"Bearer {self.fullbay_api_key}",
            "Content-Type": "application/json",
            "User-Agent": f"FullbayIngestion/1.0.0 ({self.environment})"
        }
    
    def __str__(self) -> str:
        """String representation (excluding sensitive data)."""
        return (
            f"Config(environment={self.environment}, "
            f"db_host={self.db_host}, "
            f"db_name={self.db_name}, "
            f"api_base_url={self.fullbay_api_base_url})"
        )