"""
Unit tests for Fullbay API client.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import requests
from datetime import datetime, timezone

from src.fullbay_client import FullbayClient
from src.config import Config


class TestFullbayClient:
    """Test cases for FullbayClient class."""
    
    @pytest.fixture
    def mock_config(self):
        """Create mock configuration for testing."""
        config = Mock(spec=Config)
        config.fullbay_api_base_url = "https://api.fullbay.com"
        config.fullbay_api_version = "v1"
        config.get_fullbay_headers.return_value = {
            "Authorization": "Bearer test-token",
            "Content-Type": "application/json"
        }
        return config
    
    @pytest.fixture
    def client(self, mock_config):
        """Create FullbayClient instance for testing."""
        return FullbayClient(mock_config)
    
    def test_client_initialization(self, mock_config):
        """Test client initialization."""
        client = FullbayClient(mock_config)
        
        assert client.config == mock_config
        assert client.base_url == "https://api.fullbay.com/v1"
        assert client.session is not None
    
    @patch('requests.Session.get')
    def test_fetch_data_success(self, mock_get, client):
        """Test successful data fetching."""
        # Mock successful API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": [
                {"id": "123", "status": "completed"},
                {"id": "456", "status": "in_progress"}
            ],
            "total": 2
        }
        mock_response.headers = {}
        mock_get.return_value = mock_response
        
        result = client.fetch_data("work-orders")
        
        assert len(result) == 2
        assert result[0]["id"] == "123"
        assert result[1]["id"] == "456"
        assert "_ingestion_timestamp" in result[0]
        assert "_ingestion_source" in result[0]
        
        mock_get.assert_called_once()
    
    @patch('requests.Session.get')
    def test_fetch_data_list_response(self, mock_get, client):
        """Test data fetching with list response format."""
        # Mock API response as list
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {"id": "789", "status": "pending"},
            {"id": "101", "status": "completed"}
        ]
        mock_response.headers = {}
        mock_get.return_value = mock_response
        
        result = client.fetch_data("work-orders")
        
        assert len(result) == 2
        assert result[0]["id"] == "789"
    
    @patch('requests.Session.get')
    def test_fetch_data_rate_limited(self, mock_get, client):
        """Test handling of rate limiting."""
        # First call returns 429, second call succeeds
        rate_limited_response = Mock()
        rate_limited_response.status_code = 429
        rate_limited_response.headers = {"Retry-After": "1"}
        
        success_response = Mock()
        success_response.status_code = 200
        success_response.json.return_value = {"data": [{"id": "123"}]}
        success_response.headers = {}
        
        mock_get.side_effect = [rate_limited_response, success_response]
        
        with patch('time.sleep') as mock_sleep:
            result = client.fetch_data("work-orders")
            
            mock_sleep.assert_called_once_with(1)
            assert len(result) == 1
            assert result[0]["id"] == "123"
    
    @patch('requests.Session.get')
    def test_fetch_data_http_error(self, mock_get, client):
        """Test handling of HTTP errors."""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("Server Error")
        mock_get.return_value = mock_response
        
        with pytest.raises(Exception) as exc_info:
            client.fetch_data("work-orders")
        
        assert "Failed to fetch data from Fullbay API" in str(exc_info.value)
    
    @patch('requests.Session.get')
    def test_fetch_data_connection_error(self, mock_get, client):
        """Test handling of connection errors."""
        mock_get.side_effect = requests.exceptions.ConnectionError("Connection failed")
        
        with pytest.raises(Exception) as exc_info:
            client.fetch_data("work-orders")
        
        assert "Failed to fetch data from Fullbay API" in str(exc_info.value)
    
    def test_build_query_params_defaults(self, client):
        """Test building query parameters with defaults."""
        params = client._build_query_params()
        
        assert params["limit"] == 100
        assert params["offset"] == 0
        assert "updated_since" in params  # Should add default date filter
    
    def test_build_query_params_custom(self, client):
        """Test building query parameters with custom values."""
        params = client._build_query_params(
            limit=50,
            offset=25,
            start_date="2024-01-01T00:00:00Z",
            status="completed"
        )
        
        assert params["limit"] == 50
        assert params["offset"] == 25
        assert params["start_date"] == "2024-01-01T00:00:00Z"
        assert params["status"] == "completed"
        assert "updated_since" not in params  # Should not add default when start_date provided
    
    def test_validate_and_enrich_records(self, client):
        """Test record validation and enrichment."""
        raw_records = [
            {"id": "123", "status": "completed"},
            {"id": "456", "status": "pending"},
            {"invalid": "record"},  # Missing id
            "not_a_dict"  # Not a dictionary
        ]
        
        enriched = client._validate_and_enrich_records(raw_records)
        
        assert len(enriched) == 2  # Only valid records
        
        for record in enriched:
            assert "_ingestion_timestamp" in record
            assert "_ingestion_source" in record
            assert record["_ingestion_source"] == "fullbay_api"
    
    @patch('requests.Session.get')
    def test_test_connection_success(self, mock_get, client):
        """Test successful connection test."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response
        
        result = client.test_connection()
        
        assert result is True
    
    @patch('requests.Session.get')
    def test_test_connection_failure(self, mock_get, client):
        """Test connection test failure."""
        mock_get.side_effect = requests.exceptions.ConnectionError("Connection failed")
        
        result = client.test_connection()
        
        assert result is False