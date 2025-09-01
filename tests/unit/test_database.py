"""
Unit tests for database operations.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timezone

from src.database import DatabaseManager
from src.config import Config


class TestDatabaseManager:
    """Test cases for DatabaseManager class."""
    
    @pytest.fixture
    def mock_config(self):
        """Create mock configuration for testing."""
        config = Mock(spec=Config)
        config.db_host = "test-host"
        config.db_port = 5432
        config.db_name = "test_db"
        config.db_user = "test_user"
        config.db_password = "test_pass"
        return config
    
    @pytest.fixture
    def db_manager(self, mock_config):
        """Create DatabaseManager instance for testing."""
        return DatabaseManager(mock_config)
    
    def test_initialization(self, mock_config):
        """Test database manager initialization."""
        db_manager = DatabaseManager(mock_config)
        
        assert db_manager.config == mock_config
        assert db_manager.connection_pool is None
        assert db_manager.main_table == "fullbay_work_orders"
        assert db_manager.metadata_table == "ingestion_metadata"
    
    @patch('psycopg2.pool.SimpleConnectionPool')
    def test_connect_success(self, mock_pool, db_manager):
        """Test successful database connection."""
        # Mock connection pool and connection
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = ["PostgreSQL 13.7"]
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        
        mock_pool_instance = MagicMock()
        mock_pool_instance.getconn.return_value = mock_connection
        mock_pool.return_value = mock_pool_instance
        
        db_manager.connect()
        
        assert db_manager.connection_pool == mock_pool_instance
        mock_pool.assert_called_once()
        mock_cursor.execute.assert_called()  # Should execute table creation and indexes
    
    @patch('psycopg2.pool.SimpleConnectionPool')
    def test_connect_failure(self, mock_pool, db_manager):
        """Test database connection failure."""
        mock_pool.side_effect = Exception("Connection failed")
        
        with pytest.raises(Exception) as exc_info:
            db_manager.connect()
        
        assert "Failed to connect to database" in str(exc_info.value)
    
    def test_insert_records_empty_list(self, db_manager):
        """Test inserting empty list of records."""
        result = db_manager.insert_records([])
        assert result == 0
    
    @patch('psycopg2.pool.SimpleConnectionPool')
    def test_insert_records_success(self, mock_pool, db_manager):
        """Test successful record insertion."""
        # Mock connection and cursor
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        
        mock_pool_instance = MagicMock()
        mock_pool_instance.getconn.return_value = mock_connection
        db_manager.connection_pool = mock_pool_instance
        
        # Test records
        records = [
            {
                "id": "123",
                "work_order_number": "WO-001",
                "status": "completed",
                "_ingestion_timestamp": "2024-01-01T12:00:00Z",
                "_ingestion_source": "fullbay_api"
            },
            {
                "id": "456",
                "work_order_number": "WO-002",
                "status": "pending",
                "_ingestion_timestamp": "2024-01-01T12:00:00Z",
                "_ingestion_source": "fullbay_api"
            }
        ]
        
        result = db_manager.insert_records(records)
        
        assert result == 2
        assert mock_cursor.execute.call_count == 2  # One call per record
        mock_connection.commit.assert_called_once()
    
    def test_process_record_valid(self, db_manager):
        """Test processing valid record."""
        raw_record = {
            "id": "123",
            "work_order_number": "WO-001",
            "customer_id": "456",
            "customer_name": "Test Customer",
            "status": "completed",
            "created_at": "2024-01-01T10:00:00Z",
            "updated_at": "2024-01-01T12:00:00Z",
            "total_amount": "150.75",
            "_ingestion_timestamp": "2024-01-01T12:00:00Z"
        }
        
        processed = db_manager._process_record(raw_record)
        
        assert processed["fullbay_id"] == "123"
        assert processed["work_order_number"] == "WO-001"
        assert processed["customer_id"] == "456"
        assert processed["customer_name"] == "Test Customer"
        assert processed["status"] == "completed"
        assert processed["total_amount"] == 150.75
        assert processed["raw_data"] == raw_record
    
    def test_process_record_missing_id(self, db_manager):
        """Test processing record missing required ID."""
        raw_record = {"status": "completed"}
        
        with pytest.raises(ValueError) as exc_info:
            db_manager._process_record(raw_record)
        
        assert "Record missing required 'id' field" in str(exc_info.value)
    
    def test_parse_timestamp_valid_formats(self, db_manager):
        """Test parsing various timestamp formats."""
        test_cases = [
            "2024-01-01T12:00:00.123Z",
            "2024-01-01T12:00:00Z",
            "2024-01-01T12:00:00.123",
            "2024-01-01T12:00:00",
            "2024-01-01 12:00:00"
        ]
        
        for timestamp_str in test_cases:
            result = db_manager._parse_timestamp(timestamp_str)
            assert isinstance(result, datetime)
            assert result.tzinfo == timezone.utc
    
    def test_parse_timestamp_invalid(self, db_manager):
        """Test parsing invalid timestamp."""
        result = db_manager._parse_timestamp("invalid-timestamp")
        assert result is None
        
        result = db_manager._parse_timestamp(None)
        assert result is None
    
    def test_parse_decimal_various_formats(self, db_manager):
        """Test parsing various decimal formats."""
        test_cases = [
            (123.45, 123.45),
            ("123.45", 123.45),
            ("$123.45", 123.45),
            ("1,234.56", 1234.56),
            ("$1,234.56", 1234.56),
            (None, None),
            ("", None),
            ("invalid", None)
        ]
        
        for input_val, expected in test_cases:
            result = db_manager._parse_decimal(input_val)
            assert result == expected
    
    def test_extract_vehicle_info(self, db_manager):
        """Test extracting vehicle information."""
        # Test with vehicle field
        record1 = {"vehicle": {"make": "Ford", "model": "F-150", "year": 2020}}
        result1 = db_manager._extract_vehicle_info(record1)
        assert result1 == {"make": "Ford", "model": "F-150", "year": 2020}
        
        # Test with individual fields
        record2 = {"make": "Toyota", "model": "Camry", "vin": "123456789"}
        result2 = db_manager._extract_vehicle_info(record2)
        assert result2 == {"make": "Toyota", "model": "Camry", "vin": "123456789"}
        
        # Test with no vehicle info
        record3 = {"id": "123", "status": "completed"}
        result3 = db_manager._extract_vehicle_info(record3)
        assert result3 is None
    
    def test_close_connection(self, db_manager):
        """Test closing database connection."""
        mock_pool = MagicMock()
        db_manager.connection_pool = mock_pool
        
        db_manager.close()
        
        mock_pool.closeall.assert_called_once()
    
    @patch('psycopg2.pool.SimpleConnectionPool')
    def test_test_connection_success(self, mock_pool, db_manager):
        """Test successful connection test."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = [1]
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        
        mock_pool_instance = MagicMock()
        mock_pool_instance.getconn.return_value = mock_connection
        db_manager.connection_pool = mock_pool_instance
        
        result = db_manager.test_connection()
        
        assert result is True
        mock_cursor.execute.assert_called_with("SELECT 1")
    
    @patch('psycopg2.pool.SimpleConnectionPool')
    def test_test_connection_failure(self, mock_pool, db_manager):
        """Test connection test failure."""
        mock_pool_instance = MagicMock()
        mock_pool_instance.getconn.side_effect = Exception("Connection failed")
        db_manager.connection_pool = mock_pool_instance
        
        result = db_manager.test_connection()
        
        assert result is False