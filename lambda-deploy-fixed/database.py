"""
Database manager for PostgreSQL operations with proper psycopg2 connection handling.

Implements connection pooling, proper error handling, rollback, and resource management.
"""

import logging
import psycopg2
import psycopg2.extras
from psycopg2.pool import SimpleConnectionPool
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone

from config import Config

logger = logging.getLogger(__name__)


class DatabaseManager:
    """
    Database manager with proper psycopg2 connection handling and pooling.
    """
    
    def __init__(self, config: Config):
        """
        Initialize database manager with connection pool.
        
        Args:
            config: Configuration object containing database settings
        """
        self.config = config
        self.connection_pool = None
        self._pool_initialized = False
        
    def _initialize_pool(self):
        """Initialize connection pool if not already done."""
        if not self._pool_initialized:
            try:
                logger.info("Initializing database connection pool...")
                self.connection_pool = SimpleConnectionPool(
                    1, 5,  # min and max connections
                    host=self.config.db_host,
                    port=self.config.db_port,
                    database=self.config.db_name,
                    user=self.config.db_user,
                    password=self.config.db_password,
                    cursor_factory=psycopg2.extras.RealDictCursor,
                    # Connection timeout settings
                    connect_timeout=30,
                    options='-c statement_timeout=300000',  # 5 minutes
                    # Connection keepalive settings
                    keepalives=1,
                    keepalives_idle=30,
                    keepalives_interval=10,
                    keepalives_count=5
                )
                self._pool_initialized = True
                logger.info("Database connection pool initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize connection pool: {e}")
                raise
    
    def _get_connection(self):
        """
        Get a connection from the pool with proper error handling.
        
        Returns:
            Database connection
            
        Raises:
            Exception: If connection cannot be established
        """
        if not self._pool_initialized:
            self._initialize_pool()
        
        try:
            conn = self.connection_pool.getconn()
            
            # Check if connection is still valid
            if conn.closed != 0:
                logger.warning("Connection was closed, getting new connection")
                self.connection_pool.putconn(conn, close=True)
                conn = self.connection_pool.getconn()
            
            # Test connection with a simple query
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            
            return conn
            
        except Exception as e:
            logger.error(f"Failed to get database connection: {e}")
            raise Exception(f"Database connection failed: {e}")
    
    def _return_connection(self, conn, close=False):
        """
        Return connection to pool with proper error handling.
        
        Args:
            conn: Database connection
            close: Whether to close the connection
        """
        try:
            if conn and not conn.closed:
                self.connection_pool.putconn(conn, close=close)
            elif conn and conn.closed:
                logger.warning("Connection was already closed")
        except Exception as e:
            logger.error(f"Error returning connection to pool: {e}")
    
    def connect(self):
        """
        Test database connectivity and initialize pool.
        
        Raises:
            Exception: If connection fails
        """
        try:
            logger.info("Connecting to database...")
            self._initialize_pool()
            
            # Test connection
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT version()")
                    version = cursor.fetchone()
                    logger.info(f"Connected to PostgreSQL: {version[0] if version else 'Unknown'}")
            
            logger.info("Database connection established successfully")
            
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise Exception(f"Failed to connect to database: {e}")
    
    def close(self):
        """Close all connections in the pool."""
        try:
            if self.connection_pool:
                self.connection_pool.closeall()
                logger.info("Database connection pool closed")
        except Exception as e:
            logger.error(f"Error closing connection pool: {e}")
    
    def insert_records(self, records: List[Dict[str, Any]]) -> int:
        """
        Insert records into the database with proper error handling.
        
        Args:
            records: List of records to insert
            
        Returns:
            Number of records successfully inserted
        """
        if not records:
            logger.info("No records to insert")
            return 0
        
        inserted_count = 0
        conn = None
        
        try:
            conn = self._get_connection()
            
            # Use context manager for cursor to ensure proper cleanup
            with conn.cursor() as cursor:
                for i, record in enumerate(records):
                    try:
                        # Insert into raw data table
                        self._insert_raw_record(cursor, record)
                        
                        # Insert into line items table
                        line_items_inserted = self._insert_line_items(cursor, record)
                        inserted_count += line_items_inserted
                        
                        # Commit after each record to avoid long transactions
                        conn.commit()
                        
                        if (i + 1) % 10 == 0:
                            logger.info(f"Processed {i + 1}/{len(records)} records")
                            
                    except Exception as e:
                        logger.error(f"Error processing record {i}: {e}")
                        # Rollback on error
                        conn.rollback()
                        continue
            
            logger.info(f"Successfully inserted {inserted_count} line items from {len(records)} records")
            return inserted_count
            
        except Exception as e:
            logger.error(f"Error inserting records: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                self._return_connection(conn)
    
    def _insert_raw_record(self, cursor, record: Dict[str, Any]):
        """
        Insert raw record into fullbay_raw_data table.
        
        Args:
            cursor: Database cursor
            record: Record to insert
        """
        try:
            cursor.execute("""
                INSERT INTO fullbay_raw_data (
                    raw_data, 
                    source_system, 
                    ingestion_timestamp,
                    created_at
                ) VALUES (%s, %s, %s, %s)
                RETURNING id
            """, (
                psycopg2.extras.Json(record),
                'fullbay_api',
                datetime.now(timezone.utc),
                datetime.now(timezone.utc)
            ))
            
            raw_data_id = cursor.fetchone()[0]
            record['_raw_data_id'] = raw_data_id
            
        except Exception as e:
            logger.error(f"Error inserting raw record: {e}")
            raise
    
    def _insert_line_items(self, cursor, record: Dict[str, Any]) -> int:
        """
        Insert line items from record into fullbay_line_items table.
        
        Args:
            cursor: Database cursor
            record: Record containing line items
            
        Returns:
            Number of line items inserted
        """
        try:
            # Extract line items from the record
            line_items = self._extract_line_items(record)
            
            if not line_items:
                return 0
            
            inserted_count = 0
            
            for line_item in line_items:
                cursor.execute("""
                    INSERT INTO fullbay_line_items (
                        raw_data_id,
                        fullbay_invoice_id,
                        line_item_id,
                        description,
                        quantity,
                        unit_price,
                        total_price,
                        part_number,
                        labor_hours,
                        labor_rate,
                        tax_amount,
                        discount_amount,
                        created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    record.get('_raw_data_id'),
                    record.get('primaryKey', record.get('id')),
                    line_item.get('id'),
                    line_item.get('description', ''),
                    line_item.get('quantity', 0),
                    line_item.get('unit_price', 0),
                    line_item.get('total_price', 0),
                    line_item.get('part_number', ''),
                    line_item.get('labor_hours', 0),
                    line_item.get('labor_rate', 0),
                    line_item.get('tax_amount', 0),
                    line_item.get('discount_amount', 0),
                    datetime.now(timezone.utc)
                ))
                inserted_count += 1
            
            return inserted_count
            
        except Exception as e:
            logger.error(f"Error inserting line items: {e}")
            raise
    
    def _extract_line_items(self, record: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract line items from a record.
        
        Args:
            record: Record containing line items
            
        Returns:
            List of line items
        """
        # This is a placeholder - adjust based on actual Fullbay API response structure
        line_items = record.get('line_items', [])
        
        if not line_items and isinstance(record.get('items'), list):
            line_items = record.get('items', [])
        
        if not line_items and isinstance(record.get('services'), list):
            line_items = record.get('services', [])
        
        return line_items if isinstance(line_items, list) else []
    
    def log_execution_metadata(
        self,
        execution_id: str,
        start_time: datetime,
        status: str,
        end_time: datetime,
        records_processed: int = 0,
        records_inserted: int = 0,
        error_message: Optional[str] = None,
        api_endpoint: Optional[str] = None
    ):
        """
        Log execution metadata to the database.
        
        Args:
            execution_id: Unique execution identifier
            start_time: Execution start time
            status: Execution status (SUCCESS, ERROR, etc.)
            end_time: Execution end time
            records_processed: Number of records processed
            records_inserted: Number of records inserted
            error_message: Error message if any
            api_endpoint: API endpoint used
        """
        conn = None
        try:
            conn = self._get_connection()
            
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO ingestion_metadata (
                        execution_id,
                        start_time,
                        end_time,
                        status,
                        records_processed,
                        records_inserted,
                        error_message,
                        api_endpoint,
                        created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    execution_id,
                    start_time,
                    end_time,
                    status,
                    records_processed,
                    records_inserted,
                    error_message,
                    api_endpoint,
                    datetime.now(timezone.utc)
                ))
                
                conn.commit()
                logger.info(f"Execution metadata logged: {execution_id}")
                
        except Exception as e:
            logger.error(f"Failed to log execution metadata: {e}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                self._return_connection(conn)
    
    def test_connection(self) -> bool:
        """
        Test database connection.
        
        Returns:
            True if connection is successful
        """
        try:
            self.connect()
            return True
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False