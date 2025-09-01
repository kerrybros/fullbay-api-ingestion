"""
Database manager for persisting Fullbay API data to AWS RDS.

Handles PostgreSQL connections, table creation, and data insertion.
Designed for easy extension with deduplication and reliability features.
"""

import json
import logging
import psycopg2
import psycopg2.extras
from psycopg2.pool import SimpleConnectionPool
from contextlib import contextmanager
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
import boto3

from config import Config

logger = logging.getLogger(__name__)


class DatabaseManager:
    """
    Manager for database operations including connection handling and data persistence.
    """
    
    def __init__(self, config: Config):
        """
        Initialize database manager.
        
        Args:
            config: Configuration object containing database connection details
        """
        self.config = config
        self.connection_pool: Optional[SimpleConnectionPool] = None
        self.connection = None
        
        # Table configuration
        self.raw_data_table = "fullbay_raw_data"
        self.line_items_table = "fullbay_line_items"
        self.metadata_table = "ingestion_metadata"
        
        # CloudWatch client for metrics
        try:
            self.cloudwatch = boto3.client('cloudwatch', region_name=getattr(config, 'aws_region', 'us-east-1'))
            self.metrics_enabled = True
        except Exception as e:
            logger.warning(f"CloudWatch metrics disabled: {e}")
            self.cloudwatch = None
            self.metrics_enabled = False
    
    def connect(self):
        """
        Establish database connection and create tables if they don't exist.
        
        Raises:
            Exception: If connection fails
        """
        try:
            logger.info("Connecting to database...")
            
            # Create connection pool for better resource management
            self.connection_pool = SimpleConnectionPool(
                1, 5,  # min and max connections
                host=self.config.db_host,
                port=self.config.db_port,
                database=self.config.db_name,
                user=self.config.db_user,
                password=self.config.db_password,
                cursor_factory=psycopg2.extras.RealDictCursor
            )
            
            # Test connection and create tables
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT version();")
                    version = cursor.fetchone()['version']
                    logger.info(f"Connected to PostgreSQL: {version}")
                    
                    # Create tables
                    self._create_tables(cursor)
                    conn.commit()
            
            logger.info("Database connection established and tables ready")
            
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise Exception(f"Failed to connect to database: {e}")
    
    @contextmanager
    def _get_connection(self):
        """
        Context manager for database connections.
        
        Yields:
            Database connection from the pool
        """
        conn = None
        try:
            conn = self.connection_pool.getconn()
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            raise e
        finally:
            if conn:
                self.connection_pool.putconn(conn)
    
    def _create_tables(self, cursor):
        """
        Create database tables if they don't exist.
        
        Args:
            cursor: Database cursor
        """
        # Raw JSON backup table
        create_raw_data_table = f"""
        CREATE TABLE IF NOT EXISTS {self.raw_data_table} (
            id SERIAL PRIMARY KEY,
            fullbay_invoice_id VARCHAR(50) UNIQUE NOT NULL,
            raw_json_data JSONB NOT NULL,
            ingestion_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            processed BOOLEAN DEFAULT FALSE,
            processing_errors TEXT
        );
        """
        
                 # Flattened line items table - UPDATED 73-COLUMN SCHEMA
        create_line_items_table = f"""
        CREATE TABLE IF NOT EXISTS {self.line_items_table} (
            id SERIAL PRIMARY KEY,
            raw_data_id INTEGER REFERENCES {self.raw_data_table}(id) ON DELETE CASCADE,
            
                    -- === INVOICE LEVEL INFO (7 columns) ===
        fullbay_invoice_id VARCHAR(50) NOT NULL,
        invoice_number VARCHAR(50),
        invoice_date DATE,
        due_date DATE,
        shop_title VARCHAR(255),
        shop_email VARCHAR(255),
        shop_address TEXT,
            
            -- === CUSTOMER INFO (6 columns) ===
            customer_id INTEGER,
            customer_title VARCHAR(255),
            customer_external_id VARCHAR(50),
            customer_main_phone VARCHAR(50),
            customer_secondary_phone VARCHAR(50),
            customer_billing_address TEXT,
            
            -- === SERVICE ORDER INFO (5 columns) ===
            fullbay_service_order_id VARCHAR(50),
            repair_order_number VARCHAR(50),
            service_order_created TIMESTAMP WITH TIME ZONE,
            service_order_start_date TIMESTAMP WITH TIME ZONE,
            service_order_completion_date TIMESTAMP WITH TIME ZONE,
            
            -- === UNIT/VEHICLE INFO (8 columns) ===
                         unit_id VARCHAR(50),
             unit VARCHAR(50),
             unit_type VARCHAR(100),
            unit_year VARCHAR(10),
            unit_make VARCHAR(100),
            unit_model VARCHAR(100),
            unit_vin VARCHAR(50),
            unit_license_plate VARCHAR(20),
            
            -- === PRIMARY TECHNICIAN INFO (2 columns) ===
            primary_technician VARCHAR(255),
            primary_technician_number VARCHAR(50),
            
            -- === COMPLAINT/WORK ORDER INFO (6 columns) ===
            fullbay_complaint_id INTEGER,
            complaint_type VARCHAR(100),
            complaint_subtype VARCHAR(100),
            complaint_note TEXT,
            complaint_cause TEXT,
            complaint_authorized BOOLEAN,
            
                         -- === CORRECTION/SERVICE INFO (7 columns) ===
             fullbay_correction_id INTEGER,
             correction_title VARCHAR(255),
             global_component VARCHAR(255),
             global_system VARCHAR(255),
             global_service VARCHAR(255),
             recommended_correction TEXT,
             service_description TEXT,
            
            -- === LINE ITEM DETAILS (15 columns) ===
            line_item_type VARCHAR(20) NOT NULL, -- 'PART', 'LABOR', 'SUPPLY', 'FREIGHT', 'MISC'
            
            -- For Parts
            fullbay_part_id INTEGER,
            part_description TEXT,
            shop_part_number VARCHAR(100),
            vendor_part_number VARCHAR(100),
            part_category VARCHAR(255),
            
            -- For Labor/Services  
            labor_description TEXT,
            labor_rate_type VARCHAR(50),
            assigned_technician VARCHAR(255),
            assigned_technician_number VARCHAR(50),
            
            -- === QUANTITIES ===
            quantity DECIMAL(10,3),
            to_be_returned_quantity DECIMAL(10,3),
            returned_quantity DECIMAL(10,3),
            
                         -- === HOURS (for labor items) ===
             so_hours DECIMAL(8,2),
             technician_portion INTEGER, -- Percentage of work for this technician
            
                                                 -- === FINANCIAL DETAILS (Per Line Item) ===
                        unit_cost DECIMAL(10,2),
                        unit_price DECIMAL(10,2),
                        line_total DECIMAL(10,2),
                        price_overridden BOOLEAN DEFAULT FALSE,
            
                         -- === TAX INFO ===
             taxable BOOLEAN DEFAULT TRUE,
             tax_rate DECIMAL(5,2),
 
             line_tax DECIMAL(10,2),  -- Calculated tax amount for this line
             sales_total DECIMAL(10,2),  -- Line total + line tax
            
            -- === CLASSIFICATION ===
            inventory_item BOOLEAN DEFAULT FALSE,
            core_type VARCHAR(50),
            sublet BOOLEAN DEFAULT FALSE,
            
                         -- === SERVICE ORDER TOTALS (1 column) ===
             so_supplies_total DECIMAL(10,2),  -- Invoice-level supplies total
            
            -- === METADATA (2 columns) ===
            ingestion_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            ingestion_source VARCHAR(100) DEFAULT 'fullbay_api'
        );
        """
        
        # Metadata table for tracking ingestion runs
        create_metadata_table = f"""
        CREATE TABLE IF NOT EXISTS {self.metadata_table} (
            id SERIAL PRIMARY KEY,
            execution_id VARCHAR(255) UNIQUE NOT NULL,
            start_time TIMESTAMP WITH TIME ZONE NOT NULL,
            end_time TIMESTAMP WITH TIME ZONE,
            status VARCHAR(50) NOT NULL,
            records_processed INTEGER DEFAULT 0,
            records_inserted INTEGER DEFAULT 0,
            records_updated INTEGER DEFAULT 0,
            raw_records_stored INTEGER DEFAULT 0,
            line_items_created INTEGER DEFAULT 0,
            error_message TEXT,
            api_endpoint VARCHAR(255),
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        # Execute table creation
        cursor.execute(create_raw_data_table)
        cursor.execute(create_line_items_table)
        cursor.execute(create_metadata_table)
        
        # Create indexes for better performance
        create_indexes = [
            # Raw data table indexes
            f"CREATE INDEX IF NOT EXISTS idx_{self.raw_data_table}_invoice_id ON {self.raw_data_table}(fullbay_invoice_id);",
            f"CREATE INDEX IF NOT EXISTS idx_{self.raw_data_table}_processed ON {self.raw_data_table}(processed);",
            f"CREATE INDEX IF NOT EXISTS idx_{self.raw_data_table}_ingestion ON {self.raw_data_table}(ingestion_timestamp);",
            
            # Line items table indexes  
            f"CREATE INDEX IF NOT EXISTS idx_{self.line_items_table}_invoice_id ON {self.line_items_table}(fullbay_invoice_id);",
            f"CREATE INDEX IF NOT EXISTS idx_{self.line_items_table}_invoice_date ON {self.line_items_table}(invoice_date);",
            f"CREATE INDEX IF NOT EXISTS idx_{self.line_items_table}_customer_id ON {self.line_items_table}(customer_id);",
            f"CREATE INDEX IF NOT EXISTS idx_{self.line_items_table}_repair_order ON {self.line_items_table}(repair_order_number);",
            f"CREATE INDEX IF NOT EXISTS idx_{self.line_items_table}_unit_vin ON {self.line_items_table}(unit_vin);",
            f"CREATE INDEX IF NOT EXISTS idx_{self.line_items_table}_line_type ON {self.line_items_table}(line_item_type);",
            f"CREATE INDEX IF NOT EXISTS idx_{self.line_items_table}_part_number ON {self.line_items_table}(shop_part_number);",
            f"CREATE INDEX IF NOT EXISTS idx_{self.line_items_table}_technician ON {self.line_items_table}(assigned_technician);",
            f"CREATE INDEX IF NOT EXISTS idx_{self.line_items_table}_ingestion ON {self.line_items_table}(ingestion_timestamp);",
            
            # Metadata table indexes
            f"CREATE INDEX IF NOT EXISTS idx_{self.metadata_table}_execution_id ON {self.metadata_table}(execution_id);",
            f"CREATE INDEX IF NOT EXISTS idx_{self.metadata_table}_start_time ON {self.metadata_table}(start_time);"
        ]
        
        for index_sql in create_indexes:
            cursor.execute(index_sql)
        
        logger.info("Database tables and indexes created/verified")
    
    def insert_records(self, records: List[Dict[str, Any]]) -> int:
        """
        Insert Fullbay invoice records into the two-table flattened structure.
        
        Process:
        1. Store raw JSON in fullbay_raw_data table
        2. Flatten each invoice into multiple line items
        3. Store flattened data in fullbay_line_items table
        
        Args:
            records: List of Fullbay invoice JSON records
            
        Returns:
            Total number of line items created
            
        Raises:
            Exception: If insertion fails
        """
        if not records:
            logger.info("No records to insert")
            return 0
        
        total_line_items_created = 0
        raw_records_stored = 0
        
        processing_start_time = datetime.now(timezone.utc)
        errors_count = 0
        
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    logger.info(f"Processing {len(records)} invoice records...")
                    
                    for record in records:
                        try:
                            # Step 1: Store raw JSON data
                            raw_data_id = self._store_raw_data(cursor, record)
                            raw_records_stored += 1
                            
                            # Step 2: Flatten invoice into line items
                            line_items = self._flatten_invoice_to_line_items(record, raw_data_id)
                            
                            # Step 3: Insert flattened line items
                            line_items_count = self._insert_line_items(cursor, line_items)
                            total_line_items_created += line_items_count
                            
                            logger.info(f"Invoice {record.get('primaryKey', 'unknown')}: "
                                      f"Created {line_items_count} line items")
                            
                        except Exception as record_error:
                            errors_count += 1
                            logger.warning(f"Failed to process invoice {record.get('primaryKey', 'unknown')}: {record_error}")
                            # Mark raw data as having processing errors
                            try:
                                cursor.execute(f"""
                                    UPDATE {self.raw_data_table} 
                                    SET processing_errors = %s 
                                    WHERE fullbay_invoice_id = %s
                                """, (str(record_error), record.get('primaryKey')))
                            except:
                                pass
                            continue
                    
                    # Commit all insertions
                    conn.commit()
                    logger.info(f"Successfully processed {raw_records_stored} invoices, "
                              f"created {total_line_items_created} line items")
                    
                    # Send monitoring metrics
                    self.send_ingestion_summary_metrics(
                        processing_start_time, 
                        raw_records_stored, 
                        total_line_items_created,
                        errors_count
                    )
                    
        except Exception as e:
            logger.error(f"Batch processing failed: {e}")
            raise Exception(f"Failed to process records: {e}")
        
        return total_line_items_created
    
    def _store_raw_data(self, cursor, record: Dict[str, Any]) -> int:
        """
        Store raw JSON invoice data in the backup table.
        
        Args:
            cursor: Database cursor
            record: Raw Fullbay invoice JSON
            
        Returns:
            ID of the inserted raw data record
        """
        fullbay_invoice_id = record.get('primaryKey')
        if not fullbay_invoice_id:
            raise ValueError("Invoice missing primaryKey")
        
        insert_sql = f"""
        INSERT INTO {self.raw_data_table} (
            fullbay_invoice_id, 
            raw_json_data, 
            processed
        ) VALUES (%s, %s, %s)
        ON CONFLICT (fullbay_invoice_id) DO UPDATE SET
            raw_json_data = EXCLUDED.raw_json_data,
            ingestion_timestamp = CURRENT_TIMESTAMP,
            processed = FALSE,
            processing_errors = NULL
        RETURNING id;
        """
        
        cursor.execute(insert_sql, (fullbay_invoice_id, json.dumps(record), False))
        raw_data_id = cursor.fetchone()['id']
        
        logger.debug(f"Stored raw data for invoice {fullbay_invoice_id}, ID: {raw_data_id}")
        return raw_data_id
    
    def _flatten_invoice_to_line_items(self, record: Dict[str, Any], raw_data_id: int) -> List[Dict[str, Any]]:
        """
        Flatten a Fullbay invoice JSON into multiple line item records.
        
        This is the main flattening logic that converts:
        1 Invoice JSON → Multiple line item records
        
        Args:
            record: Raw Fullbay invoice JSON
            raw_data_id: ID of the raw data record
            
        Returns:
            List of flattened line item records ready for database insertion
        """
        line_items = []
        
        try:
            # Step 1: Validate input data quality
            validation_results = self._validate_invoice_data(record)
            if validation_results['critical_errors']:
                raise ValueError(f"Critical validation errors: {validation_results['critical_errors']}")
            
            # Log warnings for non-critical issues
            if validation_results['warnings']:
                logger.warning(f"Data quality warnings for invoice {record.get('primaryKey', 'unknown')}: "
                             f"{validation_results['warnings']}")
            
            # Extract common context that will be repeated on every row
            invoice_context = self._extract_invoice_context(record)
            
            # Get service order data
            service_order = record.get('ServiceOrder', {})
            complaints = service_order.get('Complaints', [])
            
            logger.info(f"Processing invoice {invoice_context['fullbay_invoice_id']} with {len(complaints)} complaints")
            
            # Process each complaint
            for complaint in complaints:
                complaint_context = self._extract_complaint_context(complaint, invoice_context, service_order)
                
                corrections = complaint.get('Corrections', [])
                
                # Process each correction within the complaint
                for correction in corrections:
                    correction_context = self._extract_correction_context(correction, complaint_context)
                    
                    # Process parts for this correction
                    parts = correction.get('Parts', [])
                    if parts:
                        parts_line_items = self._process_parts(parts, correction_context)
                        line_items.extend(parts_line_items)
                    
                    # Process labor for this correction
                    labor_line_items = self._process_labor(correction, complaint, correction_context)
                    line_items.extend(labor_line_items)
            
            # Step 2: Add SHOP SUPPLIES line item (always create, even if 0)
            shop_supplies_item = self._create_shop_supplies_line_item(invoice_context, raw_data_id)
            line_items.append(shop_supplies_item)
            logger.info(f"Added SHOP SUPPLIES line item for invoice {invoice_context['fullbay_invoice_id']}")
            
            # Step 3: Add MISC CHARGES line items if miscCharges array exists
            misc_charges = record.get('miscCharges', [])
            if misc_charges:
                for misc_charge in misc_charges:
                    misc_line_item = self._create_misc_charge_line_item(misc_charge, invoice_context, raw_data_id)
                    line_items.append(misc_line_item)
                logger.info(f"Added {len(misc_charges)} MISC CHARGES line items for invoice {invoice_context['fullbay_invoice_id']}")
            
            # Step 4: Validate generated line items
            line_items = self._validate_and_clean_line_items(line_items)
            
            # Step 5: Add raw_data_id to all line items
            for item in line_items:
                item['raw_data_id'] = raw_data_id
            
            logger.info(f"Flattened invoice into {len(line_items)} line items")
            return line_items
            
        except Exception as e:
            logger.error(f"Error flattening invoice {record.get('primaryKey', 'unknown')}: {e}")
            raise
    
    def _extract_invoice_context(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract invoice-level context that will be repeated on every line item.
        
        Args:
            record: Raw Fullbay invoice JSON
            
        Returns:
            Dict containing invoice context fields
        """
        service_order = record.get('ServiceOrder', {})
        customer = record.get('Customer', {})
        so_customer = service_order.get('Customer', {})
        unit = service_order.get('Unit', {})
        
        # Use service order customer if available, otherwise use invoice customer
        customer_data = so_customer if so_customer else customer
        
        context = {
            # Invoice level
            'fullbay_invoice_id': record.get('primaryKey'),
            'invoice_number': record.get('invoiceNumber'),
            'invoice_date': self._parse_date(record.get('invoiceDate')),
            'due_date': self._parse_date(record.get('dueDate')),
    
            
            # Shop info
            'shop_title': record.get('shopTitle'),
            'shop_email': record.get('shopEmail'),
            'shop_address': record.get('shopPhysicalAddress'),
            
            # Customer info
            'customer_id': customer_data.get('customerId'),
            'customer_title': customer_data.get('title'),
            'customer_external_id': customer_data.get('externalId'),
            'customer_main_phone': customer_data.get('mainPhone'),
            'customer_secondary_phone': customer_data.get('secondaryPhone'),
            'customer_billing_address': record.get('customerBillingAddress'),
            
            # Service order info
            'fullbay_service_order_id': service_order.get('primaryKey'),
            'repair_order_number': service_order.get('repairOrderNumber'),
            'service_order_created': self._parse_datetime(service_order.get('created')),
            'service_order_start_date': self._parse_datetime(service_order.get('startDateTime')),
            'service_order_completion_date': self._parse_datetime(service_order.get('completionDateTime')),
            
            # Unit/Vehicle info
            'unit_id': unit.get('customerUnitId'),
            'unit': unit.get('number'),
            'unit_type': unit.get('type'),
            'unit_year': unit.get('year'),
            'unit_make': unit.get('make'),
            'unit_model': unit.get('model'),
            'unit_vin': unit.get('vin'),
            'unit_license_plate': unit.get('licensePlate'),
            
            # Primary technician
            'primary_technician': service_order.get('technician'),
            'primary_technician_number': service_order.get('technicianNumber'),
            
                         # Service order totals (for context)
             'so_supplies_total': self._parse_decimal(record.get('suppliesTotal')),  # Invoice-level supplies total
        }
        
        return context
    
    def _extract_complaint_context(self, complaint: Dict[str, Any], invoice_context: Dict[str, Any], service_order: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract complaint-level context.
        
        Args:
            complaint: Complaint JSON object
            invoice_context: Invoice context from _extract_invoice_context
            service_order: Service order JSON object
            
        Returns:
            Dict containing complaint context (includes invoice context)
        """
        context = invoice_context.copy()
        
        # Add complaint-specific data
        context.update({
            'fullbay_complaint_id': complaint.get('primaryKey'),
            'complaint_type': complaint.get('type'),
            'complaint_subtype': complaint.get('subType'),
            'complaint_note': complaint.get('note'),
            'complaint_cause': complaint.get('cause'),
            'complaint_authorized': complaint.get('authorized') == 'Yes',
        })
        
        return context
    
    def _extract_correction_context(self, correction: Dict[str, Any], complaint_context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract correction-level context.
        
        Args:
            correction: Correction JSON object
            complaint_context: Complaint context from _extract_complaint_context
            
        Returns:
            Dict containing correction context (includes complaint context)
        """
        context = complaint_context.copy()
        
        # Add correction-specific data
        context.update({
            'fullbay_correction_id': correction.get('primaryKey'),
            'correction_title': correction.get('title'),
            'global_component': correction.get('globalComponent'),
            'global_system': correction.get('globalSystem'),
            'global_service': correction.get('globalService'),
            'recommended_correction': correction.get('recommendedCorrection'),
            'service_description': correction.get('actualCorrection'),
        })
        
        return context
    
    def _process_parts(self, parts: List[Dict[str, Any]], context: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Process parts for a correction, grouping identical parts correctly.
        
        Grouping Logic: Group parts ONLY if:
        - Same shop_part_number
        - Same unit_price  
        - Same correction (already guaranteed since we're processing one correction)
        - Effective quantity > 0 (quantity - returned_qty > 0)
        
        Args:
            parts: List of part objects from a correction
            context: Correction context
            
        Returns:
            List of part line items (only for parts with positive effective quantity)
        """
        if not parts:
            return []
        
        # Group parts by part_number + unit_price
        parts_groups = {}
        
        for part in parts:
            shop_part_number = part.get('shopPartNumber', '')
            selling_price = self._parse_decimal(part.get('sellingPrice'))
            
            # Calculate effective quantity (quantity - returned_qty)
            original_qty = self._parse_decimal(part.get('quantity', 0))
            returned_qty = self._parse_decimal(part.get('returnedQuantity', 0))
            effective_qty = original_qty - returned_qty
            
            # Skip parts with effective quantity <= 0
            if effective_qty <= 0:
                continue
            
            # Create grouping key
            group_key = f"{shop_part_number}|{selling_price}"
            
            if group_key not in parts_groups:
                             parts_groups[group_key] = {
                 'parts': [],
                 'total_effective_quantity': 0,
                 'total_price': 0
             }
            
            parts_groups[group_key]['parts'].append(part)
            parts_groups[group_key]['total_effective_quantity'] += effective_qty
            
            parts_groups[group_key]['total_price'] += self._parse_decimal(part.get('sellingPrice', 0)) * effective_qty
        
        # Create line items from grouped parts
        line_items = []
        
        for group_key, group_data in parts_groups.items():
            # Use the first part in the group as the template
            template_part = group_data['parts'][0]
            
            line_item = context.copy()
            line_item.update({
                'line_item_type': self._classify_part_type(template_part),
                'fullbay_part_id': template_part.get('primaryKey'),
                'part_description': template_part.get('description'),
                'shop_part_number': template_part.get('shopPartNumber'),
                'vendor_part_number': template_part.get('vendorPartNumber'),
                'part_category': template_part.get('partCategory'),
                
                # Grouped quantities and totals (using effective quantity)
                'quantity': group_data['total_effective_quantity'],  # Effective quantity (original - returned)
                'to_be_returned_quantity': sum(self._parse_decimal(p.get('toBeReturnedQuantity', 0)) for p in group_data['parts']),
                'returned_quantity': sum(self._parse_decimal(p.get('returnedQuantity', 0)) for p in group_data['parts']),
                
                                # Financial details
                'unit_cost': self._parse_decimal(template_part.get('cost')),
                'unit_price': self._parse_decimal(template_part.get('sellingPrice')),
                'line_total': group_data['total_price'],
                'price_overridden': template_part.get('sellingPriceOverridden') == 'Yes',
                
                                 # Classification
                 'taxable': template_part.get('taxable') == 'Yes',
                 'inventory_item': template_part.get('inventory') == 'Yes',
                 'core_type': template_part.get('coreType'),
                 'sublet': template_part.get('sublet') == 'Yes',
            })
            
            line_items.append(line_item)
        
        logger.debug(f"Grouped {len(parts)} parts into {len(line_items)} line items for correction {context.get('fullbay_correction_id')}")
        return line_items
    
    def _process_labor(self, correction: Dict[str, Any], complaint: Dict[str, Any], context: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Process labor for a correction, creating separate rows per technician.
        
        Args:
            correction: Correction JSON object
            complaint: Parent complaint JSON object
            context: Correction context
            
        Returns:
            List of labor line items (one per technician)
        """
        line_items = []
        
        # Get assigned technicians from the complaint
        assigned_technicians = complaint.get('AssignedTechnicians', [])
        
        if not assigned_technicians:
            # No specific technician assignments, but there's labor - create one labor row
            if self._parse_decimal(correction.get('laborHoursTotal', 0)) > 0:
                line_item = context.copy()
                line_item.update({
                    'line_item_type': 'LABOR',
                    'labor_description': correction.get('actualCorrection') or correction.get('recommendedCorrection'),
                    'labor_rate_type': correction.get('laborRate'),
                    'assigned_technician': context.get('primary_technician'),
                    'assigned_technician_number': context.get('primary_technician_number'),
                    
                                         'so_hours': self._parse_decimal(correction.get('laborHoursTotal')),  # Use total if no individual hours
                     'technician_portion': 100,
                    
                                            'line_total': self._parse_decimal(correction.get('laborTotal')),
                        'taxable': correction.get('taxable') != 'No',
                })
                line_items.append(line_item)
        else:
            # Create separate labor row for each technician
            for tech_assignment in assigned_technicians:
                line_item = context.copy()
                line_item.update({
                    'line_item_type': 'LABOR',
                    'labor_description': correction.get('actualCorrection') or correction.get('recommendedCorrection'),
                    'labor_rate_type': correction.get('laborRate'),
                    'assigned_technician': tech_assignment.get('technician'),
                    'assigned_technician_number': tech_assignment.get('technicianNumber'),
                    
                                         'so_hours': self._parse_decimal(tech_assignment.get('actualHours')),  # This tech's hours
                     'technician_portion': tech_assignment.get('portion', 100),
                    
                    # Calculate this technician's portion of the labor cost
                                            'line_total': self._calculate_technician_labor_cost(
                            correction.get('laborTotal'),
                            tech_assignment.get('portion', 100)
                        ),
                    'taxable': correction.get('taxable') != 'No',
                })
                line_items.append(line_item)
        
        logger.debug(f"Created {len(line_items)} labor line items for correction {context.get('fullbay_correction_id')}")
        return line_items
    
    def _create_shop_supplies_line_item(self, invoice_context: Dict[str, Any], raw_data_id: int) -> Dict[str, Any]:
        """
        Create a SHOP SUPPLIES line item from invoice-level suppliesTotal.
        
        This line item:
        - Is NOT associated with any specific correction
        - Gets the suppliesTotal value from the invoice level
        - Has line_item_type = 'SHOP SUPPLIES'
        - Does NOT have service_description (no correction context)
        
        Args:
            invoice_context: Invoice context from _extract_invoice_context
            raw_data_id: ID of the raw data record
            
        Returns:
            SHOP SUPPLIES line item dictionary
        """
        # Start with invoice context (includes customer, unit, shop info)
        line_item = invoice_context.copy()
        
        # Override/add supplies-specific fields
        line_item.update({
            'line_item_type': 'SHOP SUPPLIES',
            'fullbay_part_id': None,  # No specific part
            'part_description': 'Shop Supplies',  # Generic description
            'shop_part_number': None,  # No part number
            'vendor_part_number': None,  # No vendor part number
            'part_category': 'Shop Supplies',  # Generic category
            
            # No labor fields
            'labor_description': None,
            'labor_rate_type': None,
            'assigned_technician': None,
            'assigned_technician_number': None,
            
                         # Quantity and financial details
             'quantity': 1,  # Always 1 for supplies total
             'to_be_returned_quantity': 0,
             'returned_quantity': 0,
             'so_hours': None,
             'technician_portion': None,
            
            # Financial details from invoice suppliesTotal
            'unit_cost': None,  # No unit cost for supplies total
            'unit_price': None,  # No unit price for supplies total
            'line_total_cost': None,  # No cost breakdown
            'line_total': self._parse_decimal(invoice_context.get('so_supplies_total', 0)),
            'price_overridden': False,
            
                         # Classification
             'taxable': True,  # Assume taxable
             'tax_rate': None,
             'inventory_item': False,
             'core_type': None,
             'sublet': False,
            
            # No correction context
            'fullbay_correction_id': None,
            'correction_title': None,
            'global_component': None,
            'global_system': None,
            'global_service': None,
            'recommended_correction': None,
            'service_description': None,  # No correction context for supplies
        })
        
        return line_item
     
    def _create_misc_charge_line_item(self, misc_charge: Dict[str, Any], invoice_context: Dict[str, Any], raw_data_id: int) -> Dict[str, Any]:
        """
        Create a MISC CHARGE line item from invoice-level miscCharges array.
        
        This line item:
        - Is NOT associated with any specific correction
        - Gets the amount value from the misc charge object
        - Has line_item_type = 'MISC(QUICKBOOKSITEMTYPE)'
        - Does NOT have service_description (no correction context)
        
        Args:
            misc_charge: Misc charge object from miscCharges array
            invoice_context: Invoice context from _extract_invoice_context
            raw_data_id: ID of the raw data record
            
        Returns:
            MISC CHARGE line item dictionary
        """
        # Start with invoice context (includes customer, unit, shop info)
        line_item = invoice_context.copy()
        
        # Get the quickbooks item type for the line item type
        quickbooks_item_type = misc_charge.get('quickbooksItemType', 'UNKNOWN')
        line_item_type = f"MISC({quickbooks_item_type})"
        
        # Override/add misc charge-specific fields
        line_item.update({
            'line_item_type': line_item_type,
            'fullbay_part_id': None,  # No specific part
            'part_description': f"Misc Charge - {quickbooks_item_type}",  # Description with type
            'shop_part_number': None,  # No part number
            'vendor_part_number': None,  # No vendor part number
            'part_category': f"Misc Charges - {quickbooks_item_type}",  # Category with type
            
            # No labor fields
            'labor_description': None,
            'labor_rate_type': None,
            'assigned_technician': None,
            'assigned_technician_number': None,
            
            # Quantity and financial details
            'quantity': 1,  # Always 1 for misc charges
            'to_be_returned_quantity': 0,
            'returned_quantity': 0,
            'so_hours': None,
            'technician_portion': None,
            
            # Financial details from misc charge amount
            'unit_cost': None,  # No unit cost for misc charges
            'unit_price': None,  # No unit price for misc charges
            'line_total': self._parse_decimal(misc_charge.get('amount', 0)),
            'price_overridden': False,
            
            # Classification
            'taxable': True,  # Assume taxable
            'tax_rate': None,
            'inventory_item': False,
            'core_type': None,
            'sublet': False,
            
            # No correction context
            'fullbay_correction_id': None,
            'correction_title': None,
            'global_component': None,
            'global_system': None,
            'global_service': None,
            'recommended_correction': None,
            'service_description': None,  # No correction context for misc charges
        })
        
        return line_item
     
    def _classify_part_type(self, part: Dict[str, Any]) -> str:
        """
        Classify a part into the appropriate line item type.
        
        Args:
            part: Part JSON object
            
        Returns:
            Line item type ('PART', 'SUPPLY', 'FREIGHT', 'MISC')
        """
        # IMPORTANT: All items from the parts array are always classified as 'PART'
        # This ensures consistent classification and proper service description mapping
        return 'PART'
    
    def _calculate_technician_labor_cost(self, total_labor_cost: Any, portion: int) -> float:
        """
        Calculate a technician's portion of labor cost.
        
        Args:
            total_labor_cost: Total labor cost for the correction
            portion: Technician's percentage (e.g., 100 for 100%)
            
        Returns:
            Technician's portion of the cost
        """
        total_cost = self._parse_decimal(total_labor_cost) or 0
        portion_decimal = (portion or 100) / 100.0
        return total_cost * portion_decimal
    
    def _calculate_line_tax(self, line_item: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate tax for a line item and update the line item with tax calculations.
        
        Tax calculation logic:
        - If taxable = True and tax_rate exists, calculate tax
        - Tax rate is a whole number (e.g., 6 = 6%)
        - line_tax = line_total × (tax_rate / 100)
        - sales_total = line_total + line_tax
        
        Args:
            line_item: Line item dictionary
            
        Returns:
            Updated line item with tax calculations
        """
        # Start with the original line item
        updated_item = line_item.copy()
        
        # Get the line total price
        line_total = self._parse_decimal(line_item.get('line_total', 0)) or 0
        
        # Check if this line is taxable and has a tax rate
        is_taxable = line_item.get('taxable', True)
        tax_rate = self._parse_decimal(line_item.get('tax_rate', 0)) or 0
        
        if is_taxable and tax_rate > 0 and line_total > 0:
            # Calculate tax amount (tax_rate is whole number, so divide by 100)
            line_tax = line_total * (tax_rate / 100.0)
            sales_total = line_total + line_tax
            
            # Update the line item with tax calculations
            updated_item.update({
                'line_tax': round(line_tax, 2),
                'sales_total': round(sales_total, 2)
            })
        else:
            # No tax applicable
            updated_item.update({
                'line_tax': 0.0,
                'sales_total': line_total
            })
        
        return updated_item
    
    def _parse_date(self, date_str: Any) -> Optional[str]:
        """Parse date string to YYYY-MM-DD format."""
        if not date_str:
            return None
        try:
            # Handle different date formats
            if isinstance(date_str, str):
                # Try parsing YYYY-MM-DD format
                if len(date_str) == 10 and '-' in date_str:
                    return date_str
            return str(date_str)[:10] if date_str else None
        except:
            return None
    
    def _parse_datetime(self, datetime_str: Any) -> Optional[datetime]:
        """Parse datetime string to datetime object."""
        if not datetime_str:
            return None
        try:
            # Handle different timestamp formats  
            formats = [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d %H:%M:%S.%f",
                "%Y-%m-%dT%H:%M:%S.%f"
            ]
            
            for fmt in formats:
                try:
                    return datetime.strptime(str(datetime_str), fmt).replace(tzinfo=timezone.utc)
                except ValueError:
                    continue
                    
            return None
        except:
            return None
    
    def _parse_decimal(self, value: Any) -> float:
        """Parse decimal/numeric values."""
        if value is None:
            return 0.0
        try:
            if isinstance(value, (int, float)):
                return float(value)
            elif isinstance(value, str):
                # Remove currency symbols and commas
                cleaned = value.replace("$", "").replace(",", "").strip()
                if cleaned:
                    return float(cleaned)
            return 0.0
        except (ValueError, TypeError):
            return 0.0
    
    def _insert_line_items(self, cursor, line_items: List[Dict[str, Any]]) -> int:
        """
        Insert flattened line items into the line_items table.
        
        Args:
            cursor: Database cursor
            line_items: List of flattened line item records
            
        Returns:
            Number of line items successfully inserted
        """
        if not line_items:
            return 0
        
        inserted_count = 0
        
                 # Prepare SQL statement - UPDATED 73-COLUMN SCHEMA
        insert_sql = f"""
        INSERT INTO {self.line_items_table} (
                    raw_data_id, fullbay_invoice_id, invoice_number, invoice_date, due_date,
        shop_title, shop_email, shop_address,
            customer_id, customer_title, customer_external_id, customer_main_phone, 
            customer_secondary_phone, customer_billing_address,
            fullbay_service_order_id, repair_order_number, service_order_created, 
            service_order_start_date, service_order_completion_date,
            unit_id, unit, unit_type, unit_year, unit_make, unit_model, unit_vin, unit_license_plate,
            primary_technician, primary_technician_number,
            fullbay_complaint_id, complaint_type, complaint_subtype, complaint_note, 
            complaint_cause, complaint_authorized,
            fullbay_correction_id, correction_title, global_component, global_system, 
            global_service, recommended_correction, service_description,
            line_item_type, fullbay_part_id, part_description, shop_part_number, 
            vendor_part_number, part_category,
            labor_description, labor_rate_type, assigned_technician, assigned_technician_number,
                                                 quantity, to_be_returned_quantity, returned_quantity,
                        so_hours, technician_portion,
                        unit_cost, unit_price, line_total, price_overridden,
                        taxable, tax_rate, line_tax, sales_total, inventory_item, core_type, sublet,
                          so_supplies_total,
            ingestion_timestamp, ingestion_source
        )         VALUES (
            %(raw_data_id)s, %(fullbay_invoice_id)s, %(invoice_number)s, %(invoice_date)s, %(due_date)s,
            %(shop_title)s, %(shop_email)s, %(shop_address)s,
            %(customer_id)s, %(customer_title)s, %(customer_external_id)s, %(customer_main_phone)s,
            %(customer_secondary_phone)s, %(customer_billing_address)s,
            %(fullbay_service_order_id)s, %(repair_order_number)s, %(service_order_created)s,
            %(service_order_start_date)s, %(service_order_completion_date)s,
            %(unit_id)s, %(unit)s, %(unit_type)s, %(unit_year)s, %(unit_make)s, %(unit_model)s, %(unit_vin)s, %(unit_license_plate)s,
            %(primary_technician)s, %(primary_technician_number)s,
            %(fullbay_complaint_id)s, %(complaint_type)s, %(complaint_subtype)s, %(complaint_note)s,
            %(complaint_cause)s, %(complaint_authorized)s,
            %(fullbay_correction_id)s, %(correction_title)s, %(global_component)s, %(global_system)s,
            %(global_service)s, %(recommended_correction)s, %(service_description)s,
            %(line_item_type)s, %(fullbay_part_id)s, %(part_description)s, %(shop_part_number)s,
            %(vendor_part_number)s, %(part_category)s,
            %(labor_description)s, %(labor_rate_type)s, %(assigned_technician)s, %(assigned_technician_number)s,
                                                 %(quantity)s, %(to_be_returned_quantity)s, %(returned_quantity)s,
                        %(so_hours)s, %(technician_portion)s,
                        %(unit_cost)s, %(unit_price)s, %(line_total)s, %(price_overridden)s,
                        %(taxable)s, %(tax_rate)s, %(line_tax)s, %(sales_total)s, %(inventory_item)s, %(core_type)s, %(sublet)s,
                          %(so_supplies_total)s,
            CURRENT_TIMESTAMP, 'fullbay_api'
        )
        """
        
        try:
            for line_item in line_items:
                try:
                    # Ensure all required fields have values (None for missing fields)
                    processed_item = self._prepare_line_item_for_insertion(line_item)
                    
                    cursor.execute(insert_sql, processed_item)
                    inserted_count += 1
                    
                except Exception as item_error:
                    logger.warning(f"Failed to insert line item {line_item.get('line_item_type', 'unknown')} "
                                 f"for invoice {line_item.get('fullbay_invoice_id', 'unknown')}: {item_error}")
                    continue
            
            logger.info(f"Successfully inserted {inserted_count} line items")
            return inserted_count
            
        except Exception as e:
            logger.error(f"Error during line item insertion: {e}")
            raise
    
    def _prepare_line_item_for_insertion(self, line_item: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepare a line item dict for database insertion by ensuring all fields are present.
        
        Args:
            line_item: Line item dict from flattening process
            
        Returns:
            Dict with all database fields, using None for missing values
        """
                 # Define all possible fields with their default values - UPDATED 73-COLUMN SCHEMA
        default_fields = {
            'raw_data_id': None,
            'fullbay_invoice_id': None,
            'invoice_number': None,
            'invoice_date': None,
            'due_date': None,
    
            'shop_title': None,
            'shop_email': None,
            'shop_address': None,
            'customer_id': None,
            'customer_title': None,
            'customer_external_id': None,
            'customer_main_phone': None,
            'customer_secondary_phone': None,
            'customer_billing_address': None,
            'fullbay_service_order_id': None,
            'repair_order_number': None,
            'service_order_created': None,
            'service_order_start_date': None,
            'service_order_completion_date': None,
            'unit_id': None,
            'unit': None,
            'unit_type': None,
            'unit_year': None,
            'unit_make': None,
            'unit_model': None,
            'unit_vin': None,
            'unit_license_plate': None,
            'primary_technician': None,
            'primary_technician_number': None,
            'fullbay_complaint_id': None,
            'complaint_type': None,
            'complaint_subtype': None,
            'complaint_note': None,
            'complaint_cause': None,
            'complaint_authorized': False,
            'fullbay_correction_id': None,
            'correction_title': None,
            'global_component': None,
            'global_system': None,
            'global_service': None,
            'recommended_correction': None,
            'service_description': None,
            'line_item_type': None,
            'fullbay_part_id': None,
            'part_description': None,
            'shop_part_number': None,
            'vendor_part_number': None,
            'part_category': None,
            'labor_description': None,
            'labor_rate_type': None,
            'assigned_technician': None,
            'assigned_technician_number': None,
                         'quantity': None,
             'to_be_returned_quantity': None,
             'returned_quantity': None,
                          'so_hours': None,
             'technician_portion': None,
                                     'unit_cost': None,
                        'unit_price': None,
                        'line_total': None,
                        'price_overridden': False,
            'taxable': True,
            'tax_rate': None,
            'tax_amount': None,
            'line_tax': None,  # Calculated tax amount for this line
            'sales_total': None,  # Line total + line tax
            'inventory_item': False,
                         'core_type': None,
             'sublet': False,
                          'so_supplies_total': None,  # Invoice-level supplies total
        }
        
        # Start with defaults and update with actual values
        prepared_item = default_fields.copy()
        prepared_item.update(line_item)
        
        # Calculate tax for this line item
        prepared_item = self._calculate_line_tax(prepared_item)
        
        return prepared_item
    
    def _process_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process and validate a single record for database insertion.
        
        Args:
            record: Raw record from API
            
        Returns:
            Processed record ready for database insertion
            
        Raises:
            ValueError: If record is invalid
        """
        # Validate required fields
        if "id" not in record:
            raise ValueError("Record missing required 'id' field")
        
        # Extract values with defaults
        processed = {
            "fullbay_id": str(record["id"]),
            "work_order_number": record.get("work_order_number"),
            "customer_id": str(record.get("customer_id")) if record.get("customer_id") else None,
            "customer_name": record.get("customer_name"),
            "vehicle_info": self._extract_vehicle_info(record),
            "status": record.get("status"),
            "created_at": self._parse_timestamp(record.get("created_at")),
            "updated_at": self._parse_timestamp(record.get("updated_at")),
            "total_amount": self._parse_decimal(record.get("total_amount")),
            "labor_amount": self._parse_decimal(record.get("labor_amount")),
            "parts_amount": self._parse_decimal(record.get("parts_amount")),
            "tax_amount": self._parse_decimal(record.get("tax_amount")),
            "raw_data": record,  # Store complete raw data as JSON
            "ingestion_timestamp": record.get("_ingestion_timestamp"),
            "ingestion_source": record.get("_ingestion_source", "fullbay_api")
        }
        
        return processed
    
    def _extract_vehicle_info(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Extract vehicle information from record.
        
        Args:
            record: Raw record
            
        Returns:
            Vehicle info dict or None
        """
        vehicle_fields = ["vehicle", "vehicle_info", "asset"]
        
        for field in vehicle_fields:
            if field in record and record[field]:
                return record[field]
        
        # Try to construct from individual fields
        vehicle_info = {}
        for field in ["make", "model", "year", "vin", "license_plate"]:
            if field in record and record[field]:
                vehicle_info[field] = record[field]
        
        return vehicle_info if vehicle_info else None
    
    def _parse_timestamp(self, timestamp_str: Optional[str]) -> Optional[datetime]:
        """
        Parse timestamp string to datetime object.
        
        Args:
            timestamp_str: Timestamp string
            
        Returns:
            Parsed datetime or None
        """
        if not timestamp_str:
            return None
        
        try:
            # Handle different timestamp formats
            formats = [
                "%Y-%m-%dT%H:%M:%S.%fZ",
                "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%dT%H:%M:%S.%f",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d %H:%M:%S"
            ]
            
            for fmt in formats:
                try:
                    return datetime.strptime(timestamp_str, fmt).replace(tzinfo=timezone.utc)
                except ValueError:
                    continue
            
            logger.warning(f"Could not parse timestamp: {timestamp_str}")
            return None
            
        except Exception as e:
            logger.warning(f"Error parsing timestamp '{timestamp_str}': {e}")
            return None
    
    def _parse_decimal(self, value: Any) -> Optional[float]:
        """
        Parse decimal/monetary values.
        
        Args:
            value: Value to parse
            
        Returns:
            Parsed decimal value or None
        """
        if value is None:
            return None
        
        try:
            if isinstance(value, (int, float)):
                return float(value)
            elif isinstance(value, str):
                # Remove currency symbols and commas
                cleaned = value.replace("$", "").replace(",", "").strip()
                if cleaned:
                    return float(cleaned)
            return None
        except (ValueError, TypeError):
            logger.warning(f"Could not parse decimal value: {value}")
            return None
    
    def log_execution_metadata(
        self, 
        execution_id: str, 
        start_time: datetime, 
        status: str,
        **kwargs
    ):
        """
        Log execution metadata for monitoring and debugging.
        
        Args:
            execution_id: Unique execution identifier
            start_time: Execution start time
            status: Execution status (SUCCESS, ERROR, etc.)
            **kwargs: Additional metadata
        """
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    insert_sql = f"""
                    INSERT INTO {self.metadata_table} (
                        execution_id, start_time, end_time, status,
                        records_processed, records_inserted, records_updated,
                        error_message, api_endpoint
                    ) VALUES (
                        %(execution_id)s, %(start_time)s, %(end_time)s, %(status)s,
                        %(records_processed)s, %(records_inserted)s, %(records_updated)s,
                        %(error_message)s, %(api_endpoint)s
                    )
                    ON CONFLICT (execution_id) DO UPDATE SET
                        end_time = EXCLUDED.end_time,
                        status = EXCLUDED.status,
                        records_processed = EXCLUDED.records_processed,
                        records_inserted = EXCLUDED.records_inserted,
                        records_updated = EXCLUDED.records_updated,
                        error_message = EXCLUDED.error_message
                    """
                    
                    cursor.execute(insert_sql, {
                        "execution_id": execution_id,
                        "start_time": start_time,
                        "end_time": kwargs.get("end_time", datetime.now(timezone.utc)),
                        "status": status,
                        "records_processed": kwargs.get("records_processed", 0),
                        "records_inserted": kwargs.get("records_inserted", 0),
                        "records_updated": kwargs.get("records_updated", 0),
                        "error_message": kwargs.get("error_message"),
                        "api_endpoint": kwargs.get("api_endpoint", "work-orders")
                    })
                    
                    conn.commit()
                    
        except Exception as e:
            logger.error(f"Failed to log execution metadata: {e}")
    
    def close(self):
        """Close database connection pool."""
        if self.connection_pool:
            self.connection_pool.closeall()
            logger.info("Database connection pool closed")
    
    def test_connection(self) -> bool:
        """
        Test database connection.
        
        Returns:
            True if connection test is successful
        """
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    result = cursor.fetchone()
                    return result[0] == 1
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False
    
    # ===============================================
    # DATA VALIDATION AND QUALITY CHECKS
    # ===============================================
    
    def _validate_invoice_data(self, record: Dict[str, Any]) -> Dict[str, List[str]]:
        """
        Validate invoice data quality before processing.
        
        Args:
            record: Raw Fullbay invoice JSON
            
        Returns:
            Dictionary with 'critical_errors' and 'warnings' lists
        """
        critical_errors = []
        warnings = []
        
        # Critical validations (will prevent processing)
        if not record.get('primaryKey'):
            critical_errors.append("Missing primaryKey (invoice ID)")
        
        if not record.get('invoiceNumber'):
            warnings.append("Missing invoice number")
        
        # Service order validations
        service_order = record.get('ServiceOrder', {})
        if not service_order:
            critical_errors.append("Missing ServiceOrder data")
        else:
            if not service_order.get('primaryKey'):
                warnings.append("Missing service order ID")
            
            if not service_order.get('repairOrderNumber'):
                warnings.append("Missing repair order number")
            
            # Unit validations
            unit = service_order.get('Unit', {})
            if not unit:
                warnings.append("Missing Unit data")
            # Note: Missing VIN, make, or model are not warnings - they're data quality observations
            # from the source system that we cannot control
        
        # Customer validations
        customer = record.get('Customer', {}) or service_order.get('Customer', {})
        if not customer:
            warnings.append("Missing Customer data")
        else:
            if not customer.get('customerId'):
                warnings.append("Missing customer ID")
            if not customer.get('title'):
                warnings.append("Missing customer name")
        
        # Financial validations
        try:
            total = self._parse_decimal(record.get('total', 0))
            # Note: Zero or negative invoice totals are not warnings - they can represent
            # legitimate credits, returns, or adjustments in the source system
        except:
            warnings.append("Invalid invoice total format")
        
        # Complaints and corrections validations
        complaints = service_order.get('Complaints', [])
        if not complaints:
            warnings.append("No complaints/work items found")
        else:
            for i, complaint in enumerate(complaints):
                if not complaint.get('primaryKey'):
                    warnings.append(f"Complaint {i+1} missing ID")
                
                corrections = complaint.get('Corrections', [])
                if not corrections:
                    warnings.append(f"Complaint {i+1} has no corrections")
                else:
                    for j, correction in enumerate(corrections):
                        if not correction.get('primaryKey'):
                            warnings.append(f"Correction {j+1} in complaint {i+1} missing ID")
        
        return {
            'critical_errors': critical_errors,
            'warnings': warnings
        }
    
    def _validate_and_clean_line_items(self, line_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Validate and clean generated line items.
        
        Args:
            line_items: List of generated line items
            
        Returns:
            Cleaned and validated line items
        """
        cleaned_items = []
        
        for i, item in enumerate(line_items):
            try:
                # Validate required fields
                if not item.get('fullbay_invoice_id'):
                    logger.warning(f"Line item {i} missing invoice ID - skipping")
                    continue
                
                if not item.get('line_item_type'):
                    logger.warning(f"Line item {i} missing type - skipping")
                    continue
                
                # Validate financial data - preserve negative values (returns/credits)
                line_total = item.get('line_total', 0)
                if line_total is None:
                    logger.warning(f"Line item {i} has null price - setting to 0")
                    item['line_total'] = 0
                # Note: Negative values are preserved as they represent returns/credits
                
                # Validate part-specific fields
                if item.get('line_item_type') in ['PART', 'SUPPLY']:
                    if not item.get('shop_part_number'):
                        logger.warning(f"Part line item {i} missing part number")
                    
                    quantity = item.get('quantity', 0)
                    if quantity is None:
                        logger.warning(f"Part line item {i} has null quantity - setting to 1")
                        item['quantity'] = 1
                    # Note: Negative quantities are preserved as they represent returns/credits
                
                # Validate labor-specific fields
                elif item.get('line_item_type') == 'LABOR':
                     hours = item.get('so_hours', 0)
                     if hours is None:
                         logger.warning(f"Labor line item {i} has null hours - setting to 0")
                         item['so_hours'] = 0
                     # Note: 0.0 hours are valid (no labor time recorded)
                     
                     if not item.get('assigned_technician'):
                         logger.warning(f"Labor line item {i} missing technician assignment")
                
                # Clean up None values and ensure consistent data types
                item = self._clean_line_item_data(item)
                cleaned_items.append(item)
                
            except Exception as e:
                logger.error(f"Error validating line item {i}: {e} - skipping item")
                continue
        
        logger.info(f"Validated {len(line_items)} line items, {len(cleaned_items)} passed validation")
        return cleaned_items
    
    def _clean_line_item_data(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """
        Clean and standardize line item data.
        
        Args:
            item: Line item dictionary
            
        Returns:
            Cleaned line item dictionary
        """
        cleaned = {}
        
        for key, value in item.items():
            # Handle None values
            if value is None:
                cleaned[key] = None
                continue
                
            # Clean string fields
            if isinstance(value, str):
                cleaned[key] = value.strip() if value.strip() else None
            
                        # Ensure numeric fields are properly typed
            elif key in ['quantity', 'unit_cost', 'unit_price', 
                        'line_total', 'so_hours']:
                try:
                    if value == '' or value is None:
                        cleaned[key] = None
                    else:
                        cleaned[key] = float(value)
                except (ValueError, TypeError):
                    logger.warning(f"Could not convert {key}='{value}' to float, setting to None")
                    cleaned[key] = None
            
            # Ensure integer fields
            elif key in ['customer_id', 'fullbay_complaint_id', 'fullbay_correction_id', 
                        'fullbay_part_id', 'technician_portion']:
                try:
                    if value == '' or value is None:
                        cleaned[key] = None
                    else:
                        cleaned[key] = int(float(value))  # Handle string numbers
                except (ValueError, TypeError):
                    logger.warning(f"Could not convert {key}='{value}' to int, setting to None")
                    cleaned[key] = None
            
            # Boolean fields
            elif key in ['complaint_authorized', 'price_overridden', 
                        'taxable', 'inventory_item', 'sublet']:
                if isinstance(value, bool):
                    cleaned[key] = value
                elif isinstance(value, str):
                    cleaned[key] = value.lower() in ['true', 'yes', '1', 'on']
                else:
                    cleaned[key] = bool(value)
            
            else:
                cleaned[key] = value
        
        return cleaned
    
    # ===============================================
    # CLOUDWATCH METRICS AND MONITORING
    # ===============================================
    
    def send_cloudwatch_metrics(self, metrics_data: Dict[str, Any]):
        """
        Send custom metrics to CloudWatch for monitoring.
        
        Args:
            metrics_data: Dictionary containing metric data
        """
        if not self.metrics_enabled:
            return
            
        try:
            metric_data = []
            namespace = 'FullbayIngestion'
            timestamp = datetime.now(timezone.utc)
            
            # Core ingestion metrics
            if 'records_processed' in metrics_data:
                metric_data.append({
                    'MetricName': 'RecordsProcessed',
                    'Value': metrics_data['records_processed'],
                    'Unit': 'Count',
                    'Timestamp': timestamp
                })
            
            if 'line_items_created' in metrics_data:
                metric_data.append({
                    'MetricName': 'LineItemsCreated', 
                    'Value': metrics_data['line_items_created'],
                    'Unit': 'Count',
                    'Timestamp': timestamp
                })
            
            if 'processing_errors' in metrics_data:
                metric_data.append({
                    'MetricName': 'ProcessingErrors',
                    'Value': metrics_data['processing_errors'],
                    'Unit': 'Count',
                    'Timestamp': timestamp
                })
            
            if 'processing_duration' in metrics_data:
                metric_data.append({
                    'MetricName': 'ProcessingDuration',
                    'Value': metrics_data['processing_duration'],
                    'Unit': 'Seconds',
                    'Timestamp': timestamp
                })
            
            # Data quality metrics
            if 'data_quality_score' in metrics_data:
                metric_data.append({
                    'MetricName': 'DataQualityScore',
                    'Value': metrics_data['data_quality_score'],
                    'Unit': 'Percent',
                    'Timestamp': timestamp
                })
            
            # Financial metrics
            if 'total_invoice_value' in metrics_data:
                metric_data.append({
                    'MetricName': 'TotalInvoiceValue',
                    'Value': metrics_data['total_invoice_value'],
                    'Unit': 'None',  # Dollar amount
                    'Timestamp': timestamp
                })
            
            # Send metrics in batches (CloudWatch limit is 20 per call)
            for i in range(0, len(metric_data), 20):
                batch = metric_data[i:i+20]
                self.cloudwatch.put_metric_data(
                    Namespace=namespace,
                    MetricData=batch
                )
            
            logger.info(f"Sent {len(metric_data)} metrics to CloudWatch")
            
        except Exception as e:
            logger.error(f"Failed to send CloudWatch metrics: {e}")
    
    def calculate_data_quality_metrics(self) -> Dict[str, float]:
        """
        Calculate data quality metrics for monitoring.
        
        Returns:
            Dictionary containing data quality scores
        """
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    # Get total line items for percentage calculations
                    cursor.execute("SELECT COUNT(*) FROM fullbay_line_items")
                    total_items = cursor.fetchone()[0]
                    
                    if total_items == 0:
                        return {'overall_quality_score': 100.0}
                    
                    quality_checks = {}
                    
                                        # Check for missing critical fields
                    checks = [
                        ("missing_invoice_numbers", "invoice_number IS NULL OR invoice_number = ''"),
                        ("missing_customer_info", "customer_id IS NULL OR customer_title IS NULL"),
                        ("missing_unit_info", "unit_vin IS NULL OR unit_make IS NULL"),
                        ("zero_prices", "line_total = 0"),  # Only flag zero prices, not negative (returns/credits)
                        ("missing_labor_hours", "line_item_type = 'LABOR' AND so_hours IS NULL"),  # Only flag null hours, not 0.0
                        ("missing_part_numbers", "line_item_type IN ('PART', 'SUPPLY') AND (shop_part_number IS NULL OR shop_part_number = '')")
                    ]
                    
                    total_issues = 0
                    for check_name, condition in checks:
                        cursor.execute(f"SELECT COUNT(*) FROM fullbay_line_items WHERE {condition}")
                        issue_count = cursor.fetchone()[0]
                        quality_checks[check_name] = issue_count
                        total_issues += issue_count
                    
                    # Calculate overall quality score (percentage of records without issues)
                    quality_score = max(0, 100 - (total_issues * 100.0 / total_items))
                    quality_checks['overall_quality_score'] = round(quality_score, 2)
                    quality_checks['total_items_checked'] = total_items
                    quality_checks['total_issues_found'] = total_issues
                    
                    return quality_checks
                    
        except Exception as e:
            logger.error(f"Error calculating data quality metrics: {e}")
            return {'overall_quality_score': 0.0, 'error': str(e)}
    
    def get_ingestion_statistics(self) -> Dict[str, Any]:
        """
        Get comprehensive ingestion statistics for reporting.
        
        Returns:
            Dictionary containing ingestion statistics
        """
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    stats = {}
                    
                    # Raw data statistics
                    cursor.execute("""
                        SELECT 
                            COUNT(*) as total_raw_records,
                            COUNT(CASE WHEN processed = true THEN 1 END) as processed_records,
                            COUNT(CASE WHEN processing_errors IS NOT NULL THEN 1 END) as error_records,
                            MAX(ingestion_timestamp) as last_ingestion
                        FROM fullbay_raw_data
                    """)
                    raw_stats = cursor.fetchone()
                    stats.update(dict(raw_stats))
                    
                    # Line items statistics
                    cursor.execute("""
                        SELECT 
                            COUNT(*) as total_line_items,
                            COUNT(DISTINCT fullbay_invoice_id) as unique_invoices,
                            COUNT(DISTINCT customer_id) as unique_customers,
                            SUM(line_total) as total_financial_value,
                            MAX(ingestion_timestamp) as last_line_item_created
                        FROM fullbay_line_items
                    """)
                    line_stats = cursor.fetchone()
                    stats.update(dict(line_stats))
                    
                    # Line item type breakdown
                    cursor.execute("""
                        SELECT 
                            line_item_type,
                            COUNT(*) as count,
                            SUM(line_total) as total_value
                        FROM fullbay_line_items
                        GROUP BY line_item_type
                    """)
                    type_breakdown = {}
                    for row in cursor.fetchall():
                        type_breakdown[row['line_item_type']] = {
                            'count': row['count'],
                            'total_value': float(row['total_value'] or 0)
                        }
                    stats['line_item_breakdown'] = type_breakdown
                    
                    # Recent activity (last 24 hours)
                    cursor.execute("""
                        SELECT 
                            COUNT(*) as recent_line_items,
                            COUNT(DISTINCT fullbay_invoice_id) as recent_invoices
                        FROM fullbay_line_items
                        WHERE ingestion_timestamp >= NOW() - INTERVAL '24 hours'
                    """)
                    recent_stats = cursor.fetchone()
                    stats.update({
                        'recent_line_items': recent_stats['recent_line_items'],
                        'recent_invoices': recent_stats['recent_invoices']
                    })
                    
                    return stats
                    
        except Exception as e:
            logger.error(f"Error getting ingestion statistics: {e}")
            return {'error': str(e)}
    
    def send_ingestion_summary_metrics(self, processing_start_time: datetime, records_processed: int, 
                                     line_items_created: int, errors_count: int = 0):
        """
        Send summary metrics after an ingestion run.
        
        Args:
            processing_start_time: When processing started
            records_processed: Number of records processed
            line_items_created: Number of line items created
            errors_count: Number of errors encountered
        """
        try:
            # Calculate processing duration
            duration = (datetime.now(timezone.utc) - processing_start_time).total_seconds()
            
            # Get data quality metrics
            quality_metrics = self.calculate_data_quality_metrics()
            
            # Get financial total from recent ingestion
            total_value = 0
            try:
                with self._get_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("""
                            SELECT SUM(line_total) as total
                            FROM fullbay_line_items
                            WHERE ingestion_timestamp >= %s
                        """, (processing_start_time,))
                        result = cursor.fetchone()
                        total_value = float(result['total'] or 0)
            except:
                pass
            
            # Prepare metrics data
            metrics_data = {
                'records_processed': records_processed,
                'line_items_created': line_items_created,
                'processing_errors': errors_count,
                'processing_duration': duration,
                'data_quality_score': quality_metrics.get('overall_quality_score', 0),
                'total_invoice_value': total_value
            }
            
            # Send to CloudWatch
            self.send_cloudwatch_metrics(metrics_data)
            
            # Log summary
            logger.info(f"Ingestion Summary - Records: {records_processed}, "
                       f"Line Items: {line_items_created}, Errors: {errors_count}, "
                       f"Duration: {duration:.2f}s, Quality: {quality_metrics.get('overall_quality_score', 0):.1f}%")
            
        except Exception as e:
            logger.error(f"Error sending ingestion summary metrics: {e}")