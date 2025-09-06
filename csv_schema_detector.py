#!/usr/bin/env python3
"""
CSV Schema Detector and Database Table Creator
Automatically detects CSV schema and creates corresponding database tables.
"""

import os
import sys
import pandas as pd
import psycopg2
import psycopg2.extras
from datetime import datetime
from typing import Dict, List, Any, Optional
import re

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

try:
    from config import Config
    from database import DatabaseManager
except ImportError as e:
    print(f"Import error: {e}")
    sys.exit(1)

class CSVSchemaDetector:
    """Detects CSV schema and creates database tables."""
    
    def __init__(self):
        """Initialize the schema detector."""
        self.config = Config()
        self.db_manager = DatabaseManager(self.config)
        self.db_manager.connect()
        
    def detect_csv_schema(self, csv_file_path: str) -> Dict[str, Any]:
        """
        Detect the schema of a CSV file.
        
        Args:
            csv_file_path: Path to the CSV file
            
        Returns:
            Dictionary containing schema information
        """
        print(f"🔍 Analyzing CSV file: {csv_file_path}")
        
        try:
            # Read the CSV file
            df = pd.read_csv(csv_file_path)
            
            # Check if last row appears to be a totals row and remove it
            original_row_count = len(df)
            df = self._remove_totals_row(df)
            rows_removed = original_row_count - len(df)
            
            if rows_removed > 0:
                print(f"⚠️  Detected and removed {rows_removed} totals/summary row(s)")
            
            schema_info = {
                'file_path': csv_file_path,
                'file_name': os.path.basename(csv_file_path),
                'table_name': self._generate_table_name(csv_file_path),
                'row_count': len(df),
                'original_row_count': original_row_count,
                'totals_rows_removed': rows_removed,
                'column_count': len(df.columns),
                'columns': []
            }
            
            # Analyze each column
            for column in df.columns:
                column_info = self._analyze_column(df, column)
                schema_info['columns'].append(column_info)
            
            return schema_info
            
        except Exception as e:
            print(f"❌ Error analyzing CSV: {e}")
            return None
    
    def _remove_totals_row(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Detect and remove totals/summary rows from the end of the DataFrame.
        
        Args:
            df: The DataFrame to clean
            
        Returns:
            DataFrame with totals rows removed
        """
        if len(df) == 0:
            return df
        
        # Common indicators of totals rows
        totals_indicators = [
            'total', 'totals', 'summary', 'grand total', 'subtotal',
            'totaal', 'summe', 'gesamt', 'total general'
        ]
        
        # Check the last few rows for totals indicators
        rows_to_check = min(3, len(df))  # Check last 3 rows or all if less than 3
        
        for i in range(rows_to_check):
            row_index = len(df) - 1 - i
            row = df.iloc[row_index]
            
            # Check if any cell in the row contains totals indicators
            for col in df.columns:
                cell_value = str(row[col]).lower().strip()
                
                # Check for exact matches or partial matches
                for indicator in totals_indicators:
                    if indicator in cell_value:
                        print(f"🔍 Found totals indicator '{indicator}' in row {row_index + 1}, column '{col}'")
                        # Remove this row and all rows after it
                        df = df.iloc[:row_index]
                        break
                else:
                    continue
                break
            else:
                continue
            break
        
        return df
    
    def _generate_table_name(self, file_path: str) -> str:
        """Generate a table name from the file path."""
        # Always use the same table name for employee statistics
        return "employee_statistics"
    
    def _analyze_column(self, df: pd.DataFrame, column_name: str) -> Dict[str, Any]:
        """Analyze a single column to determine its data type and properties."""
        series = df[column_name]
        
        # Clean column name for database
        clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', column_name.lower())
        clean_name = re.sub(r'_+', '_', clean_name).strip('_')
        
        column_info = {
            'original_name': column_name,
            'clean_name': clean_name,
            'data_type': self._determine_data_type(series),
            'nullable': series.isnull().any(),
            'unique_values': series.nunique(),
            'sample_values': series.dropna().head(3).tolist()
        }
        
        # Add specific type information
        if column_info['data_type'] == 'VARCHAR':
            max_length = series.astype(str).str.len().max()
            column_info['max_length'] = max_length
            column_info['suggested_length'] = min(max_length * 2, 255)  # Add some buffer
        
        return column_info
    
    def _determine_data_type(self, series: pd.Series) -> str:
        """Determine the appropriate PostgreSQL data type for a pandas Series."""
        # Check for numeric types
        if pd.api.types.is_numeric_dtype(series):
            if pd.api.types.is_integer_dtype(series):
                return 'INTEGER'
            else:
                return 'DECIMAL(10,2)'
        
        # Check for datetime
        if pd.api.types.is_datetime64_any_dtype(series):
            return 'TIMESTAMP'
        
        # Check for boolean
        if pd.api.types.is_bool_dtype(series):
            return 'BOOLEAN'
        
        # Check for date strings
        if series.dtype == 'object':
            # Try to detect date patterns
            sample_values = series.dropna().head(10)
            if len(sample_values) > 0:
                date_patterns = [
                    r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
                    r'\d{2}/\d{2}/\d{4}',  # MM/DD/YYYY
                    r'\d{2}-\d{2}-\d{4}',  # MM-DD-YYYY
                ]
                
                for pattern in date_patterns:
                    if sample_values.astype(str).str.match(pattern).any():
                        return 'DATE'
        
        # Default to VARCHAR
        return 'VARCHAR'
    
    def generate_create_table_sql(self, schema_info: Dict[str, Any]) -> str:
        """Generate CREATE TABLE SQL from schema information."""
        table_name = schema_info['table_name']
        
        sql_parts = [f"CREATE TABLE IF NOT EXISTS {table_name} ("]
        sql_parts.append("    id SERIAL PRIMARY KEY,")
        
        # Add columns
        for column in schema_info['columns']:
            clean_name = column['clean_name']
            data_type = column['data_type']
            
            # Adjust VARCHAR length
            if data_type == 'VARCHAR' and 'suggested_length' in column:
                data_type = f"VARCHAR({column['suggested_length']})"
            
            # Add NULL constraint
            null_constraint = "NULL" if column['nullable'] else "NOT NULL"
            
            sql_parts.append(f"    {clean_name} {data_type} {null_constraint},")
        
        # Add metadata columns
        sql_parts.append("    file_name VARCHAR(255),")
        sql_parts.append("    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        sql_parts.append(");")
        
        return "\n".join(sql_parts)
    
    def create_table_from_schema(self, schema_info: Dict[str, Any]) -> bool:
        """Create a database table from the schema information."""
        try:
            sql = self.generate_create_table_sql(schema_info)
            
            with self.db_manager._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql)
                    conn.commit()
            
            print(f"✅ Table '{schema_info['table_name']}' created successfully!")
            return True
            
        except Exception as e:
            print(f"❌ Error creating table: {e}")
            return False
    
    def check_file_already_loaded(self, csv_file_path: str, table_name: str) -> bool:
        """Check if this file has already been loaded into the table."""
        try:
            file_name = os.path.basename(csv_file_path)
            
            with self.db_manager._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE file_name = %s", (file_name,))
                    result = cursor.fetchone()
                    count = result[0] if result else 0
                    
                    if count > 0:
                        cursor.execute(f"SELECT loaded_at FROM {table_name} WHERE file_name = %s ORDER BY loaded_at DESC LIMIT 1", (file_name,))
                        result = cursor.fetchone()
                        last_loaded = result[0] if result else 'Unknown'
                        print(f"⚠️  File '{file_name}' already loaded on {last_loaded}")
                        print(f"📊 Found {count:,} existing records from this file")
                        return True
                    
            return False
            
        except Exception as e:
            print(f"⚠️  Could not check for existing data: {e}")
            return False
    
    def load_csv_to_table(self, csv_file_path: str, table_name: str, skip_duplicates: bool = True) -> bool:
        """Load CSV data into the database table."""
        try:
            print(f"📥 Loading CSV data into table '{table_name}'...")
            
            # Check if file already loaded
            if skip_duplicates and self.check_file_already_loaded(csv_file_path, table_name):
                response = input("❓ File already exists. Do you want to load it again anyway? (y/n): ")
                if response.lower() != 'y':
                    print("❌ Loading cancelled - file already exists")
                    return False
                print("⚠️  Proceeding with duplicate data...")
            
            # Read CSV
            df = pd.read_csv(csv_file_path)
            
            # Remove totals rows (same logic as in schema detection)
            original_count = len(df)
            df = self._remove_totals_row(df)
            if len(df) < original_count:
                print(f"⚠️  Removed {original_count - len(df)} totals row(s) during data loading")
            
            # Clean column names
            df.columns = [re.sub(r'[^a-zA-Z0-9_]', '_', col.lower()) for col in df.columns]
            df.columns = [re.sub(r'_+', '_', col).strip('_') for col in df.columns]
            
            # Add metadata
            df['file_name'] = os.path.basename(csv_file_path)
            df['loaded_at'] = datetime.now()
            
            # Convert to list of tuples for insertion
            data_tuples = [tuple(row) for row in df.values]
            columns = ', '.join(df.columns)
            placeholders = ', '.join(['%s'] * len(df.columns))
            
            with self.db_manager._get_connection() as conn:
                with conn.cursor() as cursor:
                    insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                    cursor.executemany(insert_sql, data_tuples)
                    conn.commit()
            
            print(f"✅ Loaded {len(df)} rows into table '{table_name}'")
            return True
            
        except Exception as e:
            print(f"❌ Error loading CSV: {e}")
            return False
    
    def process_csv_file(self, csv_file_path: str, auto_confirm: bool = False) -> bool:
        """Complete process: detect schema, create table, and load data."""
        print(f"\n🚀 Processing CSV file: {csv_file_path}")
        print("=" * 60)
        
        # Step 1: Detect schema
        schema_info = self.detect_csv_schema(csv_file_path)
        if not schema_info:
            return False
        
        # Step 2: Display schema information
        self._display_schema_info(schema_info)
        
        # Step 3: Ask for confirmation (unless auto_confirm is True)
        if not auto_confirm:
            print(f"\n📊 Ready to load {schema_info['row_count']:,} rows into table '{schema_info['table_name']}'")
            response = input("\n❓ Do you want to proceed with loading the data? (y/n): ")
            if response.lower() != 'y':
                print("❌ Operation cancelled by user")
                return False
        
        # Step 4: Load data
        if not self.load_csv_to_table(csv_file_path, schema_info['table_name']):
            return False
        
        print(f"\n✅ Successfully processed {csv_file_path}")
        return True
    
    def _display_schema_info(self, schema_info: Dict[str, Any]):
        """Display schema information in a readable format."""
        print(f"\n📋 SCHEMA ANALYSIS:")
        print(f"File: {schema_info['file_name']}")
        print(f"Table Name: {schema_info['table_name']}")
        print(f"Original Rows: {schema_info['original_row_count']:,}")
        print(f"Rows After Cleanup: {schema_info['row_count']:,}")
        
        if schema_info.get('totals_rows_removed', 0) > 0:
            print(f"⚠️  Totals Rows Removed: {schema_info['totals_rows_removed']}")
        
        print(f"Columns: {schema_info['column_count']}")
        
        print(f"\n📊 COLUMN DETAILS:")
        print("-" * 80)
        print(f"{'Column Name':<25} {'Type':<15} {'Nullable':<10} {'Unique':<10} {'Sample'}")
        print("-" * 80)
        
        for column in schema_info['columns']:
            nullable = "Yes" if column['nullable'] else "No"
            sample_str = str(column['sample_values'])[:30] + "..." if len(str(column['sample_values'])) > 30 else str(column['sample_values'])
            print(f"{column['clean_name']:<25} {column['data_type']:<15} {nullable:<10} {column['unique_values']:<10} {sample_str}")
    
    def show_loaded_files(self, table_name: str = "employee_statistics"):
        """Show what files have already been loaded into the table."""
        try:
            with self.db_manager._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"""
                        SELECT 
                            file_name,
                            COUNT(*) as row_count,
                            MIN(loaded_at) as first_loaded,
                            MAX(loaded_at) as last_loaded
                        FROM {table_name} 
                        GROUP BY file_name 
                        ORDER BY first_loaded DESC
                    """)
                    
                    results = cursor.fetchall()
                    
                    if results:
                        print(f"\n📁 FILES ALREADY LOADED IN '{table_name}':")
                        print("-" * 80)
                        print(f"{'File Name':<50} {'Rows':<10} {'First Loaded':<20} {'Last Loaded'}")
                        print("-" * 80)
                        
                        for row in results:
                            file_name = row[0]
                            row_count = row[1]
                            first_loaded = row[2].strftime('%Y-%m-%d %H:%M:%S') if row[2] else 'N/A'
                            last_loaded = row[3].strftime('%Y-%m-%d %H:%M:%S') if row[3] else 'N/A'
                            
                            print(f"{file_name:<50} {row_count:<10,} {first_loaded:<20} {last_loaded}")
                        
                        total_rows = sum(row[1] for row in results)
                        print("-" * 80)
                        print(f"{'TOTAL':<50} {total_rows:<10,}")
                    else:
                        print(f"\n📁 No files loaded in '{table_name}' yet")
                        
        except Exception as e:
            print(f"❌ Error checking loaded files: {e}")
    
    def close(self):
        """Close database connection."""
        if self.db_manager:
            self.db_manager.close()

def main():
    """Main function to process CSV files."""
    if len(sys.argv) < 2:
        print("Usage: python csv_schema_detector.py <csv_file_path> [--schema-only]")
        print("       python csv_schema_detector.py --show-loaded")
        print("  --schema-only: Only analyze schema, don't create table or load data")
        print("  --show-loaded: Show what files have already been loaded")
        sys.exit(1)
    
    # Check for show-loaded option
    if "--show-loaded" in sys.argv:
        detector = CSVSchemaDetector()
        try:
            detector.show_loaded_files()
        finally:
            detector.close()
        return
    
    csv_file_path = sys.argv[1]
    schema_only = "--schema-only" in sys.argv
    
    if not os.path.exists(csv_file_path):
        print(f"❌ File not found: {csv_file_path}")
        sys.exit(1)
    
    detector = CSVSchemaDetector()
    try:
        if schema_only:
            # Just analyze schema, don't create anything
            print(f"\n🔍 SCHEMA ANALYSIS ONLY - No database changes will be made")
            print("=" * 60)
            schema_info = detector.detect_csv_schema(csv_file_path)
            if schema_info:
                detector._display_schema_info(schema_info)
                sql = detector.generate_create_table_sql(schema_info)
                print(f"\n📝 GENERATED SQL (for reference only):")
                print("-" * 60)
                print(sql)
                print("-" * 60)
                print(f"\n✅ Schema analysis completed. No database changes made.")
            else:
                print("❌ Schema analysis failed!")
                sys.exit(1)
        else:
            # Full processing with confirmation
            success = detector.process_csv_file(csv_file_path, auto_confirm=False)
            if success:
                print("\n🎉 CSV processing completed successfully!")
            else:
                print("\n❌ CSV processing failed!")
                sys.exit(1)
    finally:
        detector.close()

if __name__ == "__main__":
    main()
