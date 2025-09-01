#!/usr/bin/env python3
"""
Simple database connection test to get detailed error information.
"""

import os
import psycopg2
import psycopg2.extras

# Set up environment variables for testing
os.environ.setdefault('DB_HOST', 'fullbay-postgres-db.cc74yq2i4j8n.us-east-1.rds.amazonaws.com')
os.environ.setdefault('DB_PORT', '5432')
os.environ.setdefault('DB_NAME', 'fullbay_data')
os.environ.setdefault('DB_USER', 'postgres')
os.environ.setdefault('DB_PASSWORD', '5255Tillman')

def test_direct_connection():
    """Test direct database connection with detailed error reporting."""
    print("🔌 Testing direct database connection...")
    print(f"Host: {os.environ.get('DB_HOST')}")
    print(f"Port: {os.environ.get('DB_PORT')}")
    print(f"Database: {os.environ.get('DB_NAME')}")
    print(f"User: {os.environ.get('DB_USER')}")
    print(f"Password: {'*' * len(os.environ.get('DB_PASSWORD', ''))}")
    print()
    
    try:
        # Try direct connection
        conn = psycopg2.connect(
            host=os.environ.get('DB_HOST'),
            port=int(os.environ.get('DB_PORT')),
            dbname=os.environ.get('DB_NAME'),
            user=os.environ.get('DB_USER'),
            password=os.environ.get('DB_PASSWORD'),
            cursor_factory=psycopg2.extras.RealDictCursor
        )
        
        with conn.cursor() as cursor:
            cursor.execute("SELECT version();")
            version = cursor.fetchone()['version']
            print(f"✅ Database connection successful!")
            print(f"📊 PostgreSQL version: {version}")
            
            # Check if tables exist
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name IN ('fullbay_raw_data', 'fullbay_line_items', 'ingestion_metadata')
            """)
            
            tables = [row['table_name'] for row in cursor.fetchall()]
            print(f"📋 Tables found: {', '.join(tables) if tables else 'None'}")
        
        conn.close()
        return True
        
    except psycopg2.OperationalError as e:
        print(f"❌ Database connection failed (OperationalError): {e}")
        print(f"   Error code: {e.pgcode}")
        print(f"   Error message: {e.pgerror}")
        return False
    except psycopg2.Error as e:
        print(f"❌ Database connection failed (psycopg2.Error): {e}")
        print(f"   Error code: {e.pgcode}")
        print(f"   Error message: {e.pgerror}")
        return False
    except Exception as e:
        print(f"❌ Database connection failed (Exception): {e}")
        print(f"   Error type: {type(e).__name__}")
        return False

if __name__ == "__main__":
    success = test_direct_connection()
    if success:
        print("\n🎉 Database connection test passed!")
    else:
        print("\n❌ Database connection test failed!")
        print("\n📋 Troubleshooting suggestions:")
        print("  - Check if the database server is running")
        print("  - Verify the connection details are correct")
        print("  - Check if your IP is whitelisted for the database")
        print("  - Verify the database credentials are correct")
