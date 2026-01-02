"""Simple test for Oracle database connection and CFMSPRO schema access."""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database import OracleConnection


def test_oracle_connection():
    """Test Oracle database connection."""
    print("\n" + "="*60)
    print("Testing Oracle Database Connection")
    print("="*60)
    
    # Create connection
    oracle = OracleConnection(connection_name="test_oracle")
    
    # Test 1: Connect to database
    print("\n[Test 1] Connecting to Oracle database...")
    if not oracle.connect():
        print("✗ FAILED: Could not connect to Oracle database")
        return False
    print("✓ PASSED: Successfully connected to Oracle database")
    
    # Test 2: Check connection status
    print("\n[Test 2] Checking connection status...")
    if not oracle.is_connected():
        print("✗ FAILED: Connection is not active")
        oracle.disconnect()
        return False
    print("✓ PASSED: Connection is active")
    
    # Test 3: Access CFMSPRO schema
    print("\n[Test 3] Testing access to CFMSPRO schema...")
    try:
        # Query to check if schema exists and we have access
        schema_query = """
            SELECT COUNT(*) as table_count 
            FROM all_tables 
            WHERE owner = 'CFMSPRO'
        """
        result = oracle.execute_query(schema_query)
        
        if result and len(result) > 0:
            table_count = result[0][0]
            print(f"✓ PASSED: CFMSPRO schema accessible. Found {table_count} tables")
        else:
            print("✗ FAILED: Could not access CFMSPRO schema or schema does not exist")
            oracle.disconnect()
            return False
    except Exception as e:
        print(f"✗ FAILED: Error accessing CFMSPRO schema: {str(e)}")
        oracle.disconnect()
        return False
    
    # Test 4: List tables in CFMSPRO schema
    print("\n[Test 4] Listing tables in CFMSPRO schema...")
    try:
        tables_query = """
            SELECT table_name 
            FROM all_tables 
            WHERE owner = 'CFMSPRO' 
            ORDER BY table_name
            FETCH FIRST 10 ROWS ONLY
        """
        tables = oracle.execute_query(tables_query)
        
        if tables and len(tables) > 0:
            print(f"✓ PASSED: Found {len(tables)} tables (showing first 10):")
            for table in tables:
                print(f"  - {table[0]}")
        else:
            print("⚠ WARNING: No tables found in CFMSPRO schema")
    except Exception as e:
        print(f"✗ FAILED: Error listing tables: {str(e)}")
        oracle.disconnect()
        return False
    
    # Test 5: Query a table from CFMSPRO schema
    print("\n[Test 5] Testing query on CFMSPRO schema table...")
    try:
        # Get first table name
        first_table_query = """
            SELECT table_name 
            FROM all_tables 
            WHERE owner = 'CFMSPRO' 
            AND ROWNUM = 1
        """
        first_table_result = oracle.execute_query(first_table_query)
        
        if first_table_result and len(first_table_result) > 0:
            table_name = first_table_result[0][0]
            print(f"  Testing query on table: CFMSPRO.{table_name}")
            
            # Try to get row count
            count_query = f"SELECT COUNT(*) FROM CFMSPRO.{table_name}"
            count_result = oracle.execute_query(count_query)
            
            if count_result:
                row_count = count_result[0][0]
                print(f"✓ PASSED: Successfully queried CFMSPRO.{table_name} ({row_count} rows)")
            else:
                print(f"⚠ WARNING: Could not query CFMSPRO.{table_name}")
        else:
            print("⚠ WARNING: No tables available to test query")
    except Exception as e:
        print(f"⚠ WARNING: Error querying table: {str(e)}")
    
    # Cleanup
    print("\n[Cleanup] Disconnecting from database...")
    oracle.disconnect()
    print("✓ Disconnected successfully")
    
    print("\n" + "="*60)
    print("All tests completed!")
    print("="*60 + "\n")
    
    return True


if __name__ == "__main__":
    success = test_oracle_connection()
    sys.exit(0 if success else 1)

