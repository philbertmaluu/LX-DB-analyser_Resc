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
    
    # Display connection info (without password)
    print(f"\n[Connection Info]")
    print(f"  Host: {oracle.host}")
    print(f"  Port: {oracle.port}")
    print(f"  SID: {oracle.sid}")
    print(f"  User: {oracle.user}")
    
    # Test 1: Connect to database
    print("\n[Test 1] Connecting to Oracle database...")
    if not oracle.connect():
        print("✗ FAILED: Could not connect to Oracle database")
        print("\n[Debug Info]")
        print("  - Check if credentials in .env file are correct")
        print("  - Verify username format (may need to be uppercase or include domain)")
        print("  - Ensure user has proper database access permissions")
        return False
    print("✓ PASSED: Successfully connected to Oracle database")
    
    # Test 2: Check connection status
    print("\n[Test 2] Checking connection status...")
    # Try a simple query to verify connection
    try:
        test_query = oracle.execute_query("SELECT 1 FROM DUAL")
        if test_query:
            print("✓ PASSED: Connection is active and responding")
        else:
            print("⚠ WARNING: Connection check query returned no results")
    except Exception as e:
        print(f"✗ FAILED: Connection check failed: {str(e)}")
        oracle.disconnect()
        return False
    
    # Test 3: List all schemas in the database
    print("\n[Test 3] Listing all schemas in the database...")
    try:
        schemas_query = """
            SELECT DISTINCT username as schema_name
            FROM all_users
            ORDER BY username
        """
        schemas = oracle.execute_query(schemas_query)
        
        if schemas and len(schemas) > 0:
            print(f"✓ PASSED: Found {len(schemas)} schemas in the database")
            print(f"\n  Showing all schemas:")
            for schema in schemas:
                schema_name = schema[0]
                # Highlight CFMSPRO if found
                marker = " ← CFMSPRO schema" if schema_name.upper() == "CFMSPRO" else ""
                print(f"  - {schema_name}{marker}")
        else:
            print("⚠ WARNING: No schemas found")
    except Exception as e:
        print(f"✗ FAILED: Error listing schemas: {str(e)}")
        oracle.disconnect()
        return False
    
    # Test 4: Access CFMSPRO schema
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
    
    # Test 5: List tables in CFMSPRO schema
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
    
    # Test 6: Query a table from CFMSPRO schema
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

