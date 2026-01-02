"""Extract receipt data from Oracle database."""

from typing import List, Optional, Dict, Any
from src.database import OracleConnection


class ReceiptExtractor:
    """Extracts unreconciled receipts from CFMSPRO.RECEIPT_DETAILS."""
    
    def __init__(self, db_connection: Optional[OracleConnection] = None):
        """
        Initialize receipt extractor.
        
        Args:
            db_connection: Oracle database connection. If None, creates a new one.
        """
        self.db_connection = db_connection
        self._own_connection = db_connection is None
    
    def get_unreconciled_receipts(self) -> List[Dict[str, Any]]:
        """
        Extract all unreconciled receipts (STATUS='U') from CFMSPRO.RECEIPT_DETAILS.
        
        Returns:
            List of receipt records as dictionaries
        """
        if not self.db_connection:
            self.db_connection = OracleConnection(connection_name="receipt_extractor")
            if not self.db_connection.connect():
                print("✗ Failed to connect to database for receipt extraction")
                return []
        
        query = """
            SELECT *
            FROM CFMSPRO.RECEIPT_DETAILS
            WHERE STATUS = 'U'
            ORDER BY ID
        """
        
        try:
            results = self.db_connection.execute_query(query)
            
            if not results:
                print("ℹ No unreconciled receipts found")
                return []
            
            # Get column names from cursor description
            cursor = self.db_connection.get_connection().cursor()
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            cursor.close()
            
            # Convert tuples to dictionaries
            receipts = []
            for row in results:
                receipt_dict = dict(zip(columns, row))
                receipts.append(receipt_dict)
            
            print(f"✓ Extracted {len(receipts)} unreconciled receipts")
            return receipts
            
        except Exception as e:
            print(f"✗ Error extracting receipts: {str(e)}")
            return []
    
    def get_receipt_by_id(self, receipt_id: Any) -> Optional[Dict[str, Any]]:
        """
        Get a specific receipt by ID.
        
        Args:
            receipt_id: Receipt ID to retrieve
            
        Returns:
            Receipt record as dictionary or None if not found
        """
        if not self.db_connection:
            self.db_connection = OracleConnection(connection_name="receipt_extractor")
            if not self.db_connection.connect():
                return None
        
        query = """
            SELECT *
            FROM CFMSPRO.RECEIPT_DETAILS
            WHERE ID = :receipt_id
        """
        
        try:
            cursor = self.db_connection.get_connection().cursor()
            cursor.execute(query, {"receipt_id": receipt_id})
            row = cursor.fetchone()
            columns = [desc[0] for desc in cursor.description]
            cursor.close()
            
            if row:
                return dict(zip(columns, row))
            return None
            
        except Exception as e:
            print(f"✗ Error retrieving receipt {receipt_id}: {str(e)}")
            return None
    
    def __enter__(self):
        """Context manager entry."""
        if not self.db_connection:
            self.db_connection = OracleConnection(connection_name="receipt_extractor")
            self.db_connection.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        if self._own_connection and self.db_connection:
            self.db_connection.disconnect()
