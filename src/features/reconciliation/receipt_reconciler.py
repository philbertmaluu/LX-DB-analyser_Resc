"""Main reconciliation orchestrator for receipts."""

from typing import List, Dict, Any, Optional
from src.database import OracleConnection
from src.features.reconciliation.receipts.extractor import ReceiptExtractor
from src.features.reconciliation.receipts.validator import ReceiptValidator
from src.models.receipt import Receipt
from config.reconciliation_config import (
    MIN_CONFIDENCE_SCORE,
    AUTO_RECONCILE_THRESHOLD,
    ENABLE_AUTO_RECONCILE,
)


class ReceiptReconciler:
    """Orchestrates the receipt reconciliation process."""
    
    def __init__(self, db_connection: Optional[OracleConnection] = None):
        """
        Initialize receipt reconciler.
        
        Args:
            db_connection: Oracle database connection
        """
        self.db_connection = db_connection or OracleConnection(connection_name="reconciler")
        self.extractor = ReceiptExtractor(db_connection=self.db_connection)
        self.validator = ReceiptValidator(db_connection=self.db_connection)
        self._connected = False
    
    def connect(self) -> bool:
        """Connect to database."""
        if not self._connected:
            if self.db_connection.connect():
                self._connected = True
                return True
            return False
        return True
    
    def reconcile_receipts(self, limit: Optional[int] = None) -> Dict[str, Any]:
        """
        Orchestrate the full reconciliation flow.
        
        Args:
            limit: Optional limit on number of receipts to process
            
        Returns:
            Dictionary with reconciliation results
        """
        if not self.connect():
            return {
                "success": False,
                "error": "Failed to connect to database",
                "processed": 0,
                "reconciled": 0,
                "failed": 0,
                "needs_review": 0
            }
        
        # Step 1: Extract unreconciled receipts
        print("\n[Step 1] Extracting unreconciled receipts...")
        receipts = self.extractor.get_unreconciled_receipts()
        
        if not receipts:
            return {
                "success": True,
                "message": "No unreconciled receipts found",
                "processed": 0,
                "reconciled": 0,
                "failed": 0,
                "needs_review": 0
            }
        
        if limit:
            receipts = receipts[:limit]
        
        print(f"Found {len(receipts)} receipts to process")
        
        # Step 2: Validate each receipt
        print("\n[Step 2] Validating receipts with AI...")
        results = {
            "processed": len(receipts),
            "reconciled": 0,
            "failed": 0,
            "needs_review": 0,
            "details": []
        }
        
        for i, receipt_data in enumerate(receipts, 1):
            receipt_id = receipt_data.get("ID")
            receipt_number = receipt_data.get("RECEIPT_NUMBER")
            print(f"\nProcessing receipt {i}/{len(receipts)}: ID={receipt_id}, Number={receipt_number}")
            
            # Validate receipt
            validation_result = self.validator.validate_receipt(receipt_data)
            
            status = validation_result.get("status")
            confidence = validation_result.get("confidence", 0)
            
            result_detail = {
                "receipt_id": receipt_id,
                "receipt_number": receipt_number,
                "status": status,
                "confidence": confidence,
                "reasoning": validation_result.get("reasoning", ""),
                "issues": validation_result.get("issues", [])
            }
            
            # Step 3: Update status if validated
            if status == "VALID" and confidence >= MIN_CONFIDENCE_SCORE:
                if confidence >= AUTO_RECONCILE_THRESHOLD and ENABLE_AUTO_RECONCILE:
                    if self.update_receipt_status(receipt_id, "R"):
                        print(f"✓ Auto-reconciled receipt {receipt_id} (confidence: {confidence}%)")
                        results["reconciled"] += 1
                        result_detail["action"] = "reconciled"
                    else:
                        print(f"✗ Failed to update receipt {receipt_id}")
                        results["failed"] += 1
                        result_detail["action"] = "update_failed"
                else:
                    print(f"⚠ Receipt {receipt_id} validated but needs review (confidence: {confidence}%)")
                    results["needs_review"] += 1
                    result_detail["action"] = "needs_review"
            elif status == "INVALID":
                print(f"✗ Receipt {receipt_id} is invalid")
                results["failed"] += 1
                result_detail["action"] = "invalid"
            else:
                print(f"⚠ Receipt {receipt_id} needs manual review")
                results["needs_review"] += 1
                result_detail["action"] = "needs_review"
            
            results["details"].append(result_detail)
        
        # Summary
        print("\n" + "="*60)
        print("Reconciliation Summary")
        print("="*60)
        print(f"Processed: {results['processed']}")
        print(f"Reconciled: {results['reconciled']}")
        print(f"Failed: {results['failed']}")
        print(f"Needs Review: {results['needs_review']}")
        print("="*60)
        
        results["success"] = True
        return results
    
    def update_receipt_status(self, receipt_id: Any, new_status: str) -> bool:
        """
        Update receipt status in database.
        
        Args:
            receipt_id: Receipt ID to update
            new_status: New status ('R' for reconciled)
            
        Returns:
            True if update successful, False otherwise
        """
        if not self.db_connection or not self.db_connection.is_connected():
            print("✗ Database connection not available")
            return False
        
        query = """
            UPDATE CFMSPRO.RECEIPT_DETAILS
            SET STATUS = :new_status,
                UPDATED_AT = SYSTIMESTAMP,
                RECONCILED_AT = SYSTIMESTAMP,
                RECONCILED_BY = 'SYSTEM'
            WHERE ID = :receipt_id
        """
        
        try:
            return self.db_connection.execute_update(
                query,
                params=(new_status, receipt_id)
            )
        except Exception as e:
            print(f"✗ Error updating receipt {receipt_id}: {str(e)}")
            return False
    
    def disconnect(self) -> None:
        """Disconnect from database."""
        if self._connected and self.db_connection:
            self.db_connection.disconnect()
            self._connected = False
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()

