"""Receipt data model based on actual RECEIPT_DETAILS table schema."""

from typing import Optional, Dict, Any
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class Receipt:
    """Represents a receipt record from CFMSPRO.RECEIPT_DETAILS."""
    
    id: int
    receipt_number: str
    receipt_detail_no: Optional[int] = None
    employer_id: int = None
    office_id: int = None
    member_id: Optional[int] = None
    receipt_date: Optional[datetime] = None
    month: int = None
    year: int = None
    main_scheme_id: int = None
    scheme_id: int = None
    amount: float = None
    status: str = 'U'
    receipt_type: str = None
    apportion_type: str = None
    penalty_id: Optional[int] = None
    adjustment_id: Optional[int] = None
    reconciled_by: Optional[str] = None
    created_by: Optional[str] = None
    reconciled_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    source_pro: Optional[str] = None
    deleted_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    deleted_by: Optional[int] = None
    memsalary_amount: Optional[float] = None
    dsis_flag: Optional[str] = None
    eoffice_reference: Optional[str] = None
    employer_office_id: Optional[str] = None
    additional_fields: Dict[str, Any] = field(default_factory=dict)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Receipt':
        """
        Create Receipt instance from database dictionary.
        
        Args:
            data: Dictionary from database query result
            
        Returns:
            Receipt instance
        """
        # Map database column names (uppercase) to Python attributes
        receipt = cls(
            id=cls._safe_int(data.get('ID') or data.get('id')),
            receipt_number=str(data.get('RECEIPT_NUMBER') or data.get('receipt_number', '')),
            receipt_detail_no=cls._safe_int(data.get('RECEIPT_DETAIL_NO') or data.get('receipt_detail_no')),
            employer_id=cls._safe_int(data.get('EMPLOYER_ID') or data.get('employer_id')),
            office_id=cls._safe_int(data.get('OFFICE_ID') or data.get('office_id')),
            member_id=cls._safe_int(data.get('MEMBER_ID') or data.get('member_id')),
            month=cls._safe_int(data.get('MONTH') or data.get('month')),
            year=cls._safe_int(data.get('YEAR') or data.get('year')),
            main_scheme_id=cls._safe_int(data.get('MAIN_SCHEME_ID') or data.get('main_scheme_id')),
            scheme_id=cls._safe_int(data.get('SCHEME_ID') or data.get('scheme_id')),
            amount=cls._safe_float(data.get('AMOUNT') or data.get('amount')),
            status=str(data.get('STATUS') or data.get('status', 'U')).upper(),
            receipt_type=str(data.get('RECEIPT_TYPE') or data.get('receipt_type', '')),
            apportion_type=str(data.get('APPORTION_TYPE') or data.get('apportion_type', '')),
            penalty_id=cls._safe_int(data.get('PENALTY_ID') or data.get('penalty_id')),
            adjustment_id=cls._safe_int(data.get('ADJUSTMENT_ID') or data.get('adjustment_id')),
            reconciled_by=data.get('RECONCILED_BY') or data.get('reconciled_by'),
            created_by=data.get('CREATED_BY') or data.get('created_by'),
            source_pro=data.get('SOURCE_PRO') or data.get('source_pro'),
            updated_by=cls._safe_int(data.get('UPDATED_BY') or data.get('updated_by')),
            deleted_by=cls._safe_int(data.get('DELETED_BY') or data.get('deleted_by')),
            memsalary_amount=cls._safe_float(data.get('MEMSALARY_AMOUNT') or data.get('memsalary_amount')),
            dsis_flag=data.get('DSIS_FLAG') or data.get('dsis_flag'),
            eoffice_reference=data.get('EOFFICE_REFERENCE') or data.get('eoffice_reference'),
            employer_office_id=data.get('EMPLOYER_OFFICE_ID') or data.get('employer_office_id'),
        )
        
        # Handle timestamp fields
        receipt.receipt_date = cls._parse_date(data.get('RECEIPT_DATE') or data.get('receipt_date'))
        receipt.reconciled_at = cls._parse_timestamp(data.get('RECONCILED_AT') or data.get('reconciled_at'))
        receipt.created_at = cls._parse_timestamp(data.get('CREATED_AT') or data.get('created_at'))
        receipt.updated_at = cls._parse_timestamp(data.get('UPDATED_AT') or data.get('updated_at'))
        receipt.deleted_at = cls._parse_timestamp(data.get('DELETED_AT') or data.get('deleted_at'))
        
        return receipt
    
    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        """Safely convert value to float."""
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    @staticmethod
    def _safe_int(value: Any) -> Optional[int]:
        """Safely convert value to int."""
        if value is None:
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None
    
    @staticmethod
    def _parse_date(value: Any) -> Optional[datetime]:
        """Parse date value to datetime."""
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        try:
            if isinstance(value, str):
                for fmt in ['%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%d-%m-%Y', '%m/%d/%Y', '%d/%m/%Y']:
                    try:
                        return datetime.strptime(value, fmt)
                    except ValueError:
                        continue
            return None
        except Exception:
            return None
    
    @staticmethod
    def _parse_timestamp(value: Any) -> Optional[datetime]:
        """Parse timestamp value to datetime."""
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        try:
            if isinstance(value, str):
                for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d', '%d-%m-%Y %H:%M:%S']:
                    try:
                        return datetime.strptime(value, fmt)
                    except ValueError:
                        continue
            return None
        except Exception:
            return None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert Receipt to dictionary."""
        return {
            'ID': self.id,
            'RECEIPT_NUMBER': self.receipt_number,
            'RECEIPT_DETAIL_NO': self.receipt_detail_no,
            'EMPLOYER_ID': self.employer_id,
            'OFFICE_ID': self.office_id,
            'MEMBER_ID': self.member_id,
            'RECEIPT_DATE': self.receipt_date.isoformat() if self.receipt_date else None,
            'MONTH': self.month,
            'YEAR': self.year,
            'MAIN_SCHEME_ID': self.main_scheme_id,
            'SCHEME_ID': self.scheme_id,
            'AMOUNT': self.amount,
            'STATUS': self.status,
            'RECEIPT_TYPE': self.receipt_type,
            'APPORTION_TYPE': self.apportion_type,
            'PENALTY_ID': self.penalty_id,
            'ADJUSTMENT_ID': self.adjustment_id,
            'RECONCILED_BY': self.reconciled_by,
            'CREATED_BY': self.created_by,
            'RECONCILED_AT': self.reconciled_at.isoformat() if self.reconciled_at else None,
            'CREATED_AT': self.created_at.isoformat() if self.created_at else None,
            'UPDATED_AT': self.updated_at.isoformat() if self.updated_at else None,
            'SOURCE_PRO': self.source_pro,
            'DELETED_AT': self.deleted_at.isoformat() if self.deleted_at else None,
            'UPDATED_BY': self.updated_by,
            'DELETED_BY': self.deleted_by,
            'MEMSALARY_AMOUNT': self.memsalary_amount,
            'DSIS_FLAG': self.dsis_flag,
            'EOFFICE_REFERENCE': self.eoffice_reference,
            'EMPLOYER_OFFICE_ID': self.employer_office_id,
        }
    
    def __str__(self) -> str:
        """String representation of receipt."""
        return f"Receipt(ID={self.id}, Number={self.receipt_number}, Status={self.status}, Amount={self.amount}, Employer={self.employer_id})"
