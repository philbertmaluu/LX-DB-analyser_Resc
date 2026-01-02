"""Validation tools for receipt reconciliation."""

from typing import Dict, Any, Optional
from langchain.tools import tool
from src.database import OracleConnection


@tool
def check_receipt_validity(receipt_data: Dict[str, Any]) -> str:
    """
    Check if a receipt is valid (has required fields and proper format).
    
    Args:
        receipt_data: Dictionary containing receipt information
        
    Returns:
        Validation result as string
    """
    required_fields = ['ID', 'RECEIPT_NUMBER', 'AMOUNT', 'STATUS', 'EMPLOYER_ID', 'OFFICE_ID', 'MONTH', 'YEAR', 'SCHEME_ID', 'RECEIPT_TYPE', 'APPORTION_TYPE']
    missing_fields = [field for field in required_fields if receipt_data.get(field) is None]
    
    if missing_fields:
        return f"INVALID: Missing required fields: {', '.join(missing_fields)}"
    
    # Check amount is positive
    amount = receipt_data.get('AMOUNT')
    try:
        if amount is None or float(amount) <= 0:
            return "INVALID: Amount must be positive"
    except (ValueError, TypeError):
        return "INVALID: Amount must be a valid number"
    
    # Check status is valid
    status = str(receipt_data.get('STATUS', '')).upper()
    valid_statuses = ['U', 'R', 'REV', 'I']
    if status not in valid_statuses:
        return f"INVALID: Status must be one of {valid_statuses}, got '{status}'"
    
    # Check receipt type is valid
    receipt_type = str(receipt_data.get('RECEIPT_TYPE', ''))
    valid_types = ['1', '2', '3', '4', '5']
    if receipt_type not in valid_types:
        return f"INVALID: Receipt type must be one of {valid_types}, got '{receipt_type}'"
    
    # Check apportion type is valid
    apportion_type = str(receipt_data.get('APPORTION_TYPE', ''))
    valid_apportions = ['Auto', 'Normal']
    if apportion_type not in valid_apportions:
        return f"INVALID: Apportion type must be one of {valid_apportions}, got '{apportion_type}'"
    
    return "VALID: Receipt has all required fields and valid format"


@tool
def check_employer_assignment(receipt_data: Dict[str, Any]) -> str:
    """
    Check if receipt belongs to correct employer/office/scheme.
    
    Args:
        receipt_data: Dictionary containing receipt information
        
    Returns:
        Validation result as string
    """
    employer_id = receipt_data.get('EMPLOYER_ID')
    office_id = receipt_data.get('OFFICE_ID')
    scheme_id = receipt_data.get('SCHEME_ID')
    main_scheme_id = receipt_data.get('MAIN_SCHEME_ID')
    employer_office_id = receipt_data.get('EMPLOYER_OFFICE_ID')
    
    if not employer_id:
        return "INVALID: Missing employer assignment"
    
    if not office_id:
        return "INVALID: Missing office assignment"
    
    if not scheme_id:
        return "INVALID: Missing scheme assignment"
    
    if not main_scheme_id:
        return "WARNING: Missing main scheme assignment"
    
    # Additional validation could check against database
    return f"VALID: Assigned to Employer={employer_id}, Office={office_id}, Scheme={scheme_id}, MainScheme={main_scheme_id}"


@tool
def check_logical_consistency(receipt_data: Dict[str, Any]) -> str:
    """
    Check if amount, month, year, and type make logical sense.
    
    Args:
        receipt_data: Dictionary containing receipt information
        
    Returns:
        Validation result as string
    """
    issues = []
    
    # Check month
    month = receipt_data.get('MONTH')
    if month is not None:
        try:
            month_int = int(month)
            if month_int < 1 or month_int > 12:
                issues.append(f"Invalid month: {month_int}")
        except (ValueError, TypeError):
            issues.append(f"Invalid month format: {month}")
    
    # Check year
    year = receipt_data.get('YEAR')
    if year is not None:
        try:
            year_int = int(year)
            if year_int < 2000 or year_int > 2100:
                issues.append(f"Suspicious year: {year_int}")
        except (ValueError, TypeError):
            issues.append(f"Invalid year format: {year}")
    
    # Check amount reasonableness
    amount = receipt_data.get('AMOUNT')
    if amount is not None:
        try:
            amount_float = float(amount)
            if amount_float < 0:
                issues.append("Negative amount")
            elif amount_float > 1000000000:  # 1 billion threshold
                issues.append("Extremely large amount")
        except (ValueError, TypeError):
            issues.append("Invalid amount format")
    
    # Check receipt date consistency with month/year if both exist
    receipt_date = receipt_data.get('RECEIPT_DATE')
    if receipt_date and month and year:
        try:
            from datetime import datetime
            if isinstance(receipt_date, str):
                date_obj = datetime.strptime(receipt_date.split()[0], '%Y-%m-%d')
                if date_obj.month != int(month) or date_obj.year != int(year):
                    issues.append(f"Receipt date inconsistent with month/year: date={receipt_date}, month={month}, year={year}")
        except Exception:
            pass  # Skip date validation if parsing fails
    
    if issues:
        return f"INCONSISTENT: {', '.join(issues)}"
    
    return "CONSISTENT: All fields are logically valid"


@tool
def check_duplicate(receipt_data: Dict[str, Any], db_connection: Optional[OracleConnection] = None) -> str:
    """
    Check if receipt is a duplicate of an existing reconciled receipt.
    
    Args:
        receipt_data: Dictionary containing receipt information
        db_connection: Optional database connection for duplicate checking
        
    Returns:
        Validation result as string
    """
    if not db_connection:
        return "WARNING: Cannot check duplicates without database connection"
    
    receipt_id = receipt_data.get('ID')
    receipt_number = receipt_data.get('RECEIPT_NUMBER')
    amount = receipt_data.get('AMOUNT')
    employer_id = receipt_data.get('EMPLOYER_ID')
    month = receipt_data.get('MONTH')
    year = receipt_data.get('YEAR')
    receipt_type = receipt_data.get('RECEIPT_TYPE')
    penalty_id = receipt_data.get('PENALTY_ID')
    adjustment_id = receipt_data.get('ADJUSTMENT_ID')
    
    if not all([receipt_id, receipt_number, amount, employer_id, month, year, receipt_type]):
        return "WARNING: Insufficient data to check duplicates"
    
    try:
        # Check for similar receipts with status 'R' (reconciled) using the unique constraint fields
        query = """
            SELECT COUNT(*) as duplicate_count
            FROM CFMSPRO.RECEIPT_DETAILS
            WHERE STATUS = 'R'
            AND ID != :receipt_id
            AND MONTH = :month
            AND RECEIPT_NUMBER = :receipt_number
            AND YEAR = :year
            AND EMPLOYER_ID = :employer_id
            AND RECEIPT_TYPE = :receipt_type
            AND (PENALTY_ID = :penalty_id OR (PENALTY_ID IS NULL AND :penalty_id IS NULL))
            AND (ADJUSTMENT_ID = :adjustment_id OR (ADJUSTMENT_ID IS NULL AND :adjustment_id IS NULL))
        """
        
        cursor = db_connection.get_connection().cursor()
        cursor.execute(query, {
            'receipt_id': receipt_id,
            'receipt_number': receipt_number,
            'month': month,
            'year': year,
            'employer_id': employer_id,
            'receipt_type': receipt_type,
            'penalty_id': penalty_id,
            'adjustment_id': adjustment_id
        })
        result = cursor.fetchone()
        cursor.close()
        
        if result and result[0] > 0:
            return f"DUPLICATE: Found {result[0]} similar reconciled receipt(s)"
        
        return "UNIQUE: No duplicates found"
        
    except Exception as e:
        return f"ERROR: Could not check duplicates: {str(e)}"


@tool
def check_business_rules(receipt_data: Dict[str, Any]) -> str:
    """
    Check if receipt satisfies business rules (explicit or implicit).
    
    Args:
        receipt_data: Dictionary containing receipt information
        
    Returns:
        Validation result as string
    """
    violations = []
    
    # Rule 1: Status must be 'U' for unreconciled
    status = str(receipt_data.get('STATUS', '')).upper()
    if status != 'U':
        violations.append(f"Status should be 'U' but is '{status}'")
    
    # Rule 2: Receipt must have a receipt number
    receipt_number = receipt_data.get('RECEIPT_NUMBER')
    if not receipt_number or len(str(receipt_number).strip()) == 0:
        violations.append("Missing receipt number")
    
    # Rule 3: Month and year should be consistent with receipt date if both exist
    month = receipt_data.get('MONTH')
    year = receipt_data.get('YEAR')
    receipt_date = receipt_data.get('RECEIPT_DATE')
    if receipt_date and month and year:
        try:
            from datetime import datetime
            if isinstance(receipt_date, str):
                date_obj = datetime.strptime(receipt_date.split()[0], '%Y-%m-%d')
                if date_obj.month != int(month) or date_obj.year != int(year):
                    violations.append("Receipt date inconsistent with month/year")
        except Exception:
            pass
    
    # Rule 4: Amount should be reasonable
    amount = receipt_data.get('AMOUNT')
    if amount:
        try:
            amount_float = float(amount)
            if amount_float <= 0:
                violations.append("Amount must be positive")
            elif amount_float > 10000000:
                violations.append("Amount exceeds maximum threshold")
        except (ValueError, TypeError):
            violations.append("Invalid amount format")
    
    # Rule 5: If deleted_at is set, receipt should not be processed
    deleted_at = receipt_data.get('DELETED_AT')
    if deleted_at:
        violations.append("Receipt is marked as deleted")
    
    if violations:
        return f"RULE_VIOLATION: {', '.join(violations)}"
    
    return "COMPLIANT: All business rules satisfied"
