"""AI-powered receipt validation using LangChain ReAct agents."""

from typing import Dict, Any, Optional, List
from langchain.agents import AgentExecutor, create_react_agent
from langchain_openai import ChatOpenAI
from langchain.prompts import PromptTemplate
from langchain.tools import Tool
from src.database import OracleConnection
from src.features.reconciliation.receipts.tools import (
    check_receipt_validity,
    check_employer_assignment,
    check_logical_consistency,
    check_duplicate,
    check_business_rules,
)
import os
from dotenv import load_dotenv

load_dotenv()


class ReceiptValidator:
    """Validates receipts using LangChain ReAct agent with validation tools."""
    
    def __init__(self, db_connection: Optional[OracleConnection] = None, model_name: str = "gpt-4o-mini"):
        """
        Initialize receipt validator.
        
        Args:
            db_connection: Oracle database connection for duplicate checking
            model_name: OpenAI model to use for validation
        """
        self.db_connection = db_connection
        self.model_name = model_name
        self.llm = ChatOpenAI(
            model=model_name,
            temperature=0,
            openai_api_key=os.getenv("OPENAI_API_KEY")
        )
        self.agent = self._create_agent()
    
    def _create_agent(self) -> AgentExecutor:
        """Create ReAct agent with validation tools."""
        # Prepare tools with database connection if available
        tools = [
            check_receipt_validity,
            check_employer_assignment,
            check_logical_consistency,
            check_business_rules,
        ]
        
        # Add duplicate check tool with database connection
        if self.db_connection:
            duplicate_tool = Tool(
                name="check_duplicate",
                func=lambda receipt_data: check_duplicate(receipt_data, self.db_connection),
                description="Check if receipt is a duplicate of an existing reconciled receipt. Input should be a dictionary with receipt data."
            )
            tools.append(duplicate_tool)
        
        # Create ReAct prompt
        prompt = PromptTemplate.from_template("""
You are an expert receipt validation agent. Your task is to validate receipts for reconciliation.

You have access to the following tools:
{tools}

Use the following format:

Question: the input question you must answer
Thought: you should always think about what to do
Action: the action to take, should be one of [{tool_names}]
Action Input: the input to the action
Observation: the result of the action
... (this Thought/Action/Action Input/Observation can repeat N times)
Thought: I now know the final answer
Final Answer: Provide a comprehensive validation result with:
1. Overall validation status (VALID/INVALID/NEEDS_REVIEW)
2. Confidence score (0-100)
3. Detailed reasoning
4. Any issues found

Question: {input}
Thought: {agent_scratchpad}
""")
        
        # Create agent
        agent = create_react_agent(self.llm, tools, prompt)
        
        # Create executor
        return AgentExecutor(
            agent=agent,
            tools=tools,
            verbose=True,
            handle_parsing_errors=True,
            max_iterations=10
        )
    
    def validate_receipt(self, receipt_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate a receipt using AI agent.
        
        Args:
            receipt_data: Receipt data dictionary
            
        Returns:
            Dictionary with validation results:
            - status: VALID/INVALID/NEEDS_REVIEW
            - confidence: 0-100 score
            - reasoning: Detailed explanation
            - issues: List of issues found
        """
        try:
            # Format receipt data for agent
            receipt_str = self._format_receipt_for_agent(receipt_data)
            
            # Run agent
            result = self.agent.invoke({
                "input": f"Validate this receipt for reconciliation: {receipt_str}"
            })
            
            # Parse agent response
            return self._parse_agent_response(result, receipt_data)
            
        except Exception as e:
            return {
                "status": "ERROR",
                "confidence": 0,
                "reasoning": f"Validation error: {str(e)}",
                "issues": [f"Validation failed: {str(e)}"]
            }
    
    def _format_receipt_for_agent(self, receipt_data: Dict[str, Any]) -> str:
        """Format receipt data as string for agent input."""
        key_fields = [
            'ID', 'RECEIPT_NUMBER', 'STATUS', 'AMOUNT', 'MONTH', 'YEAR',
            'EMPLOYER_ID', 'OFFICE_ID', 'MEMBER_ID',
            'MAIN_SCHEME_ID', 'SCHEME_ID', 'RECEIPT_TYPE', 'APPORTION_TYPE',
            'RECEIPT_DATE', 'EMPLOYER_OFFICE_ID', 'PENALTY_ID', 'ADJUSTMENT_ID'
        ]
        
        formatted = []
        for field in key_fields:
            value = receipt_data.get(field)
            if value is not None:
                formatted.append(f"{field}: {value}")
        
        return "\n".join(formatted)
    
    def _parse_agent_response(self, agent_result: Dict[str, Any], receipt_data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse agent response into structured validation result."""
        output = agent_result.get("output", "")
        
        # Extract status
        status = "NEEDS_REVIEW"
        if "VALID" in output.upper() and "INVALID" not in output.upper():
            status = "VALID"
        elif "INVALID" in output.upper():
            status = "INVALID"
        
        # Extract confidence (look for number 0-100)
        import re
        confidence_match = re.search(r'(\d+)\s*(?:%|confidence|score)', output, re.IGNORECASE)
        confidence = int(confidence_match.group(1)) if confidence_match else 50
        
        # Extract issues
        issues = []
        if "INVALID" in output or "ERROR" in output or "VIOLATION" in output:
            # Try to extract specific issues
            issue_patterns = [
                r'Missing[^:]*:?\s*([^\n]+)',
                r'Invalid[^:]*:?\s*([^\n]+)',
                r'ERROR[^:]*:?\s*([^\n]+)',
                r'VIOLATION[^:]*:?\s*([^\n]+)',
            ]
            for pattern in issue_patterns:
                matches = re.findall(pattern, output, re.IGNORECASE)
                issues.extend(matches)
        
        return {
            "status": status,
            "confidence": confidence,
            "reasoning": output,
            "issues": issues if issues else [],
            "receipt_id": receipt_data.get("ID"),
            "receipt_number": receipt_data.get("RECEIPT_NUMBER")
        }
    
    def validate_batch(self, receipts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Validate multiple receipts.
        
        Args:
            receipts: List of receipt dictionaries
            
        Returns:
            List of validation results
        """
        results = []
        for receipt in receipts:
            result = self.validate_receipt(receipt)
            results.append(result)
        return results

