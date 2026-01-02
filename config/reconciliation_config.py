"""Reconciliation configuration settings."""

import os
from dotenv import load_dotenv
from typing import Optional

load_dotenv()


# AI Model Configuration
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
OPENAI_TEMPERATURE = float(os.getenv("OPENAI_TEMPERATURE", "0"))

# Validation Thresholds
MIN_CONFIDENCE_SCORE = float(os.getenv("MIN_CONFIDENCE_SCORE", "80"))
AUTO_RECONCILE_THRESHOLD = float(os.getenv("AUTO_RECONCILE_THRESHOLD", "90"))
MANUAL_REVIEW_THRESHOLD = float(os.getenv("MANUAL_REVIEW_THRESHOLD", "70"))

# Business Rules
MAX_RECEIPT_AMOUNT = float(os.getenv("MAX_RECEIPT_AMOUNT", "10000000"))
MIN_RECEIPT_AMOUNT = float(os.getenv("MIN_RECEIPT_AMOUNT", "0.01"))
VALID_MONTHS = list(range(1, 13))
VALID_YEARS = list(range(2000, 2101))

# Reconciliation Settings
BATCH_SIZE = int(os.getenv("RECONCILIATION_BATCH_SIZE", "10"))
ENABLE_AUTO_RECONCILE = os.getenv("ENABLE_AUTO_RECONCILE", "true").lower() == "true"
LOG_LEVEL = os.getenv("RECONCILIATION_LOG_LEVEL", "INFO")

