"""
Test configuration for Bigdata search tests.

Loads environment variables and provides test utilities.
"""

import os
import sys
from pathlib import Path
from typing import Optional

try:
    from dotenv import load_dotenv
    DOTENV_AVAILABLE = True
except ImportError:
    DOTENV_AVAILABLE = False
    print("Warning: python-dotenv not available. Install with: pip install python-dotenv")

# Load environment variables from .env file if available
if DOTENV_AVAILABLE:
    # Look for .env file in the test directory
    env_path = Path(__file__).parent / '.env'
    if env_path.exists():
        load_dotenv(env_path)
        print(f"✅ Loaded environment from {env_path}")
    else:
        print(f"⚠️  No .env file found at {env_path}")
        print("   Copy env_example.txt to .env and fill in your credentials")

class TestConfig:
    """Configuration class for Bigdata search tests."""
    
    # API Credentials
    BIGDATA_USERNAME: Optional[str] = os.getenv("BIGDATA_USERNAME")
    BIGDATA_PASSWORD: Optional[str] = os.getenv("BIGDATA_PASSWORD")
    
    # Test Entity Configuration
    TEST_ENTITY_ID: str = os.getenv("TEST_ENTITY_ID", "DD3BB1")  # Tesla
    TEST_COMPANY_NAME: str = os.getenv("TEST_COMPANY_NAME", "Tesla")
    TEST_TICKER: str = os.getenv("TEST_TICKER", "TSLA")
    
    # Test Date Ranges
    TEST_DATE_START: str = os.getenv("TEST_DATE_START", "2024-01-01")
    TEST_DATE_END: str = os.getenv("TEST_DATE_END", "2024-12-31")
    
    @classmethod
    def validate_credentials(cls) -> bool:
        """Validate that required credentials are available."""
        if not cls.BIGDATA_USERNAME or not cls.BIGDATA_PASSWORD:
            print("❌ Missing Bigdata credentials!")
            print("   Set BIGDATA_USERNAME and BIGDATA_PASSWORD environment variables")
            print("   Or create a .env file in the test directory")
            return False
        return True
    
    @classmethod
    def print_config(cls):
        """Print the current test configuration (without passwords)."""
        print("🔧 Test Configuration:")
        print(f"   Username: {cls.BIGDATA_USERNAME or 'NOT SET'}")
        print(f"   Password: {'SET' if cls.BIGDATA_PASSWORD else 'NOT SET'}")
        print(f"   Test Entity: {cls.TEST_COMPANY_NAME} ({cls.TEST_ENTITY_ID})")
        print(f"   Test Ticker: {cls.TEST_TICKER}")
        print(f"   Date Range: {cls.TEST_DATE_START} to {cls.TEST_DATE_END}")

# Common test data
TEST_QUERIES = {
    "news": [
        "earnings report",
        "quarterly results", 
        "revenue growth",
        "market share"
    ],
    "transcripts": [
        "guidance for next quarter",
        "margin outlook",
        "production targets",
        "competitive landscape"
    ],
    "filings": [
        "risk factors",
        "competition",
        "regulatory compliance",
        "business strategy"
    ],
    "universal": [
        "autonomous driving progress",
        "sustainability initiatives",
        "market expansion",
        "technology development"
    ],
    "knowledge_graph": [
        "Tesla",
        "Microsoft", 
        "Apple",
        "automotive"
    ]
}

# Test parameter combinations
TEST_PARAMS = {
    "date_ranges": [
        "today",
        "yesterday", 
        "last_week",
        "last_30_days",
        "last_90_days",
        "year_to_date",
        "2024-01-01,2024-03-31"  # Q1 2024
    ],
    "transcript_types": [
        ["EARNINGS_CALL"],
        ["CONFERENCE_CALL"],
        ["EARNINGS_CALL", "CONFERENCE_CALL"]
    ],
    "section_metadata": [
        ["QA"],
        ["MANAGEMENT_DISCUSSION"],
        ["QUESTION"],
        ["ANSWER"],
        ["QA", "MANAGEMENT_DISCUSSION"]
    ],
    "filing_types": [
        ["SEC_10_K"],
        ["SEC_10_Q"], 
        ["SEC_8_K"],
        ["SEC_10_K", "SEC_10_Q"],
        ["SEC_10_K", "SEC_10_Q", "SEC_8_K"]
    ],
    "fiscal_periods": [
        {"fiscal_year": 2024},
        {"fiscal_year": 2023},
        {"fiscal_year": 2024, "fiscal_quarter": 1},
        {"fiscal_year": 2024, "fiscal_quarter": 2},
        {"fiscal_year": 2024, "fiscal_quarter": 3}
    ],
    "max_results": [1, 3, 5, 10],
    "rerank_thresholds": [0.1, 0.3, 0.5, 0.7, 0.9]
} 