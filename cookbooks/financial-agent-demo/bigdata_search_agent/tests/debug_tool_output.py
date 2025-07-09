#!/usr/bin/env python3
"""Debug script to test news search tool output format."""

import asyncio
import sys
from pathlib import Path

# Add parent directories to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

from test_config import TestConfig
from bigdata_search_agent.tools import bigdata_news_search

async def main():
    """Debug news search tool output."""
    print("🔍 Debugging News Search Tool Output")
    print("=" * 50)
    
    # Check configuration
    config = TestConfig()
    if not config.validate_credentials():
        print("❌ Cannot run tests without valid credentials")
        return
    
    print(f"✅ Using entity ID: {config.TEST_ENTITY_ID} ({config.TEST_COMPANY_NAME})")
    
    # Test the actual tool call that the test is using
    print("\n1️⃣ Testing Tool Call (like in test)...")
    try:
        result = await bigdata_news_search.ainvoke({
            "queries": ["Tesla"],
            "max_results": 3,
            "entity_ids": [config.TEST_ENTITY_ID]
        })
        
        print(f"   Result type: {type(result)}")
        print(f"   Result length: {len(result)} characters")
        print(f"   '--- SOURCE' count: {result.count('--- SOURCE')}")
        print(f"   'Title:' count: {result.count('Title:')}")
        print(f"   'Content:' count: {result.count('Content:')}")
        
        print("\n   First 800 characters of result:")
        print("   " + "="*60)
        print(result[:800])
        print("   " + "="*60)
        
        if "No valid" in result or "No results" in result:
            print("   ⚠️  Found 'No results' message in output")
        
    except Exception as e:
        print(f"   ❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
    
    # Test empty query (like in test 1 and 2)
    print("\n2️⃣ Testing Empty Query (like test 1 & 2)...")
    try:
        result = await bigdata_news_search.ainvoke({
            "queries": [""],  # Empty query
            "max_results": 3,
            "entity_ids": [config.TEST_ENTITY_ID],
            "date_range": "last_90_days"
        })
        
        print(f"   Result type: {type(result)}")
        print(f"   Result length: {len(result)} characters")
        print(f"   '--- SOURCE' count: {result.count('--- SOURCE')}")
        
        print("\n   First 500 characters of result:")
        print("   " + "="*60)
        print(result[:500])
        print("   " + "="*60)
        
    except Exception as e:
        print(f"   ❌ Error: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main()) 