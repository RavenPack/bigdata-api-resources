#!/usr/bin/env python3
"""Debug script to test news search functionality."""

import asyncio
import sys
from pathlib import Path

# Add parent directories to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

from test_config import TestConfig
from bigdata_search_agent.utils import bigdata_knowledge_graph_async, bigdata_news_search_async

async def main():
    """Debug news search issues."""
    print("🔍 Debugging News Search Issues")
    print("=" * 50)
    
    # Check configuration
    config = TestConfig()
    if not config.validate_credentials():
        print("❌ Cannot run tests without valid credentials")
        return
    
    print(f"✅ Using entity ID: {config.TEST_ENTITY_ID} ({config.TEST_COMPANY_NAME})")
    
    # Test 1: Knowledge graph search for Tesla
    print("\n1️⃣ Testing Knowledge Graph Search for Tesla...")
    try:
        companies = await bigdata_knowledge_graph_async('companies', 'Tesla', max_results=5)
        print(f"   Found {len(companies)} Tesla companies:")
        for i, company in enumerate(companies[:3], 1):
            print(f"   {i}. ID: {company.get('id', 'N/A')}, Name: {company.get('name', 'N/A')}, Ticker: {company.get('ticker', 'N/A')}")
    except Exception as e:
        print(f"   ❌ Error: {str(e)}")
    
    # Test 2: Simple news search without filters
    print("\n2️⃣ Testing Simple News Search (no filters)...")
    try:
        results = await bigdata_news_search_async(['Tesla'], max_results=3)
        print(f"   Found {len(results)} news results")
        if results:
            for i, result in enumerate(results[:2], 1):
                print(f"   {i}. {result.get('title', 'No title')[:80]}...")
        else:
            print("   No news results found")
    except Exception as e:
        print(f"   ❌ Error: {str(e)}")
    
    # Test 3: News search with entity ID
    print(f"\n3️⃣ Testing News Search with Entity ID ({config.TEST_ENTITY_ID})...")
    try:
        results = await bigdata_news_search_async([''], max_results=3, entity_ids=[config.TEST_ENTITY_ID])
        print(f"   Found {len(results)} news results with entity filter")
        if results:
            for i, result in enumerate(results[:2], 1):
                print(f"   {i}. {result.get('title', 'No title')[:80]}...")
        else:
            print("   No news results found with entity filter")
    except Exception as e:
        print(f"   ❌ Error: {str(e)}")
    
    # Test 4: News search with broader date range
    print("\n4️⃣ Testing News Search with Broader Date Range...")
    try:
        results = await bigdata_news_search_async(['Tesla'], max_results=3, date_range='last_year')
        print(f"   Found {len(results)} news results in last year")
        if results:
            for i, result in enumerate(results[:2], 1):
                print(f"   {i}. {result.get('title', 'No title')[:80]}...")
        else:
            print("   No news results found in last year")
    except Exception as e:
        print(f"   ❌ Error: {str(e)}")
    
    # Test 5: Test sources knowledge graph
    print("\n5️⃣ Testing News Sources Knowledge Graph...")
    try:
        sources = await bigdata_knowledge_graph_async('sources', 'Reuters', max_results=3)
        print(f"   Found {len(sources)} news sources matching 'Reuters':")
        for i, source in enumerate(sources[:3], 1):
            print(f"   {i}. {source.get('name', 'N/A')} ({source.get('key', 'N/A')})")
    except Exception as e:
        print(f"   ❌ Error: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main()) 