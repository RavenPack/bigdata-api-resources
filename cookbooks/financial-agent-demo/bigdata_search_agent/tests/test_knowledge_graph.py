#!/usr/bin/env python3
"""
Knowledge Graph Tests for Bigdata Search Tools

Tests various aspects of the knowledge graph functionality:
- Company discovery by name, ticker, variations
- Source discovery with filtering
- Autosuggest functionality
- Error handling and edge cases
"""

import asyncio
import sys
from pathlib import Path

# Add parent directories to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

from test_config import TestConfig, TEST_QUERIES
from bigdata_search_agent.tools import bigdata_knowledge_graph

class KnowledgeGraphTester:
    """Test suite for knowledge graph functionality."""
    
    def __init__(self):
        self.config = TestConfig()
        self.results = []
    
    async def test_company_search_exact_names(self):
        """Test company search with exact company names."""
        print("\n🏢 Testing Company Search - Exact Names")
        print("-" * 50)
        
        companies = ["Tesla Inc", "Microsoft Corporation", "Apple Inc", "Amazon.com Inc"]
        
        for company in companies:
            try:
                print(f"\n🔍 Searching for: {company}")
                result = await bigdata_knowledge_graph.ainvoke({
                    "search_type": "companies",
                    "search_term": company,
                    "max_results": 3
                })
                
                print(f"✅ Found results for {company}")
                print(result[:200] + "..." if len(result) > 200 else result)
                self.results.append(("company_exact", company, "success", len(result)))
                
            except Exception as e:
                print(f"❌ Error searching for {company}: {str(e)}")
                self.results.append(("company_exact", company, "error", str(e)))
    
    async def test_company_search_tickers(self):
        """Test company search with ticker symbols."""
        print("\n📊 Testing Company Search - Ticker Symbols")
        print("-" * 50)
        
        tickers = ["TSLA", "MSFT", "AAPL", "AMZN", "GOOGL", "META"]
        
        for ticker in tickers:
            try:
                print(f"\n🔍 Searching for ticker: {ticker}")
                result = await bigdata_knowledge_graph.ainvoke({
                    "search_type": "companies",
                    "search_term": ticker,
                    "max_results": 3
                })
                
                print(f"✅ Found results for {ticker}")
                print(result[:200] + "..." if len(result) > 200 else result)
                self.results.append(("company_ticker", ticker, "success", len(result)))
                
            except Exception as e:
                print(f"❌ Error searching for {ticker}: {str(e)}")
                self.results.append(("company_ticker", ticker, "error", str(e)))
    
    async def test_company_search_partial_names(self):
        """Test company search with partial/common names."""
        print("\n🔤 Testing Company Search - Partial Names")
        print("-" * 50)
        
        partial_names = ["Tesla", "Microsoft", "Apple", "Amazon", "Google", "Meta"]
        
        for name in partial_names:
            try:
                print(f"\n🔍 Searching for partial name: {name}")
                result = await bigdata_knowledge_graph.ainvoke({
                    "search_type": "companies",
                    "search_term": name,
                    "max_results": 5  # More results for partial matches
                })
                
                print(f"✅ Found results for {name}")
                print(result[:300] + "..." if len(result) > 300 else result)
                self.results.append(("company_partial", name, "success", len(result)))
                
            except Exception as e:
                print(f"❌ Error searching for {name}: {str(e)}")
                self.results.append(("company_partial", name, "error", str(e)))
    
    async def test_source_search_specific(self):
        """Test source search with specific news source names."""
        print("\n📰 Testing Source Search - Specific Sources")
        print("-" * 50)
        
        sources = ["Reuters", "Seeking Alpha", "Aljazeera", "Quartr", "CNBC"]
        
        for source in sources:
            try:
                print(f"\n🔍 Searching for source: {source}")
                result = await bigdata_knowledge_graph.ainvoke({
                    "search_type": "sources",
                    "search_term": source,
                    "max_results": 3
                })
                
                print(f"✅ Found results for {source}")
                print(result[:200] + "..." if len(result) > 200 else result)
                self.results.append(("source_specific", source, "success", len(result)))
                
            except Exception as e:
                print(f"❌ Error searching for {source}: {str(e)}")
                self.results.append(("source_specific", source, "error", str(e)))
    
    async def test_source_search_with_filters(self):
        """Test source search with credibility filtering."""
        print("\n⭐ Testing Source Search - With Credibility Filters")
        print("-" * 50)
        
        search_terms = ["financial", "automotive", "technology"]
        credibility_ranks = [1, 2, 3]  # Test different credibility levels
        
        for term in search_terms:
            for rank in credibility_ranks:
                try:
                    print(f"\n🔍 Searching for '{term}' sources with rank {rank}")
                    result = await bigdata_knowledge_graph.ainvoke({
                        "search_type": "sources",
                        "search_term": term,
                        "max_results": 3,
                        "source_rank_filter": rank
                    })
                    
                    print(f"✅ Found rank-{rank} results for {term}")
                    print(result[:200] + "..." if len(result) > 200 else result)
                    self.results.append(("source_filtered", f"{term}_rank{rank}", "success", len(result)))
                    
                except Exception as e:
                    print(f"❌ Error searching for {term} with rank {rank}: {str(e)}")
                    self.results.append(("source_filtered", f"{term}_rank{rank}", "error", str(e)))
    

    
    async def test_max_results_variations(self):
        """Test different max_results values."""
        print("\n📊 Testing Max Results Variations")
        print("-" * 50)
        
        max_results_values = [1, 3, 5, 10, 20]
        test_term = self.config.TEST_COMPANY_NAME
        
        for max_results in max_results_values:
            try:
                print(f"\n🔍 Testing max_results={max_results} for {test_term}")
                result = await bigdata_knowledge_graph.ainvoke({
                    "search_type": "companies",
                    "search_term": test_term,
                    "max_results": max_results
                })
                
                print(f"✅ Got results with max_results={max_results}")
                # Count actual results returned (rough estimate)
                result_count = result.count("--- COMPANY") if "--- COMPANY" in result else 0
                print(f"   Estimated results returned: {result_count}")
                self.results.append(("max_results", f"{test_term}_{max_results}", "success", result_count))
                
            except Exception as e:
                print(f"❌ Error with max_results={max_results}: {str(e)}")
                self.results.append(("max_results", f"{test_term}_{max_results}", "error", str(e)))
    
    async def test_error_handling(self):
        """Test error handling with invalid inputs."""
        print("\n⚠️  Testing Error Handling")
        print("-" * 50)
        
        # Test invalid search types
        invalid_tests = [
            {"search_type": "invalid_type", "search_term": "test", "expected": "invalid search type"},
            {"search_type": "companies", "search_term": "", "expected": "empty search term"},
            {"search_type": "companies", "search_term": "nonexistent_company_xyz123", "expected": "no results"},
        ]
        
        for test_case in invalid_tests:
            try:
                print(f"\n🔍 Testing: {test_case}")
                result = await bigdata_knowledge_graph.ainvoke(test_case)
                
                if "No" in result or "not found" in result or "Error" in result:
                    print(f"✅ Proper error handling: {result[:100]}...")
                    self.results.append(("error_handling", test_case["search_type"], "handled", result[:100]))
                else:
                    print(f"⚠️  Unexpected result: {result[:100]}...")
                    self.results.append(("error_handling", test_case["search_type"], "unexpected", result[:100]))
                
            except Exception as e:
                print(f"✅ Exception caught as expected: {str(e)}")
                self.results.append(("error_handling", test_case["search_type"], "exception", str(e)))
    
    def print_summary(self):
        """Print test summary."""
        print("\n" + "="*60)
        print("🧪 KNOWLEDGE GRAPH TEST SUMMARY")
        print("="*60)
        
        # Group results by test type
        test_types = {}
        for test_type, term, status, result in self.results:
            if test_type not in test_types:
                test_types[test_type] = {"success": 0, "error": 0, "handled": 0, "unexpected": 0}
            test_types[test_type][status] += 1
        
        for test_type, counts in test_types.items():
            total = sum(counts.values())
            success_rate = (counts["success"] + counts["handled"]) / total * 100
            print(f"\n📊 {test_type.upper()}:")
            print(f"   Total tests: {total}")
            print(f"   Success: {counts['success']}")
            print(f"   Errors: {counts['error']}")
            print(f"   Handled: {counts['handled']}")
            print(f"   Success rate: {success_rate:.1f}%")
        
        total_tests = len(self.results)
        total_success = sum(1 for _, _, status, _ in self.results if status in ["success", "handled"])
        overall_success_rate = total_success / total_tests * 100
        
        print(f"\n🎯 OVERALL RESULTS:")
        print(f"   Total tests: {total_tests}")
        print(f"   Overall success rate: {overall_success_rate:.1f}%")

async def main():
    """Run all knowledge graph tests."""
    print("🧪 Bigdata Knowledge Graph Tests")
    print("=" * 60)
    
    # Check configuration
    config = TestConfig()
    config.print_config()
    
    if not config.validate_credentials():
        print("\n❌ Cannot run tests without valid credentials")
        return
    
    # Initialize tester
    tester = KnowledgeGraphTester()
    
    # Run all tests
    try:
        await tester.test_company_search_exact_names()
        await tester.test_company_search_tickers()
        await tester.test_company_search_partial_names()
        await tester.test_source_search_specific()
        await tester.test_source_search_with_filters()
        await tester.test_max_results_variations()
        await tester.test_error_handling()
        
    except KeyboardInterrupt:
        print("\n⏹️  Tests interrupted by user")
    except Exception as e:
        print(f"\n❌ Unexpected error during testing: {str(e)}")
    
    # Print summary
    tester.print_summary()

if __name__ == "__main__":
    asyncio.run(main()) 