#!/usr/bin/env python3
"""
Test Scenarios Script for search_on_premium_sources.py
This script tests various configurations and edge cases of the search functionality.
"""

import os
import sys
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any

# Import the search functions
from search_in_premium_sources import (
    search_premium_sources,
    save_documents_to_json,
    count_total_chunks,
    all_my_premium_source_ids,
    premium_news_source_ids,
    quartr_transcripts_source_ids
)

class SearchTestRunner:
    """Test runner for different search scenarios"""
    
    def __init__(self):
        """Initialize the test runner"""
        self.test_results = []
        self.start_time = None
        
    def start_test_session(self):
        """Start a new test session"""
        self.start_time = datetime.now()
        print("=" * 80)
        print("PREMIUM SOURCES SEARCH - TEST SCENARIOS")
        print("=" * 80)
        print(f"Test session started at: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
    def end_test_session(self):
        """End the test session and display summary"""
        end_time = datetime.now()
        duration = end_time - self.start_time
        
        print("=" * 80)
        print("TEST SESSION SUMMARY")
        print("=" * 80)
        print(f"Session ended at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Total duration: {duration}")
        print(f"Tests run: {len(self.test_results)}")
        
        # Count successful and failed tests
        successful = sum(1 for result in self.test_results if result['status'] == 'SUCCESS')
        failed = len(self.test_results) - successful
        
        print(f"Successful: {successful}")
        print(f"Failed: {failed}")
        print()
        
        # Display detailed results
        for result in self.test_results:
            status_icon = "✅" if result['status'] == 'SUCCESS' else "❌"
            print(f"{status_icon} {result['name']}: {result['status']}")
            if result['status'] == 'SUCCESS':
                print(f"   Documents: {result['documents']}, Chunks: {result['chunks']}")
            if result['error']:
                print(f"   Error: {result['error']}")
            print()
    
    def run_test(self, test_name: str, test_func, *args, **kwargs) -> Dict[str, Any]:
        """Run a single test and record results"""
        print(f"🧪 Running: {test_name}")
        print("-" * 60)
        
        start_time = time.time()
        result = {
            'name': test_name,
            'status': 'UNKNOWN',
            'documents': 0,
            'chunks': 0,
            'error': None,
            'duration': 0
        }
        
        try:
            # Run the test
            documents = test_func(*args, **kwargs)
            
            # Record results
            result['status'] = 'SUCCESS'
            result['documents'] = len(documents)
            result['chunks'] = count_total_chunks(documents)
            result['duration'] = time.time() - start_time
            
            print(f"✅ SUCCESS: Found {len(documents)} documents with {result['chunks']} chunks")
            print(f"⏱️  Duration: {result['duration']:.2f} seconds")
            
        except Exception as e:
            result['status'] = 'FAILED'
            result['error'] = str(e)
            result['duration'] = time.time() - start_time
            
            print(f"❌ FAILED: {str(e)}")
            print(f"⏱️  Duration: {result['duration']:.2f} seconds")
        
        print()
        self.test_results.append(result)
        return result

def test_basic_search():
    """Test basic search functionality"""
    return search_premium_sources("earnings call transcript")

def test_news_only_search():
    """Test search with only news sources"""
    return search_premium_sources("market analysis", document_types=["NEWS"])

def test_transcript_only_search():
    """Test search with only transcript sources"""
    return search_premium_sources("quarterly earnings call", 
                                document_types=["FACTSET_TRANSCRIPTS", "QUARTR_TRANSCRIPTS"])

def test_sec_filings_only_search():
    """Test search with only SEC filings"""
    return search_premium_sources("10-K annual report", document_types=["SEC_FILINGS"])

def test_date_range_search():
    """Test search with date range"""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    return search_premium_sources("earnings announcement", 
                                start_date=start_date.strftime("%Y-%m-%d"),
                                end_date=end_date.strftime("%Y-%m-%d"))

def test_combined_document_types():
    """Test search with specific combination of document types"""
    return search_premium_sources("financial results", 
                                document_types=["NEWS", "FACTSET_TRANSCRIPTS"])

def test_long_query():
    """Test search with a longer, more complex query"""
    long_query = "artificial intelligence technology adoption in financial services and banking sector"
    return search_premium_sources(long_query)

def test_short_query():
    """Test search with a very short query"""
    return search_premium_sources("AI")

def test_special_characters():
    """Test search with special characters"""
    return search_premium_sources("Q4 2024 earnings & revenue growth")

def test_company_specific():
    """Test search for specific company information"""
    return search_premium_sources("Apple Inc quarterly earnings call transcript")

def test_market_event():
    """Test search for market events"""
    return search_premium_sources("Federal Reserve interest rate decision")

def test_negative_search():
    """Test search that might trigger secondary search (NEWS with < 5 chunks)"""
    # This query might return few results, triggering the secondary search logic
    return search_premium_sources("obscure financial instrument", document_types=["NEWS"])

def test_all_sources():
    """Test search across all document types explicitly"""
    return search_premium_sources("financial data analysis", 
                                document_types=["NEWS", "FACTSET_TRANSCRIPTS", "QUARTR_TRANSCRIPTS", "SEC_FILINGS"])

def test_empty_results():
    """Test search that might return no results"""
    return search_premium_sources("xyz123nonexistentterm", document_types=["NEWS"])

def test_large_date_range():
    """Test search with a large date range"""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)  # 1 year
    
    return search_premium_sources("earnings call", 
                                start_date=start_date.strftime("%Y-%m-%d"),
                                end_date=end_date.strftime("%Y-%m-%d"))

def test_recent_events():
    """Test search for recent events"""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)  # Last week
    
    return search_premium_sources("market volatility", 
                                start_date=start_date.strftime("%Y-%m-%d"),
                                end_date=end_date.strftime("%Y-%m-%d"))

def display_source_information():
    """Display information about available sources"""
    print("📊 AVAILABLE PREMIUM SOURCES")
    print("=" * 60)
    print(f"Total premium sources: {len(all_my_premium_source_ids)}")
    print(f"Premium news sources: {len(premium_news_source_ids)}")
    print(f"Quartr transcript sources: {len(quartr_transcripts_source_ids)}")
    print()
    
    print("📰 NEWS SOURCES:")
    for i, source_id in enumerate(premium_news_source_ids, 1):
        print(f"  {i:2d}. {source_id}")
    print()
    
    print("📝 TRANSCRIPT SOURCES:")
    for i, source_id in enumerate(quartr_transcripts_source_ids, 1):
        print(f"  {i:2d}. {source_id}")
    print()

def run_quick_tests():
    """Run a quick set of essential tests"""
    runner = SearchTestRunner()
    runner.start_test_session()
    
    print("🚀 Running Quick Test Suite...")
    print()
    
    # Essential tests
    runner.run_test("Basic Search", test_basic_search)
    runner.run_test("News Only Search", test_news_only_search)
    runner.run_test("Transcript Only Search", test_transcript_only_search)
    runner.run_test("Date Range Search", test_date_range_search)
    
    runner.end_test_session()

def run_comprehensive_tests():
    """Run all available tests"""
    runner = SearchTestRunner()
    runner.start_test_session()
    
    print("🔍 Running Comprehensive Test Suite...")
    print()
    
    # All test scenarios
    tests = [
        ("Basic Search", test_basic_search),
        ("News Only Search", test_news_only_search),
        ("Transcript Only Search", test_transcript_only_search),
        ("SEC Filings Only Search", test_sec_filings_only_search),
        ("Date Range Search", test_date_range_search),
        ("Combined Document Types", test_combined_document_types),
        ("Long Query", test_long_query),
        ("Short Query", test_short_query),
        ("Special Characters", test_special_characters),
        ("Company Specific", test_company_specific),
        ("Market Event", test_market_event),
        ("Negative Search (Secondary Search Trigger)", test_negative_search),
        ("All Sources Explicit", test_all_sources),
        ("Empty Results", test_empty_results),
        ("Large Date Range", test_large_date_range),
        ("Recent Events", test_recent_events),
    ]
    
    for test_name, test_func in tests:
        runner.run_test(test_name, test_func)
    
    runner.end_test_session()

def run_custom_test():
    """Run a custom test with user input"""
    print("🎯 Custom Test Configuration")
    print("=" * 60)
    
    # Get search query
    query = input("Enter search query: ").strip()
    if not query:
        print("❌ Query cannot be empty")
        return
    
    # Get document types
    print("\nAvailable document types:")
    print("1. NEWS")
    print("2. FACTSET_TRANSCRIPTS") 
    print("3. QUARTR_TRANSCRIPTS")
    print("4. SEC_FILINGS")
    print("5. All (default)")
    
    choice = input("\nSelect document types (comma-separated numbers, or press Enter for all): ").strip()
    
    document_types = ["NEWS", "FACTSET_TRANSCRIPTS", "QUARTR_TRANSCRIPTS", "SEC_FILINGS"]
    if choice:
        try:
            indices = [int(x.strip()) - 1 for x in choice.split(",")]
            document_types = [document_types[i] for i in indices if 0 <= i < len(document_types)]
        except (ValueError, IndexError):
            print("❌ Invalid selection, using all document types")
    
    # Get date range
    use_dates = input("\nUse date range? (y/n, default: n): ").strip().lower()
    start_date = None
    end_date = None
    
    if use_dates == 'y':
        start_date = input("Start date (YYYY-MM-DD): ").strip()
        end_date = input("End date (YYYY-MM-DD): ").strip()
    
    # Get custom filename
    custom_filename = input("\nCustom output filename (optional): ").strip()
    if not custom_filename:
        custom_filename = None
    
    print(f"\n🔍 Running custom test...")
    print(f"Query: {query}")
    print(f"Document types: {document_types}")
    print(f"Date range: {start_date} to {end_date}" if start_date and end_date else "Date range: None")
    print(f"Output file: {custom_filename or 'auto-generated'}")
    print()
    
    # Run the test
    runner = SearchTestRunner()
    runner.start_test_session()
    
    try:
        documents = search_premium_sources(
            sentence=query,
            start_date=start_date,
            end_date=end_date,
            document_types=document_types
        )
        
        # Save results
        output_path = save_documents_to_json(documents, custom_filename)
        
        print(f"✅ Custom test completed successfully!")
        print(f"📄 Documents found: {len(documents)}")
        print(f"📊 Total chunks: {count_total_chunks(documents)}")
        print(f"💾 Results saved to: {output_path}")
        
    except Exception as e:
        print(f"❌ Custom test failed: {e}")
    
    runner.end_test_session()

def main():
    """Main function to run the test scenarios"""
    print("🧪 Premium Sources Search - Test Scenarios")
    print("=" * 60)
    
    # Display source information
    display_source_information()
    
    while True:
        print("Select test mode:")
        print("1. Quick Test Suite (4 essential tests)")
        print("2. Comprehensive Test Suite (all 16 tests)")
        print("3. Custom Test (user-defined parameters)")
        print("4. Exit")
        
        choice = input("\nEnter your choice (1-4): ").strip()
        
        if choice == '1':
            run_quick_tests()
        elif choice == '2':
            run_comprehensive_tests()
        elif choice == '3':
            run_custom_test()
        elif choice == '4':
            print("👋 Goodbye!")
            break
        else:
            print("❌ Invalid choice. Please enter 1, 2, 3, or 4.")
        
        print("\n" + "=" * 60 + "\n")

if __name__ == "__main__":
    main()
