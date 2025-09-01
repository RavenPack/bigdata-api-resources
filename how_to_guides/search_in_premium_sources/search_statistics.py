#!/usr/bin/env python3
"""
Search Statistics Script for search_in_premium_sources.py
This script analyzes how often the secondary search is triggered across different financial research scenarios.
"""

import os
import time
import json
from datetime import datetime
from typing import List, Dict, Any
from collections import defaultdict
import statistics

# Import the search functions
from search_in_premium_sources import (
    search_premium_sources,
    count_total_chunks,
    ensure_output_directory
)

class SearchStatisticsAnalyzer:
    """Analyzer for search statistics and secondary search frequency"""
    
    def __init__(self):
        """Initialize the statistics analyzer"""
        self.test_results = []
        self.secondary_search_stats = defaultdict(int)
        self.document_type_stats = defaultdict(int)
        self.chunk_count_stats = []
        self.execution_time_stats = []
        self.start_time = None
        
    def start_analysis_session(self):
        """Start a new analysis session"""
        self.start_time = datetime.now()
        print("=" * 100)
        print("PREMIUM SOURCES SEARCH - STATISTICS ANALYSIS")
        print("=" * 100)
        print(f"Analysis session started at: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
    def end_analysis_session(self):
        """End the analysis session and display comprehensive statistics"""
        end_time = datetime.now()
        duration = end_time - self.start_time
        
        print("=" * 100)
        print("COMPREHENSIVE STATISTICS REPORT")
        print("=" * 100)
        print(f"Session ended at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Total duration: {duration}")
        print(f"Total tests run: {len(self.test_results)}")
        print()
        
        # Secondary search statistics
        total_secondary_searches = sum(self.secondary_search_stats.values())
        secondary_search_rate = (total_secondary_searches / len(self.test_results)) * 100 if self.test_results else 0
        
        print("🔍 SECONDARY SEARCH STATISTICS")
        print("-" * 50)
        print(f"Total secondary searches triggered: {total_secondary_searches}")
        print(f"Secondary search rate: {secondary_search_rate:.2f}%")
        print(f"Single search rate: {100 - secondary_search_rate:.2f}%")
        print()
        
        # Document type analysis
        print("📊 DOCUMENT TYPE ANALYSIS")
        print("-" * 50)
        for doc_type, count in self.document_type_stats.items():
            percentage = (count / len(self.test_results)) * 100 if self.test_results else 0
            print(f"{doc_type}: {count} tests ({percentage:.1f}%)")
        print()
        
        # Chunk count statistics
        if self.chunk_count_stats:
            print("📈 CHUNK COUNT STATISTICS")
            print("-" * 50)
            print(f"Average chunks per test: {statistics.mean(self.chunk_count_stats):.2f}")
            print(f"Median chunks per test: {statistics.median(self.chunk_count_stats):.2f}")
            print(f"Min chunks: {min(self.chunk_count_stats)}")
            print(f"Max chunks: {max(self.chunk_count_stats)}")
            print(f"Standard deviation: {statistics.stdev(self.chunk_count_stats):.2f}")
            print()
        
        # Execution time statistics
        if self.execution_time_stats:
            print("⏱️  EXECUTION TIME STATISTICS")
            print("-" * 50)
            print(f"Average execution time: {statistics.mean(self.execution_time_stats):.2f} seconds")
            print(f"Median execution time: {statistics.median(self.execution_time_stats):.2f} seconds")
            print(f"Min execution time: {min(self.execution_time_stats):.2f} seconds")
            print(f"Max execution time: {max(self.execution_time_stats):.2f} seconds")
            print()
        
        # Secondary search breakdown by document type
        print("🎯 SECONDARY SEARCH BREAKDOWN BY DOCUMENT TYPE")
        print("-" * 50)
        for doc_type, count in self.secondary_search_stats.items():
            if count > 0:
                print(f"{doc_type}: {count} secondary searches triggered")
        print()
        
        # Save detailed results
        self.save_statistics_report()
        
    def run_search_test(self, test_name: str, query: str, document_types: List[str] = None, 
                       start_date: str = None, end_date: str = None) -> Dict[str, Any]:
        """Run a single search test and record detailed statistics"""
        print(f"🧪 Running: {test_name}")
        print(f"   Query: {query}")
        print(f"   Document types: {document_types or 'All'}")
        print("-" * 80)
        
        start_time = time.time()
        result = {
            'name': test_name,
            'query': query,
            'document_types': document_types or ["NEWS", "FACTSET_TRANSCRIPTS", "QUARTR_TRANSCRIPTS", "SEC_FILINGS"],
            'start_date': start_date,
            'end_date': end_date,
            'status': 'UNKNOWN',
            'documents': 0,
            'chunks': 0,
            'secondary_search_triggered': False,
            'error': None,
            'duration': 0,
            'timestamp': datetime.now().isoformat()
        }
        
        try:
            # Perform search
            documents, secondary_search_required = search_premium_sources(
                sentence=query,
                start_date=start_date,
                end_date=end_date,
                document_types=document_types
            )
            
            # Record results
            result['status'] = 'SUCCESS'
            result['documents'] = len(documents)
            result['chunks'] = count_total_chunks(documents)
            result['duration'] = time.time() - start_time
            
            # Record secondary search information from the function return
            result['secondary_search_triggered'] = secondary_search_required
            if secondary_search_required:
                self.secondary_search_stats['NEWS'] += 1
            
            # Update document type statistics
            for doc_type in result['document_types']:
                self.document_type_stats[doc_type] += 1
            
            # Update chunk and time statistics
            self.chunk_count_stats.append(result['chunks'])
            self.execution_time_stats.append(result['duration'])
            
            print(f"✅ SUCCESS: Found {len(documents)} documents with {result['chunks']} chunks")
            print(f"⏱️  Duration: {result['duration']:.2f} seconds")
            if result['secondary_search_triggered']:
                print("🔄 Secondary search was triggered (NEWS < 5 chunks)")
            print()
            
        except Exception as e:
            result['status'] = 'FAILED'
            result['error'] = str(e)
            result['duration'] = time.time() - start_time
            
            print(f"❌ FAILED: {str(e)}")
            print(f"⏱️  Duration: {result['duration']:.2f} seconds")
            print()
        
        self.test_results.append(result)
        return result
    
    def save_statistics_report(self):
        """Save detailed statistics report to JSON file"""
        ensure_output_directory()
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"search_statistics_report_{timestamp}.json"
        output_path = os.path.join("output", filename)
        
        # Prepare comprehensive report
        report = {
            'session_info': {
                'start_time': self.start_time.isoformat() if self.start_time else None,
                'end_time': datetime.now().isoformat(),
                'total_tests': len(self.test_results)
            },
            'summary_statistics': {
                'secondary_search_rate': (sum(self.secondary_search_stats.values()) / len(self.test_results)) * 100 if self.test_results else 0,
                'total_secondary_searches': sum(self.secondary_search_stats.values()),
                'average_chunks': statistics.mean(self.chunk_count_stats) if self.chunk_count_stats else 0,
                'average_execution_time': statistics.mean(self.execution_time_stats) if self.execution_time_stats else 0
            },
            'detailed_results': self.test_results,
            'secondary_search_breakdown': dict(self.secondary_search_stats),
            'document_type_distribution': dict(self.document_type_stats)
        }
        
        # Save to file
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        print(f"📊 Detailed statistics report saved to: {output_path}")
        return output_path

# Financial Research Analyst Questions - 50 Different Use Cases
FINANCIAL_RESEARCH_QUESTIONS = [
    # Earnings & Financial Performance
    ("Q1: Q4 2024 earnings call transcript analysis", "Q4 2024 earnings call transcript analysis", ["FACTSET_TRANSCRIPTS", "QUARTR_TRANSCRIPTS"]),
    ("Q2: Annual revenue growth trends", "Annual revenue growth trends", ["NEWS", "SEC_FILINGS"]),
    ("Q3: Quarterly earnings miss expectations", "Quarterly earnings miss expectations", ["NEWS", "FACTSET_TRANSCRIPTS"]),
    ("Q4: EBITDA margin improvement strategies", "EBITDA margin improvement strategies", ["FACTSET_TRANSCRIPTS", "SEC_FILINGS"]),
    ("Q5: Free cash flow generation analysis", "Free cash flow generation analysis", ["SEC_FILINGS", "NEWS"]),
    
    # Market Analysis & Trends
    ("Q6: Market volatility impact on tech stocks", "Market volatility impact on tech stocks", ["NEWS"]),
    ("Q7: Sector rotation strategies Q4 2024", "Sector rotation strategies Q4 2024", ["NEWS"]),
    ("Q8: Federal Reserve interest rate decision impact", "Federal Reserve interest rate decision impact", ["NEWS"]),
    ("Q9: Inflation expectations and market reaction", "Inflation expectations and market reaction", ["NEWS"]),
    ("Q10: Geopolitical risks affecting emerging markets", "Geopolitical risks affecting emerging markets", ["NEWS"]),
    
    # Company-Specific Research
    ("Q11: Apple Inc AI strategy implementation", "Apple Inc AI strategy implementation", ["NEWS", "FACTSET_TRANSCRIPTS"]),
    ("Q12: Microsoft cloud services market share", "Microsoft cloud services market share", ["NEWS", "FACTSET_TRANSCRIPTS"]),
    ("Q13: Tesla autonomous driving technology progress", "Tesla autonomous driving technology progress", ["NEWS", "FACTSET_TRANSCRIPTS"]),
    ("Q14: Amazon e-commerce competition analysis", "Amazon e-commerce competition analysis", ["NEWS", "SEC_FILINGS"]),
    ("Q15: Google advertising revenue trends", "Google advertising revenue trends", ["NEWS", "FACTSET_TRANSCRIPTS"]),
    
    # Industry Analysis
    ("Q16: Electric vehicle market penetration rates", "Electric vehicle market penetration rates", ["NEWS", "FACTSET_TRANSCRIPTS"]),
    ("Q17: Semiconductor supply chain disruptions", "Semiconductor supply chain disruptions", ["NEWS", "FACTSET_TRANSCRIPTS"]),
    ("Q18: Healthcare technology adoption trends", "Healthcare technology adoption trends", ["NEWS", "FACTSET_TRANSCRIPTS"]),
    ("Q19: Renewable energy investment patterns", "Renewable energy investment patterns", ["NEWS", "SEC_FILINGS"]),
    ("Q20: Fintech disruption in traditional banking", "Fintech disruption in traditional banking", ["NEWS", "FACTSET_TRANSCRIPTS"]),
    
    # Regulatory & Compliance
    ("Q21: SEC enforcement actions 2024", "SEC enforcement actions 2024", ["SEC_FILINGS", "NEWS"]),
    ("Q22: GDPR compliance challenges for tech companies", "GDPR compliance challenges for tech companies", ["NEWS", "SEC_FILINGS"]),
    ("Q23: Antitrust investigations in big tech", "Antitrust investigations in big tech", ["NEWS", "SEC_FILINGS"]),
    ("Q24: Environmental regulations impact on energy sector", "Environmental regulations impact on energy sector", ["NEWS", "SEC_FILINGS"]),
    ("Q25: Banking regulations post-2008 crisis", "Banking regulations post-2008 crisis", ["NEWS", "SEC_FILINGS"]),
    
    # Mergers & Acquisitions
    ("Q26: Recent M&A activity in healthcare sector", "Recent M&A activity in healthcare sector", ["NEWS", "FACTSET_TRANSCRIPTS"]),
    ("Q27: Private equity investment trends", "Private equity investment trends", ["NEWS", "FACTSET_TRANSCRIPTS"]),
    ("Q28: Cross-border acquisition challenges", "Cross-border acquisition challenges", ["NEWS", "FACTSET_TRANSCRIPTS"]),
    ("Q29: Hostile takeover defense strategies", "Hostile takeover defense strategies", ["NEWS", "FACTSET_TRANSCRIPTS"]),
    ("Q30: Post-merger integration success factors", "Post-merger integration success factors", ["NEWS", "FACTSET_TRANSCRIPTS"]),
    
    # Risk Management
    ("Q31: Credit risk assessment methodologies", "Credit risk assessment methodologies", ["NEWS", "FACTSET_TRANSCRIPTS"]),
    ("Q32: Operational risk in financial institutions", "Operational risk in financial institutions", ["NEWS", "SEC_FILINGS"]),
    ("Q33: Cybersecurity threats to financial sector", "Cybersecurity threats to financial sector", ["NEWS", "SEC_FILINGS"]),
    ("Q34: Climate risk disclosure requirements", "Climate risk disclosure requirements", ["SEC_FILINGS", "NEWS"]),
    ("Q35: Liquidity risk management strategies", "Liquidity risk management strategies", ["NEWS", "SEC_FILINGS"]),
    
    # Investment Strategies
    ("Q36: Value investing vs growth investing", "Value investing vs growth investing", ["NEWS", "FACTSET_TRANSCRIPTS"]),
    ("Q37: ESG investment criteria implementation", "ESG investment criteria implementation", ["NEWS", "SEC_FILINGS"]),
    ("Q38: Quantitative trading strategies", "Quantitative trading strategies", ["NEWS", "FACTSET_TRANSCRIPTS"]),
    ("Q39: Alternative investment opportunities", "Alternative investment opportunities", ["NEWS", "FACTSET_TRANSCRIPTS"]),
    ("Q40: International diversification benefits", "International diversification benefits", ["NEWS", "FACTSET_TRANSCRIPTS"]),
    
    # Economic Indicators
    ("Q41: Unemployment rate impact on consumer spending", "Unemployment rate impact on consumer spending", ["NEWS"]),
    ("Q42: GDP growth correlation with stock returns", "GDP growth correlation with stock returns", ["NEWS"]),
    ("Q43: Consumer confidence index trends", "Consumer confidence index trends", ["NEWS"]),
    ("Q44: Housing market indicators analysis", "Housing market indicators analysis", ["NEWS"]),
    ("Q45: Manufacturing PMI interpretation", "Manufacturing PMI interpretation", ["NEWS"]),
    
    # Specialized Financial Topics
    ("Q46: Cryptocurrency regulation developments", "Cryptocurrency regulation developments", ["NEWS"]),
    ("Q47: Central bank digital currency initiatives", "Central bank digital currency initiatives", ["NEWS"]),
    ("Q48: Sustainable finance market growth", "Sustainable finance market growth", ["NEWS", "SEC_FILINGS"]),
    ("Q49: Digital transformation in financial services", "Digital transformation in financial services", ["NEWS", "FACTSET_TRANSCRIPTS"]),
    ("Q50: Pandemic recovery impact on markets", "Pandemic recovery impact on markets", ["NEWS", "FACTSET_TRANSCRIPTS"])
]

def run_comprehensive_statistics():
    """Run comprehensive statistics analysis on all 50 financial research questions"""
    analyzer = SearchStatisticsAnalyzer()
    analyzer.start_analysis_session()
    
    print("🚀 Running Comprehensive Statistics Analysis...")
    print(f"Testing {len(FINANCIAL_RESEARCH_QUESTIONS)} financial research scenarios")
    print()
    
    # Run all tests
    for i, (test_name, query, doc_types) in enumerate(FINANCIAL_RESEARCH_QUESTIONS, 1):
        print(f"Progress: {i}/{len(FINANCIAL_RESEARCH_QUESTIONS)}")
        analyzer.run_search_test(test_name, query, doc_types)
        
        # Small delay to avoid overwhelming the API
        time.sleep(1)
    
    analyzer.end_analysis_session()

def run_quick_statistics():
    """Run quick statistics on a subset of questions"""
    analyzer = SearchStatisticsAnalyzer()
    analyzer.start_analysis_session()
    
    print("⚡ Running Quick Statistics Analysis...")
    print("Testing 10 representative financial research scenarios")
    print()
    
    # Select every 5th question for quick analysis
    quick_tests = FINANCIAL_RESEARCH_QUESTIONS[::5][:10]
    
    for i, (test_name, query, doc_types) in enumerate(quick_tests, 1):
        print(f"Progress: {i}/{len(quick_tests)}")
        analyzer.run_search_test(test_name, query, doc_types)
        time.sleep(1)
    
    analyzer.end_analysis_session()

def run_custom_statistics():
    """Run custom statistics with user-defined questions"""
    print("🎯 Custom Statistics Analysis")
    print("=" * 60)
    
    # Get custom questions
    custom_questions = []
    print("Enter your financial research questions (one per line, press Enter twice to finish):")
    
    while True:
        question = input("Question: ").strip()
        if not question:
            break
        custom_questions.append(question)
    
    if not custom_questions:
        print("❌ No questions provided")
        return
    
    # Get document types
    print("\nSelect document types:")
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
    
    # Run custom analysis
    analyzer = SearchStatisticsAnalyzer()
    analyzer.start_analysis_session()
    
    print(f"\n🔍 Running custom analysis on {len(custom_questions)} questions...")
    print(f"Document types: {document_types}")
    print()
    
    for i, question in enumerate(custom_questions, 1):
        test_name = f"Custom Q{i}: {question[:50]}..."
        analyzer.run_search_test(test_name, question, document_types)
        time.sleep(1)
    
    analyzer.end_analysis_session()

def main():
    """Main function to run the statistics analysis"""
    print("📊 Premium Sources Search - Statistics Analysis")
    print("=" * 60)
    print("This script analyzes secondary search frequency across 50 financial research scenarios")
    print()
    
    while True:
        print("Select analysis mode:")
        print("1. Quick Statistics (10 representative questions)")
        print("2. Comprehensive Statistics (all 50 questions)")
        print("3. Custom Statistics (user-defined questions)")
        print("4. Exit")
        
        choice = input("\nEnter your choice (1-4): ").strip()
        
        if choice == '1':
            run_quick_statistics()
        elif choice == '2':
            run_comprehensive_statistics()
        elif choice == '3':
            run_custom_statistics()
        elif choice == '4':
            print("👋 Goodbye!")
            break
        else:
            print("❌ Invalid choice. Please enter 1, 2, 3, or 4.")
        
        print("\n" + "=" * 60 + "\n")

if __name__ == "__main__":
    main()
