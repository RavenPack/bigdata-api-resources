#!/usr/bin/env python3
"""
Transcript Search Tests for Bigdata Search Tools

Tests 8 key transcript search scenarios:
1. Entity ID + Date Range
2. Entity ID + Fiscal Quarters  
3. Entity ID + Similarity (Tesla Inc + what was mentioned about sales guidance)
4. Entity ID + Similarity + Fiscal Quarter (Tesla Inc + what did analysts ask about macro economic color + FY2025 Q1)
5. Entity ID + Similarity + Section (Tesla Inc + what did analysts ask about macro economic color + FY2025 Q1 + QA)
6. Reporting Entity ID + Similarity (Tesla Inc + What guidance did Elon give around EVs)
7. Similarity Only (What did companies say about Tesla's sales guidance)
8. Similarity Only + Date Range (What did companies say about Tesla's sales guidance + last 90 days)
"""

import asyncio
import sys
from pathlib import Path

# Add parent directories to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

from test_config import TestConfig, TEST_QUERIES
from bigdata_search_agent.tools import bigdata_transcript_search

# Rich imports for better table formatting
try:
    from rich.console import Console
    from rich.table import Table
    from rich.text import Text
    from rich import box
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False
    print("Rich not available. Install with: pip install rich")

class TranscriptSearchTester:
    """Test suite for transcript search functionality."""
    
    def __init__(self):
        self.config = TestConfig()
        self.results = []
        self.console = Console() if RICH_AVAILABLE else None
        
        # Test data
        self.test_entity_id = self.config.TEST_ENTITY_ID  # Tesla: DD3BB1
    
    def _extract_first_title(self, result_text: str) -> str:
        """Extract the first title from search results."""
        if "Title:" in result_text:
            lines = result_text.split("\n")
            for line in lines:
                if line.strip().startswith("Title:"):
                    title = line.replace("Title:", "").strip()
                    return title[:60] + "..." if len(title) > 60 else title
        return "No title found"
    
    def _extract_first_content(self, result_text: str) -> str:
        """Extract the first content snippet from search results."""
        if "Content:" in result_text:
            lines = result_text.split("\n")
            for line in lines:
                if line.strip().startswith("Content:"):
                    content = line.replace("Content:", "").strip()
                    return content[:800] + "..." if len(content) > 800 else content
        return "No content found"
    
    async def test_1_entity_id_plus_date_range(self):
        """Test 1: Entity ID + Date Range"""
        print("\n📅 Test 1: Entity ID + Date Range")
        print("-" * 50)
        
        # Single test with one date range - no query needed, just entity and date filtering
        date_range = "last_90_days"
        
        try:
            print(f"\n🔍 Searching Tesla transcripts for date range: {date_range}")
            result = await bigdata_transcript_search.ainvoke({
                "queries": [""],  # Empty query - pure entity + date filtering
                "max_results": 3,
                "entity_ids": [self.test_entity_id],
                "date_range": date_range
            })
            
            print(f"✅ Found results for {date_range}")
            
            # Extract key info for table display
            result_count = result.count("--- TRANSCRIPT RESULT") if "--- TRANSCRIPT RESULT" in result else 0
            first_title = self._extract_first_title(result)
            first_content = self._extract_first_content(result)
            
            # Store actual query and parameters used
            self.results.append(("entity_date", f"entity:{self.test_entity_id} + date:{date_range}", "success", result_count, first_title, first_content))
            
        except Exception as e:
            print(f"❌ Error with date range {date_range}: {str(e)}")
            self.results.append(("entity_date", f"entity:{self.test_entity_id} + date:{date_range}", "error", 0, "N/A", str(e)[:100]))
    
    async def test_2_entity_id_plus_fiscal_quarters(self):
        """Test 2: Entity ID + Fiscal Quarters"""
        print("\n📊 Test 2: Entity ID + Fiscal Quarters")
        print("-" * 50)
        
        # Single test with one fiscal quarter - no query needed, just entity and fiscal filtering
        fiscal_year = 2024
        fiscal_quarter = 3
        period_str = f"FY{fiscal_year}Q{fiscal_quarter}"
        
        try:
            print(f"\n🔍 Searching Tesla transcripts for {period_str}")
            result = await bigdata_transcript_search.ainvoke({
                "queries": [""],  # Empty query - pure entity + fiscal filtering
                "max_results": 3,
                "entity_ids": [self.test_entity_id],
                "fiscal_year": fiscal_year,
                "fiscal_quarter": fiscal_quarter
            })
            
            print(f"✅ Found results for {period_str}")
            
            result_count = result.count("--- TRANSCRIPT RESULT") if "--- TRANSCRIPT RESULT" in result else 0
            first_title = self._extract_first_title(result)
            first_content = self._extract_first_content(result)
            
            self.results.append(("entity_fiscal", f"entity:{self.test_entity_id} + {period_str}", "success", result_count, first_title, first_content))
            
        except Exception as e:
            print(f"❌ Error with fiscal period {period_str}: {str(e)}")
            self.results.append(("entity_fiscal", f"entity:{self.test_entity_id} + {period_str}", "error", 0, "N/A", str(e)[:100]))
    
    async def test_3_entity_id_plus_similarity(self):
        """Test 3: Entity ID + Similarity (Tesla Inc + what was mentioned about sales guidance)"""
        print("\n🎯 Test 3: Entity ID + Similarity (Tesla Inc + what was mentioned about sales guidance)")
        print("-" * 50)
        
        # Single test with one query
        query = "what was mentioned about sales guidance"
        
        try:
            print(f"\n🔍 Searching Tesla transcripts for: '{query}'")
            result = await bigdata_transcript_search.ainvoke({
                "queries": [query],
                "max_results": 5,
                "entity_ids": [self.test_entity_id],
                "transcript_types": ["EARNINGS_CALL"]
            })
            
            print(f"✅ Found results for '{query}'")
            
            result_count = result.count("--- TRANSCRIPT RESULT") if "--- TRANSCRIPT RESULT" in result else 0
            first_title = self._extract_first_title(result)
            first_content = self._extract_first_content(result)
            
            self.results.append(("entity_similarity", f"'{query}' + entity:{self.test_entity_id} + EARNINGS_CALL", "success", result_count, first_title, first_content))
            
        except Exception as e:
            print(f"❌ Error searching for '{query}': {str(e)}")
            self.results.append(("entity_similarity", f"'{query}' + entity:{self.test_entity_id} + EARNINGS_CALL", "error", 0, "N/A", str(e)[:100]))
    
    async def test_4_entity_id_plus_similarity_plus_fiscal_quarter(self):
        """Test 4: Entity ID + Similarity + Fiscal Quarter (Tesla Inc + what did analysts ask about macro economic color + FY2025 Q1)"""
        print("\n📈 Test 4: Entity ID + Similarity + Fiscal Quarter (Tesla Inc + what did analysts ask about macro economic color + FY2025 Q1)")
        print("-" * 50)
        
        # Single test with one query
        query = "what did analysts ask about macro economic color"
        
        try:
            print(f"\n🔍 Searching Tesla FY2025Q1 transcripts for: '{query}'")
            result = await bigdata_transcript_search.ainvoke({
                "queries": [query],
                "max_results": 5,
                "entity_ids": [self.test_entity_id],
                "transcript_types": ["EARNINGS_CALL"],
                "fiscal_year": 2025,
                "fiscal_quarter": 1
            })
            
            print(f"✅ Found results for '{query}' in FY2025Q1")
            
            result_count = result.count("--- TRANSCRIPT RESULT") if "--- TRANSCRIPT RESULT" in result else 0
            first_title = self._extract_first_title(result)
            first_content = self._extract_first_content(result)
            
            self.results.append(("entity_similarity_fiscal", f"'{query}' + entity:{self.test_entity_id} + FY2025Q1", "success", result_count, first_title, first_content))
            
        except Exception as e:
            print(f"❌ Error searching for '{query}' in FY2025Q1: {str(e)}")
            self.results.append(("entity_similarity_fiscal", f"'{query}' + entity:{self.test_entity_id} + FY2025Q1", "error", 0, "N/A", str(e)[:100]))
    
    async def test_5_entity_id_plus_similarity_plus_fiscal_quarter_plus_section(self):
        """Test 5: Entity ID + Similarity + Section (Tesla Inc + what did analysts ask about macro economic color + FY2025 Q1 + QA)"""
        print("\n❓ Test 5: Entity ID + Similarity + Section (Tesla Inc + what did analysts ask about macro economic color + FY2025 Q1 + QA)")
        print("-" * 50)
        
        # Single test with one query
        query = "what did analysts ask about macro economic color"
        
        try:
            print(f"\n🔍 Searching Tesla FY2025Q1 Q&A sections for: '{query}'")
            result = await bigdata_transcript_search.ainvoke({
                "queries": [query],
                "max_results": 5,
                "entity_ids": [self.test_entity_id],
                "transcript_types": ["EARNINGS_CALL"],
                "fiscal_year": 2025,
                "fiscal_quarter": 1,
                "section_metadata": ["QA"]
            })
            
            print(f"✅ Found Q&A results for '{query}' in FY2025Q1")
            
            result_count = result.count("--- TRANSCRIPT RESULT") if "--- TRANSCRIPT RESULT" in result else 0
            first_title = self._extract_first_title(result)
            first_content = self._extract_first_content(result)
            
            self.results.append(("entity_similarity_fiscal_section", f"'{query}' + entity:{self.test_entity_id} + FY2025Q1 + QA", "success", result_count, first_title, first_content))
            
        except Exception as e:
            print(f"❌ Error searching Q&A for '{query}' in FY2025Q1: {str(e)}")
            self.results.append(("entity_similarity_fiscal_section", f"'{query}' + entity:{self.test_entity_id} + FY2025Q1 + QA", "error", 0, "N/A", str(e)[:100]))
    
    async def test_6_reporting_entity_id_plus_similarity(self):
        """Test 6: Reporting Entity ID + Similarity (Tesla Inc + What guidance did Elon give around EVs)"""
        print("\n🏢 Test 6: Reporting Entity ID + Similarity (Tesla Inc + What guidance did Elon give around EVs)")
        print("-" * 50)
        
        # Single test with one query
        query = "What guidance did Elon give around EVs"
        
        try:
            print(f"\n🔍 Searching transcripts filed by Tesla for: '{query}'")
            result = await bigdata_transcript_search.ainvoke({
                "queries": [query],
                "max_results": 5,
                "reporting_entity_ids": [self.test_entity_id],  # Use reporting_entity_ids for transcripts filed BY Tesla
                "transcript_types": ["EARNINGS_CALL"]
            })
            
            print(f"✅ Found reporting entity results for '{query}'")
            
            result_count = result.count("--- TRANSCRIPT RESULT") if "--- TRANSCRIPT RESULT" in result else 0
            first_title = self._extract_first_title(result)
            first_content = self._extract_first_content(result)
            
            self.results.append(("reporting_entity_similarity", f"'{query}' + reporting_entity:{self.test_entity_id} + EARNINGS_CALL", "success", result_count, first_title, first_content))
            
        except Exception as e:
            print(f"❌ Error with reporting entity search for '{query}': {str(e)}")
            self.results.append(("reporting_entity_similarity", f"'{query}' + reporting_entity:{self.test_entity_id} + EARNINGS_CALL", "error", 0, "N/A", str(e)[:100]))
    
    async def test_7_similarity_only(self):
        """Test 7: Similarity Only (What did companies say about Tesla's sales guidance)"""
        print("\n🔍 Test 7: Similarity Only (What did companies say about Tesla's sales guidance)")
        print("-" * 50)
        
        # Single test with one query
        query = "What did companies say about Tesla's sales guidance"
        
        try:
            print(f"\n🔍 Open search for: '{query}'")
            result = await bigdata_transcript_search.ainvoke({
                "queries": [query],
                "max_results": 5,
                "transcript_types": ["EARNINGS_CALL"]
                # No entity_ids - let the query find Tesla mentions naturally
            })
            
            print(f"✅ Found open search results for '{query}'")
            
            result_count = result.count("--- TRANSCRIPT RESULT") if "--- TRANSCRIPT RESULT" in result else 0
            first_title = self._extract_first_title(result)
            first_content = self._extract_first_content(result)
            
            self.results.append(("similarity_only", f"'{query}' (no entity filter)", "success", result_count, first_title, first_content))
            
        except Exception as e:
            print(f"❌ Error with open search for '{query}': {str(e)}")
            self.results.append(("similarity_only", f"'{query}' (no entity filter)", "error", 0, "N/A", str(e)[:100]))
    
    async def test_8_similarity_only_plus_date_range(self):
        """Test 8: Similarity Only + Date Range (What did companies say about Tesla's sales guidance + last 90 days)"""
        print("\n📅 Test 8: Similarity Only + Date Range (What did companies say about Tesla's sales guidance + last 90 days)")
        print("-" * 50)
        
        # Single test with one query and one date range
        query = "What did companies say about Tesla's sales guidance"
        date_range = "last_90_days"
        
        try:
            print(f"\n🔍 Open search for: '{query}' in {date_range}")
            result = await bigdata_transcript_search.ainvoke({
                "queries": [query],
                "max_results": 5,
                "transcript_types": ["EARNINGS_CALL"],
                "date_range": date_range
                # No entity_ids - let the query find Tesla mentions naturally
            })
            
            print(f"✅ Found open search results for '{query}' in {date_range}")
            
            result_count = result.count("--- TRANSCRIPT RESULT") if "--- TRANSCRIPT RESULT" in result else 0
            first_title = self._extract_first_title(result)
            first_content = self._extract_first_content(result)
            
            self.results.append(("similarity_only_date", f"'{query}' + {date_range}", "success", result_count, first_title, first_content))
            
        except Exception as e:
            print(f"❌ Error with open search for '{query}' in {date_range}: {str(e)}")
            self.results.append(("similarity_only_date", f"'{query}' + {date_range}", "error", 0, "N/A", str(e)[:100]))
    
    def print_summary(self):
        """Print test summary with Rich tables."""
        print("\n" + "="*60)
        print("🧪 TRANSCRIPT SEARCH TEST SUMMARY")
        print("="*60)
        
        if RICH_AVAILABLE and self.console:
            # Create detailed results with a better format
            print("\n")
            self.console.print("📊 Detailed Test Results", style="bold blue")
            print()
            
            for i, (test_type, query_info, status, result_count, first_title, first_content) in enumerate(self.results, 1):
                # Create individual panels for each test result
                status_emoji = "✅" if status == "success" else "❌"
                status_color = "green" if status == "success" else "red"
                
                # Create a panel for each test
                self.console.print(f"[bold cyan]Test {i}: {test_type.replace('_', ' ').title()}[/bold cyan]")
                self.console.print(f"[yellow]Query:[/yellow] {query_info}")
                self.console.print(f"[{status_color}]Status:[/{status_color}] {status_emoji} {status.upper()}")
                self.console.print(f"[green]Results Found:[/green] {result_count}")
                
                if first_title and first_title != "No title found":
                    self.console.print(f"[blue]First Title:[/blue] {first_title}")
                
                if first_content and first_content != "No content found":
                    # Show more content without truncation
                    self.console.print(f"[white]First Content:[/white]")
                    self.console.print(f"[dim]{first_content}[/dim]")
                
                # Add separator between tests
                if i < len(self.results):
                    self.console.print("─" * 80, style="dim")
                    print()
            
            # Summary table - much simpler
            print()
            self.console.print("📈 Summary by Test Type", style="bold blue")
            
            summary_table = Table(
                box=box.SIMPLE,
                show_header=True,
                header_style="bold magenta"
            )
            
            summary_table.add_column("Test Type", style="cyan", no_wrap=True)
            summary_table.add_column("Status", justify="center")
            summary_table.add_column("Results", justify="center", style="green")
            summary_table.add_column("Query", style="yellow")
            
            # Group results by test type for summary
            for test_type, query_info, status, result_count, first_title, first_content in self.results:
                status_display = "✅ SUCCESS" if status == "success" else "❌ ERROR"
                query_short = query_info[:50] + "..." if len(query_info) > 50 else query_info
                
                summary_table.add_row(
                    test_type.replace("_", " "),
                    status_display,
                    str(result_count),
                    query_short
                )
            
            self.console.print(summary_table)
            
            # Overall results
            total_tests = len(self.results)
            total_success = sum(1 for _, _, status, _, _, _ in self.results if status == "success")
            overall_success_rate = total_success / total_tests * 100
            
            print("\n")
            self.console.print("🎯 Overall Results", style="bold blue")
            self.console.print(f"• Total tests: [bold]{total_tests}[/bold]")
            self.console.print(f"• Overall success rate: [bold green]{overall_success_rate:.1f}%[/bold green]")
            self.console.print(f"• Test Entity: [bold]{self.config.TEST_COMPANY_NAME}[/bold] ([cyan]{self.test_entity_id}[/cyan])")
            self.console.print(f"• Focus: [italic]8 comprehensive test scenarios[/italic]")
            self.console.print(f"• Rerank Threshold: [bold yellow]0.1[/bold yellow] (applied by default)")
            
        else:
            # Fallback to simple text output if Rich not available
            print("\n## 📊 Detailed Test Results (Rich not available - install with: pip install rich)\n")
            
            print("| Test Type | Actual Query + Filters | Status | Results | First Title | First Content |")
            print("|-----------|------------------------|---------|---------|-------------|---------------|")
            
            for test_type, query_info, status, result_count, first_title, first_content in self.results:
                # Truncate long content for table display
                query_display = query_info[:25] + "..." if len(query_info) > 25 else query_info
                title_display = first_title[:40] + "..." if len(first_title) > 40 else first_title
                content_display = first_content[:50] + "..." if len(first_content) > 50 else first_content
                
                # Status emoji
                status_emoji = "✅" if status == "success" else "❌"
                
                print(f"| {test_type} | {query_display} | {status_emoji} {status} | {result_count} | {title_display} | {content_display} |")

async def main():
    """Run all transcript search tests."""
    print("🧪 Bigdata Transcript Search Tests - 8 Scenarios")
    print("=" * 60)
    
    # Check configuration
    config = TestConfig()
    config.print_config()
    
    if not config.validate_credentials():
        print("\n❌ Cannot run tests without valid credentials")
        return
    
    # Initialize tester
    tester = TranscriptSearchTester()
    
    # Run all 8 tests
    try:
        await tester.test_1_entity_id_plus_date_range()
        await tester.test_2_entity_id_plus_fiscal_quarters()
        await tester.test_3_entity_id_plus_similarity()
        await tester.test_4_entity_id_plus_similarity_plus_fiscal_quarter()
        await tester.test_5_entity_id_plus_similarity_plus_fiscal_quarter_plus_section()
        await tester.test_6_reporting_entity_id_plus_similarity()
        await tester.test_7_similarity_only()
        await tester.test_8_similarity_only_plus_date_range()
        
    except KeyboardInterrupt:
        print("\n⏹️  Tests interrupted by user")
    except Exception as e:
        print(f"\n❌ Unexpected error during testing: {str(e)}")
    
    # Print summary
    tester.print_summary()

if __name__ == "__main__":
    asyncio.run(main()) 