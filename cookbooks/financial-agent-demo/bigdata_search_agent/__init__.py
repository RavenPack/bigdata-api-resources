"""
Bigdata Search Tools - A standalone module for Bigdata.com API search functionality.

This module provides:
- Core search utilities for News, Transcripts, Filings, and Universal search
- LangChain tool wrappers for easy integration
- LangGraph workflow for intelligent search orchestration
- Knowledge graph functionality for entity and source discovery
"""

from .utils import (
    bigdata_news_search_async,
    bigdata_transcript_search_async,
    bigdata_filings_search_async,
    bigdata_universal_search_async,
    bigdata_knowledge_graph_async,
)

from .tools import (
    bigdata_news_search,
    bigdata_transcript_search,
    bigdata_filings_search,
    bigdata_universal_search,
    bigdata_knowledge_graph,
)

from .state import (
    SearchStrategy,
    SearchStrategies,
    SearchResult,
    BigdataSearchState,
    BigdataSearchStateInput,
    BigdataSearchStateOutput,
    SearchStrategyState,
    SearchStrategyOutput,
)

from .configuration import (
    BigdataSearchConfiguration,
    BigdataToolType,
)

from .graph import (
    bigdata_search_graph,
    generate_search_plan,
    execute_search_strategy,
    gather_search_results,
    compile_final_results,
)

__version__ = "0.1.0"
__all__ = [
    # Core utilities
    "bigdata_news_search_async",
    "bigdata_transcript_search_async", 
    "bigdata_filings_search_async",
    "bigdata_universal_search_async",
    "bigdata_knowledge_graph_async",
    # LangChain tools
    "bigdata_news_search",
    "bigdata_transcript_search",
    "bigdata_filings_search", 
    "bigdata_universal_search",
    "bigdata_knowledge_graph",
    # State models
    "SearchStrategy",
    "SearchStrategies",
    "SearchResult",
    "BigdataSearchState",
    "BigdataSearchStateInput",
    "BigdataSearchStateOutput",
    "SearchStrategyState",
    "SearchStrategyOutput",
    # Configuration
    "BigdataSearchConfiguration",
    "BigdataToolType",
    # Graph workflow
    "bigdata_search_graph",
    "generate_search_plan",
    "execute_search_strategy",
    "gather_search_results",
    "compile_final_results",
] 