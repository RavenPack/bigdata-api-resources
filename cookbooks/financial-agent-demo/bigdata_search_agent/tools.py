"""
LangChain Tool Wrappers for Bigdata.com API Integration

This module provides @tool decorated async functions that wrap the core Bigdata.com search utilities 
for seamless integration with LangChain agents and workflows. Each tool corresponds to a major 
Bigdata.com API endpoint and handles parameter validation, async execution, and result formatting.

## Core Tools Available:

### Content Search Tools:
- **`bigdata_news_search`**: Premium news content from global publishers
- **`bigdata_transcript_search`**: Corporate transcripts with section detection  
- **`bigdata_filings_search`**: SEC filings and regulatory documents
- **`bigdata_universal_search`**: Cross-document search with unified ranking

### Discovery Tools:
- **`bigdata_knowledge_graph`**: Company/source ID lookup and entity discovery

## Tool Architecture Patterns:

### Async Execution:
All tools are fully async and designed for concurrent execution within the LangGraph workflow. 
They wrap the underlying async utility functions from `utils.py` with LangChain tool decorators.

### Parameter Handling:
- **Type Safety**: All parameters are properly typed with Optional[] for flexibility
- **Validation**: Tools handle parameter cleaning and validation internally
- **Defaults**: Sensible defaults provided for most optional parameters
- **Entity IDs**: Support for entity_ids filtering across content search tools

### Result Formatting:
- **Structured Output**: All tools return formatted strings ready for LLM consumption
- **Consistent Format**: Standardized result structure across all tools
- **Error Handling**: Graceful error handling with descriptive error messages
- **Content Limits**: Raw content truncated to prevent token overflow

### Filter Support:
- **Date Ranges**: Rolling ("last_week") and absolute ("2024-01-01,2024-12-31") formats
- **Entity Filtering**: Company entity IDs for targeted searches  
- **Type Filtering**: Document types, filing types, transcript types, etc.
- **Quality Filtering**: Rerank thresholds, source credibility ranks

## Important Notes for Developers:

**Tool Registration**: Tools are automatically registered via @tool decorator - they can be used 
directly in LangChain agents or called programmatically from the graph workflow

**Async Context**: All tools must be called with `await` - they're designed for the async 
execution context of the LangGraph workflow

**Error Resilience**: Tools catch and format exceptions as strings rather than raising them, 
ensuring workflow continuity even when individual API calls fail

**Result Size**: Tools limit content size to prevent token overflow in LLM contexts - 
raw content is truncated at reasonable limits for different content types

**Knowledge Graph Dependency**: Content search tools often require entity IDs from the 
knowledge graph tool - this creates a natural workflow dependency

## Extension Guidelines:

### Adding New Tools:
1. Create async utility function in `utils.py` 
2. Add @tool decorated wrapper in this module
3. Follow naming pattern: `bigdata_{tool_name}_search`
4. Include comprehensive docstring with parameter descriptions
5. Add tool to graph.py tool_map if needed for workflow integration

### Parameter Extensions:
- Add new parameters as Optional[] types for backward compatibility
- Update parameter cleaning logic in graph.py `_clean_tool_parameters()`
- Document parameter behavior in tool docstring

### Result Format Changes:
- Maintain backward compatibility with existing result structure
- Consider LLM token limits when adding new content fields
- Test result formatting with different content sizes

## Integration with Main Workflow:

Tools are called by the `execute_search_strategy` node in the main graph workflow. The workflow:
1. Selects appropriate tool based on strategy type
2. Prepares and validates parameters  
3. Executes tool asynchronously
4. Captures results and metadata for compilation

Tool selection is handled via the `tool_map` dictionary in `graph.py` - add new tools there 
for automatic workflow integration.
"""

from typing import List, Optional, Dict, Any
from langchain_core.tools import tool

from .utils import (
    bigdata_news_search_async,
    bigdata_transcript_search_async,
    bigdata_filings_search_async,
    bigdata_universal_search_async,
    bigdata_knowledge_graph_async,
)

NEWS_SEARCH_DESCRIPTION = (
    "Search Bigdata premium news content with multilingual support. "
    "Useful for finding recent news articles, press releases, and media coverage "
    "from thousands of high-quality global news sources."
)

@tool(description=NEWS_SEARCH_DESCRIPTION)
async def bigdata_news_search(
    queries: List[str],
    max_results: int = 5,
    date_range: Optional[str] = None,
    source_ids: Optional[List[str]] = None,
    entity_ids: Optional[List[str]] = None
) -> str:
    """
    Search Bigdata news content with premium publisher access.
    
    Args:
        queries: List of search queries to execute
        max_results: Maximum number of results to return per query (default: 5)
        date_range: Date range filter in format:
            - Rolling: "today", "yesterday", "last_week", "last_month", "last_90_days", etc.
            - Absolute: "2024-01-01,2024-12-31"
        source_ids: List of news source IDs to filter by (use knowledge graph to find source IDs)
        entity_ids: List of company entity IDs to filter by (use knowledge graph to find IDs)
        rerank_threshold: Rerank threshold for similarity searches (0.0-1.0)
        
    Returns:
        Formatted string with news search results
    """
    try:
        results = await bigdata_news_search_async(
            search_queries=queries,
            max_results=max_results,
            date_range=date_range,
            source_ids=source_ids,
            entity_ids=entity_ids,
            include_raw_content=True
        )
        
        if not results:
            return "No news results found for the given queries."
        
        # Format results into readable string
        formatted_output = "News search results:\n\n"
        
        for i, result in enumerate(results, 1):
            formatted_output += f"--- SOURCE {i}: {result['title']} ---\n"
            formatted_output += f"Title: {result['title']}\n"
            formatted_output += f"URL: {result['url']}\n===\n"
            formatted_output += f"Most relevant content from source: {result['content']}\n===\n"
            formatted_output += f"Content: {result['content']}\n"
            if result.get('raw_content'):
                formatted_output += f"Full source content limited to 5000 tokens: {result['raw_content'][:20000]}\n\n"
            formatted_output += f"{'='*80}\n\n"
        
        return formatted_output
        
    except Exception as e:
        return f"Error executing news search: {str(e)}"

TRANSCRIPT_SEARCH_DESCRIPTION = (
    "Search corporate transcripts including earnings calls, conference calls, and investor meetings. "
    "Features advanced section detection (Q&A, management discussion) and speaker identification. "
    "Useful for finding specific quotes, financial guidance, and management commentary."
)

@tool(description=TRANSCRIPT_SEARCH_DESCRIPTION)
async def bigdata_transcript_search(
    queries: List[str],
    max_results: int = 5,
    transcript_types: Optional[List[str]] = None,
    section_metadata: Optional[List[str]] = None,
    fiscal_year: Optional[int] = None,
    fiscal_quarter: Optional[int] = None,
    entity_ids: Optional[List[str]] = None,
    reporting_entity_ids: Optional[List[str]] = None,
    date_range: Optional[str] = None
) -> str:
    """
    Search corporate transcript content with section detection and speaker identification.
    
    Args:
        queries: List of search queries to execute
        max_results: Maximum number of results to return per query (default: 5)
        transcript_types: Types of transcripts to search:
            - "EARNINGS_CALL", "CONFERENCE_CALL", "ANALYST_INVESTOR_SHAREHOLDER_MEETING"
            - "GENERAL_PRESENTATION", "GUIDANCE_CALL", "SALES_REVENUE_CALL"
        section_metadata: Specific sections to search:
            - "QA" (Q&A section), "QUESTION" (questions only), "ANSWER" (answers only)
            - "MANAGEMENT_DISCUSSION" (management discussion section)
        fiscal_year: Filter by fiscal year (e.g., 2024)
        fiscal_quarter: Filter by fiscal quarter (1, 2, 3, or 4)
        entity_ids: List of company entity IDs to filter by (documents mentioning these entities)
        reporting_entity_ids: List of entity IDs for companies that filed the transcripts
        date_range: Date range filter (rolling or absolute format)
        
    Returns:
        Formatted string with transcript search results
    """
    try:
        results = await bigdata_transcript_search_async(
            search_queries=queries,
            max_results=max_results,
            transcript_types=transcript_types,
            section_metadata=section_metadata,
            fiscal_year=fiscal_year,
            fiscal_quarter=fiscal_quarter,
            entity_ids=entity_ids,
            reporting_entity_ids=reporting_entity_ids,
            date_range=date_range,
            include_raw_content=True
        )
        
        if not results:
            return "No transcript results found for the given queries."
        
        # Format results into readable string
        formatted_output = "Transcript Search Results:\n\n"
        
        for i, result in enumerate(results, 1):
            formatted_output += f"--- TRANSCRIPT RESULT {i} ---\n"
            formatted_output += f"Title: {result['title']}\n"
            formatted_output += f"Content: {result['content']}\n"
            formatted_output += f"URL: {result['url']}\n"
            if result.get('document_timestamp'):
                formatted_output += f"Date: {result['document_timestamp']}\n"
            if result.get('chunk_index') is not None:
                formatted_output += f"Section: Chunk {result['chunk_index']}\n"
            formatted_output += f"Relevance Score: {result['score']:.3f}\n\n"
        
        return formatted_output
        
    except Exception as e:
        return f"Error executing transcript search: {str(e)}"

FILINGS_SEARCH_DESCRIPTION = (
    "Search SEC filings and corporate regulatory documents including 10-K, 10-Q, 8-K forms. "
    "Supports filtering by filing type, fiscal periods, and reporting entities. "
    "Useful for finding financial disclosures, risk factors, and regulatory compliance information."
)

@tool(description=FILINGS_SEARCH_DESCRIPTION)
async def bigdata_filings_search(
    queries: List[str],
    max_results: int = 5,
    filing_types: Optional[List[str]] = None,
    fiscal_year: Optional[int] = None,
    fiscal_quarter: Optional[int] = None,
    reporting_entity_ids: Optional[List[str]] = None,
    entity_ids: Optional[List[str]] = None,
    date_range: Optional[str] = None
) -> str:
    """
    Search SEC filings and regulatory documents with advanced filtering.
    
    Args:
        queries: List of search queries to execute
        max_results: Maximum number of results to return per query (default: 5)
        filing_types: Types of SEC filings to search:
            - "SEC_10_K" (annual reports), "SEC_10_Q" (quarterly reports)
            - "SEC_8_K" (current reports), "SEC_20_F" (foreign company annual reports)
            - "SEC_S_1" (IPO registration), "SEC_S_3" (shelf registration), "SEC_6_K" (foreign reports)
        fiscal_year: Filter by fiscal year (e.g., 2024)
        fiscal_quarter: Filter by fiscal quarter (1, 2, 3, or 4)
        reporting_entity_ids: Entity IDs of companies that filed the documents
        entity_ids: Entity IDs of companies mentioned in the documents
        date_range: Date range filter (rolling or absolute format)
        
    Returns:
        Formatted string with filings search results
    """
    try:
        results = await bigdata_filings_search_async(
            search_queries=queries,
            max_results=max_results,
            filing_types=filing_types,
            fiscal_year=fiscal_year,
            fiscal_quarter=fiscal_quarter,
            reporting_entity_ids=reporting_entity_ids,
            entity_ids=entity_ids,
            date_range=date_range,
            include_raw_content=True
        )
        
        if not results:
            return "No filings results found for the given queries."
        
        # Format results into readable string
        formatted_output = "Filings Search Results:\n\n"
        
        for i, result in enumerate(results, 1):
            formatted_output += f"--- FILING RESULT {i} ---\n"
            formatted_output += f"Title: {result['title']}\n"
            formatted_output += f"Content: {result['content']}\n"
            formatted_output += f"URL: {result['url']}\n"
            if result.get('document_timestamp'):
                formatted_output += f"Filed: {result['document_timestamp']}\n"
            formatted_output += f"Relevance Score: {result['score']:.3f}\n\n"
        
        return formatted_output
        
    except Exception as e:
        return f"Error executing filings search: {str(e)}"

UNIVERSAL_SEARCH_DESCRIPTION = (
    "Search across all Bigdata document types (news, transcripts, filings) with unified ranking. "
    "Provides comprehensive results across all content types for broad research topics. "
    "Useful for getting a complete picture across different information sources."
)

@tool(description=UNIVERSAL_SEARCH_DESCRIPTION)
async def bigdata_universal_search(
    queries: List[str],
    max_results: int = 5,
    document_types: Optional[List[str]] = None,
    entity_ids: Optional[List[str]] = None,
    date_range: Optional[str] = None,
    rerank_threshold: Optional[float] = None
) -> str:
    """
    Search across all Bigdata document types with unified result ranking.
    
    Args:
        queries: List of search queries to execute
        max_results: Maximum number of results to return per query (default: 5)
        document_types: Specific document types to include:
            - "NEWS", "TRANSCRIPTS", "FILINGS", "FILES", "ALL"
        entity_ids: List of company entity IDs to filter by
        date_range: Date range filter (rolling or absolute format)
        rerank_threshold: Rerank threshold for similarity searches (0.0-1.0)
        
    Returns:
        Formatted string with universal search results across all document types
    """
    try:
        results = await bigdata_universal_search_async(
            search_queries=queries,
            max_results=max_results,
            document_types=document_types,
            entity_ids=entity_ids,
            date_range=date_range,
            rerank_threshold=rerank_threshold,
            include_raw_content=True
        )
        
        if not results:
            return "No results found across any document types for the given queries."
        
        # Format results into readable string
        formatted_output = "Universal Search Results (All Document Types):\n\n"
        
        for i, result in enumerate(results, 1):
            formatted_output += f"--- RESULT {i} ---\n"
            formatted_output += f"Title: {result['title']}\n"
            formatted_output += f"Content: {result['content']}\n"
            formatted_output += f"URL: {result['url']}\n"
            if result.get('source_name'):
                formatted_output += f"Source: {result['source_name']}\n"
            if result.get('document_timestamp'):
                formatted_output += f"Date: {result['document_timestamp']}\n"
            formatted_output += f"Relevance Score: {result['score']:.3f}\n\n"
        
        return formatted_output
        
    except Exception as e:
        return f"Error executing universal search: {str(e)}"

KNOWLEDGE_GRAPH_DESCRIPTION = (
    "Search Bigdata knowledge graph to find company IDs and source information. "
    "Essential for discovering entity IDs needed for targeted searches and finding credible sources. "
    "Supports company lookup by name/ticker and source discovery with credibility rankings."
)

@tool(description=KNOWLEDGE_GRAPH_DESCRIPTION)
async def bigdata_knowledge_graph(
    search_type: str,
    search_term: str,
    max_results: int = 10,
    country_filter: Optional[str] = None,
    source_rank_filter: Optional[int] = None
) -> str:
    """
    Search Bigdata knowledge graph for companies and sources.
    
    Args:
        search_type: Type of search to perform:
            - "companies": Find company entities by name or ticker
            - "sources": Find news sources and publications
            - "autosuggest": Generic search across entities
        search_term: Term to search for (company name, ticker, source name, etc.)
        max_results: Maximum number of results to return (default: 10)
        country_filter: Filter sources by country (only for source searches)
        source_rank_filter: Filter sources by credibility rank 1-5 (only for source searches)
        
    Returns:
        Formatted string with knowledge graph entities and their IDs
    """
    try:
        # Build filters for source searches
        filters = {}
        if search_type == "sources":
            if country_filter:
                filters['country'] = country_filter
            if source_rank_filter:
                filters['source_rank'] = source_rank_filter
        
        results = await bigdata_knowledge_graph_async(
            search_type=search_type,
            search_term=search_term,
            max_results=max_results,
            filters=filters if filters else None
        )
        
        if not results:
            return f"No {search_type} found for '{search_term}'."
        
        # Format results based on search type
        if search_type == "companies":
            formatted_output = f"Companies found for '{search_term}':\n\n"
            
            for i, company in enumerate(results, 1):
                formatted_output += f"--- COMPANY {i} ---\n"
                formatted_output += f"Name: {company.get('name', 'Unknown')}\n"
                formatted_output += f"Entity ID: {company.get('id', 'Unknown')}\n"
                if company.get('ticker'):
                    formatted_output += f"Ticker: {company['ticker']}\n"
                if company.get('country'):
                    formatted_output += f"Country: {company['country']}\n"
                if company.get('sector'):
                    formatted_output += f"Sector: {company['sector']}\n"
                if company.get('description'):
                    formatted_output += f"Description: {company['description']}\n"
                formatted_output += "\n"
                
        elif search_type == "sources":
            formatted_output = f"News sources found for '{search_term}':\n\n"
            
            for i, source in enumerate(results, 1):
                formatted_output += f"--- SOURCE {i} ---\n"
                formatted_output += f"Name: {source.get('name', 'Unknown')}\n"
                formatted_output += f"Source ID: {source.get('id', 'Unknown')}\n"
                if source.get('source_rank'):
                    formatted_output += f"Credibility Rank: {source['source_rank']}/5\n"
                if source.get('country'):
                    formatted_output += f"Country: {source['country']}\n"
                if source.get('url'):
                    formatted_output += f"URL: {source['url']}\n"
                if source.get('description'):
                    formatted_output += f"Description: {source['description']}\n"
                formatted_output += "\n"
                
        else:  # autosuggest
            formatted_output = f"Entities found for '{search_term}':\n\n"
            
            for i, entity in enumerate(results, 1):
                formatted_output += f"--- ENTITY {i} ---\n"
                # Handle different entity types
                if 'name' in entity:
                    formatted_output += f"Name: {entity['name']}\n"
                if 'id' in entity:
                    formatted_output += f"ID: {entity['id']}\n"
                if 'entity_type' in entity:
                    formatted_output += f"Type: {entity['entity_type']}\n"
                if 'description' in entity:
                    formatted_output += f"Description: {entity['description']}\n"
                formatted_output += "\n"
        
        return formatted_output
        
    except Exception as e:
        return f"Error executing knowledge graph search: {str(e)}" 