"""
Core Search Utilities for Bigdata.com API Integration

This module provides the foundational async search functions that directly interface with the 
synchronous bigdata_client library. It acts as the async adapter layer, handling client 
management, query construction, error recovery, and result standardization for all Bigdata.com 
search capabilities.

## Core Architecture:

### Async Adapter Pattern:
The bigdata_client library is synchronous, but our workflow requires async execution. This 
module wraps all API calls using `asyncio.run_in_executor()` to execute them in thread pools 
while maintaining async interfaces for the LangGraph workflow.

### Client Management:
- **Singleton Pattern**: Global `_bigdata_client` instance prevents authentication rate limiting
- **Thread-Safe Access**: `_bigdata_client_lock` ensures safe concurrent access
- **Auto-Recovery**: Automatic client reset on authentication errors
- **Environment Config**: Credentials loaded from BIGDATA_USERNAME/BIGDATA_PASSWORD env vars

### Query Construction:
- **Hybrid Search**: Combines Similarity (semantic) + Keyword (exact) searches for best results
- **Flexible Filtering**: Entity IDs, date ranges, document types, fiscal periods
- **Query Composition**: Uses `&` (AND) and `|` (OR) operators to build complex queries
- **Null Query Handling**: Supports entity-only or filter-only searches without text queries

## Available Search Functions:

### Content Search Functions:
- **`bigdata_news_search_async`**: Premium news with source/entity filtering
- **`bigdata_transcript_search_async`**: Corporate transcripts with section detection
- **`bigdata_filings_search_async`**: SEC filings with form type filtering  
- **`bigdata_universal_search_async`**: Cross-document unified search

### Discovery Functions:
- **`bigdata_knowledge_graph_async`**: Entity/source lookup for targeted filtering

## Key Implementation Patterns:

### Thread Pool Execution:
All API calls use the pattern:
```python
loop = asyncio.get_event_loop()
documents = await loop.run_in_executor(None, execute_search_function)
```
This ensures non-blocking execution while interfacing with synchronous APIs.

### Error Handling Strategy:
- **Authentication Recovery**: Automatic client reset on auth errors (token expiration)
- **Graceful Degradation**: Individual query failures don't crash batch operations
- **Rate Limiting**: Built-in delays between queries in the same batch
- **Error Logging**: Detailed error messages for debugging API issues

### Result Standardization:
All search functions return consistent dictionary structures via `_format_search_results()`:
- Document metadata (title, URL, timestamp, sentiment)
- Content data (text, relevance scores, chunk indices)
- Source information (name, credibility rank)
- Entity relationships (mentioned entities, reporting entities)

### Parameter Validation:
- **Date Range Parsing**: Supports both rolling ("last_week") and absolute formats
- **Type Mapping**: Converts string parameters to proper enum types
- **Filter Composition**: Builds complex filter combinations from simple inputs
- **Optional Parameters**: Graceful handling of None/missing parameters

## Important Notes for Developers:

**Thread Safety**: The global client instance uses async locks - always use `await get_bigdata_client()` 
rather than accessing `_bigdata_client` directly

**Rate Limiting**: Built-in 1-second delays between queries in batches - extend delay if hitting 
rate limits frequently

**Authentication**: Client automatically resets on auth errors, but ensure environment variables 
are properly set before first use

**Memory Management**: Large result sets are truncated to prevent memory issues - tune 
`max_results` based on expected content volume

**Query Performance**: Hybrid search (Similarity | Keyword) provides best results but may be 
slower than pure keyword searches for simple queries

**Entity Dependencies**: Content searches often require entity IDs from knowledge graph - 
plan workflows accordingly

## Extension Points:

### Adding New Search Types:
1. Create new async function following naming pattern: `bigdata_{type}_search_async`
2. Implement query construction logic using existing patterns
3. Use `_format_search_results()` for consistent output formatting
4. Add corresponding tool wrapper in `tools.py`
5. Update graph workflow tool_map if needed

### Custom Filtering:
- Extend parameter mapping dictionaries (e.g., `filing_type_map`)
- Add new filter types to query construction logic
- Update `_clean_tool_parameters()` in graph.py for validation

### Result Enhancement:
- Extend `_format_search_results()` to include additional metadata
- Consider token limits when adding new content fields
- Maintain backward compatibility with existing result structure

## Performance Considerations:

**Client Reuse**: The singleton pattern prevents repeated authentication overhead - don't create 
multiple client instances

**Batch Operations**: Multiple queries in a single call are more efficient than separate calls 
due to connection reuse

**Content Truncation**: Raw content is limited to prevent LLM token overflow - balance between 
detail and performance

**Async Efficiency**: Thread pool execution allows concurrent processing while respecting 
API rate limits

This module is the foundation for all Bigdata.com integration - changes here affect the entire 
search workflow. Test thoroughly with real API calls when modifying core functions.
"""

import os
import asyncio
import datetime
from typing import List, Optional, Dict, Any, Union
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Bigdata imports with error handling
try:
    from bigdata_client import Bigdata
    from bigdata_client.query import Similarity, Keyword, Entity
    from bigdata_client.models.search import DocumentType, SortBy
    from bigdata_client.daterange import AbsoluteDateRange, RollingDateRange
    from bigdata_client.query import TranscriptTypes, SectionMetadata, FilingTypes, FiscalYear, FiscalQuarter, ReportingEntity
    BIGDATA_AVAILABLE = True
except ImportError:
    BIGDATA_AVAILABLE = False
    print("Warning: bigdata_client not available. Install it to use Bigdata search functionality.")

# Global Bigdata client instance for reuse (prevents authentication rate limiting)
_bigdata_client = None
_bigdata_client_lock = asyncio.Lock()

async def get_bigdata_client():
    """
    Get or create a shared Bigdata client instance.
    This prevents authentication rate limiting by reusing the same JWT session.
    
    Returns:
        Bigdata: Authenticated client instance
        
    Raises:
        ValueError: If bigdata_client not available or credentials not set
    """
    global _bigdata_client
    
    if not BIGDATA_AVAILABLE:
        raise ValueError("bigdata_client not available. Please install it with: pip install bigdata-client")
    
    async with _bigdata_client_lock:
        if _bigdata_client is None:
            username = os.environ.get("BIGDATA_USERNAME")
            password = os.environ.get("BIGDATA_PASSWORD")
            
            if not username or not password:
                raise ValueError("BIGDATA_USERNAME and BIGDATA_PASSWORD environment variables must be set")
            
            # Create client in thread pool since it's synchronous
            loop = asyncio.get_event_loop()
            _bigdata_client = await loop.run_in_executor(None, lambda: Bigdata(username, password))
        
        return _bigdata_client

async def reset_bigdata_client():
    """
    Reset the shared Bigdata client instance.
    Useful for handling token expiration or connection issues.
    """
    global _bigdata_client
    async with _bigdata_client_lock:
        _bigdata_client = None

def _parse_date_range(date_range: Optional[str]):
    """
    Parse date range string into Bigdata date range object.
    
    Args:
        date_range: Date range string in format:
            - Rolling: "today", "yesterday", "this_week", "last_week", "last_7_days", 
              "last_month"/"last_30_days", "last_90_days", "year_to_date", "last_year"
            - Absolute: "YYYY-MM-DD,YYYY-MM-DD" format
            
    Returns:
        RollingDateRange or AbsoluteDateRange object, or None
    """
    if not date_range:
        return None
        
    # Rolling date ranges
    rolling_ranges = {
        "today": RollingDateRange.TODAY,
        "yesterday": RollingDateRange.YESTERDAY, 
        "this_week": RollingDateRange.THIS_WEEK,
        "last_week": RollingDateRange.LAST_WEEK,
        "last_7_days": RollingDateRange.LAST_SEVEN_DAYS,
        "last_month": RollingDateRange.LAST_THIRTY_DAYS,
        "last_30_days": RollingDateRange.LAST_THIRTY_DAYS,
        "last_90_days": RollingDateRange.LAST_NINETY_DAYS,
        "year_to_date": RollingDateRange.YEAR_TO_DATE,
        "last_year": RollingDateRange.LAST_YEAR,
    }
    
    if date_range in rolling_ranges:
        return rolling_ranges[date_range]
    
    # Absolute date range "YYYY-MM-DD,YYYY-MM-DD"
    if "," in date_range:
        try:
            start_date, end_date = date_range.split(",")
            return AbsoluteDateRange(
                f"{start_date.strip()}T00:00:00",
                f"{end_date.strip()}T23:59:59"
            )
        except ValueError:
            # Silently return None for invalid date formats
            return None
    
    # Silently return None for unrecognized date formats
    return None

def _format_search_results(documents, include_raw_content: bool = True) -> List[Dict[str, Any]]:
    """
    Format Bigdata API response into consistent result format.
    
    Args:
        documents: Raw documents from Bigdata API
        include_raw_content: Whether to include full chunk content
        
    Returns:
        List of formatted result dictionaries
    """
    results = []
    for doc in documents:
        for chunk in doc.chunks:
            result = {
                # Document metadata
                'title': getattr(doc, 'headline', getattr(doc, 'title', 'Document')),
                'url': getattr(doc, 'url', f"bigdata://document/{getattr(doc, 'id', 'unknown')}"),
                'content': chunk.text if hasattr(chunk, 'text') else '',
                'score': chunk.relevance if hasattr(chunk, 'relevance') else 0.0,
                'raw_content': chunk.text if include_raw_content and hasattr(chunk, 'text') else None,

                # Additional metadata
                'chunk_index': chunk.chunk if hasattr(chunk, 'chunk') else 0,
                'document_id': getattr(doc, 'id', None),
                'document_timestamp': getattr(doc, 'timestamp', None),
                'document_sentiment': getattr(doc, 'sentiment', None),
                'language': getattr(doc, 'language', None),
                'chunk_sentiment': chunk.sentiment if hasattr(chunk, 'sentiment') else None,
                
                # Source metadata
                'source_name': getattr(doc.source, 'name', None) if hasattr(doc, 'source') else None,
                'source_key': getattr(doc.source, 'key', None) if hasattr(doc, 'source') else None,
                
                # Entity metadata
                'entities': chunk.entities if hasattr(chunk, 'entities') else []
            }
            results.append(result)
    
    return results

async def bigdata_news_search_async(
    search_queries: List[str],
    max_results: int = 10,
    date_range: Optional[str] = None,
    source_ids: Optional[List[str]] = None,
    entity_ids: Optional[List[str]] = None,
    rerank_threshold: Optional[float] = 0.1,
    include_raw_content: bool = True
) -> List[Dict[str, Any]]:
    """
    Search Bigdata news content with premium publisher access.
    
    Args:
        search_queries: List of search queries to execute
        max_results: Maximum number of results to return per query
        date_range: Date range filter (rolling or absolute format)
        source_ids: List of specific news source IDs to filter by
        entity_ids: List of entity IDs to filter by
        rerank_threshold: Rerank threshold for similarity searches (0.0-1.0)
        include_raw_content: Whether to include full chunk content
        
    Returns:
        List of search result dictionaries with news content
        
    Raises:
        ValueError: If Bigdata client not available or credentials not set
    """
    if not BIGDATA_AVAILABLE:
        raise ValueError("bigdata_client not available. Please install it with: pip install bigdata-client")
    
    bigdata = await get_bigdata_client()
    all_results = []
    
    for query in search_queries:
        try:
            # Run in thread pool since bigdata_client is synchronous
            loop = asyncio.get_event_loop()
            
            def execute_news_search():
                # Import Source class for source filtering
                from bigdata_client.query import Similarity, Keyword, Entity, Source
                
                # Handle query construction based on whether we have a text query
                if query and query.strip():
                    # Build the query - use hybrid search (Similarity OR Keyword) for best results
                    search_query = Similarity(query) | Keyword(query)
                else:
                    # No text query - start with None, will be built from entity/temporal filters
                    search_query = None
                
                # Add entity filtering if provided
                if entity_ids:
                    entity_queries = [Entity(entity_id) for entity_id in entity_ids]
                    entity_query = entity_queries[0]
                    for additional_entity in entity_queries[1:]:
                        entity_query = entity_query | additional_entity
                    
                    if search_query is not None:
                        search_query = search_query & entity_query
                    else:
                        search_query = entity_query
                
                # Add source filtering if provided
                if source_ids:
                    source_queries = [Source(source_id) for source_id in source_ids]
                    source_query = source_queries[0]
                    for additional_source in source_queries[1:]:
                        source_query = source_query | additional_source
                    
                    if search_query is not None:
                        search_query = search_query & source_query
                    else:
                        search_query = source_query
                
                # Set up search parameters
                search_kwargs = {
                    'scope': DocumentType.NEWS,
                }
                
                # Add date range if provided
                date_range_obj = _parse_date_range(date_range)
                if date_range_obj:
                    search_kwargs['date_range'] = date_range_obj
                    
                # Add rerank threshold if provided
                if rerank_threshold is not None:
                    search_kwargs['rerank_threshold'] = rerank_threshold
                
                # Create and run search
                search = bigdata.search.new(search_query, **search_kwargs)
                documents = search.run(max_results)
                
                return documents
            
            # Execute search in thread pool
            documents = await loop.run_in_executor(None, execute_news_search)
            
            # Format results
            formatted_results = _format_search_results(documents, include_raw_content)
            all_results.extend(formatted_results)
            
            # Add delay between queries to respect rate limits
            if len(search_queries) > 1:
                await asyncio.sleep(1.0)
                
        except Exception as e:
            # Check if this is an authentication error and reset client if needed
            error_str = str(e).lower()
            if any(auth_error in error_str for auth_error in ['authentication', 'unauthorized', 'token', 'jwt', 'login']):
                print(f"Authentication error detected, resetting Bigdata client: {str(e)}")
                await reset_bigdata_client()
            
            print(f"Error processing Bigdata news query '{query}': {str(e)}")
            continue
    
    return all_results

async def bigdata_transcript_search_async(
    search_queries: List[str],
    max_results: int = 10,
    transcript_types: Optional[List[str]] = None,
    section_metadata: Optional[List[str]] = None,
    fiscal_year: Optional[int] = None,
    fiscal_quarter: Optional[int] = None,
    entity_ids: Optional[List[str]] = None,
    reporting_entity_ids: Optional[List[str]] = None,
    date_range: Optional[str] = None,
    rerank_threshold: Optional[float] = 0.1,
    include_raw_content: bool = True
) -> List[Dict[str, Any]]:
    """
    Search Bigdata transcript content with hybrid search (semantic + keyword) and advanced filtering.
    
    Args:
        search_queries: List of search queries to execute
        max_results: Maximum number of results to return per query
        transcript_types: List of transcript types to filter by
        section_metadata: List of transcript sections to filter by
        fiscal_year: Fiscal year to filter by
        fiscal_quarter: Fiscal quarter to filter by (1-4)
        entity_ids: List of entity IDs mentioned in transcripts
        reporting_entity_ids: List of entity IDs that filed the transcripts
        date_range: Date range filter (rolling or absolute format)
        rerank_threshold: Rerank threshold for similarity searches (0.0-1.0)
        include_raw_content: Whether to include full chunk content
        
    Returns:
        List of search result dictionaries with transcript content
        
    Raises:
        ValueError: If Bigdata client not available or credentials not set
    """
    if not BIGDATA_AVAILABLE:
        raise ValueError("bigdata_client not available. Please install it with: pip install bigdata-client")
    
    bigdata = await get_bigdata_client()
    all_results = []
    
    for query in search_queries:
        try:
            # Run in thread pool since bigdata_client is synchronous
            loop = asyncio.get_event_loop()
            
            def execute_transcript_search():
                # Handle query construction based on whether we have a text query
                if query and query.strip():
                    # Build the query - use hybrid search (Similarity OR Keyword) for best results
                    search_query = Similarity(query) | Keyword(query)
                else:
                    # No text query - start with None, will be built from entity/temporal filters
                    search_query = None
                
                # Add entity filtering if provided (documents mentioning these entities)
                if entity_ids:
                    entity_queries = [Entity(entity_id) for entity_id in entity_ids]
                    entity_query = entity_queries[0]
                    for additional_entity in entity_queries[1:]:
                        entity_query = entity_query | additional_entity
                    
                    if search_query is not None:
                        search_query = search_query & entity_query
                    else:
                        search_query = entity_query
                
                # Add reporting entity filtering if provided (companies that filed the transcripts)
                if reporting_entity_ids:
                    reporting_queries = [Entity(entity_id) for entity_id in reporting_entity_ids]
                    reporting_query = reporting_queries[0]
                    for additional_entity in reporting_queries[1:]:
                        reporting_query = reporting_query | additional_entity
                    
                    if search_query is not None:
                        search_query = search_query & reporting_query
                    else:
                        search_query = reporting_query
                
                # Add transcript type filtering
                if transcript_types:
                    transcript_type_map = {
                        "EARNINGS_CALL": TranscriptTypes.EARNINGS_CALL,
                        "CONFERENCE_CALL": TranscriptTypes.CONFERENCE_CALL,
                        "ANALYST_INVESTOR_SHAREHOLDER_MEETING": TranscriptTypes.ANALYST_INVESTOR_SHAREHOLDER_MEETING,
                        "GENERAL_PRESENTATION": TranscriptTypes.GENERAL_PRESENTATION,
                        "GUIDANCE_CALL": TranscriptTypes.GUIDANCE_CALL,
                        "SALES_REVENUE_CALL": TranscriptTypes.SALES_REVENUE_CALL,
                        "SPECIAL_SITUATION_MA": TranscriptTypes.SPECIAL_SITUATION_MA,
                    }
                    
                    for transcript_type in transcript_types:
                        if transcript_type in transcript_type_map:
                            search_query = search_query & transcript_type_map[transcript_type]
                
                # Add section metadata filtering
                if section_metadata:
                    section_map = {
                        "QA": SectionMetadata.QA,
                        "QUESTION": SectionMetadata.QUESTION,
                        "ANSWER": SectionMetadata.ANSWER,
                        "MANAGEMENT_DISCUSSION": SectionMetadata.MANAGEMENT_DISCUSSION,
                    }
                    
                    # Build section query with OR operator (like entities)
                    section_queries = []
                    for section in section_metadata:
                        if section in section_map:
                            section_queries.append(section_map[section])
                    
                    if section_queries:
                        section_query = section_queries[0]
                        for additional_section in section_queries[1:]:
                            section_query = section_query | additional_section
                        
                        if search_query is not None:
                            search_query = search_query & section_query
                        else:
                            search_query = section_query
                
                # Add fiscal filters
                if fiscal_year:
                    fiscal_filter = FiscalYear(fiscal_year)
                    if search_query is not None:
                        search_query = search_query & fiscal_filter
                    else:
                        search_query = fiscal_filter
                        
                if fiscal_quarter:
                    quarter_filter = FiscalQuarter(fiscal_quarter)
                    if search_query is not None:
                        search_query = search_query & quarter_filter
                    else:
                        search_query = quarter_filter
                
                # Set up search parameters
                search_kwargs = {
                    'scope': DocumentType.TRANSCRIPTS,
                }
                
                # Add date range if provided
                date_range_obj = _parse_date_range(date_range)
                if date_range_obj:
                    search_kwargs['date_range'] = date_range_obj
                    
                # Add rerank threshold if provided
                if rerank_threshold is not None:
                    search_kwargs['rerank_threshold'] = rerank_threshold
                
                # Create and run search
                search = bigdata.search.new(search_query, **search_kwargs)
                documents = search.run(max_results)
                
                return documents
            
            # Execute search in thread pool
            documents = await loop.run_in_executor(None, execute_transcript_search)
            
            # Format results
            formatted_results = _format_search_results(documents, include_raw_content)
            all_results.extend(formatted_results)
            
            # Add delay between queries to respect rate limits
            if len(search_queries) > 1:
                await asyncio.sleep(1.0)
                
        except Exception as e:
            # Check if this is an authentication error and reset client if needed
            error_str = str(e).lower()
            if any(auth_error in error_str for auth_error in ['authentication', 'unauthorized', 'token', 'jwt', 'login']):
                print(f"Authentication error detected, resetting Bigdata client: {str(e)}")
                await reset_bigdata_client()
            
            print(f"Error processing Bigdata transcript query '{query}': {str(e)}")
            continue
    
    return all_results

async def bigdata_filings_search_async(
    search_queries: List[str],
    max_results: int = 10,
    filing_types: Optional[List[str]] = None,
    fiscal_year: Optional[int] = None,
    fiscal_quarter: Optional[int] = None,
    reporting_entity_ids: Optional[List[str]] = None,
    entity_ids: Optional[List[str]] = None,
    date_range: Optional[str] = None,
    rerank_threshold: Optional[float] = 0.1,
    include_raw_content: bool = True
) -> List[Dict[str, Any]]:
    """
    Search Bigdata filings content with SEC form types and reporting entity filtering.
    
    Args:
        search_queries: List of search queries to execute
        max_results: Maximum number of results to return per query
        filing_types: List of SEC filing types (e.g., ["SEC_10_K", "SEC_10_Q", "SEC_8_K"])
        fiscal_year: Fiscal year filter
        fiscal_quarter: Fiscal quarter filter
        reporting_entity_ids: List of entity IDs for companies that filed the documents
        entity_ids: List of entity IDs mentioned in the documents
        date_range: Date range filter (rolling or absolute format)
        rerank_threshold: Rerank threshold for hybrid searches (0.0-1.0)
        include_raw_content: Whether to include full chunk content
        
    Returns:
        List of search result dictionaries with filings content
        
    Raises:
        ValueError: If Bigdata client not available or credentials not set
    """
    if not BIGDATA_AVAILABLE:
        raise ValueError("bigdata_client not available. Please install it with: pip install bigdata-client")
    
    bigdata = await get_bigdata_client()
    all_results = []
    
    for query in search_queries:
        try:
            # Run in thread pool since bigdata_client is synchronous
            loop = asyncio.get_event_loop()
            
            def execute_filings_search():
                # Handle query construction based on whether we have a text query
                if query and query.strip():
                    # Build the query - use hybrid search (Similarity OR Keyword) for best results
                    search_query = Similarity(query) | Keyword(query)
                else:
                    # No text query - start with None, will be built from entity/temporal filters
                    search_query = None
                
                # Add entity filtering if provided
                if entity_ids:
                    entity_queries = [Entity(entity_id) for entity_id in entity_ids]
                    entity_query = entity_queries[0]
                    for additional_entity in entity_queries[1:]:
                        entity_query = entity_query | additional_entity
                    
                    if search_query is not None:
                        search_query = search_query & entity_query
                    else:
                        search_query = entity_query
                
                # Add reporting entity filtering if provided
                if reporting_entity_ids:
                    reporting_queries = [ReportingEntity(entity_id) for entity_id in reporting_entity_ids]
                    reporting_query = reporting_queries[0]
                    for additional_entity in reporting_queries[1:]:
                        reporting_query = reporting_query | additional_entity
                    
                    if search_query is not None:
                        search_query = search_query & reporting_query
                    else:
                        search_query = reporting_query
                
                # Add filing type filtering
                if filing_types:
                    filing_type_map = {
                        "SEC_10_K": FilingTypes.SEC_10_K,
                        "SEC_10_Q": FilingTypes.SEC_10_Q,
                        "SEC_8_K": FilingTypes.SEC_8_K,
                        "SEC_20_F": FilingTypes.SEC_20_F,
                        "SEC_S_1": FilingTypes.SEC_S_1,
                        "SEC_S_3": FilingTypes.SEC_S_3,
                        "SEC_6_K": FilingTypes.SEC_6_K,
                    }
                    
                    # Build filing type query with OR operator (like entities)
                    filing_type_queries = []
                    for filing_type in filing_types:
                        if filing_type in filing_type_map:
                            filing_type_queries.append(filing_type_map[filing_type])
                    
                    if filing_type_queries:
                        filing_type_query = filing_type_queries[0]
                        for additional_filing_type in filing_type_queries[1:]:
                            filing_type_query = filing_type_query | additional_filing_type
                        
                        if search_query is not None:
                            search_query = search_query & filing_type_query
                        else:
                            search_query = filing_type_query
                
                # Add fiscal filters
                if fiscal_year:
                    fiscal_filter = FiscalYear(fiscal_year)
                    if search_query is not None:
                        search_query = search_query & fiscal_filter
                    else:
                        search_query = fiscal_filter
                        
                if fiscal_quarter:
                    quarter_filter = FiscalQuarter(fiscal_quarter)
                    if search_query is not None:
                        search_query = search_query & quarter_filter
                    else:
                        search_query = quarter_filter
                
                # Set up search parameters
                search_kwargs = {
                    'scope': DocumentType.FILINGS,
                }
                
                # Add date range if provided
                date_range_obj = _parse_date_range(date_range)
                if date_range_obj:
                    search_kwargs['date_range'] = date_range_obj
                    
                # Add rerank threshold if provided
                if rerank_threshold is not None:
                    search_kwargs['rerank_threshold'] = rerank_threshold
                
                # Create and run search
                search = bigdata.search.new(search_query, **search_kwargs)
                documents = search.run(max_results)
                
                return documents
            
            # Execute search in thread pool
            documents = await loop.run_in_executor(None, execute_filings_search)
            
            # Format results
            formatted_results = _format_search_results(documents, include_raw_content)
            all_results.extend(formatted_results)
            
            # Add delay between queries to respect rate limits
            if len(search_queries) > 1:
                await asyncio.sleep(1.0)
                
        except Exception as e:
            # Check if this is an authentication error and reset client if needed
            error_str = str(e).lower()
            if any(auth_error in error_str for auth_error in ['authentication', 'unauthorized', 'token', 'jwt', 'login']):
                print(f"Authentication error detected, resetting Bigdata client: {str(e)}")
                await reset_bigdata_client()
            
            print(f"Error processing Bigdata filings query '{query}': {str(e)}")
            continue
    
    return all_results

async def bigdata_universal_search_async(
    search_queries: List[str],
    max_results: int = 10,
    document_types: Optional[List[str]] = None,
    entity_ids: Optional[List[str]] = None,
    date_range: Optional[str] = None,
    rerank_threshold: Optional[float] = 0.1,
    include_raw_content: bool = True
) -> List[Dict[str, Any]]:
    """
    Search across all Bigdata document types with unified result ranking.
    
    Args:
        search_queries: List of search queries to execute
        max_results: Maximum number of results to return per query
        document_types: List of document types to include (e.g., ["NEWS", "TRANSCRIPTS", "FILINGS"])
        entity_ids: List of entity IDs to filter by
        date_range: Date range filter (rolling or absolute format)
        rerank_threshold: Rerank threshold for similarity searches (0.0-1.0)
        include_raw_content: Whether to include full chunk content
        
    Returns:
        List of search result dictionaries across all document types
        
    Raises:
        ValueError: If Bigdata client not available or credentials not set
    """
    if not BIGDATA_AVAILABLE:
        raise ValueError("bigdata_client not available. Please install it with: pip install bigdata-client")
    
    bigdata = await get_bigdata_client()
    all_results = []
    
    for query in search_queries:
        try:
            # Run in thread pool since bigdata_client is synchronous
            loop = asyncio.get_event_loop()
            
            def execute_universal_search():
                # Handle query construction based on whether we have a text query
                if query and query.strip():
                    # Build the query - use hybrid search (Similarity OR Keyword) for best results
                    search_query = Similarity(query) | Keyword(query)
                else:
                    # No text query - start with None, will be built from entity/temporal filters
                    search_query = None
                
                # Add entity filtering if provided
                if entity_ids:
                    entity_queries = [Entity(entity_id) for entity_id in entity_ids]
                    entity_query = entity_queries[0]
                    for additional_entity in entity_queries[1:]:
                        entity_query = entity_query | additional_entity
                    
                    if search_query is not None:
                        search_query = search_query & entity_query
                    else:
                        search_query = entity_query
                
                # Set up search parameters
                if document_types:
                    # Map document type strings to DocumentType enums
                    doc_type_map = {
                        "NEWS": DocumentType.NEWS,
                        "TRANSCRIPTS": DocumentType.TRANSCRIPTS,
                        "FILINGS": DocumentType.FILINGS,
                        "FILES": DocumentType.FILES,
                        "ALL": DocumentType.ALL,
                    }
                    
                    # Use the first valid document type or ALL if multiple
                    if len(document_types) == 1 and document_types[0] in doc_type_map:
                        scope = doc_type_map[document_types[0]]
                    else:
                        scope = DocumentType.ALL
                else:
                    scope = DocumentType.ALL
                
                search_kwargs = {
                    'scope': scope,
                }
                
                # Add date range if provided
                date_range_obj = _parse_date_range(date_range)
                if date_range_obj:
                    search_kwargs['date_range'] = date_range_obj
                    
                # Add rerank threshold if provided
                if rerank_threshold is not None:
                    search_kwargs['rerank_threshold'] = rerank_threshold
                
                # Create and run search
                search = bigdata.search.new(search_query, **search_kwargs)
                documents = search.run(max_results)
                
                return documents
            
            # Execute search in thread pool
            documents = await loop.run_in_executor(None, execute_universal_search)
            
            # Format results
            formatted_results = _format_search_results(documents, include_raw_content)
            all_results.extend(formatted_results)
            
            # Add delay between queries to respect rate limits
            if len(search_queries) > 1:
                await asyncio.sleep(1.0)
                
        except Exception as e:
            # Check if this is an authentication error and reset client if needed
            error_str = str(e).lower()
            if any(auth_error in error_str for auth_error in ['authentication', 'unauthorized', 'token', 'jwt', 'login']):
                print(f"Authentication error detected, resetting Bigdata client: {str(e)}")
                await reset_bigdata_client()
            
            print(f"Error processing Bigdata universal query '{query}': {str(e)}")
            continue
    
    return all_results

async def bigdata_knowledge_graph_async(
    search_type: str,
    search_term: str,
    max_results: int = 10,
    filters: Optional[Dict[str, Any]] = None
) -> List[Dict[str, Any]]:
    """
    Search Bigdata knowledge graph for companies and sources.
    
    Args:
        search_type: Type of search ("companies", "sources", "autosuggest")
        search_term: Term to search for
        max_results: Maximum number of results to return
        filters: Additional filters (for sources: country, source_rank, etc.)
        
    Returns:
        List of knowledge graph entities
        
    Raises:
        ValueError: If Bigdata client not available, credentials not set, or invalid search type
    """
    if not BIGDATA_AVAILABLE:
        raise ValueError("bigdata_client not available. Please install it with: pip install bigdata-client")
    
    if search_type not in ["companies", "sources", "autosuggest"]:
        raise ValueError(f"Invalid search_type '{search_type}'. Must be one of: companies, sources, autosuggest")
    
    bigdata = await get_bigdata_client()
    
    try:
        # Run in thread pool since bigdata_client is synchronous
        loop = asyncio.get_event_loop()
        
        def execute_knowledge_graph_search():
            if search_type == "companies":
                # Search for companies
                results = bigdata.knowledge_graph.find_companies(search_term)
                # Limit results
                return results[:max_results] if len(results) > max_results else results
                
            elif search_type == "sources":
                # Search for sources with optional filtering
                results = bigdata.knowledge_graph.find_sources(search_term)
                
                # Apply filters if provided
                if filters:
                    filtered_results = []
                    for source in results:
                        # Filter by country if specified
                        if 'country' in filters:
                            if getattr(source, 'country', None) != filters['country']:
                                continue
                        
                        # Filter by source rank if specified
                        if 'source_rank' in filters:
                            source_rank = getattr(source, 'source_rank', None)
                            if source_rank != str(filters['source_rank']):
                                continue
                        
                        filtered_results.append(source)
                    
                    results = filtered_results
                
                # Limit results
                return results[:max_results] if len(results) > max_results else results
                
            elif search_type == "autosuggest":
                # Generic autosuggest search
                results = bigdata.knowledge_graph.autosuggest(search_term)
                # Limit results
                return results[:max_results] if len(results) > max_results else results
        
        # Execute search in thread pool
        raw_results = await loop.run_in_executor(None, execute_knowledge_graph_search)
        
        # Format results into consistent dictionary format
        formatted_results = []
        for item in raw_results:
            # Convert each result to dictionary format
            if hasattr(item, '__dict__'):
                result_dict = {}
                for key, value in item.__dict__.items():
                    # Convert datetime objects to strings
                    if isinstance(value, datetime.datetime):
                        result_dict[key] = value.isoformat()
                    else:
                        result_dict[key] = value
                formatted_results.append(result_dict)
            else:
                # Fallback for simple objects
                formatted_results.append({"result": str(item)})
        
        return formatted_results
        
    except Exception as e:
        # Check if this is an authentication error and reset client if needed
        error_str = str(e).lower()
        if any(auth_error in error_str for auth_error in ['authentication', 'unauthorized', 'token', 'jwt', 'login']):
            print(f"Authentication error detected, resetting Bigdata client: {str(e)}")
            await reset_bigdata_client()
        
        print(f"Error processing Bigdata knowledge graph search '{search_term}': {str(e)}")
        return [] 