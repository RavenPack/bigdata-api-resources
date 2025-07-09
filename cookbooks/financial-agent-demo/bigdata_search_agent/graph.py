"""
Bigdata Search Agent Graph

This module implements a LangGraph-based workflow that orchestrates comprehensive financial research 
using the Bigdata.com API ecosystem. The graph takes a research topic and automatically generates 
multiple complementary search strategies, executes them in parallel, and compiles results into a 
coherent research report.

## Core Workflow:
1. **Planning Phase**: LLM analyzes the topic and generates multiple search strategies
2. **Execution Phase**: Parallel execution of strategies using different Bigdata tools  
3. **Compilation Phase**: LLM synthesizes all results into a final report

## Key Components:

### Workflow Nodes:
- `generate_search_plan`: Creates search strategies using LLM analysis
- `execute_search_strategy`: Executes individual strategies with appropriate Bigdata tools
- `gather_search_results`: Aggregates results and calculates metadata
- `compile_final_results`: Synthesizes findings into final report

### Tool Integration:
Supports all major Bigdata.com tools:
- News search, transcript search, filings search
- Universal search, knowledge graph queries
- Automatic parameter cleaning and validation

### Real-time Updates:
- Streams progress updates throughout execution
- Token-by-token streaming during LLM synthesis
- Detailed performance and quality metrics

## Important Notes 

**State Management**: Uses BigdataSearchState with typed inputs/outputs for robust data flow

**Parallel Execution**: Leverages LangGraph's Send() API for concurrent strategy execution - 
be careful when modifying the routing logic in `initiate_parallel_searches()`

**Parameter Handling**: `_clean_tool_parameters()` sanitizes tool inputs - extend this function 
when adding new tool types or parameters

**Error Resilience**: Individual strategy failures don't crash the workflow - failed searches 
are tracked in metadata and excluded from final compilation

**Configuration**: Highly configurable via BigdataSearchConfiguration - most behavior can be 
tuned without code changes

**Streaming**: Real-time updates are critical for UX - always use the stream writer when 
adding new functionality

## Future Extension Points:
- Add new tool types in `execute_search_strategy` tool_map
- Extend parameter cleaning logic for new tool parameters  
- Customize result compilation prompts in prompts.py
- Add deduplication logic in `gather_search_results`
"""

import time
import asyncio
from typing import List, Dict, Any, Optional
from datetime import datetime

from langchain.chat_models import init_chat_model
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.runnables import RunnableConfig

from langgraph.constants import Send
from langgraph.graph import START, END, StateGraph
from langgraph.config import get_stream_writer

from .state import (
    BigdataSearchState,
    BigdataSearchStateInput,
    BigdataSearchStateOutput,
    SearchStrategy,
    SearchStrategies,
    SearchResult,
    SearchStrategyState,
    SearchStrategyOutput,
)

from .configuration import BigdataSearchConfiguration
from .prompts import (
    search_plan_generator_instructions,
    result_compilation_instructions,
)

def get_today_str() -> str:
    """Get today's date as a string."""
    return datetime.now().strftime("%Y-%m-%d")

def get_config_value(value):
    """Get configuration value, handling None values."""
    return value if value is not None else ""

def _clean_tool_parameters(params: Dict[str, Any], tool_type: str) -> Dict[str, Any]:
    """Clean and validate tool parameters to ensure they match expected types."""
    cleaned = {}
    
    for key, value in params.items():
        # Skip None values
        if value is None:
            continue
            
        # Handle list parameters that might come as booleans or strings
        if key in ["transcript_types", "section_metadata", "filing_types", "document_types", "sources"]:
            if isinstance(value, bool):
                # Skip boolean values for list parameters
                continue
            elif isinstance(value, str):
                # Convert single string to list
                cleaned[key] = [value]
            elif isinstance(value, list):
                # Keep valid lists
                cleaned[key] = value
        
        # Handle numeric parameters
        elif key in ["fiscal_year", "fiscal_quarter", "max_results"]:
            if isinstance(value, (int, float)):
                cleaned[key] = int(value)
            elif isinstance(value, str) and value.isdigit():
                cleaned[key] = int(value)
        
        # Handle float parameters
        elif key in ["rerank_threshold"]:
            if isinstance(value, (int, float)):
                cleaned[key] = float(value)
            elif isinstance(value, str):
                try:
                    cleaned[key] = float(value)
                except ValueError:
                    continue
        
        # Handle string parameters
        elif key in ["search_type", "date_range"]:
            if isinstance(value, str):
                cleaned[key] = value
        
        # Handle dict parameters
        elif key in ["filters"]:
            if isinstance(value, dict):
                cleaned[key] = value
        
        # For other parameters, keep as-is if they're basic types
        elif isinstance(value, (str, int, float, bool, list, dict)):
            cleaned[key] = value
    
    return cleaned

## Core Workflow Nodes

async def generate_search_plan(state: BigdataSearchState, config: RunnableConfig):
    """Generate comprehensive search strategies for the given topic.
    
    This node:
    1. Analyzes the search topic using an LLM
    2. Creates multiple complementary search strategies
    3. Returns strategies for parallel execution
    
    Args:
        state: Current graph state containing the search topic
        config: Configuration for models and search parameters
        
    Returns:
        Dict containing the generated search strategies
    """
    # Get stream writer for real-time updates
    writer = get_stream_writer()
    
    # Get inputs
    topic = state["topic"]
    
    # Stream initial status
    writer({
        "type": "planning_start",
        "message": f"🎯 Analyzing research topic: '{topic}'",
        "timestamp": datetime.now().isoformat()
    })
    
    # Get configuration
    configurable = BigdataSearchConfiguration.from_runnable_config(config)
    search_depth = state.get("search_depth", configurable.search_depth)
    number_of_queries = configurable.number_of_queries
    
    writer({
        "type": "planning_config",
        "message": f"📋 Planning {search_depth} search strategies with {number_of_queries} queries each",
        "search_depth": search_depth,
        "number_of_queries": number_of_queries
    })
    
    # Set up planner model
    planner_provider = get_config_value(configurable.planner_provider)
    planner_model = get_config_value(configurable.planner_model)
    planner_model_kwargs = get_config_value(configurable.planner_model_kwargs or {})
    
    writer({
        "type": "planning_model",
        "message": f"🤖 Using {planner_provider}:{planner_model} for strategy generation"
    })
    
    # Note: Disable thinking for structured output to avoid compatibility issues
    planner_llm = init_chat_model("google_genai:gemini-2.5-flash")
    
    # Format system instructions
    system_instructions = search_plan_generator_instructions.format(
        topic=topic,
        search_depth=search_depth,
        number_of_queries=number_of_queries,
        today=get_today_str()
    )
    
    writer({
        "type": "planning_thinking",
        "message": "🧠 LLM analyzing topic and generating search strategies..."
    })
    
    # Generate search strategies
    structured_llm = planner_llm.with_structured_output(SearchStrategies)
    start_time = time.time()
    
    search_strategies_result = await structured_llm.ainvoke([
        SystemMessage(content=system_instructions),
        HumanMessage(content="Generate comprehensive search strategies for this research topic.")
    ])
    
    generation_time = time.time() - start_time
    
    # Extract strategies
    strategies = search_strategies_result.strategies
    
    # Stream strategy preview
    writer({
        "type": "planning_complete",
        "message": f"✅ Generated {len(strategies)} search strategies ({generation_time:.1f}s)",
        "generation_time": generation_time,
        "strategy_count": len(strategies)
    })
    
    # Stream individual strategy details
    for i, strategy in enumerate(strategies, 1):
        writer({
            "type": "strategy_preview",
            "strategy_index": i,
            "tool_type": strategy.tool_type,
            "description": strategy.description,
            "query_count": len(strategy.search_queries),
            "priority": strategy.priority,
            "message": f"📊 Strategy {i}/{len(strategies)}: {strategy.tool_type.upper()} - {strategy.description}"
        })
        
        # Stream the actual queries for this strategy
        for j, query in enumerate(strategy.search_queries, 1):
            writer({
                "type": "query_preview",
                "strategy_index": i,
                "query_index": j,
                "query": query,
                "message": f"  🔍 Query {j}: {query}"
            })
    
    writer({
        "type": "planning_ready",
        "message": f"🚀 Ready to execute {len(strategies)} parallel searches"
    })
    
    return {"search_strategies": strategies}



async def execute_search_strategy(state: SearchStrategyState, config: RunnableConfig):
    """Execute a single search strategy using the appropriate Bigdata tool.
    
    This node:
    1. Selects the appropriate Bigdata tool based on strategy type
    2. Prepares parameters including entity IDs and date ranges
    3. Executes the search and captures results with metadata
    
    Args:
        state: Individual search strategy state
        config: Configuration for the workflow
        
    Returns:
        Dict containing the completed search result
    """
    # Get stream writer for real-time updates
    writer = get_stream_writer()
    
    # Get state
    strategy = state["strategy"]
    entity_ids = state.get("entity_ids")
    global_date_range = state.get("global_date_range")
    
    # Stream search initiation
    writer({
        "type": "search_start",
        "tool_type": strategy.tool_type,
        "message": f"🔍 Starting {strategy.tool_type.upper()} search: {strategy.description}",
        "query_count": len(strategy.search_queries),
        "timestamp": datetime.now().isoformat()
    })
    
    # Get configuration
    configurable = BigdataSearchConfiguration.from_runnable_config(config)
    
    # Import tools dynamically
    from .tools import (
        bigdata_news_search,
        bigdata_transcript_search,
        bigdata_filings_search,
        bigdata_universal_search,
        bigdata_knowledge_graph,
    )
    
    writer({
        "type": "tool_selection",
        "tool_type": strategy.tool_type,
        "message": f"🛠️  Selected {strategy.tool_type} tool for execution"
    })
    
    # Select tool based on strategy type
    tool_map = {
        "news": bigdata_news_search,
        "transcripts": bigdata_transcript_search,
        "filings": bigdata_filings_search,
        "universal": bigdata_universal_search,
        "knowledge_graph": bigdata_knowledge_graph,
    }
    
    selected_tool = tool_map.get(strategy.tool_type)
    if not selected_tool:
        writer({
            "type": "error",
            "message": f"❌ Unknown tool type: {strategy.tool_type}",
            "error": f"Tool type '{strategy.tool_type}' not found"
        })
        raise ValueError(f"Unknown tool type: {strategy.tool_type}")
    
    writer({
        "type": "parameter_prep",
        "message": f"⚙️  Preparing search parameters..."
    })
    
    # Prepare and validate parameters
    tool_params = {
        "queries": strategy.search_queries,
        "max_results": configurable.max_results_per_strategy,
    }
    
    # Clean and validate strategy parameters
    cleaned_params = _clean_tool_parameters(strategy.parameters, strategy.tool_type)
    tool_params.update(cleaned_params)
    
    writer({
        "type": "parameters_ready",
        "tool_type": strategy.tool_type,
        "max_results": configurable.max_results_per_strategy,
        "strategy_params": cleaned_params,
        "message": f"📋 Parameters configured: {len(cleaned_params)} custom parameters"
    })
    
    # Add global parameters for search tools (not knowledge_graph)
    if strategy.tool_type != "knowledge_graph":
        if entity_ids:
            tool_params["entity_ids"] = entity_ids
            writer({
                "type": "entity_filter",
                "entity_count": len(entity_ids),
                "message": f"🏢 Adding entity filter: {len(entity_ids)} entities"
            })
        if global_date_range:
            tool_params["date_range"] = global_date_range
            writer({
                "type": "date_filter",
                "date_range": global_date_range,
                "message": f"📅 Date range filter: {global_date_range}"
            })
        # Add default rerank threshold
        if "rerank_threshold" not in tool_params and strategy.tool_type in ["news", "universal"]:
            tool_params["rerank_threshold"] = configurable.bigdata_rerank_threshold
            writer({
                "type": "rerank_config",
                "threshold": configurable.bigdata_rerank_threshold,
                "message": f"📊 Rerank threshold: {configurable.bigdata_rerank_threshold}"
            })
    
    # Special handling for knowledge_graph tool parameters
    if strategy.tool_type == "knowledge_graph":
        # Ensure required parameters are present
        if "search_type" not in tool_params:
            tool_params["search_type"] = "companies"  # Default search type
        if "search_term" not in tool_params:
            # Use first query as search term
            tool_params["search_term"] = strategy.search_queries[0] if strategy.search_queries else ""
        # Remove queries parameter for knowledge_graph
        tool_params.pop("queries", None)
        
        writer({
            "type": "knowledge_graph_config",
            "search_type": tool_params.get("search_type"),
            "search_term": tool_params.get("search_term"),
            "message": f"🧠 Knowledge graph search: {tool_params.get('search_type')} for '{tool_params.get('search_term')}'"
        })
    
    # Stream rate limiting info
    writer({
        "type": "rate_limit_info",
        "delay": configurable.bigdata_rate_limit_delay,
        "message": f"⏱️  Rate limit: {configurable.bigdata_rate_limit_delay}s between requests"
    })
    
    # Stream complete final parameters before API call
    writer({
        "type": "final_parameters",
        "tool_type": strategy.tool_type,
        "parameters": tool_params,
        "message": f"🔧 Final tool parameters: {tool_params}"
    })
    
    # Execute tool with timing
    writer({
        "type": "api_start",
        "tool_type": strategy.tool_type,
        "message": f"🚀 Executing {strategy.tool_type} API call..."
    })
    
    start_time = time.time()
    try:
        raw_results = await selected_tool.ainvoke(tool_params)
        execution_time = time.time() - start_time
        
        # Stream success results
        writer({
            "type": "api_success",
            "tool_type": strategy.tool_type,
            "execution_time": execution_time,
            "message": f"✅ {strategy.tool_type.upper()} search completed ({execution_time:.1f}s)"
        })
        
        # Try to extract result count from the string response
        result_length = len(raw_results) if isinstance(raw_results, str) else 0
        if result_length > 0:
            # Estimate result quality based on length
            quality_indicator = "🟢 High" if result_length > 1000 else "🟡 Medium" if result_length > 500 else "🔴 Low"
            writer({
                "type": "result_quality",
                "tool_type": strategy.tool_type,
                "content_length": result_length,
                "quality": quality_indicator,
                "message": f"📊 Result quality: {quality_indicator} ({result_length:,} chars)"
            })
        
        # Create SearchResult
        search_result = SearchResult(
            strategy=strategy,
            results=[{"raw_output": raw_results}],  # Tool returns formatted string
            metadata={
                "execution_time": execution_time,
                "query_count": len(strategy.search_queries),
                "tool_type": strategy.tool_type,
                "parameters_used": tool_params,
                "success": True,
                "content_length": result_length
            }
        )
        
        writer({
            "type": "search_complete",
            "tool_type": strategy.tool_type,
            "success": True,
            "message": f"🎉 {strategy.tool_type.upper()} search strategy completed successfully"
        })
        
    except Exception as e:
        execution_time = time.time() - start_time
        error_message = str(e)
        
        # Stream error with details
        writer({
            "type": "api_error",
            "tool_type": strategy.tool_type,
            "execution_time": execution_time,
            "error": error_message,
            "message": f"❌ {strategy.tool_type.upper()} search failed ({execution_time:.1f}s): {error_message}"
        })
        
        # Check for specific error types and provide helpful context
        if "rate limit" in error_message.lower():
            writer({
                "type": "rate_limit_hit",
                "tool_type": strategy.tool_type,
                "message": f"⏳ Rate limit encountered - consider increasing delay"
            })
        elif "authentication" in error_message.lower():
            writer({
                "type": "auth_error",
                "tool_type": strategy.tool_type,
                "message": f"🔐 Authentication issue - check API credentials"
            })
        elif "timeout" in error_message.lower():
            writer({
                "type": "timeout_error",
                "tool_type": strategy.tool_type,
                "message": f"⏰ Request timeout - API may be slow"
            })
        
        print(f"Error executing search strategy {strategy.tool_type}: {str(e)}")
        
        # Create failed SearchResult
        search_result = SearchResult(
            strategy=strategy,
            results=[],
            metadata={
                "execution_time": execution_time,
                "query_count": len(strategy.search_queries),
                "tool_type": strategy.tool_type,
                "parameters_used": tool_params,
                "success": False,
                "error": error_message
            }
        )
        
        writer({
            "type": "search_complete",
            "tool_type": strategy.tool_type,
            "success": False,
            "message": f"💔 {strategy.tool_type.upper()} search strategy failed"
        })
    
    return {"completed_searches": [search_result]}

async def gather_search_results(state: BigdataSearchState, config: RunnableConfig):
    """Gather and process all completed search results.
    
    This node:
    1. Collects all search results from parallel execution
    2. Performs deduplication if enabled
    3. Calculates quality scores and metadata
    
    Args:
        state: Current state with completed searches
        config: Configuration for the workflow
        
    Returns:
        Dict with processed results and metadata
    """
    # Get stream writer for real-time updates
    writer = get_stream_writer()
    
    # Get completed searches
    completed_searches = state.get("completed_searches", [])
    
    writer({
        "type": "gathering_start",
        "total_searches": len(completed_searches),
        "message": f"📊 Gathering results from {len(completed_searches)} completed searches"
    })
    
    # Get configuration
    configurable = BigdataSearchConfiguration.from_runnable_config(config)
    
    # Analyze search success/failure
    successful_searches = [s for s in completed_searches if s.metadata.get("success", False)]
    failed_searches = [s for s in completed_searches if not s.metadata.get("success", False)]
    
    writer({
        "type": "success_analysis",
        "successful_count": len(successful_searches),
        "failed_count": len(failed_searches),
        "success_rate": len(successful_searches) / len(completed_searches) * 100 if completed_searches else 0,
        "message": f"📈 Success rate: {len(successful_searches)}/{len(completed_searches)} ({len(successful_searches)/len(completed_searches)*100:.1f}%)"
    })
    
    # Debug mode: Stream detailed parameter information from completed searches
    configurable = BigdataSearchConfiguration.from_runnable_config(config)
    if configurable.debug_mode:
        writer({
            "type": "debug_mode_enabled", 
            "message": "🔧 Debug mode enabled - showing detailed tool parameters"
        })
        
        for i, search in enumerate(completed_searches, 1):
            strategy = search.strategy
            parameters = search.metadata.get("parameters_used", {})
            
            writer({
                "type": "debug_tool_parameters",
                "search_index": i,
                "tool_type": strategy.tool_type,
                "strategy_description": strategy.description,
                "search_queries": strategy.search_queries,
                "parameters": parameters,
                "success": search.metadata.get("success", False),
                "execution_time": search.metadata.get("execution_time", 0),
                "message": f"🔍 Search {i}/{len(completed_searches)}: {strategy.tool_type.upper()}"
            })
    
    # Stream individual search summaries
    for i, search in enumerate(completed_searches, 1):
        status_emoji = "✅" if search.metadata.get("success", False) else "❌"
        execution_time = search.metadata.get("execution_time", 0)
        tool_type = search.strategy.tool_type
        
        writer({
            "type": "search_summary",
            "search_index": i,
            "tool_type": tool_type,
            "success": search.metadata.get("success", False),
            "execution_time": execution_time,
            "content_length": search.metadata.get("content_length", 0),
            "message": f"  {status_emoji} {tool_type.upper()}: {execution_time:.1f}s"
        })
    
    writer({
        "type": "metadata_calculation",
        "message": "🧮 Calculating search metadata and performance metrics..."
    })
    
    # Calculate source metadata
    total_execution_time = sum(s.metadata.get("execution_time", 0) for s in completed_searches)
    source_metadata = {
        "total_searches": len(completed_searches),
        "successful_searches": len(successful_searches),
        "total_execution_time": total_execution_time,
        "tool_type_distribution": {},
        "search_timestamp": get_today_str(),
        "average_execution_time": total_execution_time / len(completed_searches) if completed_searches else 0,
        "total_content_length": sum(s.metadata.get("content_length", 0) for s in successful_searches)
    }
    
    # Calculate tool type distribution
    for search in completed_searches:
        tool_type = search.strategy.tool_type
        source_metadata["tool_type_distribution"][tool_type] = source_metadata["tool_type_distribution"].get(tool_type, 0) + 1
    
    writer({
        "type": "performance_metrics",
        "total_execution_time": total_execution_time,
        "average_execution_time": source_metadata["average_execution_time"],
        "total_content_length": source_metadata["total_content_length"],
        "message": f"⚡ Performance: {total_execution_time:.1f}s total, {source_metadata['average_execution_time']:.1f}s avg"
    })
    
    # Show tool type distribution
    for tool_type, count in source_metadata["tool_type_distribution"].items():
        writer({
            "type": "tool_distribution",
            "tool_type": tool_type,
            "count": count,
            "message": f"  📋 {tool_type}: {count} search{'es' if count != 1 else ''}"
        })
    
    # TODO: Implement deduplication if enabled
    if configurable.enable_cross_strategy_deduplication:
        writer({
            "type": "deduplication_skip",
            "message": "🔄 Cross-strategy deduplication: Not implemented yet"
        })
        # For now, keep all results - implement deduplication logic later
        pass
    
    writer({
        "type": "gathering_complete",
        "successful_searches": len(successful_searches),
        "total_content": source_metadata["total_content_length"],
        "message": f"✅ Result gathering complete: {len(successful_searches)} successful searches, {source_metadata['total_content_length']:,} chars total"
    })
    
    return {"source_metadata": source_metadata}

async def compile_final_results(state: BigdataSearchState, config: RunnableConfig):
    """Compile all search results into the final formatted output.
    
    This node:
    1. Takes all search results and entity discoveries
    2. Uses an LLM to synthesize them into a comprehensive report
    3. Streams the LLM tokens as they're generated (when using messages stream mode)
    4. Returns the final formatted results
    
    Args:
        state: Current state with all results and metadata
        config: Configuration for the workflow
        
    Returns:
        Dict containing the final compiled results
    """
    # Get stream writer for real-time updates
    writer = get_stream_writer()
    
    # Get inputs
    topic = state["topic"]
    completed_searches = state.get("completed_searches", [])
    source_metadata = state.get("source_metadata", {})
    
    writer({
        "type": "compilation_start",
        "topic": topic,
        "total_searches": len(completed_searches),
        "successful_searches": source_metadata.get("successful_searches", 0),
        "message": f"📝 Starting report compilation for: '{topic}'"
    })
    
    # Get configuration
    configurable = BigdataSearchConfiguration.from_runnable_config(config)
    
    writer({
        "type": "data_preparation",
        "message": "📋 Preparing search results for LLM synthesis..."
    })
    
    # Format search results for LLM
    search_results_str = ""
    successful_results = 0
    total_content_length = 0
    
    for i, search in enumerate(completed_searches, 1):
        search_results_str += f"\n--- SEARCH STRATEGY {i}: {search.strategy.tool_type.upper()} ---\n"
        search_results_str += f"Description: {search.strategy.description}\n"
        search_results_str += f"Success: {search.metadata.get('success', False)}\n"
        
        if search.results:
            successful_results += 1
            for result in search.results:
                if isinstance(result, dict) and "raw_output" in result:
                    content = result['raw_output']
                    total_content_length += len(content)
                    search_results_str += f"Results:\n{content}\n"
        else:
            search_results_str += f"No results (Error: {search.metadata.get('error', 'Unknown')})\n"
        
        search_results_str += f"{'='*60}\n"
        
        # Stream progress for each strategy processed
        writer({
            "type": "strategy_processed",
            "strategy_index": i,
            "tool_type": search.strategy.tool_type,
            "success": search.metadata.get("success", False),
            "message": f"  📄 Processed {search.strategy.tool_type} strategy ({i}/{len(completed_searches)})"
        })
    
    writer({
        "type": "data_ready",
        "successful_results": successful_results,
        "total_content_length": total_content_length,
        "message": f"📊 Data prepared: {successful_results} successful results, {total_content_length:,} chars"
    })
    
    # Set up writer model for compilation
    writer_provider = get_config_value(configurable.writer_provider)
    writer_model_name = get_config_value(configurable.writer_model)
    writer_model_kwargs = get_config_value(configurable.writer_model_kwargs or {})
    
    writer({
        "type": "llm_setup",
        "provider": writer_provider,
        "model": writer_model_name,
        "message": f"🤖 Setting up {writer_provider}:{writer_model_name} for report synthesis"
    })
    
    writer_model = init_chat_model(
        model=writer_model_name,
        model_provider=writer_provider,
        model_kwargs=writer_model_kwargs
    )
    
    # Format system instructions
    system_instructions = result_compilation_instructions.format(
        topic=topic,
        search_results=search_results_str,
        source_metadata=source_metadata
    )
    
    instruction_length = len(system_instructions)
    writer({
        "type": "prompt_ready",
        "instruction_length": instruction_length,
        "message": f"📝 Prompt prepared: {instruction_length:,} chars (including {total_content_length:,} chars of results)"
    })
    
    writer({
        "type": "synthesis_start",
        "message": "🧠 Starting LLM synthesis - streaming tokens as they generate..."
    })
    
    # Compile final results with streaming
    start_time = time.time()
    
    # Stream the LLM response token by token
    final_results_content = ""
    async for chunk in writer_model.astream([
        SystemMessage(content=system_instructions),
        HumanMessage(content="Compile these search results into a comprehensive research summary.")
    ]):
        if hasattr(chunk, 'content') and chunk.content:
            final_results_content += chunk.content
    
    synthesis_time = time.time() - start_time
    
    report_length = len(final_results_content)
    writer({
        "type": "synthesis_complete",
        "synthesis_time": synthesis_time,
        "report_length": report_length,
        "message": f"✅ Report synthesis complete ({synthesis_time:.1f}s): {report_length:,} chars generated"
    })
    
    # Stream final report statistics
    report_lines = final_results_content.count('\n') + 1
    estimated_words = len(final_results_content.split())
    
    writer({
        "type": "report_stats",
        "report_length": report_length,
        "report_lines": report_lines,
        "estimated_words": estimated_words,
        "compression_ratio": report_length / total_content_length if total_content_length > 0 else 0,
        "message": f"📊 Final report: {estimated_words:,} words, {report_lines:,} lines"
    })
    
    # Stream the clean markdown version
    writer({
        "type": "markdown_output",
        "content": final_results_content,
        "message": "📄 Clean markdown version ready"
    })
    
    writer({
        "type": "workflow_complete",
        "topic": topic,
        "total_time": source_metadata.get("total_execution_time", 0) + synthesis_time,
        "successful_searches": successful_results,
        "final_report_length": report_length,
        "message": f"🎉 Workflow complete! Research report generated for '{topic}'"
    })
    
    return {"final_results": final_results_content}

## Routing Functions

def initiate_parallel_searches(state: BigdataSearchState):
    """Create parallel search tasks using Send() API."""
    return [
        Send("execute_search_strategy", {
            "topic": state["topic"],
            "strategy": s,
            "entity_ids": state.get("entity_preference"),  # Use entity_preference directly if provided
            "global_date_range": state.get("date_range"),
            "completed_searches": []
        })
        for s in state["search_strategies"]
    ]

## Graph Assembly

# Individual search strategy sub-graph
search_strategy_builder = StateGraph(SearchStrategyState, output=SearchStrategyOutput)
search_strategy_builder.add_node("execute_search_strategy", execute_search_strategy)
search_strategy_builder.add_edge(START, "execute_search_strategy")
search_strategy_builder.add_edge("execute_search_strategy", END)

# Main Bigdata search graph
builder = StateGraph(
    BigdataSearchState,
    input=BigdataSearchStateInput,
    output=BigdataSearchStateOutput,
    config_schema=BigdataSearchConfiguration
)

# Add nodes
builder.add_node("generate_search_plan", generate_search_plan)
builder.add_node("execute_search_strategy", search_strategy_builder.compile())
builder.add_node("gather_search_results", gather_search_results)
builder.add_node("compile_final_results", compile_final_results)

# Add edges
builder.add_edge(START, "generate_search_plan")
builder.add_conditional_edges(
    "generate_search_plan",
    initiate_parallel_searches,
    ["execute_search_strategy"]
)
builder.add_edge("execute_search_strategy", "gather_search_results")
builder.add_edge("gather_search_results", "compile_final_results")
builder.add_edge("compile_final_results", END)

# Compile the graph
bigdata_search_graph = builder.compile() 