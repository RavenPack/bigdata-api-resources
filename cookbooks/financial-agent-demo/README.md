# Bigdata Search Agent

🚀 **Advanced AI-Powered Financial Research Agent** using Bigdata.com API with LangGraph workflows, real-time streaming, and intelligent multi-strategy search orchestration.

## 🌟 Key Features

### 🧠 **Intelligent Search Orchestration**
- **LangGraph Workflow**: Sophisticated multi-stage research pipeline with parallel execution
- **AI Strategy Generation**: LLM automatically creates multiple complementary search strategies
- **Hybrid Search**: Combines semantic similarity and keyword matching for optimal results
- **Smart Parameter Handling**: Automatic query optimization and parameter validation

### ⚡ **Real-Time Streaming Interface**
- **Live Progress Updates**: Real-time workflow status with detailed execution metrics
- **Token-by-Token Synthesis**: Stream LLM report generation as it happens
- **Rich Terminal UI**: Beautiful progress bars, status indicators, and formatted results
- **Debug Mode**: Detailed parameter inspection and API call tracing

### 🔍 **Comprehensive Data Sources**
- **Premium News**: Global publishers with multilingual support and source credibility ranking
- **Corporate Transcripts**: Earnings calls, presentations with section detection (Q&A, management discussion)
- **SEC Filings**: 10-K, 10-Q, 8-K with fiscal period and entity filtering
- **Universal Search**: Cross-document search with unified result ranking
- **Knowledge Graph**: Company/source ID discovery for targeted filtering

### 🎯 **Advanced Filtering & Targeting**
- **Entity-Based Filtering**: Target specific companies using knowledge graph IDs
- **Temporal Filtering**: Rolling ("last_week") and absolute date ranges
- **Document Type Filtering**: Filing types, transcript sections, source credibility
- **Fiscal Period Filtering**: Year/quarter targeting for financial documents

## 🏗️ Architecture Overview

### LangGraph Workflow Pipeline

```
📊 Planning Phase
    ↓
🔍 Parallel Execution Phase  
    ↓
📋 Results Gathering Phase
    ↓  
📝 LLM Synthesis Phase
```

#### **Core Workflow Nodes:**

1. **`generate_search_plan`**: LLM analyzes research topic and creates multiple complementary search strategies
2. **`execute_search_strategy`**: Parallel execution of strategies using appropriate Bigdata tools with parameter optimization
3. **`gather_search_results`**: Aggregates results, calculates performance metrics, and prepares for synthesis
4. **`compile_final_results`**: LLM synthesizes all findings into comprehensive research report

#### **Async Tool Ecosystem:**

- **Thread-Safe Client Management**: Singleton pattern prevents authentication rate limiting
- **Error Resilience**: Individual strategy failures don't crash the workflow
- **Auto-Recovery**: Automatic client reset on authentication errors
- **Rate Limiting**: Built-in delays and request management

## 🚀 Quick Start

### 1. Install Dependencies

```bash
# Using uv (recommended)
uv sync

# Or using pip
pip install -e .
```

### 2. Set Environment Variables

Copy `.env.example` to `.env` and configure:

```bash
cp .env.example .env
```

Edit `.env`:
```env
BIGDATA_USERNAME=your_username_here
BIGDATA_PASSWORD=your_password_here
GOOGLE_API_KEY=your_google_api_key_here
```

### 3. Run Interactive Demo

```bash
# Full streaming demo with Rich UI
python -m bigdata_search_agent.streaming_example

# Debug mode for detailed execution tracing
python -m bigdata_search_agent.streaming_example --debug
```

## 💡 Usage Examples

### Basic Research Query

```python
from bigdata_search_agent import bigdata_search_graph

# Define research topic
input_state = {
    "topic": "Analyze Tesla's Q3 2024 earnings impact on EV market competition",
    "search_depth": 3,  # Number of complementary strategies
    "max_results_per_strategy": 20
}

# Configure LLM providers
config = {
    "configurable": {
        "planner_provider": "google_genai",
        "planner_model": "gemini-2.5-flash",
        "writer_provider": "google_genai", 
        "writer_model": "gemini-2.5-flash"
    }
}

# Execute comprehensive research workflow
result = await bigdata_search_graph.ainvoke(input_state, config)
print(result["final_report"])
```

### Real-Time Streaming with Progress Tracking

```python
# Stream live workflow execution
async for chunk in bigdata_search_graph.astream(
    input_state, 
    config=config,
    stream_mode="custom"
):
    # Handle different event types
    if chunk.get("type") == "planning_complete":
        print(f"🎯 Generated {chunk.get('strategy_count')} search strategies")
    
    elif chunk.get("type") == "search_start":
        tool_type = chunk.get("tool_type")
        print(f"🔍 Starting {tool_type.upper()} search...")
    
    elif chunk.get("type") == "synthesis_start":
        print("🧠 LLM synthesizing final report...")
    
    elif chunk.get("type") == "markdown_output":
        print("📄 Final Report:")
        print(chunk.get("content"))
```

### Targeted Entity Research

```python
# Research specific companies with entity filtering
input_state = {
    "topic": "Compare renewable energy strategies across major tech companies",
    "entity_preference": ["ENTITY_123", "ENTITY_456"],  # Found via knowledge graph
    "date_range": "last_90_days",
    "search_depth": 2
}

result = await bigdata_search_graph.ainvoke(input_state, config)
```

### Advanced Configuration

```python
# Fine-tune search behavior
config = {
    "configurable": {
        # LLM Configuration
        "planner_provider": "google_genai",
        "planner_model": "gemini-2.5-flash",
        "writer_provider": "google_genai",
        "writer_model": "gemini-2.5-flash",
        
        # Search Parameters
        "max_results_per_strategy": 30,
        "search_depth": 4,
        "number_of_queries": 3,
        "bigdata_rerank_threshold": 0.15,
        "bigdata_rate_limit_delay": 1.5,
        
        # Features
        "enable_cross_strategy_deduplication": True,
        "debug_mode": True
    }
}
```

## 🛠️ Individual Tool Usage

### Knowledge Graph Discovery

```python
from bigdata_search_agent.tools import bigdata_knowledge_graph

# Find company entity IDs
companies = await bigdata_knowledge_graph(
    search_type="companies",
    search_term="Tesla",
    max_results=5
)

# Discover credible news sources
sources = await bigdata_knowledge_graph(
    search_type="sources", 
    search_term="financial news",
    source_rank_filter=5  # Highest credibility
)
```

### Targeted Content Search

```python
from bigdata_search_agent.tools import (
    bigdata_news_search,
    bigdata_transcript_search,
    bigdata_filings_search
)

# Premium news with entity targeting
news_results = await bigdata_news_search(
    queries=["EV market competition", "electric vehicle sales"],
    entity_ids=["ENTITY_123"],  # Tesla entity ID
    date_range="last_month",
    max_results=10
)

# Earnings call transcripts
transcript_results = await bigdata_transcript_search(
    queries=["guidance", "forecast", "outlook"],
    transcript_types=["EARNINGS_CALL"],
    section_metadata=["QA"],  # Q&A sections only
    fiscal_quarter=3,
    fiscal_year=2024
)

# SEC filings search
filing_results = await bigdata_filings_search(
    queries=["risk factors", "competition"],
    filing_types=["SEC_10_K", "SEC_10_Q"],
    reporting_entity_ids=["ENTITY_123"]
)
```

## 📁 Project Structure

```
bigdata_search_agent/
├── __init__.py              # Main exports and graph access
├── graph.py                 # 🧠 LangGraph workflow orchestration
├── tools.py                 # 🔧 LangChain tool wrappers  
├── utils.py                 # ⚙️  Core async API utilities
├── state.py                 # 📊 Pydantic state models
├── configuration.py         # ⚙️  Configuration management
├── prompts.py              # 💬 LLM prompts and templates
├── streaming_example.py    # 🎨 Interactive Rich UI demo
└── tests/                  # 🧪 Comprehensive test suite
    ├── test_*.py           # Individual tool tests
    └── debug_*.py          # Debugging utilities
```

## ⚙️ Configuration Options

### LLM Provider Support
- **Google Gemini**: `google_genai` (gemini-2.5-flash, gemini-pro)
- **OpenAI**: `openai` (gpt-4, gpt-3.5-turbo)
- **Anthropic**: `anthropic` (claude-3-sonnet, claude-3-haiku)
- **Custom**: Any LangChain-compatible provider

### Search Customization
- **Search Depth**: Number of parallel search strategies (1-5)
- **Results Per Strategy**: Content volume control (5-50)
- **Query Variations**: Multiple query formulations per strategy (1-5)
- **Rerank Threshold**: Semantic similarity filtering (0.0-1.0)
- **Rate Limiting**: API request pacing (0.5-5.0 seconds)

### Advanced Features
- **Cross-Strategy Deduplication**: Remove duplicate content across searches
- **Debug Mode**: Detailed parameter and execution tracing
- **Custom Date Ranges**: Rolling and absolute temporal filtering
- **Entity Targeting**: Company-specific research focus

## 🔧 Development & Extension

### Adding New Search Tools

1. **Create async utility function** in `utils.py`:
```python
async def bigdata_new_tool_search_async(
    search_queries: List[str],
    # ... parameters
) -> List[Dict[str, Any]]:
    # Implementation following existing patterns
```

2. **Add LangChain tool wrapper** in `tools.py`:
```python
@tool(description="New tool description")
async def bigdata_new_tool_search(
    queries: List[str],
    # ... parameters
) -> str:
    # Tool wrapper implementation
```

3. **Update workflow integration** in `graph.py`:
```python
# Add to tool_map in execute_search_strategy
tool_map = {
    # ... existing tools
    "new_tool": bigdata_new_tool_search,
}
```

### Custom Result Processing

Extend `_format_search_results()` in `utils.py` to add new metadata fields or content processing logic.

### Workflow Customization

Modify workflow nodes in `graph.py` or add new nodes for custom processing steps.

## 🧪 Testing & Debugging

### Run Test Suite

```bash
# Full test suite
python -m pytest bigdata_search_agent/tests/

# Individual tool tests
python -m pytest bigdata_search_agent/tests/test_news_search.py

# Debug specific functionality
python bigdata_search_agent/tests/debug_news.py
```

### Debug Mode Features

- **Parameter Inspection**: View all tool parameters before API calls
- **Execution Timing**: Detailed performance metrics
- **Error Tracing**: Full stack traces for debugging
- **Content Analysis**: Raw content inspection and quality metrics

## 📚 Documentation

- **[API Documentation](bigdata_search_agent/)**: Detailed module documentation
- **[Test Examples](bigdata_search_agent/tests/)**: Working examples for each tool
- **[Configuration Guide](bigdata_search_agent/configuration.py)**: Complete configuration options

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Add comprehensive tests for new functionality
4. Run the test suite: `python -m pytest bigdata_search_agent/tests/`
5. Update documentation and docstrings
6. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙋‍♂️ Support

- **Issues**: [GitHub Issues](https://github.com/yourusername/bigdata-search-agent/issues)
- **Documentation**: See module docstrings for detailed API documentation
- **Examples**: Check the `tests/` directory for working examples

