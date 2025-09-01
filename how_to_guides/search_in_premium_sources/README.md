# Premium Sources Search Script

This Python script uses the Bigdata.com Search Service to search across premium financial news sources, transcripts, and SEC filings.

## Features

- **Premium Source Integration**: Searches across multiple premium financial data sources
- **Flexible Document Types**: Support for NEWS, FACTSET_TRANSCRIPTS, QUARTR_TRANSCRIPTS, and SEC_FILINGS
- **Date Range Filtering**: Optional start and end date parameters for time-based searches
- **Intelligent Fallback**: Automatic secondary search with negative sources when NEWS results are insufficient
- **Structured Output**: JSON output optimized for LLM consumption
- **Comprehensive Logging**: Detailed logs showing chunk counts and search progress

## Setup

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Environment Variables**:
   Create a `.env` file in the project root with your Bigdata.com API key:
   ```
   BIGDATA_API_KEY=your_api_key_here
   ```

3. **Output Directory**:
   The script automatically creates an `output/` directory for search results.

## Usage

### Basic Search
```bash
python search_in_premium_sources.py "your search sentence here"
```

### Search with Date Range
```bash
python search_in_premium_sources.py "earnings call transcript" --start-date 2024-01-01 --end-date 2024-01-31
```

### Search Specific Document Types
```bash
# Search only news sources
python search_in_premium_sources.py "market analysis" --document-types NEWS

# Search only transcripts
python search_in_premium_sources.py "earnings call" --document-types FACTSET_TRANSCRIPTS QUARTR_TRANSCRIPTS

# Search all document types (default)
python search_in_premium_sources.py "financial data" --document-types NEWS FACTSET_TRANSCRIPTS QUARTR_TRANSCRIPTS SEC_FILINGS
```

### Custom Output Filename
```bash
python search_in_premium_sources.py "search query" --output-file my_results.json
```

### Run Tests
```bash
# Quick test suite (4 essential tests)
python test_scenarios.py

# Or run specific test modes:
# 1. Quick Test Suite (4 essential tests)
# 2. Comprehensive Test Suite (all 16 tests)  
# 3. Custom Test (user-defined parameters)
# 4. Exit
```

### Run Statistics Analysis
```bash
# Quick statistics (10 representative questions)
python search_statistics.py

# Or run specific analysis modes:
# 1. Quick Statistics (10 representative questions)
# 2. Comprehensive Statistics (all 50 questions)
# 3. Custom Statistics (user-defined questions)
# 4. Exit
```

## Command Line Arguments

- `sentence`: The search query (required)
- `--start-date`: Start date for search range (YYYY-MM-DD format)
- `--end-date`: End date for search range (YYYY-MM-DD format)
- `--document-types`: List of document types to include (choices: NEWS, FACTSET_TRANSCRIPTS, QUARTR_TRANSCRIPTS, SEC_FILINGS)
- `--output-file`: Custom filename for output (optional)

## Document Types

### NEWS
Includes premium financial news sources:
- Alliance News
- Benzinga
- Fly on the Wall
- FXStreet (Economic Calendar & News)
- Midnight Trader Newswires (various regions)

### FACTSET_TRANSCRIPTS
- FactSet transcripts

### QUARTR_TRANSCRIPTS
- Quartr audios
- Quartr presentation materials
- Quartr reports
- Quartr transcripts

### SEC_FILINGS
- SEC filing documents

## Output Format

The script generates a JSON file with the following structure:

```json
[
  {
    "document_id": "unique_id",
    "headline": "Document headline",
    "source": {
      "key": "source_key",
      "name": "Source Name"
    },
    "url": "document_url",
    "timestamp": "timestamp",
    "chunks": [
      {
        "chunk_index": 0,
        "text": "Chunk text content",
        "relevance": 0.95,
        "sentiment": "positive"
      }
    ]
  }
]
```

## Search Logic

1. **Primary Search**: Performs initial search with specified sources and document types
2. **Chunk Count Check**: If NEWS is selected and less than 5 chunks are returned, triggers secondary search
3. **Secondary Search**: Uses negative source filters (~Source) to find additional relevant content
4. **Result Combination**: Merges results from both searches into final output

## Logging

The script provides detailed logging including:
- Source initialization details
- Search progress and chunk counts
- Secondary search triggers
- File save confirmations

## Error Handling

- Validates API key presence
- Handles missing document attributes gracefully
- Provides clear error messages for common issues

## Example Use Cases

- **Earnings Analysis**: Search transcripts and news around earnings announcements
- **Market Research**: Find news and analysis on specific market events
- **Regulatory Monitoring**: Track SEC filings and related news coverage
- **Company Research**: Gather comprehensive information from multiple source types

## Testing

The `test_scenarios.py` script provides comprehensive testing capabilities:

### Test Categories

1. **Basic Functionality Tests**
   - Basic search across all document types
   - News-only searches
   - Transcript-only searches
   - SEC filings-only searches

2. **Advanced Feature Tests**
   - Date range filtering
   - Combined document type searches
   - Secondary search triggers (NEWS < 5 chunks)
   - Edge cases and error handling

3. **Query Variation Tests**
   - Long vs. short queries
   - Special characters and symbols
   - Company-specific searches
   - Market event searches

4. **Performance Tests**
   - Large date ranges
   - Recent events
   - Empty result scenarios

### Running Tests

```bash
python test_scenarios.py
```

Choose from:
- **Quick Test Suite**: 4 essential tests for basic validation
- **Comprehensive Test Suite**: All 16 tests for thorough validation
- **Custom Test**: Interactive test with user-defined parameters

### Test Output

Each test provides:
- ✅/❌ Success/failure status
- Document and chunk counts
- Execution time
- Detailed error messages for failures
- Summary report at the end

## Statistics Analysis

The `search_statistics.py` script provides comprehensive analysis of secondary search frequency:

### Analysis Features

1. **Secondary Search Detection**
   - Automatically detects when secondary searches are triggered
   - Tracks frequency across different document types
   - Provides percentage breakdowns

2. **Comprehensive Metrics**
   - Document and chunk counts per query
   - Execution time analysis
   - Success/failure rates
   - Document type distribution

3. **Financial Research Coverage**
   - 50 pre-defined financial research questions
   - Covers earnings, market analysis, company research
   - Industry analysis, regulatory compliance
   - Risk management, investment strategies
   - Economic indicators, specialized topics

### Analysis Modes

- **Quick Statistics**: 10 representative questions for fast analysis
- **Comprehensive Statistics**: All 50 questions for thorough analysis
- **Custom Statistics**: User-defined questions and document types

### Output Reports

- **Console Summary**: Real-time statistics and progress
- **JSON Report**: Detailed analysis saved to output folder
- **Secondary Search Rate**: Percentage of queries requiring secondary search
- **Performance Metrics**: Average chunks, execution times, success rates

## Files

- `search_in_premium_sources.py`: Main search script with all functionality
- `example_scenarios.py`: Example usage demonstrating different search scenarios
- `search_statistics.py`: Statistics analysis script for secondary search frequency
- `requirements.txt`: Python dependencies
- `README.md`: This documentation

## Future Enhancements

- LLM integration for automated analysis
- Advanced filtering options
- Result ranking and relevance scoring
- Export to various formats (CSV, Excel)
- Batch processing capabilities
