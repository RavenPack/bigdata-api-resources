# Retrieve Entire Articles Script for Bigdata.com

This Python script authenticates to Bigdata.com APIs, processes separate text files for keywords and sentences, explores the knowledge graph for places, and performs parallel searches to retrieve news articles.


## Requirements

- Python 3.7+
- `bigdata-client` package
- `python-dotenv` package
- Bigdata.com [API KEY](https://docs.bigdata.com/sdk-reference/introduction#api-key-beta)

## Installation

1. Install required packages:
```bash
pip install -r requirements.txt
```

2. Create a `.env` file with your Bigdata.com credentials:
```env
BIGDATA_API_KEY=your_api_key
```

## Usage

### Command-Line Interface

The script now uses a modern command-line interface with named parameters and built-in help:

```bash
# Show help and all available options
python retrieve_entire_articles.py --help
```

### Basic Usage
```bash
python retrieve_entire_articles.py <start_date> <end_date> <keywords_file> <sentences_file>
```

### With Country Filtering
```bash
python retrieve_entire_articles.py <start_date> <end_date> <keywords_file> <sentences_file> --country "United States"
```

### With Custom Log Level
```bash
python retrieve_entire_articles.py <start_date> <end_date> <keywords_file> <sentences_file> --log_level INFO
```

### Complete Example with All Options
```bash
python retrieve_entire_articles.py '2024-01-01' '2024-01-31' 'keywords.txt' 'sentences.txt' --country "United States" --log_level INFO
```

### Examples
```bash
python retrieve_entire_articles.py --country Europe --log_level INFO 2025-02-02 2025-02-03 electrification_keywords.txt electrification_sentences.txt 
```

## Parameters

### Required Parameters (Positional Arguments)
- **start_date**: Start date in YYYY-MM-DD format (e.g., '2024-01-01')
- **end_date**: End date in YYYY-MM-DD format (e.g., '2024-01-31')
- **keywords_file**: Text file containing keywords (one per line)
- **sentences_file**: Text file containing sentences (one per line)

### Optional Parameters (Named Arguments)
- **--country**: Country name for place filtering (e.g., 'United States', 'Germany', 'Japan')
- **--log_level**: Log level for the script (DEBUG, INFO, WARNING, ERROR, CRITICAL) - defaults to INFO
- **--third_party_log_level**: Log level for third-party libraries (DEBUG, INFO, WARNING, ERROR, CRITICAL) - defaults to WARNING

## File Formats

### Keywords File
The keywords file should contain one keyword per line. Keywords can be single words or multi-word phrases:

```txt
AI
artificial intelligence
machine learning
technology
blockchain
cryptocurrency
digital transformation
cloud computing
```

**Rules:**
- Each line becomes a `Keyword` object
- Keywords can be single words or multi-word phrases
- Empty lines are ignored

### Sentences File
The sentences file should contain one sentence per line. These are used to create `Similarity` objects for search queries:

```txt
AI in healthcare applications
machine learning in financial services
tech startup funding trends
venture capital investment patterns
artificial intelligence ethics and regulation
```

**Rules:**
- Each line becomes a `Similarity` object
- Sentences should be descriptive search queries
- Empty lines are ignored

## Query Construction

The script constructs search queries using the following pattern:
```
Any(places) & Any(keywords) & sentence
```

Where:
- `places`: Entity IDs found in the knowledge graph for the specified country
- `keywords`: Single-word keywords from the CSV file
- `sentence`: Multi-word sentences from the CSV file

## Output Structure

The script creates the following directory structure:
```
news_data/
├── 2024-01-01/
│   ├── 2024-01-01_AI_breakthrough_in_healthcare.json
│   └── 2024-01-01_tech_startup_funding_news.json
├── 2024-01-02/
│   └── ...
└── ...
```

Each JSON file contains the complete annotated document from Bigdata.com including:
- Full document metadata and annotations
- Entity recognition and linking
- Sentiment analysis
- Relevance scores
- Source information
- Chunk-level details
- All Bigdata.com annotations and enrichments

## Performance Overview

The script provides a detailed performance overview:

```
PER-DATE PERFORMANCE OVERVIEW
================================================================================
Date         | Time (s)   | Documents
--------------------------------------------------------------------------------
2024-01-01   | 45.23      | 150
2024-01-02   | 42.15      | 145
2024-01-03   | 48.30      | 160
...
--------------------------------------------------------------------------------
TOTAL        | 135.68     | 455
================================================================================
Average time per date: 45.23 seconds
Average documents per date: 151.7
Total execution time: 135.68 seconds
```

## API Usage Monitoring

The script automatically:
1. Checks initial quota usage with `bigdata.subscription.get_details()`
2. Reports final quota usage at completion
3. Shows total script run usage (final - initial)

## Error Handling

- Comprehensive error handling for API failures
- Graceful degradation when country/place lookup fails
- Detailed logging for debugging
- Continues processing remaining dates if one fails
- Parallel document downloading with error isolation

## Logging

The script provides comprehensive logging with configurable levels for both your script and third-party libraries:

### Script Logging Levels
- **INFO** (default): General execution progress
- **DEBUG**: Detailed query and processing information
- **WARNING**: Non-critical issues
- **ERROR**: Critical failures
- **CRITICAL**: Severe errors that may cause script termination

## Performance Features

- **Parallel Processing**: Uses ThreadPoolExecutor for concurrent document downloading
- **Configurable Workers**: Default 20 workers for document downloads
- **Efficient Search**: Rerank threshold of 0.2 for better relevance
- **Chunk Management**: Processes up to 300 chunks per search
- **Error Isolation**: Individual document failures don't stop the process

## Notes

- Each search is limited to 300 chunks to control API usage
- Documents are saved with cleaned filenames (special characters removed)
- The script processes all sentences for each date in parallel using 20 worker threads
- All API calls respect Bigdata.com rate limiting and built-in throttling
- Third-party library logging is suppressed by default for cleaner output
- Logging is completely isolated to prevent duplicate messages
