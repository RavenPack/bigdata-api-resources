# News Analysis by Topics

This project provides a comprehensive news analysis system that identifies companies, finds relevant topics, and retrieves news documents with priority scoring based on sentiment and relevance.

## Overview

The system consists of three main scripts that work together to:
1. Find company IDs in the RavenPack Knowledge Graph
2. Identify top topics discussing those companies
3. Retrieve and analyze news documents with priority scoring

## Prerequisites

- Python 3.7+
- Required Python packages (install via pip):
  ```bash
  pip install -r requirements.txt
  ```
 
- Bigdata API key (set in `.env` file as `BIGDATA_API_KEY`)

## Setup

1. Clone or download this repository
2. Create a `.env` file in the project root with your API key:
   ```
   BIGDATA_API_KEY=your_api_key_here
   ```
3. Ensure you have the required input files (see Input Files section)

## Input Files

### `input/public_companies.csv`
A CSV file containing company names to search for in the RavenPack Knowledge Graph.

**Format:**
```csv
name, mic, ticker, isin, cusip, sedol
Apple Inc.,XNAS,AAPL,,,
Micron Technology Inc.,,,US5951121038,,
NVIDIA Corporation,,,,,2379504
Figma Inc.,,,,316841105,
```

## Workflow

The analysis follows a three-step sequential process:

### Step 1: Find Company IDs
```bash
python get_company_ids.py input/public_companies.csv
```

**What it does:**
- Searches the RavenPack Knowledge Graph for company entities matching the market identifiers in your input CSV
- Retrieves RavenPack IDs for each company
- Saves results to `output/company_ids.csv`

**Output:** `output/company_ids.csv`
- Contains company names and their corresponding RavenPack IDs
- Used as input for the next step

### Step 2: Identify Top Topics
```bash
python get_top_topic_ids.py output/company_ids.csv
```

**What it does:**
- Analyzes news from the last 24 hours mentioning the companies found in Step 1
- Identifies the most frequently discussed topics related to these companies
- Saves topic information to `output/top_topics_to_search.csv`

**Output:** `output/top_topics_to_search.csv`
- Contains topic IDs, names, and descriptions
- Uses pipe (`|`) delimiter format
- Used as input for the final step

### Step 3: Retrieve and Analyze News
```bash
python get_news.py output/company_ids.csv output/top_topics_to_search.csv
```

**What it does:**
- Searches for news documents from the last 24 hours
- Filters documents by companies and topics identified in previous steps
- Calculates priority scores based on sentiment and relevance
- Generates detailed analysis reports

**Output:**
- Console output with markdown-formatted tables
- Log file: `logs/news_search.log`
- Detailed analysis per company showing:
  - Topic averages (priority, sentiment, relevance)
  - Document counts
  - Summary statistics

## Priority Calculation

The system calculates news priority using the formula:
```
Priority = |Sentiment × Relevance|
```

Where:
- **Sentiment**: Sentiment score of the news chunk (-1 to 1)
- **Relevance**: Relevance score of the chunk (0 to 1)
- **Priority**: Absolute value to prioritize both positive and negative high-impact news

## Output Structure

```
news_by_topics/
├── input/
│   └── public_companies.csv          # Input: Company names
├── output/
│   ├── company_ids.csv               # Step 1 output: Company IDs
│   └── top_topics_to_search.csv      # Step 2 output: Topic data
├── logs/
│   ├── news_search.log              # Step 3 logs
│   └── top_topics_to_search.log     # Step 2 logs
├── get_company_ids.py               # Step 1 script
├── get_top_topic_ids.py             # Step 2 script
├── get_news.py                      # Step 3 script
├── utils.py                         # Shared utility functions
├── requirements.txt                  # Python dependencies
└── README.md                        # This file
```

## Example Output

The final analysis produces tables like this:

```
## Apple Inc. (ID: RP_12345)

| Topic Name | Avg Priority | Avg Sentiment | Avg Relevance | Count |
|------------|--------------|---------------|---------------|-------|
| Stock Gain | 0.0716 | 0.2994 | 0.1902 | 16 |
| Partnership | 0.0971 | 0.5783 | 0.1629 | 30 |
| Earnings | 0.0674 | 0.425 | 0.174 | 2 |

**Summary:** 3 topics found across 48 documents
```

## Configuration

### API Configuration
- **Base URL**: `https://api.bigdata.com/v1/search`
- **Authentication**: API key via `x-api-key` header
- **Rate Limiting**: Uses 20 concurrent workers for efficient processing

### Search Parameters
- **Time Range**: Last 24 hours (configurable in code)
- **Sources**: Predefined news sources (configurable)
- **Max Results**: 100 chunks per company (configurable)

## Troubleshooting

### Common Issues

1. **API Key Not Found**
   ```
   Error: BIGDATA_API_KEY not found in environment variables
   ```
   **Solution**: Ensure your `.env` file contains the correct API key

2. **No Companies Found**
   ```
   No companies found in the CSV file
   ```
   **Solution**: Check your input CSV format and ensure company names are in the first column

3. **No Topics Found**
   ```
   No topics found in the response
   ```
   **Solution**: Verify the companies have recent news coverage and check API connectivity

### Log Files
- Check `logs/` directory for detailed error information
- Logs include timestamps and detailed error messages
- Use log levels: INFO, WARNING, ERROR

## Advanced Usage

### Customizing Search Parameters
You can modify the following parameters in the scripts:

- **Time Range**: Change `get_timestamp_range()` function
- **Concurrent Workers**: Modify `max_workers=20` in `get_news.py`
- **Max Documents**: Adjust `max_chunks` in API payload
- **Sources**: Update source IDs in `get_top_topic_ids.py`

### Extending the Analysis
- Add new metrics to priority calculation
- Implement additional filtering criteria
- Export results to different formats (JSON, Excel, etc.)

## Support

For issues or questions:
1. Check the log files for detailed error messages
2. Verify API key and network connectivity
3. Ensure input files are properly formatted
4. Review the troubleshooting section above

## License

This project is for internal use. Please ensure compliance with Bigdata API terms of service.
