# Company Daily News Script for Bigdata.com

This Python script authenticates to Bigdata.com APIs, retrieves watchlist items, and searches for news for a specific date for each company using a threadpool.

## Prerequisites

- Python 3.7+
- Bigdata.com account with API access
- `bigdata-client` Python SDK (includes `Entity` class for queries)

## Installation

1. **Clone or download the public repository**
   ```bash
   git clone https://github.com/RavenPack/bigdata-api-resources.git 
   ```

2. **Install required packages:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up environment variables:**
   
   Create a `.env` file in the same directory as the script with your Bigdata.com credentials:
   ```bash
   BIGDATA_USERNAME=your_username_here
   BIGDATA_PASSWORD=your_password_here
   ```
   
   **Note**: Never commit your `.env` file to version control. It's already in `.gitignore`.

## Usage

Run the script with a watchlist ID and optionally start and end dates:

```bash
python company_daily_news.py <watchlist_id> [start_date] [end_date] [log_level]
```

for example, this is a public available watchlist_id: 
- `e84e8897-3d10-4bcc-a178-5a282115b000`

### Examples:
```bash
# Search for today's news
python company_daily_news.py "watchlist_123"

# Search for a specific date
python company_daily_news.py "watchlist_123" "2024-01-15"

# Search for a date range
python company_daily_news.py "watchlist_123" "2024-01-01" "2024-01-31"

# Search with custom log level
python company_daily_news.py "watchlist_123" "2024-01-01" "2024-01-31" "DEBUG"
```

### Log Levels:
- **DEBUG**: Detailed information for debugging
- **INFO**: General information about program execution (default)
- **WARNING**: Warning messages for potential issues
- **ERROR**: Error messages for problems that occurred
- **CRITICAL**: Critical errors that may prevent program execution

## How It Works

1. **Authentication**: The script loads credentials from environment variables and authenticates to Bigdata.com in the main method
2. **Date Processing**: Accepts start and end dates, processes single date, or defaults to today
3. **Client Injection**: Passes the authenticated Bigdata client to the CompanyDailyNewsCollector
4. **Single Collector Initialization**: Creates one CompanyDailyNewsCollector instance with watchlist ID and client
5. **One-Time Watchlist Retrieval**: Gets all RavenPack entity IDs once during initialization
6. **Date Iteration**: For each date, passes the date directly to the news collection method
7. **Concurrent Processing**: Uses a threadpool with 10 workers to process entities concurrently
8. **Batch Execution**: Processes multiple dates sequentially, collecting comprehensive statistics
9. **Performance Tracking**: Measures execution time per date and overall performance metrics

## Output

The script provides detailed output including:

- **Overall Statistics**: Total dates processed, entities, documents, and execution time
- **Per-Entity Results**: Number of unique news documents and query time for each entity
- **Performance Metrics**: Average time per date, documents per date, and entities per date
- **Error Reporting**: Any issues encountered during processing

## Configuration

### Threadpool Workers
The default number of concurrent workers is 10. You can modify this in the `collect_news_for_watchlist` method call:

```python
summary = collector.collect_news_for_watchlist(watchlist_id, max_workers=10)
```

### Chunk Limit
The search chunk limit is set to 100 as specified. This can be modified in the `search_company_news` method.

### Date Range
The script searches for news for a specific date (YYYY-MM-DD format). If no date is provided, it defaults to today. You can modify the date range logic in the `_create_date_range` method.

## Error Handling

The script includes comprehensive error handling:

- **Authentication Errors**: Clear messages if credentials are invalid
- **API Errors**: Graceful handling of API failures with detailed error messages
- **Network Issues**: Timeout and connection error handling
- **Data Validation**: Checks for missing or malformed responses

## Troubleshooting

### Common Issues

1. **Import Error**: Make sure you have installed `bigdata-client`:
   ```bash
   pip install bigdata-client
   ```

2. **Authentication Failed**: Verify your credentials in the `.env` file

3. **Watchlist Not Found**: Check that the watchlist ID is correct and accessible

4. **No Results**: Ensure the watchlist contains entity IDs and there is recent news available

### Debug Mode

For additional debugging, you can add logging statements or modify the print statements in the script.

## Security Notes

- **Credentials**: Never hardcode credentials in the script
- **Environment Variables**: Use `.env` files for local development
- **API Keys**: Rotate credentials regularly and monitor API usage
- **Access Control**: Ensure only authorized users have access to the script

## Support

For issues related to:
- **Bigdata.com API**: Contact Bigdata.com support
- **Script Functionality**: Check the error messages and troubleshoot common issues
- **Authentication**: Verify your Bigdata.com account and API access

## License

This script is provided as-is for educational and development purposes. Please ensure compliance with Bigdata.com's terms of service and API usage policies.
