# 🚀 Crypto News Board

A Python-based news aggregator that searches for cryptocurrency news using the Bigdata.com Search service and displays results in a beautiful console interface.

## ✨ Features

- **🔍 Intelligent Source Discovery**: Automatically finds crypto-related news sources using Bigdata Knowledge Graph
- **🎯 Two-Tier Search Strategy**: Prioritizes premium sources, falls back to public sources when needed
- **⚡ Parallel Processing**: Uses ThreadPoolExecutor for concurrent searches across multiple queries
- **📊 Organized News Categories**: 5 main news types with 5 search queries each (25 total queries)
- **⏰ Flexible Time Periods**: Search across last 24 hours, 48 hours, or 7 days
- **📰 Beautiful Display**: Console output with emojis and structured formatting

## 📋 News Categories

The script includes 5 comprehensive news categories, each with 5 targeted search queries:

1. **Regulation & Government Policy**
   - Cryptocurrency regulation news
   - Crypto legislation government policy
   - Crypto ban or restriction country
   - Crypto SEC lawsuit news
   - Global crypto regulation updates

2. **Macro & Monetary Policy**
   - Crypto market impact interest rates
   - Cryptocurrency inflation hedge news
   - Crypto macroeconomic news
   - Crypto monetary policy impact
   - Digital assets safe haven demand

3. **Institutional Adoption & Partnerships**
   - Institutional adoption cryptocurrency
   - Crypto partnerships banks companies
   - Corporate adoption blockchain payments
   - Crypto custody institutional news
   - Financial institutions entering crypto

4. **Security Incidents & Hacks**
   - Cryptocurrency exchange hack latest
   - Crypto exploit DeFi bridge security
   - Crypto funds stolen attack
   - Crypto scam fraud rug pull
   - Digital asset cybersecurity incident

5. **Market & Ecosystem Developments**
   - Cryptocurrency network upgrade news
   - Crypto hard fork or token burn
   - Blockchain ecosystem adoption trends
   - Crypto scaling solutions news
   - Crypto emerging narratives 2025

## 🛠️ Installation

### Prerequisites

- Python 3.7 or higher
- Bigdata.com API key [Documentation] (https://docs.bigdata.com/sdk-reference/introduction#api-key-beta=)
- pip package manager

### Setup

1. **Clone or download the project files**
   ```bash
   git clone git@github.com:RavenPack/bigdata-api-resources.git
   cd how_to_guides/crypto_news_board
   ```

2. **Create a virtual environment (recommended)**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Set up environment variables**
   Create a `.env` file in the project root:
   ```bash
   BIGDATA_API_KEY=your_api_key_here
   ```

## 🚀 Usage

### Basic Usage

1. **Run the script**
   ```bash
   python crypto_news_board.py
   ```

2. **Select news type**
   Choose from the available news categories (1-5)

3. **Select time period**
   - 1: Last 24 hours
   - 2: Last 48 hours
   - 3: Last 7 days

4. **Wait for results**
   The script will perform parallel searches and display the news board

### Example Output

```
🚀 CRYPTO NEWS BOARD - REGULATION & GOVERNMENT POLICY 🚀
================================================================================
📅 Generated on: 2024-01-15 14:30:25
📊 Total news items: 15
================================================================================

📰 NEWS #01
------------------------------------------------------------
⏰ TIMESTAMP: 2024-01-15T14:25:00
📌 HEADLINE: SEC Approves New Crypto Regulations
📝 CONTENT:  The Securities and Exchange Commission has announced new regulations...
🏢 SOURCE:   Crypto Wire
⭐ RELEVANCE: 0.892
------------------------------------------------------------
```

## 🔧 Configuration

### Customizing Search Queries

Edit `crypto_news_search_queries.csv` to modify search queries:

```csv
news_type,sentence_to_search
Custom Category,custom search query here
Another Category,another search sentence
```

### Modifying Source Discovery

The script automatically discovers crypto sources, but you can modify the source discovery logic in the `discover_crypto_sources()` function.

## 📁 Project Structure

```
crypto_news_board/
├── crypto_news_board.py          # Main script
├── crypto_news_search_queries.csv # Search queries configuration
├── requirements.txt              # Python dependencies
├── .env                         # Environment variables (create this)
├── venv/                        # Virtual environment
└── README.md                    # This file
```

## 🔍 How It Works

### 1. Source Discovery
- Uses Bigdata Knowledge Graph to find sources with "Crypto" in the name
- Separates premium sources (D6D057) from public sources
- Creates source lists for targeted searching

### 2. Search Strategy
- **Primary Search**: Searches premium sources only
- **Fallback Search**: If <10 chunks found, searches public sources
- Uses negative source filtering to exclude premium sources in fallback

### 3. Parallel Processing
- Creates 10 worker threads for concurrent searches
- Each search query runs in parallel for maximum efficiency
- Results are collected and combined automatically

### 4. Result Processing
- Extracts relevant information from search results
- Sorts news by relevance score
- Formats output for display

## 🐛 Troubleshooting

### Common Issues

1. **ModuleNotFoundError: No module named 'bigdata_client'**
   - Ensure you've installed requirements: `pip install -r requirements.txt`
   - Check that you're in the correct virtual environment

2. **API Key Error**
   - Verify your `.env` file contains the correct `BIGDATA_API_KEY`
   - Ensure the API key is valid and has proper permissions

3. **No Results Found**
   - Check your internet connection
   - Verify the API key has access to the required sources
   - Try different time periods

### Debug Mode

Enable detailed logging by modifying the logging level in the script:

```python
logging.basicConfig(
    level=logging.DEBUG,  # Change from INFO to DEBUG
    format='%(asctime)s - %(levelname)s - %(message)s'
)
```

## 🤝 Contributing

Feel free to contribute to this project by:

1. Reporting bugs
2. Suggesting new features
3. Improving the search queries
4. Enhancing the display format
5. Optimizing performance

## 📄 License

This project is provided as-is for educational and research purposes.

## 🙏 Acknowledgments

- Bigdata.com for providing the search API
- Open source contributors for various Python libraries

## 📞 Support

Email us to support@bigdata.com

---

**Happy crypto news hunting! 🚀📰**
