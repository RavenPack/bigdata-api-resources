from bigdata_client import Bigdata
from bigdata_client.query import Similarity, Any
from bigdata_client.daterange import AbsoluteDateRange
from bigdata_client.search import ChunkLimit
from dotenv import load_dotenv
import os
import logging
from datetime import datetime, timedelta
from typing import List, Dict
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()

bigdata = Bigdata(api_key=os.getenv("BIGDATA_API_KEY"))

###########################
# Crypto premium source ID
###########################

# Crypto Wire (https://www.syndigate.info/crypto-wire/)
CRYPTO_WIRE = "D6D057"

@dataclass
class NewsItem:
    """Represents a news item for display"""
    timestamp: str
    headline: str
    chunk_text: str
    source_name: str
    relevance: float

def discover_crypto_sources():
    """Discovers crypto sources using the knowledge graph"""
    premium_sources = []
    public_sources = []

    try:
        # Use the knowledge graph to find sources with "Crypto" in the name
        all_crypto_sources = bigdata.knowledge_graph.find_sources("Crypto", limit=100)
        logger.info(f"Found {len(all_crypto_sources)} crypto sources")
        
        # Separate premium and public sources
        for source in all_crypto_sources:
            if hasattr(source, 'id'):
                source_id = source.id
                if source_id == CRYPTO_WIRE:
                    premium_sources.append(source)
                else:
                    public_sources.append(source)
        
        logger.info(f"Premium sources: {len(premium_sources)}, Public sources: {len(public_sources)}")
        
    except Exception as e:
        logger.error(f"Error discovering sources: {e}")
        print(f"❌ Error discovering sources: {e}")
        return [], []

    return premium_sources, public_sources

def load_search_queries(filename: str) -> Dict[str, List[str]]:
    """Loads search queries from a CSV file."""
    queries_by_type = {}
    try:
        with open(filename, 'r') as f:
            # Skip the header line
            next(f)
            
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                parts = line.split(',')
                if len(parts) >= 2:
                    news_type = parts[0].strip()
                    sentence = parts[1].strip()
                    
                    # Initialize list for this news type if it doesn't exist
                    if news_type not in queries_by_type:
                        queries_by_type[news_type] = []
                    
                    # Add the sentence to the list
                    if sentence:
                        queries_by_type[news_type].append(sentence)
                else:
                    logger.warning(f"Skipping line: {line} (invalid format)")
        
        logger.info(f"Loaded {sum(len(s) for s in queries_by_type.values())} search sentences from {filename}")
        return queries_by_type
    except FileNotFoundError:
        logger.error(f"Error: Search queries file '{filename}' not found.")
        print(f"❌ Error: Search queries file '{filename}' not found.")
        return {}
    except Exception as e:
        logger.error(f"Error loading search queries from {filename}: {e}")
        print(f"❌ Error loading search queries from {filename}: {e}")
        return {}

def get_date_range(period_choice: str) -> tuple[datetime, datetime]:
    """Gets the date range based on user selection."""
    now = datetime.now()
    if period_choice == "1":
        start_date = now - timedelta(hours=24)
        end_date = now
    elif period_choice == "2":
        start_date = now - timedelta(hours=48)
        end_date = now
    elif period_choice == "3":
        start_date = now - timedelta(days=7)
        end_date = now
    else:
        raise ValueError("Invalid period choice")
    return start_date, end_date

def search_worker(args):
    """Worker function for parallel search with two-tier strategy."""
    sentence, premium_sources, public_sources, start_date, end_date = args
    logger.info(f"🔍 Searching for: {sentence[:50]}...")
    
    all_documents = []

    query_text = Similarity(sentence)
    
    try:
        # First search: Premium sources only
        if premium_sources:
            logger.debug(f"  🔍 Searching premium sources for: {sentence[:50]}...")
    
            # Add premium source filter            
            query = query_text & Any(premium_sources)
            
            # Search with date range
            search_params = {
                "query": query,
                "date_range": AbsoluteDateRange(start_date, end_date),
                "rerank_threshold": 0.4
            }
            
            search = bigdata.search.new(**search_params)
            premium_documents = search.run(limit=ChunkLimit(100))
            
            # Count chunks from premium search
            premium_chunks = 0
            all_documents.extend(premium_documents)
            for doc in premium_documents:
                premium_chunks += len(doc.chunks)
                # Add chunks from clustered documents
                if doc.cluster and len(doc.cluster) > 0:
                    all_documents.extend(doc.cluster)
                    for clustered_doc in doc.cluster:
                        premium_chunks += len(clustered_doc.chunks)
            
            logger.debug(f"  ✅ Premium search found {premium_chunks} chunks")
            
            # Second search: Public sources if premium search returned less than 10 chunks
            if premium_chunks < 10 and public_sources:
                logger.debug("  🔍 Premium search returned <10 chunks, searching public sources for: {sentence[:50]}...")
                
                # Search in public sources             
                query = query_text & Any(public_sources)
                        
                # Search public sources
                public_search_params = {
                    "query": query,
                    "date_range": AbsoluteDateRange(start_date, end_date),
                    "rerank_threshold": 0.4
                }
                
                public_search = bigdata.search.new(**public_search_params)
                public_documents = public_search.run(limit=ChunkLimit(100))
            
                logger.debug(f"  ✅ Public search found {len(public_documents)} documents")
                all_documents.extend(public_documents)
                for doc in public_documents:
                    if doc.cluster and len(doc.cluster) > 0:
                        all_documents.extend(doc.cluster)      
    
        logger.info(f"✅ Total documents found for '{sentence[:50]}...': {len(all_documents)}")
        return all_documents
        
    except Exception as e:
        print(f"❌ Error searching for '{sentence[:50]}...': {e}")
        logger.error(f"Search error for '{sentence}': {e}")
        return []

def display_news_board(news_items: List[NewsItem], news_type: str):
    """Display news in a retro-style news board format"""
    print("\n" + "="*80)
    print(f"🚀 CRYPTO NEWS BOARD - {news_type.upper()} 🚀")
    print("="*80)
    print(f"📅 Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📊 Total news items: {len(news_items)}")
    print("="*80)
    
    if not news_items:
        logger.error("❌ No news found for the selected criteria.")
        return
    
    # Sort news items by relevance (highest first)
    sorted_news = sorted(news_items, key=lambda x: x.relevance, reverse=True)
    
    for i, item in enumerate(sorted_news, 1):
        print(f"\n📰 NEWS #{i:02d}")
        print("-" * 60)
        print(f"⏰ TIMESTAMP: {item.timestamp}")
        print(f"📌 HEADLINE: {item.headline}")
        print(f"📝 CONTENT:  {item.chunk_text[:200]}{'...' if len(item.chunk_text) > 200 else ''}")
        print(f"🏢 SOURCE:   {item.source_name}")
        print(f"⭐ RELEVANCE: {item.relevance:.3f}")
        print("-" * 60)
    
    print("\n" + "="*80)
    print("🎯 End of News Board 🎯")
    print("="*80)

def convert_documents_to_news_items(documents: List[Any]) -> List[NewsItem]:
    """Convert documents to news items for display"""
    news_items = []
    
    for doc in documents:
        try:
            # Extract timestamp
            timestamp = getattr(doc, 'timestamp', '')
            if hasattr(timestamp, 'isoformat'):
                timestamp_str = timestamp.isoformat()
            else:
                timestamp_str = str(timestamp)
            
            # Extract headline
            headline = getattr(doc, 'headline', 'No headline')
            
            # Extract source name
            source_name = 'Unknown Source'
            if hasattr(doc, 'source') and hasattr(doc.source, 'name'):
                source_name = doc.source.name
            
            # Extract text and relevance from chunks
            chunk_text = 'No content available'
            relevance = 0.0
            
            if hasattr(doc, 'chunks') and doc.chunks:
                # Get the chunk with highest relevance
                best_chunk = max(doc.chunks, key=lambda x: getattr(x, 'relevance', 0.0))
                chunk_text = getattr(best_chunk, 'text', 'No content available')
                relevance = getattr(best_chunk, 'relevance', 0.0)
            
            news_item = NewsItem(
                timestamp=timestamp_str,
                headline=headline,
                chunk_text=chunk_text,
                source_name=source_name,
                relevance=relevance
            )
            news_items.append(news_item)
            
        except Exception as e:
            logger.error(f"Error processing document: {e}")
            continue
    
    return news_items

###########################
# Main Application
###########################

def main():
    """Main application function"""
    logger.info("🔍 CRYPTO NEWS BOARD SEARCHER 🔍")
    print("="*50)
    
    # Discover crypto sources
    premium_sources, public_sources = discover_crypto_sources()
    
    if not premium_sources:
        logger.error("❌ Error: Could not discover premium crypto sources")
        return
    
    # Load search queries
    queries_by_type = load_search_queries('crypto_news_search_queries.csv')
    
    if not queries_by_type:
        logger.error("❌ Error: Could not load search queries")
        return
    
    # Display available news types
    logger.info(" 📋 Available News Types:")
    news_types = list(queries_by_type.keys())
    for i, news_type in enumerate(news_types, 1):
        logger.info(f"  {i}. {news_type}")
    
    # Get user selection for news type
    while True:
        try:
            choice = input(f"\n🎯 Select news type (1-{len(news_types)}): ").strip()
            news_type_idx = int(choice) - 1
            
            if 0 <= news_type_idx < len(news_types):
                selected_news_type = news_types[news_type_idx]
                break
            else:
                logger.error("❌ Invalid choice. Please select a valid number.")
        except ValueError:
            logger.error("❌ Please enter a valid number.")
    
    logger.info(f"✅ Selected: {selected_news_type}")
    
    # Get sentences for the selected news type
    sentences = queries_by_type[selected_news_type]
    logger.info(f"🔍 Found {len(sentences)} search sentences for this news type")
    
    # Display period options
    logger.info("⏰ Select time period:")
    logger.info("  1. Last 24 hours")
    logger.info("  2. Last 48 hours")
    logger.info("  3. Last 7 days")
    
    # Get user selection for time period
    while True:
        try:
            period_choice = input("\n⏰ Select time period (1-3): ").strip()
            if period_choice in ["1", "2", "3"]:
                break
            else:
                logger.error("❌ Invalid choice. Please select 1, 2, or 3.")
        except ValueError:
            logger.error("❌ Please enter a valid number.")
    
    # Get date range
    start_date, end_date = get_date_range(period_choice)
    logger.info(f"📅 Searching from {start_date} to {end_date}")
    
    # Perform parallel search
    logger.info(f"🚀 Starting parallel search with {len(sentences)} queries...")
    
    # Prepare arguments for workers
    search_args = [
        (sentence, premium_sources, public_sources, start_date, end_date)
        for sentence in sentences
    ]
    
    all_documents = []
    
    # Use ThreadPoolExecutor for parallel search
    with ThreadPoolExecutor(max_workers=10) as executor:
        # Submit all search tasks
        future_to_sentence = {
            executor.submit(search_worker, args): args[0] 
            for args in search_args
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_sentence):
            sentence = future_to_sentence[future]
            try:
                documents = future.result()
                all_documents.extend(documents)
                logger.debug(f"✅ Completed search for: {sentence[:50]}...")
            except Exception as e:
                logger.error(f"❌ Error searching for '{sentence[:50]}...': {e}")
    
    logger.info(f"🎉 Search completed! Found {len(all_documents)} total documents")
    
    # Convert documents to news items
    news_items = convert_documents_to_news_items(all_documents)
    
    # Display the news board
    display_news_board(news_items, selected_news_type)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("⏹️  Search interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        logger.error(f"❌ An unexpected error occurred: {e}")