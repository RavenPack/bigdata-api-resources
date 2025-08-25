#!/usr/bin/env python3
"""
Company Daily News Script for Bigdata.com

This script authenticates to Bigdata.com APIs, retrieves watchlist items,
and searches for daily news in period for each company using a threadpool.
"""

import os
import sys
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple
from dotenv import load_dotenv

# Bigdata client imports
from bigdata_client import Bigdata
from bigdata_client.query import Entity
from bigdata_client.models.search import DocumentType
from bigdata_client.daterange import AbsoluteDateRange
from bigdata_client.search import ChunkLimit

# Load environment variables from .env file
load_dotenv()

# Configure logging
def setup_logging(log_level: str = "INFO"):
    """Setup logging configuration with the specified log level.
    
    Args:
        log_level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    # Convert string to logging level
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f'Invalid log level: {log_level}')
    
    # Configure logging format
    logging.basicConfig(
        level=numeric_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Create logger for this module
    logger = logging.getLogger(__name__)
    return logger

# Setup default logging
logger = setup_logging()


def authenticate_bigdata():
    """Authenticate to Bigdata.com and return the client instance.
    
    Returns:
        Authenticated Bigdata client instance
        
    Raises:
        SystemExit: If authentication fails or credentials are missing
    """
    # Get credentials from environment variables
    username = os.getenv('BIGDATA_USERNAME')
    password = os.getenv('BIGDATA_PASSWORD')
    
    if not username or not password:
        logger.error("BIGDATA_USERNAME and BIGDATA_PASSWORD must be set in environment variables")
        logger.error("Please create a .env file with these variables or set them in your shell")
        sys.exit(1)
    
    # Initialize Bigdata client
    try:
        client = Bigdata(username=username, password=password)
        logger.info(f"Successfully authenticated to Bigdata.com with username: {username}")
        return client
    except Exception as e:
        logger.error(f"Error authenticating to Bigdata.com: {e}")
        sys.exit(1)


class CompanyDailyNewsCollector:
    """Collects news for companies in a watchlist using Bigdata.com APIs for a specific date."""
    
    def __init__(self, watchlist_id: str, bigdata_client):
        """Initialize the collector with a watchlist ID and Bigdata client.
        
        Args:
            watchlist_id: The watchlist ID to process
            bigdata_client: Authenticated Bigdata client instance
        """
        # Store watchlist ID and client
        self.watchlist_id = watchlist_id
        self.client = bigdata_client
        
        # Get watchlist items once
        self.watchlist_items = self._get_watchlist_items(watchlist_id)
    
    def _get_watchlist_items(self, watchlist_id: str) -> List[str]:
        """Retrieve all items from a watchlist.
        
        Args:
            watchlist_id: The watchlist ID to retrieve items from
            
        Returns:
            List of RavenPack entity IDs as strings
        """
        try:
            logger.info(f"Retrieving watchlist with ID: {watchlist_id}")
            watchlist = self.client.watchlists.get(watchlist_id)
            
            if not watchlist or not hasattr(watchlist, 'items'):
                logger.warning(f"Watchlist {watchlist_id} not found or has no items")
                return []
            
            items = watchlist.items
            logger.info(f"Found {len(items)} items in watchlist")
            return items
            
        except Exception as e:
            logger.error(f"Error retrieving watchlist {watchlist_id}: {e}")
            return []
    
    def _get_today_date(self) -> str:
        """Get today's date in YYYY-MM-DD format."""
        from datetime import datetime
        return datetime.now().strftime('%Y-%m-%d')
    
    def _create_date_range(self, date: str) -> AbsoluteDateRange:
        """Create an AbsoluteDateRange for the specified date.
        
        Args:
            date: Date in YYYY-MM-DD format
            
        Returns:
            AbsoluteDateRange object for the specified date
        """
        start_time = f"{date}T00:00:00"
        end_time = f"{date}T23:59:59"
        return AbsoluteDateRange(start_time, end_time)
    
    def get_watchlist_items(self, watchlist_id: str) -> List[str]:
        """Retrieve all items from a watchlist."""
        try:
            logger.debug(f"Retrieving watchlist with ID: {watchlist_id}")
            watchlist = self.client.watchlists.get(watchlist_id)
            
            if not watchlist or not hasattr(watchlist, 'items'):
                logger.warning(f"Watchlist {watchlist_id} not found or has no items")
                return []
            
            items = watchlist.items
            logger.info(f"Found {len(items)} items in watchlist")
            return items
            
        except Exception as e:
            logger.error(f"Error retrieving watchlist {watchlist_id}: {e}")
            return []
    
    def search_company_news(self, rp_entity_id: str, date: str) -> Tuple[str, Dict]:
        """Search for news about a specific company for a specific date.
        
        Args:
            rp_entity_id: RavenPack entity ID for the company
            date: Date in YYYY-MM-DD format for the search
            
        Returns:
            Tuple of (rp_entity_id, result_dict)
        """
        start_time = time.time()
        
        try:            
            logger.debug(f"Searching news for entity: {rp_entity_id} on date: {date}")
            
            # Perform search with chunk limit of 100
            date_range = self._create_date_range(date)
            search = self.client.search.new(query=Entity(rp_entity_id), scope=DocumentType.NEWS, date_range=date_range)
            documents = search.run(limit=ChunkLimit(100))
            
            # Extract unique document IDs
            unique_docs = set()
            for doc in documents:
                unique_docs.add(doc.id)
                if doc.cluster:
                    for clustered_docs in doc.cluster:
                        unique_docs.add(clustered_docs.id)
            
            query_time = time.time() - start_time

            # Search usage
            search_usage = search.get_usage()
            
            result = {
                'rp_entity_id': rp_entity_id,
                'date': date,
                'unique_documents': len(unique_docs),
                'search_usage': search_usage,
                'query_time': query_time,
                'document_ids': list(unique_docs),
                'documents': documents
            }
            
            logger.info(f"Unique documents found on {date} for {rp_entity_id}: {len(unique_docs)}, search usage: {search_usage}")
            return rp_entity_id, result
            
        except Exception as e:
            query_time = time.time() - start_time
            logger.error(f"Error searching news for entity {rp_entity_id} on {date}: {e}")
            
            result = {
                'rp_entity_id': rp_entity_id,
                'date': date,
                'unique_documents': 0,
                'query_time': query_time,
                'document_ids': [],
                'documents': [],
                'error': str(e)
            }
            
            return rp_entity_id, result
    
    def collect_news_for_watchlist(self, date: str, max_workers: int = 10) -> Dict:
        """Collect news for all companies in a watchlist for a specific date using threadpool.
        
        Args:
            date: Date in YYYY-MM-DD format for the search
            max_workers: Maximum number of worker threads
            
        Returns:
            Dictionary containing the search results and statistics
        """
        logger.info(f"START NEWS Collection on date: {date}")
        logger.info(f"Using threadpool with {max_workers} workers")
        
        # Use stored watchlist items
        if not self.watchlist_items:
            logger.warning("No items found in watchlist")
            return {
                'watchlist_id': self.watchlist_id,
                'date': date,
                'total_entities': 0,
                'total_documents': 0,
                'entities': {},
                'errors': ['No items found in watchlist']
            }
        
        # Collect news using threadpool
        results_per_entity = {}
        errors = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all search tasks
            future_to_entity = {
                executor.submit(self.search_company_news, item, date): item 
                for item in self.watchlist_items
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_entity):
                rp_entity_id, result = future.result()
                results_per_entity[rp_entity_id] = result
                
                if 'error' in result:
                    errors.append(f"{rp_entity_id}: {result['error']}")
        
        # Calculate statistics
        total_documents = sum(result['unique_documents'] for result in results_per_entity.values())
        total_search_usage = sum(result.get('search_usage', 0) for result in results_per_entity.values())
        
        # Create summary
        news_collection_result = {
            'watchlist_id': self.watchlist_id,
            'date': date,
            'total_entities': len(results_per_entity),
            'total_documents': total_documents,
            'total_search_usage': total_search_usage,
            'results_per_entity': results_per_entity,
            'errors': errors
        }
        
        logger.info(f"END NEWS Collection on date {date}: {total_documents} documents from {len(results_per_entity)} entities, search usage: {total_search_usage}")
        return news_collection_result
    
    def _save_news_to_csv(self, news_collection_result: Dict, date: str):
        """Save news data to CSV files organized by date and entity.
        
        Args:
            news_collection_result: The news collection result containing entity results
            date: The date for which news was collected
        """
        import csv
        import os
        
        try:
            # Create base output directory
            output_dir = "news_data"
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            
            # Create date-specific folder
            date_folder = os.path.join(output_dir, date)
            if not os.path.exists(date_folder):
                os.makedirs(date_folder)
                logger.info(f"Created folder: {date_folder}")
            
            results_per_entity = news_collection_result.get('results_per_entity', {})
            total_files_created = 0
            
            for entity_id, entity_result in results_per_entity.items():
                if 'error' in entity_result:
                    logger.warning(f"Skipping entity {entity_id} due to error: {entity_result['error']}")
                    continue
                
                documents = entity_result.get('documents', [])
                if not documents:
                    logger.debug(f"No documents for entity {entity_id} on {date}")
                    continue
                
                # Create CSV filename: entity_id_date.csv
                csv_filename = f"{entity_id}_{date}.csv"
                csv_path = os.path.join(date_folder, csv_filename)
                
                # CSV columns as specified
                csv_columns = [
                    'date', 'entity_id', 'source_key', 'source_name', 
                    'document_id', 'document_headline', 'chunk_relevant', 
                    'chunk_sentiment', 'chunk_text'
                ]
                
                with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
                    writer.writeheader()
                    
                    rows_written = 0
                    for doc in documents:
                        try:
                            # Extract document information
                            doc_id = getattr(doc, 'id', 'unknown')
                            headline = getattr(doc, 'headline', 'No headline')
                            
                            # Extract source information
                            source_key = doc.source.key
                            source_name = doc.source.name
                            
                            # Get chunks from document
                            chunks = getattr(doc, 'chunks', [])
                            
                            if not chunks:
                                # If no chunks, create one row with document-level info
                                chunk_text = getattr(doc, 'text', 'No text available')
                                chunk_relevant = getattr(doc, 'relevance', 'unknown')
                                chunk_sentiment = getattr(doc, 'sentiment', 'unknown')
                                
                                row = {
                                    'date': date,
                                    'entity_id': entity_id,
                                    'source_key': source_key,
                                    'source_name': source_name,
                                    'document_id': doc_id,
                                    'document_headline': headline,
                                    'chunk_relevant': chunk_relevant,
                                    'chunk_sentiment': chunk_sentiment,
                                    'chunk_text': chunk_text
                                }
                                writer.writerow(row)
                                rows_written += 1
                            else:
                                # Process each chunk separately
                                for chunk in chunks:
                                    try:
                                        # Extract chunk information
                                        chunk_text = getattr(chunk, 'text', 'No text available')
                                        chunk_relevant = getattr(chunk, 'relevance', 'unknown')
                                        chunk_sentiment = getattr(chunk, 'sentiment', 'unknown')
                                        
                                        # Write row to CSV for this chunk
                                        row = {
                                            'date': date,
                                            'entity_id': entity_id,
                                            'source_key': source_key,
                                            'source_name': source_name,
                                            'document_id': doc_id,
                                            'document_headline': headline,
                                            'chunk_relevant': chunk_relevant,
                                            'chunk_sentiment': chunk_sentiment,
                                            'chunk_text': chunk_text
                                        }
                                        writer.writerow(row)
                                        rows_written += 1
                                        
                                    except Exception as e:
                                        logger.warning(f"Error processing chunk in document {doc_id} for entity {entity_id}: {e}")
                                        continue
                            
                        except Exception as e:
                            logger.warning(f"Error processing document {getattr(doc, 'id', 'unknown')} for entity {entity_id}: {e}")
                            continue
                    
                    if rows_written > 0:
                        total_files_created += 1
                        logger.info(f"Created CSV file: {csv_filename} with {rows_written} rows")
                    else:
                        # Remove empty CSV file
                        os.remove(csv_path)
                        logger.debug(f"Removed empty CSV file: {csv_filename}")
            
            logger.info(f"CSV export completed for date {date}: {total_files_created} files created in {date_folder}")
            
        except Exception as e:
            logger.error(f"Error saving news data to CSV for date {date}: {e}")
            import traceback
            traceback.print_exc()

def main():
    """Main function to run the news collection script."""
    if len(sys.argv) < 2 or len(sys.argv) > 5:
        print("Usage: python company_daily_news.py <watchlist_id> [start_date] [end_date] [log_level]")
        print("Example: python company_daily_news.py 'watchlist_123'")
        print("Example: python company_daily_news.py 'watchlist_123' '2024-01-01'")
        print("Example: python company_daily_news.py 'watchlist_123' '2024-01-01' '2024-01-31'")
        print("Example: python company_daily_news.py 'watchlist_123' '2024-01-01' '2024-01-31' 'DEBUG'")
        print("\nLog levels: DEBUG, INFO, WARNING, ERROR, CRITICAL")
        sys.exit(1)
    
    watchlist_id = sys.argv[1]
    start_date = sys.argv[2] if len(sys.argv) >= 3 else None
    end_date = sys.argv[3] if len(sys.argv) >= 4 else None
    log_level = sys.argv[4] if len(sys.argv) == 5 else "INFO"
    
    # Setup logging with specified level
    try:
        global logger
        logger = setup_logging(log_level)
        logger.info(f"Logging level set to: {log_level}")
    except ValueError as e:
        print(f"Error: {e}")
        print("Valid log levels: DEBUG, INFO, WARNING, ERROR, CRITICAL")
        sys.exit(1)
    
    try:
        # If only start_date is provided, use it as single date
        if start_date and not end_date:
            dates = [start_date]
        # If both dates are provided, generate date range
        elif start_date and end_date:
            dates = generate_date_range(start_date, end_date)
            print(f"Generated date range: {start_date} to {end_date}")
            print(f"Number of dates to process: {len(dates)}")
        # If no dates provided, use today
        else:
            from datetime import datetime
            dates = [datetime.now().strftime('%Y-%m-%d')]
        
        logger.info(f"Processing {len(dates)} date(s) for watchlist: {watchlist_id}")
        logger.info(f"Date range: {dates[0]} to {dates[-1]}")
        logger.debug(f"All dates to process: {dates}")
        
        # Track overall statistics
        overall_start_time = time.time()
        daily_results = {}
        total_documents = 0
        total_search_usage = 0
        
        # Authenticate to Bigdata.com
        logger.info("Authenticating to Bigdata.com...")
        bigdata_client = authenticate_bigdata()
        
        # Initialize the collector once with the watchlist ID and client
        logger.info(f"Initializing collector for watchlist: {watchlist_id}")
        collector = CompanyDailyNewsCollector(watchlist_id=watchlist_id, bigdata_client=bigdata_client)
        
        # Process each date
        for i, date in enumerate(dates, 1):
            try:
                logger.info(f"Processing date {i} of {len(dates)}: {date}")
                logger.info("="*60)
                
                date_start_time = time.time()
                
                # Collect news for the watchlist for this specific date
                logger.debug(f"Collecting news on date: {date}")
                news_collection_result = collector.collect_news_for_watchlist(date)
                
                # Calculate date-specific statistics
                date_time = time.time() - date_start_time
                date_documents = news_collection_result.get('total_documents', 0)
                date_entities = news_collection_result.get('total_entities', 0)
                date_search_usage = news_collection_result.get('total_search_usage', 0)
                
                # Calculate entities with documents (entities that have at least one document)
                entities_with_documents = 0
                results_per_entity = news_collection_result.get('results_per_entity', {})
                for entity_result in results_per_entity.values():
                    if entity_result.get('unique_documents', 0) > 0:
                        entities_with_documents += 1
                
                # Calculate percentage of entities with documents
                percentage_entities_with_docs = (entities_with_documents / date_entities * 100) if date_entities > 0 else 0
                
                # Store daily results
                daily_results[date] = {
                    'news_collection_result': news_collection_result,
                    'processing_time': date_time,
                    'total_documents': date_documents,
                    'total_entities': date_entities,
                    'entities_with_documents': entities_with_documents,
                    'percentage_entities_with_docs': percentage_entities_with_docs,
                    'search_usage': date_search_usage
                }

                ## TODO: Here you can process the documents within the news_collection_result['results_per_entity']
                # Create a folder per date and a CSV file per entity with the following columns: date, entity_id, source_key, source_name, document_id, document_headline, chunk_text, chunk_relevant, chunk_sentiment  
                # The CSV file should be named as the entity_id_date.csv
                
                # Save news data to CSV files organized by date and entity
                collector._save_news_to_csv(news_collection_result, date)
                
                # Update overall totals
                total_documents += date_documents
                total_search_usage += date_search_usage
                
                # Log daily statistics
                logger.info(f"Daily Summary for {date}:")
                logger.info(f"  Processing Time: {date_time:.2f} seconds")
                logger.info(f"  Total Documents: {date_documents}")
                logger.info(f"  Total Entities: {date_entities}")
                logger.info(f"  Entities with Documents: {entities_with_documents} ({percentage_entities_with_docs:.1f}%)")
                logger.info(f"  Search Usage: {date_search_usage}")
                
                
                logger.info(f"Successfully completed processing for date: {date}")
                logger.info("="*60)
                
            except Exception as e:
                logger.error(f"Error processing date {date}: {e}")
                import traceback
                traceback.print_exc()
                # Store error result for this date
                daily_results[date] = {
                    'summary': {'error': str(e)},
                    'total_documents': 0,
                    'total_entities': 0,
                    'entities_with_documents': 0,
                    'percentage_entities_with_docs': 0.0,
                    'search_usage': 0
                }
                continue
        
        logger.info(f"Completed processing all {len(dates)} dates")
        
        # Calculate and print overall statistics
        overall_time = time.time() - overall_start_time
        
        logger.info("="*60)
        logger.info("OVERALL EXECUTION STATISTICS")
        logger.info("="*60)
        logger.info(f"Watchlist ID: {watchlist_id}")
        logger.info(f"Total Dates Processed: {len(dates)}")
        logger.info(f"Total Documents Retrieved: {total_documents}")
        logger.info(f"Total Search Usage: {total_search_usage}")
        logger.info(f"Total Execution Time: {overall_time:.2f} seconds")
        logger.info(f"Average Time per Date: {overall_time / len(dates):.2f} seconds")
        logger.info(f"Average Search Usage per Date: {total_search_usage / len(dates):.1f}")
        
        # Print per-date performance breakdown
        logger.info("Per-Date Performance:")
        logger.info("-" * 60)
        logger.info(f"{'Date':<12} | {'Time (s)':<10} | {'Documents':<10} | {'% Entities':<10} | {'Usage (API Query Units)':<10}")
        logger.info("-" * 60)
        
        for date, result in daily_results.items():
            search_usage = result.get('search_usage', 0)
            percentage_entities = result.get('percentage_entities_with_docs', 0)
            logger.info(f"{date:<12} | {result['processing_time']:<10.2f} | {result['total_documents']:<10} | {percentage_entities:<10.1f}% | {search_usage:<10}")
        
        logger.info("-" * 60)
        logger.info(f"Average documents per date: {total_documents / len(dates):.1f}")
        logger.info(f"Average search usage per date: {total_search_usage / len(dates):.1f}")
        
    except KeyboardInterrupt:
        logger.warning("Script interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


def generate_date_range(start_date: str, end_date: str) -> List[str]:
    """Generate a list of dates between start_date and end_date (inclusive).
    
    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        
    Returns:
        List of date strings in YYYY-MM-DD format
    """
    from datetime import datetime, timedelta
    
    try:
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        if start > end:
            raise ValueError("Start date must be before or equal to end date")
        
        dates = []
        current = start
        while current <= end:
            dates.append(current.strftime('%Y-%m-%d'))
            current += timedelta(days=1)
        
        return dates
        
    except ValueError as e:
        logger.error(f"Error parsing dates: {e}")
        logger.error("Please use YYYY-MM-DD format (e.g., '2024-01-01')")
        sys.exit(1)


if __name__ == "__main__":
    main()
