#!/usr/bin/env python3
"""
Retrieve Entire Articles Script for Bigdata.com

This script authenticates to Bigdata.com APIs, processes separate text files for keywords and sentences,
explores the knowledge graph for places, and performs parallel searches to retrieve news articles.
"""

import os
import sys
import time
import json
import logging
import argparse
from typing import Dict, List, Tuple
from dotenv import load_dotenv
from datetime import datetime, timedelta
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

# Bigdata client imports
from bigdata_client import Bigdata
from bigdata_client.query import Entity, Keyword, Similarity, Any
from bigdata_client.models.search import DocumentType
from bigdata_client.daterange import AbsoluteDateRange
from bigdata_client.search import ChunkLimit

# Load environment variables from .env file
load_dotenv()

# Configure logging
def setup_logging(log_level: str = "INFO", third_party_log_level: str = "WARNING"):
    """Setup logging configuration with the specified log levels.
    
    Args:
        log_level: Log level for this script (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        third_party_log_level: Log level for third-party libraries (default: WARNING)
    """
    # Convert string to logging level
    numeric_level = getattr(logging, log_level.upper(), None)
    third_party_numeric_level = getattr(logging, third_party_log_level.upper(), None)
    
    if not isinstance(numeric_level, int):
        raise ValueError(f'Invalid log level: {log_level}')
    if not isinstance(third_party_numeric_level, int):
        raise ValueError(f'Invalid third-party log level: {third_party_log_level}')
    
    # Clear any existing handlers to avoid duplicates
    logging.getLogger().handlers.clear()
    
    # Set root logger to a very high level to suppress all third-party logs
    logging.getLogger().setLevel(logging.WARNING)
    
    # Create logger specifically for this script
    logger = logging.getLogger(__name__)
    logger.setLevel(numeric_level)
    
    # Prevent propagation to root logger to avoid duplicates
    logger.propagate = False
    
    # Create a console handler for this logger only
    console_handler = logging.StreamHandler()
    console_handler.setLevel(numeric_level)
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(formatter)
    
    # Add handler to logger
    logger.addHandler(console_handler)
    
    # Aggressively suppress third-party loggers
    third_party_loggers = [
        'urllib3', 'requests', 'bigdata_client', 'httpx', 'asyncio', 
        'aiohttp', 'websockets', 'concurrent.futures', 'threading',
        'urllib3.connectionpool', 'urllib3.util.retry', 'requests.packages.urllib3',
        'botocore', 'boto3', 's3transfer', 'jmespath', 'charset_normalizer',
        'certifi', 'idna', 'chardet', 'PIL', 'Pillow', 'matplotlib',
        'numpy', 'pandas', 'openpyxl', 'xlrd', 'xlwt', 'dotenv',
        'concurrent', 'multiprocessing', 'subprocess', 'signal', 'select',
        'socket', 'ssl', 'http', 'email', 'xml', 'json', 'csv', 'zipfile',
        'tarfile', 'gzip', 'bz2', 'lzma', 'pathlib', 'shutil', 'tempfile'
    ]
    
    for logger_name in third_party_loggers:
        try:
            third_party_logger = logging.getLogger(logger_name)
            third_party_logger.setLevel(logging.CRITICAL)
            third_party_logger.propagate = False
            # Remove any existing handlers
            third_party_logger.handlers.clear()
        except:
            pass
    
    return logger

# Initialize logger variable (will be set up in main function)
logger = None


def authenticate_bigdata():
    """Authenticate to Bigdata.com and return the client instance.
    
    Returns:
        Authenticated Bigdata client instance
        
    Raises:
        SystemExit: If authentication fails or credentials are missing
    """
    # Get credentials from environment variables
    api_key = os.getenv('BIGDATA_API_KEY')
    
    if not api_key:
        if logger:
            logger.error("BIGDATA_API_KEY must be set in environment variables")
        else:
            print("ERROR: BIGDATA_API_KEY must be set in environment variables")
        sys.exit(1)
    
    # Initialize Bigdata client
    try:
        client = Bigdata(api_key=api_key)
        if logger:
            logger.info("Successfully authenticated to Bigdata.com")
        else:
            print("INFO: Successfully authenticated to Bigdata.com")
        return client
    except Exception as e:
        if logger:
            logger.error(f"Error authenticating to Bigdata.com: {e}")
        else:
            print(f"ERROR: Error authenticating to Bigdata.com: {e}")
        sys.exit(1)


class DateUtils:
    """Utility class for date-related operations."""
    
    @staticmethod
    def validate_date_format(date_str: str) -> bool:
        """Validate if a date string is in YYYY-MM-DD format.
        
        Args:
            date_str: Date string to validate
            
        Returns:
            bool: True if valid format, False otherwise
        """
        try:
            datetime.strptime(date_str, '%Y-%m-%d')
            return True
        except ValueError:
            return False
    
    @staticmethod
    def create_date_range(date: str) -> AbsoluteDateRange:
        """Create an absolute date range for a specific date.
        
        Args:
            date: Date string in YYYY-MM-DD format
            
        Returns:
            AbsoluteDateRange object for the specified date
        """
        start_time = f"{date}T00:00:00"
        end_time = f"{date}T23:59:59"
        return AbsoluteDateRange(start_time, end_time)
    
    @staticmethod
    def generate_date_range(start_date: str, end_date: str) -> List[str]:
        """Generate a list of dates between start_date and end_date (inclusive).
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            List of date strings in YYYY-MM-DD format
        """  
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
            if logger:
                logger.error(f"Error parsing dates: {e}")
                logger.error("Please use YYYY-MM-DD format (e.g., '2024-01-01')")
            else:
                print(f"ERROR: Error parsing dates: {e}")
                print("ERROR: Please use YYYY-MM-DD format (e.g., '2024-01-01')")
            sys.exit(1)


class TextFileProcessor:
    """Process text files containing keywords and sentences."""
    
    @staticmethod
    def process_keywords_file(keywords_file_path: str) -> List[Keyword]:
        """Process keywords file and create Keyword objects.
        
        Args:
            keywords_file_path: Path to the keywords text file
            
        Returns:
            List of Keyword objects
        """
        keywords = []
        
        try:
            with open(keywords_file_path, 'r', encoding='utf-8') as textfile:
                for line_num, line in enumerate(textfile, 1):
                    # Strip whitespace and skip empty lines
                    value = line.strip()
                    if not value:  # Skip empty lines
                        continue
                    
                    # Create Keyword object (can be single or multi-word)
                    keywords.append(Keyword(value))
                    logger.debug(f"Added keyword: {value}")
                
                logger.info(f"Processed keywords file: {len(keywords)} keywords loaded")
                return keywords
                
        except FileNotFoundError:
            if logger:
                logger.error(f"Keywords file not found: {keywords_file_path}")
            else:
                print(f"ERROR: Keywords file not found: {keywords_file_path}")
            sys.exit(1)
        except Exception as e:
            if logger:
                logger.error(f"Error processing keywords file {keywords_file_path}: {e}")
            else:
                print(f"ERROR: Error processing keywords file {keywords_file_path}: {e}")
            sys.exit(1)
    
    @staticmethod
    def process_sentences_file(sentences_file_path: str) -> List[Similarity]:
        """Process sentences file and create Similarity objects.
        
        Args:
            sentences_file_path: Path to the sentences text file
            
        Returns:
            List of Similarity objects
        """
        sentences = []
        
        try:
            with open(sentences_file_path, 'r', encoding='utf-8') as textfile:
                for line_num, line in enumerate(textfile, 1):
                    # Strip whitespace and skip empty lines
                    value = line.strip()
                    if not value:  # Skip empty lines
                        continue
                    
                    # Create Similarity object
                    sentences.append(Similarity(value))
                    logger.debug(f"Added sentence: {value}")
                
                logger.info(f"Processed sentences file: {len(sentences)} sentences loaded")
                return sentences
                
        except FileNotFoundError:
            if logger:
                logger.error(f"Sentences file not found: {sentences_file_path}")
            else:
                print(f"ERROR: Sentences file not found: {sentences_file_path}")
            sys.exit(1)
        except Exception as e:
            if logger:
                logger.error(f"Error processing sentences file {sentences_file_path}: {e}")
            else:
                print(f"ERROR: Error processing sentences file {sentences_file_path}: {e}")
            sys.exit(1)


class KnowledgeGraphExplorer:
    """Explore the Bigdata.com Knowledge Graph to find places."""
    
    def __init__(self, bigdata_client):
        """Initialize with Bigdata client.
        
        Args:
            bigdata_client: Authenticated Bigdata client instance
        """
        self.client = bigdata_client
    
    def find_places_for_country(self, country_name: str) -> List[str]:
        """Find places for a given country using the knowledge graph.
        
        Args:
            country_name: Name of the country to search for
            
        Returns:
            List of place entity IDs (first three results)
        """
        try:
            logger.info(f"Exploring knowledge graph for country: {country_name}")
            
            # Search for places using find_places
            places = self.client.knowledge_graph.find_places(country_name)
            
            if not places:
                logger.warning(f"No places found for country: {country_name}")
                return []
            
            # Get the first three place IDs
            place_ids = [place.id for place in places[:3]]
            
            logger.info(f"Found {len(place_ids)} places for {country_name}: {place_ids}")
            
            # Log place details for debugging
            for i, place in enumerate(places[:3]):
                logger.debug(f"Place {i+1}: ID={place.id}, Name={place.name}, Description={place.description}")
            
            return place_ids
            
        except Exception as e:
            logger.error(f"Error exploring knowledge graph for country {country_name}: {e}")
            return []


class NewsArticleRetriever:
    """Retrieve news articles using Bigdata.com Search service."""
    
    def __init__(self, bigdata_client):
        """Initialize with Bigdata client.
        
        Args:
            bigdata_client: Authenticated Bigdata client instance
        """
        self.client = bigdata_client
    
    def search_articles(self, places: List[str], keywords: List[Keyword], 
                       sentence: Similarity, date: str) -> Tuple[Dict, float]:
        """Search for articles using the specified query components.
        
        Args:
            places: List of place entity IDs
            keywords: List of Keyword objects
            sentence: Similarity object for the sentence
            date: Date string in YYYY-MM-DD format
            
        Returns:
            Tuple of (search_result, query_time)
        """
        start_time = time.time()
        
        try:
            logger.debug(f"Searching articles for date: {date}")
            
            # Create place entities
            place_entities = [Entity(place_id) for place_id in places]
            
            # Build the query: Any(places) & Any(keywords) & sentence
            if place_entities and keywords:
                query = Any(place_entities) & Any(keywords) & sentence
            elif place_entities:
                query = Any(place_entities) & sentence
            elif keywords:
                query = Any(keywords) & sentence
            else:
                query = sentence
            
            # Create date range for the specific date
            date_range = DateUtils.create_date_range(date)
            
            # Perform search with a rerank threshold of 0.2.
            search = self.client.search.new(
                query=query, 
                rerank_threshold=0.2,
                scope=DocumentType.NEWS, 
                date_range=date_range
            )
            
            # Run search with chunk limit
            documents = search.run(ChunkLimit(300))
            
            query_time = time.time() - start_time

            # Documents might have clustered documents under document.cluster, so we need to flatten the list
            flattened_documents = []
            for doc in documents:
                flattened_documents.append(doc)
                if doc.cluster:                    
                    flattened_documents.extend(doc.cluster)
            
            result = {
                'date': date,
                'query': str(query),
                'documents': flattened_documents,
                'query_time': query_time,
                'document_count': len(flattened_documents)
            }
            
            logger.debug(f"Search completed for {date}: {len(flattened_documents)} documents")
            return result, query_time
            
        except Exception as e:
            query_time = time.time() - start_time
            logger.error(f"Error searching articles for date {date}: {e}")
            
            result = {
                'date': date,
                'query': 'ERROR',
                'documents': [],
                'query_time': query_time,
                'document_count': 0,
                'error': str(e)
            }
            
            return result, query_time
    
    def collect_news(self, places: List[str], keywords: List[Keyword], 
                     sentences: List[Similarity], date: str, max_workers: int = 20) -> Dict:
        """Collect news articles for a specific date using parallel processing.
        
        Args:
            places: List of place entity IDs
            keywords: List of Keyword objects
            sentences: List of Similarity objects
            date: Date string in YYYY-MM-DD format
            max_workers: Maximum number of worker threads (default: 20)
            
        Returns:
            Dictionary containing the collection results and statistics
        """
        logger.info(f"START NEWS Collection on date: {date}")
        logger.info(f"Using threadpool with {max_workers} workers")
        
        if not sentences:
            logger.warning("No sentences provided for news collection")
            return {
                'date': date,
                'total_sentences': 0,
                'total_documents': 0,
                'results_per_sentence': {},
                'errors': ['No sentences provided']
            }
        
        # Collect news using threadpool
        results_per_sentence = {}
        errors = []
        total_documents = 0
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all search tasks
            future_to_sentence = {
                executor.submit(self.search_articles, places, keywords, sentence, date): sentence 
                for sentence in sentences
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_sentence):
                sentence = future_to_sentence[future]
                try:
                    search_result, query_time = future.result()
                    results_per_sentence[sentence] = search_result
                    
                    if 'error' in search_result:
                        errors.append(f"Sentence '{sentence}': {search_result['error']}")
                    else:
                        total_documents += search_result.get('document_count', 0)
                        
                except Exception as e:
                    error_msg = f"Sentence '{sentence}': {str(e)}"
                    errors.append(error_msg)
                    logger.error(f"Error processing sentence '{sentence}': {e}")
                    
                    # Create error result for this sentence
                    results_per_sentence[sentence] = {
                        'date': date,
                        'query': 'ERROR',
                        'documents': [],
                        'query_time': 0,
                        'document_count': 0,
                        'error': str(e)
                    }
        
        # Create summary
        news_collection_result = {
            'date': date,
            'total_sentences': len(sentences),
            'total_documents': total_documents,
            'results_per_sentence': results_per_sentence,
            'errors': errors
        }
        
        logger.info(f"END NEWS Collection on date {date}: {total_documents} documents from {len(sentences)} sentences")
        return news_collection_result
    
    def download_annotated_documents(self, search_result: Dict, date: str, max_workers: int = 20):
        """Download and save annotated documents for a specific date using parallel processing.
        
        Args:
            search_result: Search result containing documents
            date: Date string for folder organization
            max_workers: Maximum number of worker threads for parallel downloading (default: 10)
        """
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
            
            documents = search_result.get('documents', [])
            if not documents:
                logger.info(f"No documents to download for date: {date}")
                return
            
            logger.info(f"Starting parallel download of {len(documents)} documents")
            
            # Track download statistics
            successful_downloads = 0
            failed_downloads = 0
            errors = []
            
            def download_single_document(doc):
                """Download and save a single annotated document.
                
                Args:
                    doc: Document object to download
                    
                Returns:
                    Tuple of (success: bool, filename: str, error: str or None)
                """
                try:
                    # Create filename: date_headline.json
                    # Clean headline for filename (remove special characters)
                    headline = getattr(doc, 'headline', 'No headline')
                    clean_headline = "".join(c for c in headline if c.isalnum() or c in (' ', '-', '_')).rstrip()
                    clean_headline = clean_headline.replace(' ', '_')[:50]  # Limit length
                    
                    # Include date and headline in filename
                    filename = f"{date}_{clean_headline}.json"
                    file_path = os.path.join(date_folder, filename)
                    
                    # Download the entire annotated document using Bigdata.com's method
                    annotated_doc = doc.download_annotated_dict()
                    
                    # Save to JSON file
                    with open(file_path, 'w', encoding='utf-8') as jsonfile:
                        json.dump(annotated_doc, jsonfile, indent=2, ensure_ascii=False, default=str)
                    
                    logger.debug(f"Successfully downloaded: {filename}")
                    return True, filename, None
                    
                except Exception as e:
                    error_msg = f"Error processing document {getattr(doc, 'id', 'unknown')}: {e}"
                    logger.warning(error_msg)
                    return False, str(getattr(doc, 'id', 'unknown')), error_msg
            
            # Use ThreadPoolExecutor for parallel downloading
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all download tasks
                future_to_doc = {
                    executor.submit(download_single_document, doc): doc 
                    for doc in documents
                }
                
                # Collect results as they complete
                for future in as_completed(future_to_doc):
                    doc = future_to_doc[future]
                    try:
                        success, filename, error = future.result()
                        if success:
                            successful_downloads += 1
                        else:
                            failed_downloads += 1
                            if error:
                                errors.append(error)
                    except Exception as e:
                        failed_downloads += 1
                        error_msg = f"Unexpected error processing document {getattr(doc, 'id', 'unknown')}: {e}"
                        errors.append(error_msg)
                        logger.error(error_msg)
            
            # Log final results
            logger.info(f"Download completed: {successful_downloads} successful, {failed_downloads} failed")
            
            if errors:
                logger.warning(f"Errors encountered during download for date {date}:")
                for error in errors[:5]:  # Show first 5 errors to avoid log spam
                    logger.warning(f"  - {error}")
                if len(errors) > 5:
                    logger.warning(f"  ... and {len(errors) - 5} more errors")
            
        except Exception as e:
            logger.error(f"Error downloading documents for date {date}: {e}")


class PerformanceTracker:
    """Track and report performance metrics."""
    
    def __init__(self):
        """Initialize performance tracker."""
        self.daily_results = {}
        self.overall_start_time = time.time()
    
    def add_daily_result(self, date: str, search_result: Dict, processing_time: float):
        """Add daily search result to performance tracking.
        
        Args:
            date: Date string
            search_result: Search result dictionary
            processing_time: Time taken to process the date
        """
        self.daily_results[date] = {
            'processing_time': processing_time,
            'documents': search_result.get('document_count', 0),
            'error': search_result.get('error', None)
        }
    
    def print_performance_overview(self):
        """Print the performance overview table."""
        logger.info("="*80)
        logger.info("PER-DATE PERFORMANCE OVERVIEW")
        logger.info("="*80)
        logger.info(f"{'Date':<12} | {'Time (s)':<10} | {'Documents':<10}")
        logger.info("-"*80)
        
        total_documents = 0
        total_time = 0
        
        for date, result in sorted(self.daily_results.items()):
            if result.get('error'):
                logger.info(f"{date:<12} | {'ERROR':<10} | {'0':<10}")
            else:
                time_taken = result['processing_time']
                documents = result['documents']
                
                logger.info(f"{date:<12} | {time_taken:<10.2f} | {documents:<10}")
                
                total_documents += documents
                total_time += time_taken
        
        logger.info("-"*80)
        logger.info(f"{'TOTAL':<12} | {total_time:<10.2f} | {total_documents:<10}")
        logger.info("="*80)
        
        # Calculate averages
        if self.daily_results:
            avg_time = total_time / len(self.daily_results)
            avg_documents = total_documents / len(self.daily_results)
            
            logger.info(f"Average time per date: {avg_time:.2f} seconds")
            logger.info(f"Average documents per date: {avg_documents:.1f}")
        
        # Overall execution time
        overall_time = time.time() - self.overall_start_time
        logger.info(f"Total execution time: {overall_time:.2f} seconds")


def main():
    """Main function to run the article retrieval script."""
    parser = argparse.ArgumentParser(description="Retrieve news articles from Bigdata.com for a date range.")
    parser.add_argument("start_date", help="Start date in YYYY-MM-DD format (e.g., '2024-01-01')")
    parser.add_argument("end_date", help="End date in YYYY-MM-DD format (e.g., '2024-01-31')")
    parser.add_argument("keywords_file", help="Text file containing keywords (one per line)")
    parser.add_argument("sentences_file", help="Text file containing sentences (one per line)")
    parser.add_argument("--country", help="Optional country name for place filtering (e.g., 'United States')")
    parser.add_argument("--log_level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], help="Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL) - defaults to INFO")
    parser.add_argument("--third_party_log_level", default="WARNING", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], help="Log level for third-party libraries (default: WARNING)")
    
    args = parser.parse_args()
    
    try:
        # Setup logging with specified levels
        try:
            global logger
            logger = setup_logging(args.log_level, args.third_party_log_level)
            logger.info(f"Script logging level set to: {args.log_level}")
            logger.info(f"Third-party libraries logging level set to: {args.third_party_log_level}")
        except ValueError as e:
            print(f"Error: {e}")
            print("Valid log levels: DEBUG, INFO, WARNING, ERROR, CRITICAL")
            sys.exit(1)
        
        # Validate date formats
        if not DateUtils.validate_date_format(args.start_date):
            print(f"Error: Invalid start_date format '{args.start_date}'. Expected format: YYYY-MM-DD")
            sys.exit(1)
        if not DateUtils.validate_date_format(args.end_date):
            print(f"Error: Invalid end_date format '{args.end_date}'. Expected format: YYYY-MM-DD")
            sys.exit(1)
        
        # Generate date range
        dates = DateUtils.generate_date_range(args.start_date, args.end_date)
        logger.info(f"Generated date range: {args.start_date} to {args.end_date}")
        logger.info(f"Number of dates to process: {len(dates)}")
        
        # Authenticate to Bigdata.com
        logger.info("Authenticating to Bigdata.com...")
        bigdata_client = authenticate_bigdata()
        
        # Check initial quota usage
        logger.info("Checking initial quota usage...")
        try:
            subscription_details = bigdata_client.subscription.get_details()
            initial_usage = subscription_details.organization_quota.query_unit.used
            logger.info(f"Initial subscription usage (API Query Units): {initial_usage}")
        except Exception as e:
            logger.warning(f"Could not retrieve initial subscription details: {e}")
        
        # Process keywords file
        logger.info(f"Processing keywords file: {args.keywords_file}")
        keywords = TextFileProcessor.process_keywords_file(args.keywords_file)
        
        # Process sentences file
        logger.info(f"Processing sentences file: {args.sentences_file}")
        sentences = TextFileProcessor.process_sentences_file(args.sentences_file)
        
        if not sentences:
            logger.error("No sentences found in sentences file. At least one sentence is required.")
            sys.exit(1)
        
        if not keywords:
            logger.warning("No keywords found in keywords file. Searches will only use sentences and places.")
        
        # Explore knowledge graph for places if country is provided
        places = []
        if args.country:
            kg_explorer = KnowledgeGraphExplorer(bigdata_client)
            places = kg_explorer.find_places_for_country(args.country)
        else:
            logger.info("No country specified, skipping place filtering")
        
        # Initialize article retriever
        article_retriever = NewsArticleRetriever(bigdata_client)
        
        # Initialize performance tracker
        performance_tracker = PerformanceTracker()
        
        # Process each date
        logger.info(f"Starting to process {len(dates)} dates...")
        
        for i, date in enumerate(dates, 1):
            try:
                logger.info(f"Processing date {i} of {len(dates)}: {date}")
                logger.info("="*60)
                
                # Collect news for all sentences in parallel using threadpool
                news_collection_result = article_retriever.collect_news(
                    places=places,
                    keywords=keywords,
                    sentences=sentences,
                    date=date,
                    max_workers=20
                )
                
                # Download annotated documents for each sentence result
                results_per_sentence = news_collection_result.get('results_per_sentence', {})
                for sentence, search_result in results_per_sentence.items():
                    if not search_result.get('error'):
                        article_retriever.download_annotated_documents(search_result, date)
                
                # Add to performance tracking (aggregate results for the date)
                total_documents = news_collection_result.get('total_documents', 0)
                
                # Calculate total processing time for the date
                date_processing_time = sum(
                    result.get('query_time', 0) 
                    for result in results_per_sentence.values()
                )
                
                # Create aggregated result for performance tracking
                aggregated_result = {
                    'document_count': total_documents,
                    'error': None if not news_collection_result.get('errors') else '; '.join(news_collection_result.get('errors', []))
                }
                
                performance_tracker.add_daily_result(date, aggregated_result, date_processing_time)
                
                # Log any errors that occurred
                errors = news_collection_result.get('errors', [])
                if errors:
                    logger.warning(f"Errors encountered for date {date}:")
                    for error in errors:
                        logger.warning(f"  - {error}")
                
                logger.info(f"Successfully completed processing for date: {date}")
                logger.info("="*60)
                
            except Exception as e:
                logger.error(f"Error processing date {date}: {e}")
                traceback.print_exc()
                continue
        
        # Check final quota usage
        logger.info("Checking final quota usage...")
        try:
            subscription_details = bigdata_client.subscription.get_details()
            final_usage = subscription_details.organization_quota.query_unit.used
            logger.info(f"Final subscription usage (API Query Units): {final_usage}")
            logger.info(f"Total script run usage (API Query Units): {final_usage - initial_usage}")
        except Exception as e:
            logger.warning(f"Could not retrieve final subscription details: {e}")
        
        # Print performance overview
        performance_tracker.print_performance_overview()
        
        logger.info("Script execution completed successfully!")
        
    except KeyboardInterrupt:
        logger.warning("Script interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
