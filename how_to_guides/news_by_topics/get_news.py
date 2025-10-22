#!/usr/bin/env python3
"""
News retrieval script that queries Bigdata Search service for companies and topics.
Reads company IDs and topics from CSV files and performs concurrent searches.
"""

import logging
import csv
import sys
import requests
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
import os
from typing import List, Dict, Any
from utils import get_24_hours_timestamp_range

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/news_search.log')
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()

api_key = os.getenv("BIGDATA_API_KEY")

# Bigdata Services API configuration
BIGDATA_SEARCH_URL = "https://api.bigdata.com/v1/search"


def read_companies_from_csv(csv_file_path: str) -> List[Dict[str, str]]:
    """
    Read CSV file and extract company information
    
    Args:
        csv_file_path (str): Path to the CSV file
        
    Returns:
        list: List of company dictionaries with name and ravenpack_id
    """
    companies = []
    
    try:
        with open(csv_file_path, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            
            for row_num, row in enumerate(reader, start=2):
                # Clean the data - strip whitespace and handle None/empty values
                cleaned_row = {}
                for key, value in row.items():
                    if key is None:
                        key = ''
                    else:
                        key = key.strip()
                    
                    if value is None:
                        value = ''
                    else:
                        value = str(value).strip()
                    
                    cleaned_row[key] = value
                
                # Extract company information
                name = cleaned_row.get('Name', '')
                ravenpack_id = cleaned_row.get('ravenpack_id', '')
                
                if name and ravenpack_id:
                    companies.append({
                        'name': name,
                        'ravenpack_id': ravenpack_id
                    })
                    logger.info(f"Row {row_num}: Found company {name} with ID {ravenpack_id}")
                else:
                    logger.warning(f"Row {row_num}: Missing name or ravenpack_id")
        
        logger.info(f"Successfully extracted {len(companies)} companies from CSV")
        return companies
        
    except FileNotFoundError:
        logger.error(f"CSV file not found: {csv_file_path}")
        raise
    except Exception as e:
        logger.error(f"Error reading CSV file: {str(e)}")
        raise


def read_topics_from_csv(csv_file_path: str) -> tuple[List[str], List[Dict[str, str]]]:
    """
    Read CSV file and extract topic IDs and names
    
    Args:
        csv_file_path (str): Path to the CSV file
        
    Returns:
        tuple: (List of topic IDs, List of topic dictionaries with name and id)
    """
    topic_ids = []
    topic_names = []
    
    try:
        with open(csv_file_path, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile, delimiter='|')
            
            for row_num, row in enumerate(reader, start=2):
                # Clean the data
                cleaned_row = {}
                for key, value in row.items():
                    if key is None:
                        key = ''
                    else:
                        key = key.strip()
                    
                    if value is None:
                        value = ''
                    else:
                        value = str(value).strip()
                    
                    cleaned_row[key] = value
                
                # Extract topic information
                topic_id = cleaned_row.get('id', '')
                topic_name = cleaned_row.get('name', '')
                
                if topic_id:
                    topic_ids.append(topic_id)
                    topic_names.append({
                        'id': topic_id,
                        'name': topic_name
                    })
                    logger.info(f"Row {row_num}: Found topic {topic_name} (ID: {topic_id})")
                else:
                    logger.warning(f"Row {row_num}: No topic ID found")
        
        logger.info(f"Successfully extracted {len(topic_ids)} topic IDs from CSV")
        return topic_ids, topic_names
        
    except FileNotFoundError:
        logger.error(f"CSV file not found: {csv_file_path}")
        raise
    except Exception as e:
        logger.error(f"Error reading CSV file: {str(e)}")
        raise


def search_news_for_company(company: Dict[str, str], topic_ids: List[str], start_timestamp: str, end_timestamp: str) -> Dict[str, Any]:
    """
    Search for news for a specific company using all topic IDs
    
    Args:
        company (dict): Company information with name and ravenpack_id
        topic_ids (list): List of topic IDs to search for
        start_timestamp (str): Start timestamp for the search
        end_timestamp (str): End timestamp for the search
        
    Returns:
        dict: Company information with search results
    """
    
    headers = {
        'Content-Type': 'application/json',
        'x-api-key': api_key
    }
    
    payload = {
        "query": {
            "text": "",
            "auto_enrich_filters": False,
            "filters": {
                "timestamp": {
                    "start": start_timestamp,
                    "end": end_timestamp
                },
                "topic": {
                    "any_of": topic_ids
                },
                "entity": {
                    "all_of": [company['ravenpack_id']]
                },
                "sentiment": {
                    "values": ["negative", "positive"]
                }
            },
            "ranking_params": {
                "source_boost": 10,
                "freshness_boost": 1,
                "reranker": {
                    "enabled": False
                }
            },
            "max_chunks": 100
        }
    }
    
    try:
        logger.info(f"Searching news for company: {company['name']} (ID: {company['ravenpack_id']})")
        
        response = requests.post(BIGDATA_SEARCH_URL, headers=headers, json=payload)
        response.raise_for_status()
        
        result = response.json()
        logger.info(f"Successfully retrieved {len(result.get('results', []))} documents for {company['name']}")
        
        return {
            'company': company,
            'results': result.get('results', []),
            'metadata': result.get('metadata', {}),
            'usage': result.get('usage', {})
        }
        
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed for {company['name']}: {str(e)}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Response status: {e.response.status_code}")
            logger.error(f"Response content: {e.response.text}")
        return {
            'company': company,
            'results': [],
            'error': str(e)
        }
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON response for {company['name']}: {str(e)}")
        return {
            'company': company,
            'results': [],
            'error': f"JSON decode error: {str(e)}"
        }
    except Exception as e:
        logger.error(f"Unexpected error for {company['name']}: {str(e)}")
        return {
            'company': company,
            'results': [],
            'error': str(e)
        }


def calculate_chunk_priority(chunk: Dict[str, Any]) -> float:
    """
    Calculate priority for a chunk based on sentiment * relevance
    
    Args:
        chunk (dict): Chunk data containing sentiment and relevance
        
    Returns:
        float: Absolute priority score (abs(sentiment * relevance))
    """
    sentiment = chunk.get('sentiment', 0)
    relevance = chunk.get('relevance', 0)
    
    # Handle None values
    if sentiment is None:
        sentiment = 0
    if relevance is None:
        relevance = 0
    
    priority = abs(sentiment * relevance)
    return round(priority, 4)  # Round to 4 decimal places


def extract_topic_names_from_chunk(chunk: Dict[str, Any], valid_topic_ids: List[str]) -> List[str]:
    """
    Extract topic names from chunk detections, filtering by valid topic IDs
    
    Args:
        chunk (dict): Chunk data containing detections
        valid_topic_ids (list): List of valid topic IDs to filter by
        
    Returns:
        list: List of topic names found in the chunk that are in the valid list
    """
    topic_names = []
    detections = chunk.get('detections', [])
    
    for detection in detections:
        if detection.get('type') == 'topic':
            topic_id = detection.get('id', '')
            if topic_id and topic_id in valid_topic_ids:
                topic_names.append(topic_id)
    
    return topic_names


def calculate_topic_averages(search_result: Dict[str, Any], topic_names: List[str]) -> Dict[str, Dict[str, float]]:
    """
    Calculate average priority, sentiment, and relevance per topic for a company
    
    Args:
        search_result (dict): Search result containing company info and documents
        topic_names (list): List of topic names to map IDs to names
        
    Returns:
        dict: Dictionary with topic names as keys and averages as values
    """
    results = search_result['results']
    
    # Create mapping from topic IDs to names
    topic_id_to_name = {}
    valid_topic_ids = []
    for topic in topic_names:
        topic_id_to_name[topic['id']] = topic['name']
        valid_topic_ids.append(topic['id'])
    
    # Collect data per topic
    topic_data = {}
    
    for document in results:
        chunks = document.get('chunks', [])
        for chunk in chunks:
            chunk_topics = extract_topic_names_from_chunk(chunk, valid_topic_ids)
            
            for topic_id in chunk_topics:
                topic_name = topic_id_to_name.get(topic_id, topic_id)  # Use ID if name not found
                
                if topic_name not in topic_data:
                    topic_data[topic_name] = {
                        'priorities': [],
                        'sentiments': [],
                        'relevances': []
                    }
                
                priority = calculate_chunk_priority(chunk)
                sentiment = chunk.get('sentiment', 0) or 0
                relevance = chunk.get('relevance', 0) or 0
                
                topic_data[topic_name]['priorities'].append(priority)
                topic_data[topic_name]['sentiments'].append(sentiment)
                topic_data[topic_name]['relevances'].append(relevance)
    
    # Calculate averages
    topic_averages = {}
    for topic_name, data in topic_data.items():
        if data['priorities']:  # Only include topics with data
            topic_averages[topic_name] = {
                'avg_priority': round(sum(data['priorities']) / len(data['priorities']), 4),
                'avg_sentiment': round(sum(data['sentiments']) / len(data['sentiments']), 4),
                'avg_relevance': round(sum(data['relevances']) / len(data['relevances']), 4),
                'count': len(data['priorities'])
            }
    
    return topic_averages


def print_company_table(search_result: Dict[str, Any], topic_names: List[str]) -> None:
    """
    Print a markdown table for a company showing topic averages
    
    Args:
        search_result (dict): Search result containing company info and documents
        topic_names (list): List of topic dictionaries with name and id
    """
    company = search_result['company']
    
    logger.info(f"## {company['name']} (ID: {company['ravenpack_id']})")
    
    if 'error' in search_result:
        logger.error(f"**❌ ERROR:** {search_result['error']}")
        return
    
    if not search_result['results']:
        logger.info("*No news documents found for this company in the last 24 hours.*")
        return
    
    # Calculate topic averages
    topic_averages = calculate_topic_averages(search_result, topic_names)
    
    if not topic_averages:
        logger.info("*No topic data found in the documents.*")
        return
    
    # Sort topics by average priority (descending)
    sorted_topics = sorted(topic_averages.items(), key=lambda x: x[1]['avg_priority'], reverse=True)
    
    # Calculate column widths dynamically
    topic_names = [topic_name for topic_name, _ in sorted_topics]
    priorities = [f"{averages['avg_priority']}" for _, averages in sorted_topics]
    sentiments = [f"{averages['avg_sentiment']}" for _, averages in sorted_topics]
    relevances = [f"{averages['avg_relevance']}" for _, averages in sorted_topics]
    counts = [f"{averages['count']}" for _, averages in sorted_topics]
    
    # Calculate max width for each column
    max_topic_width = max(len("Topic Name"), max(len(name) for name in topic_names))
    max_priority_width = max(len("Avg Priority"), max(len(p) for p in priorities))
    max_sentiment_width = max(len("Avg Sentiment"), max(len(s) for s in sentiments))
    max_relevance_width = max(len("Avg Relevance"), max(len(r) for r in relevances))
    max_count_width = max(len("Count"), max(len(c) for c in counts))
    
    # Print markdown table with proper alignment
    logger.info(f"| {'Topic Name':<{max_topic_width}} | {'Avg Priority':<{max_priority_width}} | {'Avg Sentiment':<{max_sentiment_width}} | {'Avg Relevance':<{max_relevance_width}} | {'Count':<{max_count_width}} |")
    logger.info(f"|{'-' * (max_topic_width + 2)}|{'-' * (max_priority_width + 2)}|{'-' * (max_sentiment_width + 2)}|{'-' * (max_relevance_width + 2)}|{'-' * (max_count_width + 2)}|")
    
    for topic_name, averages in sorted_topics:
        logger.info(f"| {topic_name:<{max_topic_width}} | {averages['avg_priority']:<{max_priority_width}} | {averages['avg_sentiment']:<{max_sentiment_width}} | {averages['avg_relevance']:<{max_relevance_width}} | {averages['count']:<{max_count_width}} |")
    

    logger.info(f"**Summary:** {len(topic_averages)} topics found across {len(search_result['results'])} documents")
    logger.info("--------------------------------")


def main():
    """
    Main function to process CSV files and search for news
    """
    if len(sys.argv) != 3:
        logger.error("Usage: python get_news.py <companies_csv_file> <topics_csv_file>")
        logger.error("Example: python get_news.py public_company_ids.csv top_topics_to_search.csv")
        sys.exit(1)
    
    # Check if API key is available
    if not api_key:
        logger.error("Error: BIGDATA_API_KEY not found in environment variables")
        logger.error("Please set BIGDATA_API_KEY in your .env file")
        sys.exit(1)
    
    companies_file = sys.argv[1]
    topics_file = sys.argv[2]
    
    try:
        # Read companies from CSV
        logger.info(f"Reading companies from {companies_file}...")
        companies = read_companies_from_csv(companies_file)
        
        if not companies:
            logger.error("No companies found in the CSV file")
            sys.exit(1)
        
        # Read topics from CSV
        logger.info(f"Reading topics from {topics_file}...")
        topic_ids, topic_names = read_topics_from_csv(topics_file)
        
        if not topic_ids:
            logger.error("No topics found in the CSV file")
            sys.exit(1)
        
        logger.info(f"Found {len(companies)} companies and {len(topic_ids)} topics")
        logger.debug(f"Companies: {[c['name'] for c in companies]}")
        logger.debug(f"Topics: {[t['name'] for t in topic_names]}")
        
        # Get timestamp range once for all searches
        start_timestamp, end_timestamp = get_24_hours_timestamp_range()
        
        # Search for news using multithreading
        logger.info("Searching for news using 20 concurrent workers...")
        
        search_results = []
        
        with ThreadPoolExecutor(max_workers=20) as executor:
            # Submit all tasks
            future_to_company = {
                executor.submit(search_news_for_company, company, topic_ids, start_timestamp, end_timestamp): company 
                for company in companies
            }
            
            # Process completed tasks
            for future in as_completed(future_to_company):
                company = future_to_company[future]
                try:
                    result = future.result()
                    search_results.append(result)
                except Exception as e:
                    logger.error(f"Error processing {company['name']}: {str(e)}")
                    search_results.append({
                        'company': company,
                        'results': [],
                        'error': str(e)
                    })
        
        # Print results
        logger.info("# News Search Results Summary")
        
        total_documents = 0
        companies_with_news = 0
        
        for result in search_results:
            if result['results']:
                companies_with_news += 1
                total_documents += len(result['results'])
            print_company_table(result, topic_names)
        
        # Print final summary
        logger.info("# Final Summary")
        logger.info(f"- **Companies processed:** {len(companies)}")
        logger.info(f"- **Companies with news:** {companies_with_news}")
        logger.info(f"- **Total documents found:** {total_documents}")
        logger.info(f"- **Topics searched:** {len(topic_ids)}")
        logger.info("- **Time range:** Last 24 hours")
        
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        logger.error(f"Error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
