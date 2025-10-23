import logging
import csv
import sys
import requests
import json
from dotenv import load_dotenv
import os
from utils import get_24_hours_timestamp_range


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/top_topics_to_search.log')
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()

api_key = os.getenv("BIGDATA_API_KEY")

# Bigdata Services API configuration
BIGDATA_SEARCH_URL = "https://api.bigdata.com/v1/search/co-mentions/topics"


def read_ravenpack_ids_from_csv(csv_file_path):
    """
    Read CSV file and extract all ravenpack_ids
    
    Args:
        csv_file_path (str): Path to the CSV file
        
    Returns:
        list: List of ravenpack_ids
    """
    ravenpack_ids = []
    
    try:
        with open(csv_file_path, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            
            for row_num, row in enumerate(reader, start=2):  # Start at 2 because header is row 1
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
                
                # Extract ravenpack_id
                ravenpack_id = cleaned_row.get('ravenpack_id', '')
                
                if ravenpack_id:
                    ravenpack_ids.append(ravenpack_id)
                    logger.info(f"Row {row_num}: Found ravenpack_id {ravenpack_id}")
                else:
                    logger.warning(f"Row {row_num}: No ravenpack_id found")
        
        logger.info(f"Successfully extracted {len(ravenpack_ids)} ravenpack_ids from CSV")
        return ravenpack_ids
        
    except FileNotFoundError:
        logger.error(f"CSV file not found: {csv_file_path}")
        raise
    except Exception as e:
        logger.error(f"Error reading CSV file: {str(e)}")
        raise


def search_topics_by_ravenpack_ids(ravenpack_ids, start_date, end_date, limit=5):
    """
    Search for topics using ravenpack_ids via Bigdata Services API
    
    Args:
        ravenpack_ids (list): List of ravenpack_ids
        start_date (str): Start date for the search
        end_date (str): End date for the search
        limit (int): Maximum number of results
        
    Returns:
        dict: API response with topics data
    """
    url = BIGDATA_SEARCH_URL
    headers = {
        'Content-Type': 'application/json',
        'x-api-key': api_key
    }
    
    payload = {
        "query": {
            "filters": {
                "timestamp": {
                    "start": start_date,
                    "end": end_date
                },
                "entity": {
                    "all_of": [],
                    "any_of": ravenpack_ids,
                    "none_of": []
                },
                "source": {
                    "mode": "INCLUDE",
                    "values": [
                        "5A5702",
                        "9D69F1",
                        "DFF004",
                        "B5235B"
                    ]
                }
            },
            "entity_details": True,
            "limit": limit
        }
    }
    
    try:
        logger.info(f"Searching topics for {len(ravenpack_ids)} ravenpack_ids")
        logger.debug(f"API URL: {url}")
        logger.debug(f"Payload: {json.dumps(payload, indent=2)}")
        
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()  # Raises an HTTPError for bad responses
        
        result = response.json()
        logger.info("API response received successfully")
        logger.debug(f"Response: {json.dumps(result, indent=2)}")
        
        return result
        
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {str(e)}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Response status: {e.response.status_code}")
            logger.error(f"Response content: {e.response.text}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON response: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during API call: {str(e)}")
        raise


def extract_topics_from_response(api_response):
    """
    Extract topics with id, name and description from the API response
    
    Args:
        api_response (dict): API response from Bigdata
        
    Returns:
        list: List of topic dictionaries with id, name, and description
    """
    topics = []
    seen_topic_ids = set()
    
    try:
        if 'results' in api_response and 'topics' in api_response['results']:
            for topic in api_response['results']['topics']:
                topic_id = topic.get('id', '')
                topic_name = topic.get('name', '')
                topic_description = topic.get('description', '')
                
                if topic_id and topic_id not in seen_topic_ids:  # Avoid duplicates
                    seen_topic_ids.add(topic_id)
                    topic_data = {
                        'id': topic_id,
                        'name': topic_name,
                        'description': topic_description
                    }
                    topics.append(topic_data)
                    logger.info(f"Found topic: {topic_name} (ID: {topic_id})")
        
        logger.info(f"Successfully extracted {len(topics)} unique topics")
        return topics
        
    except Exception as e:
        logger.error(f"Error extracting topics: {str(e)}")
        raise


def main():
    """
    Main function to process CSV file and search for topics
    """
    if len(sys.argv) != 2:
        print("Usage: python get_top_topic_ids.py <input_csv_file>")
        print("Example: python get_top_topic_ids.py public_company_ids.csv")
        sys.exit(1)
    
    # Check if API key is available
    if not api_key:
        print("Error: BIGDATA_API_KEY not found in environment variables")
        print("Please set BIGDATA_API_KEY in your .env file")
        sys.exit(1)
    
    input_file = sys.argv[1]
    
    try:
        # Read ravenpack_ids from CSV
        print(f"Reading ravenpack_ids from {input_file}...")
        ravenpack_ids = read_ravenpack_ids_from_csv(input_file)
        
        if not ravenpack_ids:
            print("No ravenpack_ids found in the CSV file")
            sys.exit(1)
        
        print(f"Found {len(ravenpack_ids)} ravenpack_ids: {ravenpack_ids}")
        
        # Get timestamp range once for the search
        start_date, end_date = get_24_hours_timestamp_range()
        
        # Search for topics
        print("\nSearching for topics using ravenpack_ids...")
        api_response = search_topics_by_ravenpack_ids(ravenpack_ids, start_date, end_date)
        
        # Extract topics
        print("\nExtracting topics from response...")
        topics = extract_topics_from_response(api_response)
        
        if topics:
            print(f"\nFound {len(topics)} unique topics:")
            for i, topic in enumerate(topics, 1):
                print(f"\n  {i}. Topic: {topic['name']}")
                print(f"     ID: {topic['id']}")
                print(f"     Description: {topic['description']}")
        else:
            print("\nNo topics found in the response")
        
        # Show summary
        print("\nSummary:")
        print(f"  - Ravenpack IDs processed: {len(ravenpack_ids)}")
        print(f"  - Topics found: {len(topics)}")
        
        # Save topics to a CSV file for future use
        if topics:
            # Create output directory if it doesn't exist
            import os
            output_dir = "output"
            os.makedirs(output_dir, exist_ok=True)
            
            output_file = os.path.join(output_dir, "top_topics_to_search.csv")
            with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['name', 'id', 'description']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter='|')
                
                # Write header
                writer.writeheader()
                
                # Write topic data
                for topic in topics:
                    writer.writerow({
                        'name': topic['name'],
                        'id': topic['id'],
                        'description': topic['description']
                    })
            
            print(f"  - Topics saved to: {output_file}")
            
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        print(f"Error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
