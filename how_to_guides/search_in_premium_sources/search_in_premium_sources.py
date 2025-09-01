from bigdata_client import Bigdata
from bigdata_client.query import Similarity, Source
from bigdata_client.daterange import AbsoluteDateRange
from bigdata_client.search import ChunkLimit
from dotenv import load_dotenv
import os
import argparse
import json
import logging
from datetime import datetime
from typing import List, Optional, Dict, Any, Tuple
from dataclasses import dataclass, asdict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()

bigdata = Bigdata(api_key=os.getenv("BIGDATA_API_KEY"))

###########################
# Premium source IDs
###########################

# Document type FACTSET_TRANSCRIPTS
# FactSet transcripts
FS_TRANSCRIPTS = "D4B903"

# Document type QUARTR_TRANSCRIPTS
# Quartr sources
QUARTR_AUDIOS = "58C81A"
QUARTR_PRESENTATION_MATERIALS = "E38350"
QUARTR_REPORTS = "28DED6"
QUARTR_TRANSCRIPTS = "DA0F7F"

# Document type PREMIUM_NEWS
# Alliance News
ALLIANCE_NEWS = "ED68DC"
# Benzinga
BENZINGA = "5A5702"
# Fly on the Wall
FLY_ON_THE_WALL = "B5235B"
# FXStreet sources
FXSTREET_ECONOMIC_CALENDAR = "86BD04"
FXSTREET_NEWS = "CBEE83"
# Midnight Trader sources
MT_NEWSWIRES_ASIA_PACIFIC = "8C739A"
MT_NEWSWIRES = "9D69F1"
MT_NEWSWIRES_EMEA = "CF4113"
MT_NEWSWIRES_CRYPTO = "1B95C1"

# document type SEC_FILINGS
# SEC filings
SEC_FILINGS = "E50739"

###########################
# Data Classes
###########################

@dataclass
class Chunk:
    """Represents a text chunk from a document"""
    chunk_index: int
    text: str
    relevance: float
    sentiment: Optional[str] = None

@dataclass
class Document:
    """Represents a document with its metadata and chunks"""
    document_id: str
    headline: str
    source: Dict[str, str]  # key and name
    url: str
    timestamp: str
    chunks: List[Chunk]

###########################
# Source Lists
###########################

# All premium source IDs
all_my_premium_source_ids = [
    FS_TRANSCRIPTS,
    QUARTR_AUDIOS, QUARTR_PRESENTATION_MATERIALS, QUARTR_REPORTS, QUARTR_TRANSCRIPTS,
    ALLIANCE_NEWS, BENZINGA, FLY_ON_THE_WALL,
    FXSTREET_ECONOMIC_CALENDAR, FXSTREET_NEWS,
    MT_NEWSWIRES_ASIA_PACIFIC, MT_NEWSWIRES, MT_NEWSWIRES_EMEA, MT_NEWSWIRES_CRYPTO,
    SEC_FILINGS
]

# Premium news source IDs
premium_news_source_ids = [
    ALLIANCE_NEWS, BENZINGA, FLY_ON_THE_WALL,
    FXSTREET_ECONOMIC_CALENDAR, FXSTREET_NEWS,
    MT_NEWSWIRES_ASIA_PACIFIC, MT_NEWSWIRES, MT_NEWSWIRES_EMEA, MT_NEWSWIRES_CRYPTO
]

# Quartr transcripts source IDs
quartr_transcripts_source_ids = [
    QUARTR_AUDIOS, QUARTR_PRESENTATION_MATERIALS, QUARTR_REPORTS, QUARTR_TRANSCRIPTS
]

###########################
# Helper Functions
###########################

def get_sources_for_document_types(document_types: List[str]) -> List[Source]:
    """Create Source objects based on selected document types"""
    sources = []
    
    for doc_type in document_types:
        if doc_type == "NEWS":
            sources.extend([Source(source_id) for source_id in premium_news_source_ids])
        elif doc_type == "FACTSET_TRANSCRIPTS":
            sources.append(Source(FS_TRANSCRIPTS))
        elif doc_type == "QUARTR_TRANSCRIPTS":
            sources.extend([Source(source_id) for source_id in quartr_transcripts_source_ids])
        elif doc_type == "SEC_FILINGS":
            sources.append(Source(SEC_FILINGS))
        else:
            logger.warning(f"Unknown document type: {doc_type}")
    
    logger.info(f"Created {len(sources)} source objects for document types: {document_types}")
    return sources

def get_negative_sources_for_document_types(document_types: List[str]) -> List[Source]:
    """Create negative Source objects (using ~) for selected document types"""
    sources = []
    
    for doc_type in document_types:
        if doc_type == "NEWS":
            sources.extend([~Source(source_id) for source_id in premium_news_source_ids])
        elif doc_type == "FACTSET_TRANSCRIPTS":
            sources.append(~Source(FS_TRANSCRIPTS))
        elif doc_type == "QUARTR_TRANSCRIPTS":
            sources.extend([~Source(source_id) for source_id in quartr_transcripts_source_ids])
        elif doc_type == "SEC_FILINGS":
            sources.append(~Source(SEC_FILINGS))
    
    logger.info(f"Created {len(sources)} negative source objects for document types: {document_types}")
    return sources

def perform_search(query: Similarity, start_date: Optional[str] = None, 
                  end_date: Optional[str] = None) -> Any:
    """Perform a search with the given query and parameters"""
    search_params = {
        "query": query,
        "rerank_threshold": 0.2
    }
    
    if start_date and end_date:
        date_range = AbsoluteDateRange(start_date, end_date)
        search_params["date_range"] = date_range
        logger.info(f"Searching with date range: {start_date} to {end_date}")
    
    search = bigdata.search.new(**search_params)
    documents = search.run(limit=ChunkLimit(300))  # Limit to 300 chunks
    
    return documents

def extract_documents_from_response(documents: Any) -> tuple[List[Document], int]:
    """Extract documents from the search response and return documents with total chunk count"""
    extracted_documents = []
    total_chunks = 0
    
    # Process the document list
    for doc in documents:
        document = create_document_from_doc(doc)
        if document:
            extracted_documents.append(document)
            total_chunks += len(document.chunks)
    
        # Process docs.cluster
        if hasattr(doc, 'cluster') and doc.cluster:
            for doc in doc.cluster:
                clustered_document = create_document_from_doc(doc)
                if clustered_document:
                    extracted_documents.append(clustered_document)
                    total_chunks += len(clustered_document.chunks)
    
    logger.info(f"Extracted {len(extracted_documents)} documents with {total_chunks} total chunks")
    return extracted_documents, total_chunks

def create_document_from_doc(doc: Any) -> Optional[Document]:
    """Create a Document object from a single doc"""
    try:
        # Extract chunks and sort by chunk index
        chunks = []
        if hasattr(doc, 'chunks') and doc.chunks:
            for chunk in doc.chunks:
                chunk_obj = Chunk(
                    chunk_index=getattr(chunk, 'chunk', 0),
                    text=getattr(chunk, 'text', ''),
                    relevance=getattr(chunk, 'relevance', 0.0),
                    sentiment=getattr(chunk, 'sentiment', None)
                )
                chunks.append(chunk_obj)
            
            # Sort chunks by chunk index in ascending order
            chunks.sort(key=lambda x: x.chunk_index)
        
        # Handle timestamp conversion
        timestamp = getattr(doc, 'timestamp', '')
        if hasattr(timestamp, 'isoformat'):
            # Convert datetime object to ISO format string
            timestamp_str = timestamp.isoformat()
        else:
            # If it's already a string or other type, convert to string
            timestamp_str = str(timestamp)
        
        # Create document
        document = Document(
            document_id=getattr(doc, 'id', ''),
            headline=getattr(doc, 'headline', ''),
            source={
                'key': getattr(doc.source, 'key', '') if hasattr(doc, 'source') else '',
                'name': getattr(doc.source, 'name', '') if hasattr(doc, 'source') else ''
            },
            url=getattr(doc, 'url', ''),
            timestamp=timestamp_str,
            chunks=chunks
        )
        
        return document
        
    except Exception as e:
        logger.error(f"Error creating document: {e}")
        return None

def count_total_chunks(documents: List[Document]) -> int:
    """Count total chunks across all documents"""
    return sum(len(doc.chunks) for doc in documents)

def search_premium_sources(sentence: str, start_date: Optional[str] = None,
                         end_date: Optional[str] = None, 
                         document_types: Optional[List[str]] = None) -> Tuple[List[Document], bool]:
    """Main search method that handles both primary and secondary searches
    
    Returns:
        Tuple[List[Document], bool]: (documents, secondary_search_required)
        - documents: List of found documents
        - secondary_search_required: True if secondary search was performed, False otherwise
    """
    
    if document_types is None:
        document_types = ["NEWS", "FACTSET_TRANSCRIPTS", "QUARTR_TRANSCRIPTS", "SEC_FILINGS"]
    
    # Create sources for the selected document types
    sources = get_sources_for_document_types(document_types)
    
    # Create similarity query
    query = Similarity(sentence)
    
    # Add source filters to the query
    if sources:
        source_filter = sources[0]
        for source in sources[1:]:
            source_filter = source_filter | source
        query = query & source_filter
    
    # Perform first search
    logger.info("Performing primary search...")
    documents = perform_search(query, start_date, end_date)
    
    # Extract documents from first search
    processed_documents, first_search_chunks = extract_documents_from_response(documents)
    logger.info(f"First search returned {first_search_chunks} chunks")
    
    # Check if we need a second search (for NEWS document type with less than 5 chunks)
    secondary_search_required = False
    if "NEWS" in document_types and first_search_chunks < 5:
        logger.info("Performing secondary search with negative sources (less than 5 chunks in first search)")
        secondary_search_required = True
        
        # Create negative sources
        negative_sources = get_negative_sources_for_document_types(document_types)
        
        # Create negative query
        negative_query = Similarity(sentence)
        
        # Add negative source filters to the query
        if negative_sources:
            negative_source_filter = negative_sources[0]
            for source in negative_sources[1:]:
                negative_source_filter = negative_source_filter | source
            negative_query = negative_query & negative_source_filter
        
        # Perform second search
        secondary_documents = perform_search(negative_query, start_date, end_date)
        
        # Extract documents from second search
        negative_documents, second_search_chunks = extract_documents_from_response(secondary_documents)
        logger.info(f"Second search returned {second_search_chunks} chunks")
        
        # Combine both search results
        processed_documents.extend(negative_documents)
        total_chunks = first_search_chunks + second_search_chunks
        logger.info(f"Combined search returned {total_chunks} total chunks")
    else:
        logger.info(f"Single search completed with {first_search_chunks} chunks")
    
    return processed_documents, secondary_search_required

def ensure_output_directory():
    """Ensure the output directory exists and log its status"""
    output_dir = "output"
    if os.path.exists(output_dir):
        logger.info(f"Output directory '{output_dir}' already exists")
    else:
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"Created output directory '{output_dir}'")
    
    # Log the absolute path for clarity
    abs_path = os.path.abspath(output_dir)
    logger.info(f"Output directory absolute path: {abs_path}")
    return output_dir

def save_documents_to_json(documents: List[Document], filename: str = None):
    """Save documents to a JSON file in the output folder"""
    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"search_results_{timestamp}.json"
    
    # Ensure output directory exists
    os.makedirs("output", exist_ok=True)
    output_path = os.path.join("output", filename)
    
    # Convert documents to dictionaries
    documents_dict = [asdict(doc) for doc in documents]
    
    # Save to file
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(documents_dict, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Saved {len(documents)} documents to {output_path}")
    return output_path

###########################
# Main Function
###########################

def main():
    """Main function to run the search script"""
    parser = argparse.ArgumentParser(description="Search premium sources using Bigdata.com API")
    parser.add_argument("sentence", help="Search sentence/query")
    parser.add_argument("--start-date", help="Start date for search (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="End date for search (YYYY-MM-DD)")
    parser.add_argument("--document-types", nargs="+", 
                       choices=["NEWS", "FACTSET_TRANSCRIPTS", "QUARTR_TRANSCRIPTS", "SEC_FILINGS"],
                       default=["NEWS", "FACTSET_TRANSCRIPTS", "QUARTR_TRANSCRIPTS", "SEC_FILINGS"],
                       help="Document types to include in search")
    parser.add_argument("--output-file", help="Output filename (optional)")
    
    args = parser.parse_args()
    
    try:
        # Validate API key
        if not os.getenv("BIGDATA_API_KEY"):
            raise ValueError("BIGDATA_API_KEY environment variable is not set")
        
        # Ensure output directory exists
        ensure_output_directory()
        
        # Perform search
        documents, secondary_search_required = search_premium_sources(
            sentence=args.sentence,
            start_date=args.start_date,
            end_date=args.end_date,
            document_types=args.document_types
        )
        
        # Save results
        output_path = save_documents_to_json(documents, args.output_file)
        
        print("\nSearch completed successfully!")
        print(f"Total documents found: {len(documents)}")
        print(f"Total chunks: {count_total_chunks(documents)}")
        if secondary_search_required:
            print("🔄 Secondary search was performed")
        else:
            print("✅ Single search completed")
        print(f"Results saved to: {output_path}")
        
    except Exception as e:
        logger.error(f"Error during search: {e}")
        print(f"Error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())


