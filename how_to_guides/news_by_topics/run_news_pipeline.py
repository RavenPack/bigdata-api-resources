#!/usr/bin/env python3
"""
Sequential news pipeline script that runs the three main steps:
1. get_company_ids.py - Extract company IDs from input CSV
2. get_top_topic_ids.py - Find top topics for those companies
3. get_news.py - Search for news articles about companies and topics
"""

import logging
import subprocess
import sys
import os
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/pipeline.log')
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()

# File paths
INPUT_COMPANIES_FILE = "input/public_companies.csv"
COMPANY_IDS_OUTPUT = "output/company_ids.csv"
TOPICS_OUTPUT = "output/top_topics_to_search.csv"

def check_prerequisites():
    """
    Check if all required files and environment variables are available
    """
    logger.info("🔍 Checking prerequisites...")
    
    # Check API key
    api_key = os.getenv("BIGDATA_API_KEY")
    if not api_key:
        logger.error("❌ BIGDATA_API_KEY not found in environment variables")
        logger.error("Please set BIGDATA_API_KEY in your .env file")
        return False
    
    logger.info(f"✅ API Key found: {api_key[:7]}...{api_key[-3:]}")
    
    # Check input file
    if not os.path.exists(INPUT_COMPANIES_FILE):
        logger.error(f"❌ Input file not found: {INPUT_COMPANIES_FILE}")
        logger.error("Please ensure the input CSV file exists")
        return False
    
    logger.info(f"✅ Input file found: {INPUT_COMPANIES_FILE}")
    
    # Create necessary directories
    os.makedirs("output", exist_ok=True)
    os.makedirs("logs", exist_ok=True)
    logger.info("✅ Output and logs directories created")
    
    return True

def run_script(script_name, args, description):
    """
    Run a Python script with the given arguments
    
    Args:
        script_name (str): Name of the script to run
        args (list): List of arguments to pass to the script
        description (str): Description of what the script does
        
    Returns:
        bool: True if successful, False otherwise
    """
    logger.info(f"\n🚀 {description}")
    logger.info(f"Running: python {script_name} {' '.join(args)}")
    
    try:
        # Run the script
        result = subprocess.run(
            [sys.executable, script_name] + args,
            capture_output=True,
            text=True,
            check=True
        )
        
        # Log the output
        if result.stdout:
            logger.info("Script output:")
            for line in result.stdout.strip().split('\n'):
                logger.info(f"  {line}")
        
        logger.info(f"✅ {description} completed successfully")
        return True
        
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ {description} failed with exit code {e.returncode}")
        if e.stdout:
            logger.error("Script stdout:")
            for line in e.stdout.strip().split('\n'):
                logger.error(f"  {line}")
        if e.stderr:
            logger.error("Script stderr:")
            for line in e.stderr.strip().split('\n'):
                logger.error(f"  {line}")
        return False
    except Exception as e:
        logger.error(f"❌ Unexpected error running {script_name}: {str(e)}")
        return False

def main():
    """
    Main function to run the sequential pipeline
    """
    logger.info("=" * 60)
    logger.info("📰 NEWS PIPELINE - SEQUENTIAL EXECUTION")
    logger.info("=" * 60)
    
    # Check prerequisites
    if not check_prerequisites():
        logger.error("❌ Prerequisites check failed. Exiting.")
        sys.exit(1)
    
    # Step 1: Get Company IDs
    logger.info("\n" + "=" * 40)
    logger.info("STEP 1: EXTRACTING COMPANY IDs")
    logger.info("=" * 40)
    
    if not run_script(
        "get_company_ids.py",
        [INPUT_COMPANIES_FILE],
        "Extracting company IDs from input CSV"
    ):
        logger.error("❌ Step 1 failed. Pipeline stopped.")
        sys.exit(1)
    
    # Verify Step 1 output
    if not os.path.exists(COMPANY_IDS_OUTPUT):
        logger.error(f"❌ Expected output file not found: {COMPANY_IDS_OUTPUT}")
        sys.exit(1)
    
    logger.info(f"✅ Company IDs saved to: {COMPANY_IDS_OUTPUT}")
    
    # Step 2: Get Top Topic IDs
    logger.info("\n" + "=" * 40)
    logger.info("STEP 2: FINDING TOP TOPICS")
    logger.info("=" * 40)
    
    if not run_script(
        "get_top_topic_ids.py",
        [COMPANY_IDS_OUTPUT],
        "Finding top topics for companies"
    ):
        logger.error("❌ Step 2 failed. Pipeline stopped.")
        sys.exit(1)
    
    # Verify Step 2 output
    if not os.path.exists(TOPICS_OUTPUT):
        logger.error(f"❌ Expected output file not found: {TOPICS_OUTPUT}")
        sys.exit(1)
    
    logger.info(f"✅ Topics saved to: {TOPICS_OUTPUT}")
    
    # Step 3: Get News
    logger.info("\n" + "=" * 40)
    logger.info("STEP 3: SEARCHING FOR NEWS")
    logger.info("=" * 40)
    
    if not run_script(
        "get_news.py",
        [COMPANY_IDS_OUTPUT, TOPICS_OUTPUT],
        "Searching for news articles"
    ):
        logger.error("❌ Step 3 failed. Pipeline stopped.")
        sys.exit(1)
    
    # Pipeline completed successfully
    logger.info("\n" + "=" * 60)
    logger.info("🎉 PIPELINE COMPLETED SUCCESSFULLY!")
    logger.info("=" * 60)
    logger.info("📁 Output files:")
    logger.info(f"  - Company IDs: {COMPANY_IDS_OUTPUT}")
    logger.info(f"  - Topics: {TOPICS_OUTPUT}")
    logger.info("📋 Log files:")
    logger.info("  - Pipeline log: logs/pipeline.log")
    logger.info("  - Company IDs log: logs/company_ids.log")
    logger.info("  - Topics log: logs/top_topics_to_search.log")
    logger.info("  - News log: logs/news_search.log")

if __name__ == "__main__":
    main()
