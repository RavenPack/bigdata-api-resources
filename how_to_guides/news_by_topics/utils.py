#!/usr/bin/env python3
"""
Utility functions for news analysis scripts.
"""

import logging
from datetime import datetime, timedelta

# Configure logging for utils
logger = logging.getLogger(__name__)


def get_24_hours_timestamp_range() -> tuple[str, str]:
    """
    Get timestamp range for the last 24 hours
    
    Returns:
        tuple: (start_timestamp, end_timestamp) in ISO format
    """
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=24)
    
    start_timestamp = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_timestamp = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    logger.info(f"Search time range: {start_timestamp} to {end_timestamp}")
    return start_timestamp, end_timestamp
