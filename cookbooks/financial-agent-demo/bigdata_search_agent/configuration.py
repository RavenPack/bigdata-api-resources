import os
import warnings
import logging
from enum import Enum
from dataclasses import dataclass, fields
from typing import Any, Optional, Dict, Literal

from langchain_core.runnables import RunnableConfig
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Suppress warnings and verbose logging for clean output
warnings.filterwarnings('ignore')
os.environ.setdefault('GRPC_VERBOSITY', 'ERROR')
os.environ.setdefault('GLOG_minloglevel', '2')
os.environ.setdefault('TF_CPP_MIN_LOG_LEVEL', '3')

# Configure logging to suppress unwanted messages
logging.getLogger('google').setLevel(logging.ERROR)
logging.getLogger('google.auth').setLevel(logging.ERROR)
logging.getLogger('google.generativeai').setLevel(logging.ERROR)

class BigdataToolType(Enum):
    """Available Bigdata search tool types."""
    NEWS = "news"
    TRANSCRIPTS = "transcripts"
    FILINGS = "filings"
    KNOWLEDGE_GRAPH = "knowledge_graph"

@dataclass(kw_only=True)
class BigdataSearchConfiguration:
    """Configuration for the Bigdata search workflow."""
    
    # LLM Configuration (reuse existing model patterns)
    planner_provider: str = "google_genai"
    planner_model: str = "gemini-2.5-flash"  
    planner_model_kwargs: Optional[Dict[str, Any]] = None
    writer_provider: str = "google_genai"
    writer_model: str = "gemini-2.5-flash"
    writer_model_kwargs: Optional[Dict[str, Any]] = None
    
    # Search Configuration
    search_depth: int = 5  # Number of different search strategies to generate
    max_results_per_strategy: int = 30  # Results per search strategy
    number_of_queries: int = 3  # Number of queries per search strategy
    enable_entity_discovery: bool = True  # Whether to auto-discover entities
    enable_cross_strategy_deduplication: bool = True  # Remove duplicates across strategies
    
    # Bigdata API Configuration
    bigdata_rate_limit_delay: float = 1.0  # Delay between API calls (seconds)
    bigdata_max_retries: int = 3  # Maximum number of retries for failed requests
    bigdata_timeout: int = 60  # Request timeout in seconds
    bigdata_rerank_threshold: float = 0.1  # Default rerank threshold for similarity searches
    
    # Workflow Configuration
    max_structured_output_retries: int = 3  # Maximum retries for structured output
    include_source_metadata: bool = True  # Whether to include detailed source metadata
    debug_mode: bool = False  # Enable detailed debug output and parameter logging
    
    # Default tool preferences (can be overridden per workflow)
    default_tool_types: list[str] = None  # Default to auto-selection
    default_date_range: Optional[str] = None  # Default date range filter
    
    def __post_init__(self):
        """Set default tool types if not specified."""
        if self.default_tool_types is None:
            self.default_tool_types = [
                BigdataToolType.NEWS.value,
                BigdataToolType.TRANSCRIPTS.value,
                BigdataToolType.FILINGS.value
            ]

    @classmethod
    def from_runnable_config(
        cls, config: Optional[RunnableConfig] = None
    ) -> "BigdataSearchConfiguration":
        """Create a BigdataSearchConfiguration instance from a RunnableConfig's configurable dict.
        
        Args:
            config: Optional RunnableConfig containing configuration values
            
        Returns:
            BigdataSearchConfiguration instance with values from config, defaults for unspecified fields
        """
        configurable = (
            config["configurable"] if config and "configurable" in config else {}
        )
        values: dict[str, Any] = {}
        
        for f in fields(cls):
            if not f.init:
                continue
                
            # Get value from config
            value = configurable.get(f.name)
            
            # Only include non-None values
            if value is not None:
                values[f.name] = value
                
        return cls(**values) 