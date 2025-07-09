from typing import Annotated, List, TypedDict, Optional, Dict, Any
from pydantic import BaseModel, Field, field_validator
import operator
import json

class SearchStrategy(BaseModel):
    """Individual search strategy definition."""
    tool_type: str = Field(
        description="Type of Bigdata tool to use: 'news', 'transcripts', 'filings', 'knowledge_graph'"
    )
    search_queries: List[str] = Field(
        description="List of search queries to execute for this strategy"
    )
    parameters: Dict[str, Any] = Field(
        default_factory=dict,
        description="Tool-specific parameters (filing_types, transcript_types, etc.)"
    )
    priority: int = Field(
        default=1,
        description="Search priority/importance (1-5, higher is more important)"
    )
    description: str = Field(
        description="Human-readable description of this search strategy"
    )
    
    @field_validator('parameters', mode='before')
    @classmethod
    def parse_parameters(cls, v):
        """Parse parameters from JSON string if needed."""
        if isinstance(v, str):
            try:
                # Parse JSON string to dictionary
                return json.loads(v)
            except json.JSONDecodeError:
                # If parsing fails, return empty dict
                print(f"Warning: Failed to parse parameters JSON string: {v}")
                return {}
        elif isinstance(v, dict):
            # If already a dict, return as is
            return v
        else:
            # If neither string nor dict, return empty dict
            return {}

class SearchStrategies(BaseModel):
    """List of search strategies for the workflow."""
    strategies: List[SearchStrategy] = Field(
        description="List of search strategies to execute"
    )

class SearchResult(BaseModel):
    """Results from a completed search strategy."""
    strategy: SearchStrategy = Field(
        description="The strategy that produced this result"
    )
    results: List[Dict[str, Any]] = Field(
        description="Raw search results from Bigdata API"
    )
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="Search metadata (timing, result count, etc.)"
    )
    quality_score: Optional[float] = Field(
        default=None,
        description="Result quality assessment (0.0-1.0)"
    )



# Input/Output states for the main graph
class BigdataSearchStateInput(TypedDict):
    topic: str  # Main search topic/question

class BigdataSearchStateOutput(TypedDict):
    final_results: str  # Compiled and formatted final search results
    source_metadata: Dict[str, Any]  # Source tracking and metadata

# Main graph state
class BigdataSearchState(TypedDict):
    # Input
    topic: str  # Main search topic/question
    search_depth: int  # How many different search strategies to use
    max_results_per_strategy: int  # Results per search strategy
    entity_preference: Optional[List[str]]  # Preferred entity IDs if known
    date_range: Optional[str]  # Global date range filter
    
    # Intermediate state
    search_strategies: List[SearchStrategy]  # Generated search plans
    completed_searches: Annotated[List[SearchResult], operator.add]  # Results from parallel searches
    
    # Output
    final_results: str  # Compiled and formatted final output
    source_metadata: Dict[str, Any]  # Source tracking and metadata

# Individual search execution state (for Send() API)
class SearchStrategyState(TypedDict):
    topic: str  # Original search topic
    strategy: SearchStrategy  # The strategy to execute
    entity_ids: Optional[List[str]]  # Available entity IDs
    global_date_range: Optional[str]  # Global date range filter
    completed_searches: List[SearchResult]  # Final key for Send() API aggregation

class SearchStrategyOutput(TypedDict):
    completed_searches: List[SearchResult]  # Final key for Send() API aggregation 