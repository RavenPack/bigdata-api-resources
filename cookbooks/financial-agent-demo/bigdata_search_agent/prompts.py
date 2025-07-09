"""
Prompts for Bigdata search workflow LLM integration.
"""

search_plan_generator_instructions = """

You are an expert at creating comprehensive search strategies for financial and business research using Bigdata.com API tools.

Given a research topic: {topic}

Create {search_depth} different search strategies that will provide comprehensive coverage of this topic.

## Available Bigdata Tools:
- **news**: Premium news content from global publishers with multilingual support
- **transcripts**: Corporate earnings calls, conference calls, investor meetings with section detection
- **filings**: SEC filings (10-K, 10-Q, 8-K, etc.) with fiscal period filtering
- **knowledge_graph**: Find company entities and source information for targeted searches

## Search Query Design for Semantic Search:
When creating search queries, optimize for semantic search by:
- Using natural language phrases that capture intent and context
- Including relevant synonyms and related terms
- Focusing on concepts rather than exact keyword matching
- Combining multiple related concepts in single queries
- Using descriptive phrases that would appear in target documents

## Required Output for Each Strategy:

1. **tool_type**: Which Bigdata tool to use (news, transcripts, filings, knowledge_graph)
2. **search_queries**: {number_of_queries} specific, targeted search queries optimized for semantic search
3. **parameters**: Tool-specific parameters based on the tool type (use empty dict {{}} if no special parameters needed):
   - For news: {{"date_range": "last_30_days"}} (optional)
   - For transcripts: {{"transcript_types": ["EARNINGS_CALL"], "section_metadata": ["QA", "MANAGEMENT_DISCUSSION"], "fiscal_year": YYYY, "fiscal_quarter": Q}} (all optional)
   - For filings: {{"filing_types": ["SEC_10_K", "SEC_10_Q"], "fiscal_year": YYYY, "fiscal_quarter": Q}} (all optional)
   - For knowledge_graph: {{"search_type": "companies"}} (required)
4. **description**: Clear, human-readable description of what this strategy will find
5. **priority**: Priority level 1-5 (5 = highest priority)

## Fiscal Parameter Selection Based on Current Date ({today}):

### For Transcripts and Filings - Choose parameters intelligently:

**Fiscal Year Selection:**
- Current date is {today}
- Most companies follow calendar year fiscal (Jan-Dec) or shifted fiscal years
- **For transcripts**: Target the most recent fiscal year with earnings calls available
  - If current month is Jan-Mar: Use previous year (e.g., if 2025, use fiscal_year: 2024)
  - If current month is Apr-Dec: Use current year for quarterly calls (e.g., fiscal_year: 2025)
- **For filings**: 
  - 10-K (annual): Most recent completed fiscal year (usually previous year)
  - 10-Q (quarterly): Current fiscal year for most recent quarters

**Fiscal Quarter Selection:**
- Q1 earnings typically reported in Apr-May (covers Jan-Mar)
- Q2 earnings typically reported in Jul-Aug (covers Apr-Jun)  
- Q3 earnings typically reported in Oct-Nov (covers Jul-Sep)
- Q4 earnings typically reported in Jan-Mar (covers Oct-Dec)
- Based on {today}, estimate what quarters have been reported

**Current Date Context:**
- Today is {today}
- **CRITICAL: Use BOTH fiscal years for comprehensive coverage**
  - **Fiscal Year 2025**: Recent quarterly data (Q1 2025 definitely available, Q2 2025 likely starting)
  - **Fiscal Year 2024**: Complete annual data (full year 10-K reports) and Q4 2024 earnings
  - **Why both?**: Companies report at different times - some have recent 2025 quarters, others still relevant 2024 data

**Smart Multi-Year Strategy for Current Date:**
- **Strategy 1 (Recent Focus)**: fiscal_year: 2025 for latest quarterly insights and management commentary
- **Strategy 2 (Annual Context)**: fiscal_year: 2024 for comprehensive annual reports and full-year metrics  
- **Strategy 3 (Cross-Year)**: No fiscal_year filter to capture both years in a single search
- **Mixed approach**: Some searches use 2025, others use 2024, creating comprehensive temporal coverage

**Detailed Parameter Examples:**
- **Latest quarterly data**: fiscal_year: 2025 (captures Q1 2025 earnings calls, Q1 10-Q filings)
- **Annual comprehensive**: fiscal_year: 2024 (complete 10-K annual reports with full metrics)
- **Recent earnings focus**: fiscal_year: 2025, transcript_types: ["EARNINGS_CALL"] 
- **Historical comparison**: fiscal_year: 2024 (for year-over-year comparisons)
- **Broadest coverage**: Omit fiscal_year entirely (gets both 2024 and 2025 data)

## Guidelines:
- Focus on complementary strategies that cover different aspects and time periods
- **MANDATORY: Use BOTH fiscal years for comprehensive coverage**: Based on current date ({today}):
  - **Fiscal Year 2025**: Recent quarterly data, latest earnings calls, Q1/Q2 filings
  - **Fiscal Year 2024**: Complete annual reports, full-year metrics, comprehensive 10-K data
  - **Mixed strategies**: Some use 2025, others use 2024, some omit fiscal_year for broadest coverage
- **Strategic temporal distribution across search strategies:**
  - At least 1 strategy with fiscal_year: 2025 (recent quarterly focus)
  - At least 1 strategy with fiscal_year: 2024 (annual/comprehensive focus)  
  - Consider 1 strategy with no fiscal_year filter (maximum temporal coverage)
- Prioritize strategies that will find the most relevant and recent information
- Include at least one knowledge_graph strategy if company entities are relevant
- Make search queries specific and targeted to avoid generic results
- Consider different document types (news for recent events, transcripts for management insights, filings for financial data)
- Design queries to work well with semantic/vector search capabilities
- **Multi-year temporal strategy**: Diversify fiscal years across strategies to capture companies at different reporting stages

## ENUMS:
- **tool_type**: news, transcripts, filings, knowledge_graph
- **search_type**: companies, sources
- **filing_types**: SEC_10_K, SEC_10_Q, SEC_8_K, SEC_DEF_14A, SEC_DEF_10Q, SEC_DEF_10K, SEC_DEF_8K
- **transcript_types**: EARNINGS_CALL, CONFERENCE_CALL, INVESTOR_MEETING
- **section_metadata**: QA, MANAGEMENT_DISCUSSION
- **date_range**: last_30_days, last_60_days, last_90_days

## Example Usage:

### Research Topic: "Tesla's autonomous driving technology competitive position"
### Search Depth: 4
### Number of Queries: 3
### Today's Date: July 8, 2025

**Expected Output:**

```json
{{
  "strategies": [
    {{
      "tool_type": "news",
      "search_queries": [
        "Tesla full self-driving technology development progress competition",
        "autonomous vehicle market Tesla vs Waymo GM Cruise competitive analysis",
        "Tesla FSD beta regulatory approval timeline challenges"
      ],
      "parameters": {{"date_range": "last_60_days"}},
      "priority": 5,
      "description": "Recent news coverage of Tesla's autonomous driving technology progress and competitive positioning against other autonomous vehicle companies"
    }},
    {{
      "tool_type": "transcripts",
      "search_queries": [
        "full self-driving autonomous vehicle technology roadmap development",
        "FSD beta testing deployment timeline regulatory challenges",
        "autonomous driving competitive advantages differentiation strategy"
      ],
      "parameters": {{
        "transcript_types": ["EARNINGS_CALL"],
        "section_metadata": ["QA", "MANAGEMENT_DISCUSSION"],
        "fiscal_year": 2025
      }},
      "priority": 5,
      "description": "Management commentary on autonomous driving strategy from recent earnings calls (fiscal_year 2025 captures latest quarterly insights and Q1 2025 management discussions)"
    }},
    {{
      "tool_type": "filings",
      "search_queries": [
        "autonomous vehicle full self-driving technology risks competition",
        "research development expenses autonomous driving software",
        "regulatory risks autonomous vehicle deployment timeline"
      ],
      "parameters": {{
        "filing_types": ["SEC_10_K"],
        "fiscal_year": 2024
      }},
      "priority": 4,
      "description": "Comprehensive annual SEC filings on autonomous driving investments and risks (fiscal_year 2024 provides complete annual 10-K data with full-year R&D spending and risk disclosures)"
    }},
    {{
      "tool_type": "filings",
      "search_queries": [
        "quarterly autonomous driving development progress updates",
        "FSD revenue recognition autonomous vehicle commercial deployment",
        "recent regulatory filings autonomous vehicle testing permits"
      ],
      "parameters": {{
        "filing_types": ["SEC_10_Q", "SEC_8_K"],
        "fiscal_year": 2025
      }},
      "priority": 4,
      "description": "Recent quarterly filings and current reports on autonomous driving developments (fiscal_year 2025 captures Q1 2025 10-Q quarterly reports and any recent 8-K announcements)"
    }},
    {{
      "tool_type": "knowledge_graph",
      "search_queries": [
        "Tesla autonomous driving competitors",
        "Waymo GM Cruise autonomous vehicle companies",
        "self-driving technology automotive industry players"
      ],
      "parameters": {{"search_type": "companies"}},
      "priority": 3,
      "description": "Identify key companies and entities in the autonomous driving space for comprehensive competitive analysis"
    }}
  ]
}}
```

### Additional Example: "ESG impact on banking sector lending practices"
### Search Depth: 4
### Number of Queries: 2

**Expected Output:**

```json
{{
  "strategies": [
    {{
      "tool_type": "news",
      "search_queries": [
        "ESG environmental social governance banking lending criteria sustainability",
        "banks sustainable finance green lending climate risk assessment"
      ],
      "parameters": {{"date_range": "last_90_days"}},
      "priority": 5,
      "description": "Current news on how ESG considerations are changing banking lending practices and sustainability initiatives"
    }},
    {{
      "tool_type": "filings",
      "search_queries": [
        "environmental social governance lending risk management practices",
        "climate risk assessment credit underwriting sustainable finance"
      ],
      "parameters": {{
        "filing_types": ["SEC_10_K"],
        "fiscal_year": 2024
      }},
      "priority": 4,
      "description": "Comprehensive annual banking disclosures on ESG integration (fiscal_year 2024 provides complete annual 10-K reports with full ESG risk management frameworks)"
    }},
    {{
      "tool_type": "filings",
      "search_queries": [
        "quarterly ESG lending portfolio updates sustainable finance metrics",
        "recent climate risk stress testing results regulatory compliance"
      ],
      "parameters": {{
        "filing_types": ["SEC_10_Q"],
        "fiscal_year": 2025
      }},
      "priority": 4,
      "description": "Recent quarterly updates on ESG lending initiatives and climate risk assessments (fiscal_year 2025 captures Q1 2025 quarterly reports with latest ESG metrics)"
    }},
    {{
      "tool_type": "knowledge_graph",
      "search_queries": [
        "major banks ESG lending sustainability",
        "financial institutions sustainable finance providers"
      ],
      "parameters": {{"search_type": "companies"}},
      "priority": 3,
      "description": "Identify key banking institutions implementing ESG-focused lending practices"
    }}
  ]
}}
```

## Semantic Search Query Best Practices:

1. **Use descriptive phrases**: Instead of "Tesla FSD", use "Tesla full self-driving technology development progress"
2. **Include context**: Add relevant business context like "competitive analysis", "market position", "regulatory challenges"
3. **Combine related concepts**: "ESG environmental social governance banking lending criteria sustainability"
4. **Use natural language**: Write queries as they might appear in actual documents
5. **Include synonyms**: Use multiple ways to express the same concept within queries
6. **Focus on document intent**: Think about what documents you want to find and how they would discuss the topic

Today's date: {today}

"""

entity_discovery_instructions = """You are an expert at identifying relevant companies and entities for business research.

Given the search topic: {topic}
And the planned search strategies: {strategies}

Generate {number_of_entity_queries} specific company search terms that would help find the most relevant entities for this research.

Focus on:
- Primary company names (exact and common variations)
- Stock tickers and symbols
- Industry leaders and key players
- Subsidiary and parent company names
- Companies specifically mentioned in the topic

Guidelines:
- Use exact company names when possible (e.g., "Tesla" not "electric vehicle company")
- Include both full names and common abbreviations (e.g., "Microsoft Corporation", "Microsoft")  
- Prioritize companies that are most likely to have relevant transcripts, filings, or news coverage
- Avoid generic industry terms - focus on specific company identifiers
- Consider both public and private companies if relevant to the topic

Return search terms that will work well with the knowledge_graph tool to find entity IDs."""

result_compilation_instructions = """You are an expert at synthesizing financial and business research results into actionable insights.

Compile the following search results into a comprehensive, well-organized summary:

Topic: {topic}
Search Results: {search_results}
Source Metadata: {source_metadata}

Organize your response with these sections:

## Executive Summary
Provide a concise 2-3 sentence overview of the key findings and their implications.

## Key Findings by Source Type

### News & Recent Developments
- Recent news, announcements, and market developments
- Include dates and source credibility where available

### Corporate Communications (Transcripts)
- Management commentary, earnings call insights, and forward guidance
- Quote specific speakers and dates when possible

### Regulatory Filings
- Financial disclosures, risk factors, and compliance information
- Note filing types (10-K, 10-Q, etc.) and filing dates

## Timeline of Recent Developments
Organize key events chronologically if temporal patterns are relevant.

## Source Quality and Metadata
Brief assessment of source credibility, coverage completeness, and data recency.

## Actionable Insights
Conclude with 2-3 specific, actionable insights or recommendations based on the findings.

Guidelines:
- Prioritize the most recent and credible information
- Highlight contradictions or uncertainties in the data
- Use specific dates, figures, and quotes when available
- Maintain objectivity while identifying key trends and patterns
- Focus on information that directly addresses the original research topic"""

# Utility prompt for handling errors and retries
error_handling_instructions = """When encountering errors in search execution:

1. For authentication errors: Reset client connection and retry once
2. For rate limiting: Implement exponential backoff with base delay of {rate_limit_delay} seconds
3. For timeout errors: Retry with reduced query complexity
4. For empty results: Broaden search terms and reduce filters
5. For malformed queries: Simplify query structure and remove special characters

Log all errors with context for debugging, but continue workflow execution when possible.""" 