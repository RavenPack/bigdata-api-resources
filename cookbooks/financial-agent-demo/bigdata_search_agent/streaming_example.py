"""
Interactive streaming example demonstrating real-time Bigdata search workflow.

This example shows how to:
1. Use multiple stream modes for comprehensive progress monitoring
2. Display live progress indicators and ASCII dashboards
3. Show real-time API execution status and performance metrics
4. Stream token-by-token report generation using LangGraph messages mode
5. Combine custom events with LLM token streaming (default behavior)

Usage:
    python -m bigdata_search_agent.streaming_example
    python -m bigdata_search_agent.streaming_example --debug
"""

import asyncio
import os
import time
import logging
import warnings
import argparse
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Suppress warnings and gRPC messages for clean output
warnings.filterwarnings('ignore')
os.environ['GRPC_VERBOSITY'] = 'ERROR'
os.environ['GLOG_minloglevel'] = '2'
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'

# Configure logging to suppress unwanted messages
logging.getLogger('google').setLevel(logging.ERROR)
logging.getLogger('google.auth').setLevel(logging.ERROR)
logging.getLogger('google.generativeai').setLevel(logging.ERROR)

from bigdata_search_agent import (
    bigdata_search_graph,
    BigdataSearchConfiguration,
)

# Rich imports for beautiful output
from rich.console import Console
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn, TimeRemainingColumn
from rich.panel import Panel
from rich.table import Table
from rich.markdown import Markdown
from rich.layout import Layout
from rich.live import Live
from rich.text import Text
from rich.columns import Columns
from rich import box

class StreamingProgressMonitor:
    """Enhanced progress monitor for the streaming workflow with Rich formatting."""
    
    def __init__(self):
        self.start_time = time.time()
        self.search_status = {}
        self.overall_progress = {
            "planning": "⏳ Pending",
            "searching": "⏳ Pending", 
            "gathering": "⏳ Pending",
            "compiling": "⏳ Pending"
        }
        self.console = Console()
        self.progress = Progress(
            TextColumn("[bold blue]{task.description}"),
            BarColumn(bar_width=40),
            "[progress.percentage]{task.percentage:>3.1f}%",
            TimeElapsedColumn(),
            console=self.console
        )
        self.overall_task = None
        self.streaming_tokens = ""  # Store streaming tokens
        self.streaming_active = False
        
    def print_header(self):
        """Print beautiful Rich header."""
        header_text = Text("🔍 BIGDATA INTERACTIVE SEARCH WORKFLOW", style="bold cyan")
        subtitle_text = Text("Real-time dual streaming demonstration", style="italic")
        
        header_panel = Panel(
            Text.assemble(header_text, "\n", subtitle_text),
            border_style="cyan",
            padding=(1, 2)
        )
        self.console.print(header_panel)
        
    def start_progress(self):
        """Initialize the Rich progress bar."""
        self.progress.start()
        self.overall_task = self.progress.add_task("Overall Progress", total=4)
        
    def update_progress(self, completed_steps: int):
        """Update the Rich progress bar."""
        if self.overall_task is not None:
            self.progress.update(self.overall_task, completed=completed_steps)
        
    def stop_progress(self):
        """Stop the Rich progress bar."""
        self.progress.stop()
        
    def start_token_streaming(self):
        """Start streaming token display."""
        self.streaming_active = True
        self.streaming_tokens = ""
        self.console.print("\n🔄 Starting LLM token streaming...", style="bold blue")
        
    def add_streaming_token(self, token: str):
        """Add a token to the streaming display."""
        if self.streaming_active:
            self.streaming_tokens += token
            # Print token without newline for real-time effect
            self.console.print(token, end="", style="green")
            
    def end_token_streaming(self):
        """End token streaming and display clean markdown."""
        if self.streaming_active:
            self.streaming_active = False
            self.console.print("\n\n✅ LLM streaming complete!", style="bold green")
            
            # Display the clean markdown version
            if self.streaming_tokens:
                markdown_panel = Panel(
                    Markdown(self.streaming_tokens),
                    title="📄 Clean Markdown Report",
                    border_style="bright_green",
                    padding=(1, 2)
                )
                self.console.print(markdown_panel)
        
    def create_status_table(self):
        """Create a status table for overall progress."""
        table = Table(title="🔄 Workflow Progress", box=box.ROUNDED)
        table.add_column("Phase", style="cyan", no_wrap=True)
        table.add_column("Status", style="magenta")
        
        for phase, status in self.overall_progress.items():
            table.add_row(phase.capitalize(), status)
        
        return table
        
    def create_search_status_table(self):
        """Create a table showing search status for each tool."""
        if not self.search_status:
            return None
            
        table = Table(title="🔍 Search Status", box=box.ROUNDED)
        table.add_column("Tool Type", style="yellow", no_wrap=True)
        table.add_column("Status", style="green")
        
        for tool_type, status in self.search_status.items():
            table.add_row(tool_type.upper(), status)
        
        return table
        
    def print_status_dashboard(self):
        """Print a comprehensive status dashboard."""
        status_table = self.create_status_table()
        search_table = self.create_search_status_table()
        
        self.console.print(status_table)
        if search_table:
            self.console.print(search_table)
            
    def update_stage(self, stage: str, status: str):
        """Update the status of a workflow stage."""
        self.overall_progress[stage] = status
        
    def update_search_status(self, tool_type: str, status: str):
        """Update the status of a specific search tool."""
        self.search_status[tool_type] = status
        
    def print_message(self, message: str, style: str = "default"):
        """Print a message with the given style."""
        self.console.print(message, style=style)
        
    def print_success(self, message: str):
        """Print a success message."""
        self.console.print(f"✅ {message}", style="bold green")
        
    def print_error(self, message: str):
        """Print an error message."""
        self.console.print(f"❌ {message}", style="bold red")
        
    def print_info(self, message: str):
        """Print an info message."""
        self.console.print(f"ℹ️ {message}", style="bold blue")
        
    def print_warning(self, message: str):
        """Print a warning message."""
        self.console.print(f"⚠️ {message}", style="bold yellow")

async def handle_custom_stream(chunk, monitor):
    """Handle custom stream events with Rich formatting."""
    chunk_type = chunk.get("type", "unknown")
    message = chunk.get("message", "")
    
    # Planning phase events
    if chunk_type == "planning_start":
        monitor.update_stage("planning", "🧠 Analyzing...")
        monitor.console.print(f"\n{message}", style="bold blue")
        
    elif chunk_type == "planning_config":
        monitor.console.print(f"  {message}", style="cyan")
        
    elif chunk_type == "planning_model":
        monitor.console.print(f"  {message}", style="dim cyan")
        
    elif chunk_type == "planning_thinking":
        monitor.console.print(f"  {message}", style="magenta")
        
    elif chunk_type == "strategy_preview":
        strategy_panel = Panel(
            message.replace("📊 Strategy ", "Strategy "),
            title=f"Strategy {chunk.get('strategy_index', '?')}",
            border_style="green",
            padding=(0, 1)
        )
        monitor.console.print(strategy_panel)
        
    elif chunk_type == "query_preview":
        monitor.console.print(f"    {message}", style="dim green")
        
    elif chunk_type == "planning_ready":
        monitor.update_stage("planning", "✅ Complete")
        monitor.print_success(message.replace("🚀 ", ""))
        
    # Search execution events
    elif chunk_type == "search_start":
        tool_type = chunk.get("tool_type", "unknown")
        monitor.update_stage("searching", "🔍 Executing...")
        monitor.update_search_status(tool_type, "⏳ Starting...")
        search_panel = Panel(
            message,
            title=f"Starting {tool_type.upper()} Search",
            border_style="yellow",
            padding=(0, 1)
        )
        monitor.console.print(search_panel)
        
    elif chunk_type == "final_parameters":
        tool_type = chunk.get("tool_type", "unknown")
        parameters = chunk.get("parameters", {})
        
        # Create a formatted parameter display
        param_lines = []
        for key, value in parameters.items():
            if isinstance(value, list):
                if len(value) <= 3:
                    param_lines.append(f"    {key}: {value}")
                else:
                    param_lines.append(f"    {key}: [{len(value)} items]")
            elif isinstance(value, str) and len(value) > 50:
                param_lines.append(f"    {key}: {value[:50]}...")
            else:
                param_lines.append(f"    {key}: {value}")
        
        param_display = "\n".join(param_lines)
        
        parameter_panel = Panel(
            f"Tool: {tool_type.upper()}\n\n{param_display}",
            title="🔧 Final Tool Parameters",
            border_style="blue",
            padding=(0, 1)
        )
        monitor.console.print(parameter_panel)
        
    elif chunk_type == "api_start":
        tool_type = chunk.get("tool_type", "unknown")
        monitor.update_search_status(tool_type, "🚀 API Call...")
        monitor.console.print(f"  {message}", style="yellow")
        
    elif chunk_type == "api_success":
        tool_type = chunk.get("tool_type", "unknown")
        execution_time = chunk.get("execution_time", 0)
        monitor.update_search_status(tool_type, f"✅ Done ({execution_time:.1f}s)")
        monitor.print_success(message.replace("✅ ", ""))
        
    elif chunk_type == "result_quality":
        quality = chunk.get("quality", "🟡 Medium")
        content_length = chunk.get("content_length", 0)
        quality_text = Text(f"  📊 Result quality: {quality} ({content_length:,} chars)")
        
        # Color based on quality
        if "🟢" in quality:
            quality_text.stylize("bold green")
        elif "🟡" in quality:
            quality_text.stylize("bold yellow") 
        elif "🔴" in quality:
            quality_text.stylize("bold red")
            
        monitor.console.print(quality_text)
        
    elif chunk_type == "api_error":
        tool_type = chunk.get("tool_type", "unknown")
        monitor.update_search_status(tool_type, "❌ Failed")
        monitor.print_error(message.replace("❌ ", ""))
        
    elif chunk_type == "search_complete":
        tool_type = chunk.get("tool_type", "unknown")
        success = chunk.get("success", False)
        status = "✅ Success" if success else "❌ Failed"
        monitor.update_search_status(tool_type, status)
        
    # Gathering phase events
    elif chunk_type == "gathering_start":
        monitor.update_stage("searching", "✅ Complete")
        monitor.update_stage("gathering", "📊 Processing...")
        monitor.console.print(f"\n{message}", style="bold magenta")
        
    elif chunk_type == "debug_mode_enabled":
        debug_panel = Panel(
            "🔧 Debug mode active - detailed tool parameters will be displayed",
            title="🐛 Debug Mode",
            border_style="cyan",
            padding=(0, 1)
        )
        monitor.console.print(debug_panel)
        
    elif chunk_type == "debug_tool_parameters":
        search_index = chunk.get("search_index", 0)
        tool_type = chunk.get("tool_type", "unknown")
        strategy_description = chunk.get("strategy_description", "No description")
        search_queries = chunk.get("search_queries", [])
        parameters = chunk.get("parameters", {})
        success = chunk.get("success", False)
        execution_time = chunk.get("execution_time", 0)
        
        # Format parameters for display
        param_lines = []
        for key, value in parameters.items():
            if isinstance(value, list):
                if len(value) <= 3:
                    param_lines.append(f"    {key}: {value}")
                else:
                    param_lines.append(f"    {key}: [{len(value)} items: {', '.join(str(v) for v in value[:3])}...]")
            elif isinstance(value, str) and len(value) > 60:
                param_lines.append(f"    {key}: {value[:60]}...")
            else:
                param_lines.append(f"    {key}: {value}")
        
        # Format queries for display  
        query_lines = []
        for i, query in enumerate(search_queries, 1):
            if len(query) > 80:
                query_lines.append(f"    Query {i}: {query[:80]}...")
            else:
                query_lines.append(f"    Query {i}: {query}")
        
        status_icon = "✅" if success else "❌"
        status_text = f"{status_icon} {tool_type.upper()} ({execution_time:.1f}s)"
        
        debug_content = f"""Search Strategy {search_index}:
{strategy_description}

Search Queries:
{chr(10).join(query_lines)}

Tool Parameters:
{chr(10).join(param_lines)}

Execution: {status_text}"""
        
        debug_parameter_panel = Panel(
            debug_content,
            title=f"🔧 Debug: {tool_type.upper()} Parameters",
            border_style="blue",
            padding=(0, 1)
        )
        monitor.console.print(debug_parameter_panel)
        
    elif chunk_type == "success_analysis":
        monitor.console.print(f"  {message}", style="green")
        
    elif chunk_type == "performance_metrics":
        monitor.console.print(f"  {message}", style="cyan")
        
    elif chunk_type == "gathering_complete":
        monitor.update_stage("gathering", "✅ Complete")
        monitor.print_success(message.replace("✅ ", ""))
        
    # Compilation phase events
    elif chunk_type == "compilation_start":
        monitor.update_stage("compiling", "📝 Writing...")
        monitor.console.print(f"\n{message}", style="bold green")
        
    elif chunk_type == "synthesis_start":
        monitor.console.print(f"  {message}", style="magenta")
        # Start token streaming display
        monitor.start_token_streaming()
        
    elif chunk_type == "synthesis_complete":
        synthesis_time = chunk.get("synthesis_time", 0)
        report_length = chunk.get("report_length", 0)
        monitor.print_success(message.replace("✅ ", ""))
        # End token streaming
        monitor.end_token_streaming()
        
    elif chunk_type == "report_stats":
        monitor.console.print(f"  {message}", style="cyan")
        
    elif chunk_type == "markdown_output":
        content = chunk.get("content", "")
        if content and not monitor.streaming_active:
            # Display markdown if not already shown during streaming
            markdown_panel = Panel(
                Markdown(content),
                title="📄 Final Research Report",
                border_style="bright_green",
                padding=(1, 2)
            )
            monitor.console.print(markdown_panel)
        
    elif chunk_type == "workflow_complete":
        monitor.update_stage("compiling", "✅ Complete")
        total_time = chunk.get("total_time", 0)
        
        completion_panel = Panel(
            f"{message}\n🕐 Total execution time: {total_time:.1f} seconds",
            title="🎉 Workflow Complete",
            border_style="bright_green",
            padding=(1, 2)
        )
        monitor.console.print(completion_panel)

async def handle_message_stream(chunk, monitor):
    """Handle LLM message stream tokens."""
    if isinstance(chunk, tuple) and len(chunk) == 2:
        message_chunk, metadata = chunk
        
        # Extract token from the message chunk
        if hasattr(message_chunk, 'content') and message_chunk.content:
            monitor.add_streaming_token(message_chunk.content)

async def main(debug_mode=False):
    """Run the interactive streaming workflow example with dual streaming."""
    
    console = Console()
    
    if debug_mode:
        console.print("🐛 Debug mode enabled - detailed tool parameters will be shown", style="cyan")
    
    console.print("🔄 Dual streaming mode enabled - showing both custom events and LLM tokens", style="cyan")
    
    # Check if required environment variables are set
    if not os.environ.get("BIGDATA_USERNAME") or not os.environ.get("BIGDATA_PASSWORD"):
        console.print("❌ Error: BIGDATA_USERNAME and BIGDATA_PASSWORD environment variables must be set", style="bold red")
        console.print("Please set them in your environment or .env file", style="yellow")
        return
    
    # Check if Google GenAI API key is set for LLM
    if not os.environ.get("GOOGLE_API_KEY"):
        console.print("❌ Error: GOOGLE_API_KEY environment variable must be set for LLM functionality", style="bold red")
        return
    
    
    monitor = StreamingProgressMonitor()
    monitor.print_header()
    
    # Define the search topic
    search_topic = "Write me a deep dive report on how demand for memory is expected to grow with the adoption of AI. I’m particularly interested in evaluating the impact on Micron."
    topic_panel = Panel(
        f"🎯 Research Topic: [bold cyan]{search_topic}[/bold cyan]",
        border_style="blue",
        padding=(0, 1)
    )
    monitor.console.print(topic_panel)
    
    # Set up configuration
    config = {
        "configurable": {
            "search_depth": 2,  # Generate 2 search strategies
            "max_results_per_strategy": 30,  # Use configuration default
            "number_of_queries": 2,  # 2 queries per strategy
            "bigdata_rate_limit_delay": 1.5,  # Be conservative with rate limits
            "planner_provider": "google_genai",
            "planner_model": "gemini-2.5-flash",
            "writer_provider": "google_genai", 
            "writer_model": "gemini-2.5-flash",
            "debug_mode": debug_mode,  # Enable debug mode if requested
        }
    }
    
    # Prepare input state
    input_state = {
        "topic": search_topic,
        "search_depth": 2,
        "max_results_per_strategy": 20,
        "entity_preference": None,  # Can provide entity IDs if known
        "date_range": "last_90_days",  # Focus on recent information
    }
    
    # Create configuration table
    config_table = Table(title="⚙️ Workflow Configuration", box=box.ROUNDED)
    config_table.add_column("Setting", style="cyan", no_wrap=True)
    config_table.add_column("Value", style="magenta")
    
    config_table.add_row("Search Strategies", str(config['configurable']['search_depth']))
    config_table.add_row("Results per Strategy", str(config['configurable']['max_results_per_strategy']))
    config_table.add_row("Queries per Strategy", str(config['configurable']['number_of_queries']))
    config_table.add_row("Rate Limit Delay", f"{config['configurable']['bigdata_rate_limit_delay']}s")
    config_table.add_row("Planner Model", f"{config['configurable']['planner_provider']}:{config['configurable']['planner_model']}")
    config_table.add_row("Writer Model", f"{config['configurable']['writer_provider']}:{config['configurable']['writer_model']}")
    config_table.add_row("Streaming Mode", "🔄 Dual Stream (Default)")
    
    monitor.console.print(config_table)
    
    try:
        start_panel = Panel(
            "🚀 Starting interactive dual streaming workflow...\n   Watch real-time progress below!",
            title="🎬 Workflow Starting",
            border_style="bright_blue",
            padding=(1, 2)
        )
        monitor.console.print(start_panel)
        
        # Start the Rich progress bar
        monitor.start_progress()
        
        # Use dual streaming mode by default: custom events AND LLM messages
        monitor.console.print("\n🔄 Starting dual streaming mode...", style="bold cyan")
        
        # Create tasks for both streaming modes
        custom_task = asyncio.create_task(stream_custom_events(input_state, config, monitor))
        message_task = asyncio.create_task(stream_messages(input_state, config, monitor))
        
        # Run both tasks concurrently
        await asyncio.gather(custom_task, message_task)
        
        # Stop the progress bar
        monitor.stop_progress()
        
        # Display final statistics
        monitor.console.print("\n📊 Displaying final workflow statistics...", style="bold yellow")
        final_result = await bigdata_search_graph.ainvoke(input_state, config)
        
        if "source_metadata" in final_result:
            metadata = final_result["source_metadata"]
            
            stats_table = Table(title="📈 Workflow Statistics", box=box.DOUBLE_EDGE)
            stats_table.add_column("Metric", style="cyan", no_wrap=True)
            stats_table.add_column("Value", style="magenta")
            
            stats_table.add_row("Total Searches", str(metadata.get('total_searches', 0)))
            stats_table.add_row("Successful Searches", f"[green]{metadata.get('successful_searches', 0)}[/green]")
            stats_table.add_row("Success Rate", f"{(metadata.get('successful_searches', 0) / max(metadata.get('total_searches', 1), 1) * 100):.1f}%")
            stats_table.add_row("Total Execution Time", f"{metadata.get('total_execution_time', 0):.1f}s")
            stats_table.add_row("Average Time per Search", f"{metadata.get('average_execution_time', 0):.1f}s")
            stats_table.add_row("Total Content Length", f"{metadata.get('total_content_length', 0):,} chars")
            
            # Tool distribution
            tool_dist = metadata.get('tool_type_distribution', {})
            for tool, count in tool_dist.items():
                stats_table.add_row(f"  {tool.capitalize()} Searches", str(count))
                
            monitor.console.print(stats_table)
        
        # Final success message
        total_demo_time = time.time() - monitor.start_time
        success_panel = Panel(
            f"🎉 Interactive streaming workflow completed successfully!\n"
            f"🔄 Streaming mode: Dual Stream (Default)\n"
            f"🕐 Total demo time: {total_demo_time:.1f} seconds",
            title="✨ Success",
            border_style="bright_green",
            padding=(1, 2)
        )
        monitor.console.print(success_panel)
        
    except Exception as e:
        error_panel = Panel(
            f"❌ Error executing streaming workflow: {str(e)}\n"
            f"🔧 Error type: {type(e).__name__}\n"
            f"💡 Check your environment variables and network connection",
            title="💥 Workflow Error",
            border_style="red",
            padding=(1, 2)
        )
        monitor.console.print(error_panel)
        
        # Display partial results if available
        if monitor.search_status:
            partial_table = Table(title="📊 Partial Execution Status", box=box.ROUNDED)
            partial_table.add_column("Tool Type", style="yellow", no_wrap=True)
            partial_table.add_column("Status", style="red")
            
            for tool_type, status in monitor.search_status.items():
                partial_table.add_row(tool_type.upper(), status)
                
            monitor.console.print(partial_table)
        
        # Print more detailed error info for debugging
        import traceback
        monitor.console.print("\n🔍 Detailed error traceback:", style="dim red")
        traceback.print_exc()

async def stream_custom_events(input_state, config, monitor):
    """Stream custom events."""
    async for chunk in bigdata_search_graph.astream(
        input_state, 
        config=config,
        stream_mode="custom"
    ):
        await handle_custom_stream(chunk, monitor)
        
        # Update progress display periodically
        if chunk.get("type") in ["planning_complete", "gathering_complete", "synthesis_complete"]:
            completed_steps = sum(1 for status in monitor.overall_progress.values() if "✅" in status)
            monitor.update_progress(completed_steps)
            monitor.print_status_dashboard()

async def stream_messages(input_state, config, monitor):
    """Stream LLM messages."""
    async for chunk in bigdata_search_graph.astream(
        input_state, 
        config=config,
        stream_mode="messages"
    ):
        await handle_message_stream(chunk, monitor)

def run_streaming_example():
    """Synchronous wrapper for the async main function."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Run Bigdata search streaming workflow with real-time progress monitoring"
    )
    parser.add_argument(
        "--debug", 
        action="store_true",
        help="Enable debug mode to show detailed tool parameters and execution info"
    )
    args = parser.parse_args()
    
    console = Console()
    
    demo_title = "🎭 Demo Starting"
    if args.debug:
        demo_title += " (Debug Mode)"
    
    demo_panel = Panel(
        "🎬 Starting Rich Streaming Demo with Dual Stream (Default)\n   Press Ctrl+C to interrupt...",
        title=demo_title,
        border_style="bright_cyan",
        padding=(1, 2)
    )
    console.print(demo_panel)
    
    try:
        asyncio.run(main(debug_mode=args.debug))
    except KeyboardInterrupt:
        interrupt_panel = Panel(
            "⏹️ Demo interrupted by user",
            title="🛑 Interrupted",
            border_style="yellow",
            padding=(1, 2)
        )
        console.print(interrupt_panel)
    except Exception as e:
        fatal_panel = Panel(
            f"💥 Fatal error: {str(e)}\n"
            f"💡 Check your setup and try again",
            title="💀 Fatal Error",
            border_style="red",
            padding=(1, 2)
        )
        console.print(fatal_panel)

if __name__ == "__main__":
    run_streaming_example() 