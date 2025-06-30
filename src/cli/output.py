"""Rich terminal output utilities for the CLI."""

import logging
import sys
from contextlib import contextmanager
from typing import Optional

from rich.console import Console
from rich.logging import RichHandler
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table
from rich.text import Text


console = Console()


class RichOutput:
    """Provides rich terminal output with consistent styling."""

    def __init__(self, console: Optional[Console] = None):
        """Initialize the output handler.

        Args:
            console: Rich console instance (creates new one if not provided)
        """
        self.console = console or Console()

    def show_banner(self, title: str, subtitle: Optional[str] = None):
        """Display a banner with title and optional subtitle.

        Args:
            title: Main title text
            subtitle: Optional subtitle text
        """
        text = Text(title, style="bold cyan", justify="center")
        if subtitle:
            text.append(f"\n{subtitle}", style="dim white")

        panel = Panel(
            text,
            expand=False,
            border_style="bright_blue",
            padding=(1, 2)
        )
        self.console.print(panel)
        self.console.print()

    def success(self, message: str):
        """Display a success message.

        Args:
            message: Success message to display
        """
        self.console.print(f"[green]✓[/green] {message}")

    def error(self, message: str):
        """Display an error message.

        Args:
            message: Error message to display
        """
        self.console.print(f"[red]✗[/red] {message}", style="red")

    def warning(self, message: str):
        """Display a warning message.

        Args:
            message: Warning message to display
        """
        self.console.print(f"[yellow]![/yellow] {message}", style="yellow")

    def info(self, message: str):
        """Display an info message.

        Args:
            message: Info message to display
        """
        self.console.print(f"[blue]ℹ[/blue] {message}")

    @contextmanager
    def status(self, message: str):
        """Show a status spinner while performing an operation.

        Args:
            message: Status message to display

        Example:
            with output.status("Loading..."):
                # Do something
                pass
        """
        with self.console.status(message, spinner="dots"):
            yield

    def table(self, title: str, columns: list, rows: list) -> Table:
        """Create and display a formatted table.

        Args:
            title: Table title
            columns: List of column names
            rows: List of row data (list of lists)

        Returns:
            The created table
        """
        table = Table(title=title, show_header=True, header_style="bold magenta")

        for column in columns:
            table.add_column(column)

        for row in rows:
            table.add_row(*[str(cell) for cell in row])

        self.console.print(table)
        return table

    def progress_bar(self, description: str = "Processing..."):
        """Create a progress context manager.

        Args:
            description: Description for the progress bar

        Returns:
            Progress context manager

        Example:
            with output.progress_bar() as progress:
                task = progress.add_task("Processing", total=100)
                for i in range(100):
                    progress.update(task, advance=1)
        """
        return Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=self.console
        )


def setup_logging(verbosity: int = 0):
    """Configure logging with Rich handler.

    Args:
        verbosity: Verbosity level (0=WARNING, 1=INFO, 2=DEBUG, 3+=TRACE)
    """
    # Determine log level based on verbosity
    if verbosity == 0:
        level = logging.WARNING
    elif verbosity == 1:
        level = logging.INFO
    elif verbosity == 2:
        level = logging.DEBUG
    else:
        # For very high verbosity, we'll use DEBUG and show more details
        level = logging.DEBUG

    # Configure root logger
    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[
            RichHandler(
                console=console,
                rich_tracebacks=True,
                tracebacks_show_locals=(verbosity >= 3),
                show_time=True,
                show_path=(verbosity >= 2)
            )
        ]
    )

    # Adjust specific loggers
    if verbosity < 2:
        # Quiet down some noisy loggers
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("asyncio").setLevel(logging.WARNING)
        logging.getLogger("aiohttp").setLevel(logging.WARNING)

    # Set our app loggers to appropriate levels
    app_loggers = [
        "src.api",
        "src.core",
        "src.database",
        "src.cache",
        "src.cli",
        "src.utils"
    ]

    for logger_name in app_loggers:
        logger = logging.getLogger(logger_name)
        logger.setLevel(level)


class ProgressTracker:
    """Tracks and displays progress for long-running operations."""

    def __init__(self, console: Optional[Console] = None):
        """Initialize the progress tracker.

        Args:
            console: Rich console instance
        """
        self.console = console or Console()
        self.progress = None
        self.tasks = {}

    def start(self):
        """Start the progress display."""
        self.progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=self.console
        )
        self.progress.start()

    def stop(self):
        """Stop the progress display."""
        if self.progress:
            self.progress.stop()
            self.progress = None

    def add_task(self, description: str, total: Optional[int] = None) -> int:
        """Add a new task to track.

        Args:
            description: Task description
            total: Total number of items (None for indeterminate)

        Returns:
            Task ID
        """
        if not self.progress:
            self.start()

        task_id = self.progress.add_task(description, total=total)
        self.tasks[description] = task_id
        return task_id

    def update_task(self, task_id: int, advance: int = 1,
                   description: Optional[str] = None, total: Optional[int] = None):
        """Update a task's progress.

        Args:
            task_id: Task ID to update
            advance: Amount to advance progress
            description: New description (optional)
            total: New total (optional)
        """
        if self.progress:
            kwargs = {"advance": advance}
            if description is not None:
                kwargs["description"] = description
            if total is not None:
                kwargs["total"] = total

            self.progress.update(task_id, **kwargs)

    def complete_task(self, task_id: int):
        """Mark a task as completed.

        Args:
            task_id: Task ID to complete
        """
        if self.progress:
            task_info = self.progress.tasks[task_id]
            if task_info.total is not None:
                self.progress.update(task_id, completed=task_info.total)

    def __enter__(self):
        """Context manager entry."""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop()


def format_bytes(size: int) -> str:
    """Format bytes into human-readable string.

    Args:
        size: Size in bytes

    Returns:
        Formatted string (e.g., "1.5 GB")
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0:
            return f"{size:.1f} {unit}"
        size /= 1024.0
    return f"{size:.1f} PB"


def format_duration(seconds: float) -> str:
    """Format duration in seconds to human-readable string.

    Args:
        seconds: Duration in seconds

    Returns:
        Formatted string (e.g., "2h 15m 30s")
    """
    hours, remainder = divmod(int(seconds), 3600)
    minutes, seconds = divmod(remainder, 60)

    parts = []
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if seconds or not parts:
        parts.append(f"{seconds}s")

    return " ".join(parts)
