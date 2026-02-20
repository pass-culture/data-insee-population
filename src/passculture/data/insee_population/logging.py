"""Logging configuration for INSEE population processing.

Uses loguru for structured logging with verbosity control.
"""

from __future__ import annotations

import sys

from loguru import logger


def configure_logging(verbosity: int = 0) -> None:
    """Configure loguru logging level and format.

    Args:
        verbosity: 0 = INFO (default), 1 = DEBUG (verbose), -1 = WARNING (quiet)
    """
    logger.remove()

    if verbosity >= 1:
        level = "DEBUG"
        fmt = "{time:HH:mm:ss} | {level:<7} | {message}"
    elif verbosity <= -1:
        level = "WARNING"
        fmt = "{message}"
    else:
        level = "INFO"
        fmt = "{message}"

    logger.add(sys.stderr, level=level, format=fmt)
