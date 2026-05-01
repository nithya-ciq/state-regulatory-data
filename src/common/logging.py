import logging
import sys
from typing import Optional


def setup_logging(level: str = "INFO", name: Optional[str] = None) -> logging.Logger:
    """Configure and return a structured logger.

    Args:
        level: Log level string (DEBUG, INFO, WARNING, ERROR, CRITICAL).
        name: Logger name. If None, returns the root logger.

    Returns:
        Configured logger instance.
    """
    logger = logging.getLogger(name or "jurisdiction")

    if logger.handlers:
        return logger

    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logger.level)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger
