"""Centralized logging utility for KV Store."""

import logging
import logging.handlers
from pathlib import Path
from datetime import datetime


def setup_logger(name: str, log_filename: str = None, level: int = logging.INFO) -> logging.Logger:
    """
    Set up a logger with both console and file handlers.
    
    Args:
        name: Logger name (usually __name__)
        log_filename: Optional custom log filename. If None, no file logging.
        level: Logging level (default: INFO)
        
    Returns:
        Configured logger instance
    """
    # Create logs directory if it doesn't exist
    logs_dir = Path(__file__).parent.parent.parent / "logs"
    logs_dir.mkdir(exist_ok=True)
    
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Avoid duplicate handlers if logger is already configured
    if logger.handlers:
        return logger
    
    # Log format with timestamp, filename, line number, function name, and message
    log_format = "%(asctime)s | %(filename)s:%(lineno)d | %(funcName)s() | %(levelname)s | %(message)s"
    formatter = logging.Formatter(
        log_format,
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler with rotation (only for custom log filenames)
    if log_filename:
        log_file = logs_dir / log_filename
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5  # Keep 5 backup files
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


def get_timestamped_logfile(base_name: str = "server") -> str:
    """
    Generate a timestamped log filename.
    
    Args:
        base_name: Base name for the log file (default: "server")
        
    Returns:
        Timestamped log filename (e.g., "server_2026-03-08_18-40-02.log")
    """
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    return f"{base_name}_{timestamp}.log"

