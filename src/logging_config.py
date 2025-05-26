import os
import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime, timedelta

def setup_logging():
    """
    Set up logging configuration with both console and file handlers.
    Logs are rotated daily and kept for 14 days.
    """
    # Create logs directory if it doesn't exist
    logs_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
    os.makedirs(logs_dir, exist_ok=True)
    
    # Main logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Prevent adding handlers multiple times
    if logger.handlers:
        return logger
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # File handler with rotation (daily, keep for 14 days)
    log_file = os.path.join(logs_dir, 'sourcing_assistant.log')
    file_handler = TimedRotatingFileHandler(
        log_file,
        when='midnight',
        interval=1,
        backupCount=14,  # Keep logs for 14 days
        encoding='utf-8',
        delay=False
    )
    file_handler.setLevel(logging.INFO)
    
    # Formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    
    # Add handlers
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    # Log start of application
    logger.info("=" * 50)
    logger.info("Sourcing Assistant started")
    logger.info("=" * 50)
    
    return logger
