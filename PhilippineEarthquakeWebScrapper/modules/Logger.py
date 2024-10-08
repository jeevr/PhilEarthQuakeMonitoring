import logging
import time
from datetime import datetime



class Logger:
    def __init__(self, log_file='logs/scraper_log.txt'):
        # Configure logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)  # Set the overall logging level

        # Create log file with dynamic name based on date and time
        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file_name = f'{log_file}_{current_time}.txt'

        # Create handlers
        file_handler = logging.FileHandler(log_file_name)
        console_handler = logging.StreamHandler()

        # Set log level for handlers
        file_handler.setLevel(logging.DEBUG)
        console_handler.setLevel(logging.INFO)

        # Create formatters and add them to the handlers
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # Add the handlers to the logger
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

    def log_message(self, message, level='info'):
        """Logs messages to the log file with specified logging level."""
        if level == 'info':
            self.logger.info(message)
        elif level == 'error':
            self.logger.error(message)
        elif level == 'exception':
            self.logger.exception(message)
        elif level == 'warning':
            self.logger.warning(message)
        elif level == 'debug':
            self.logger.debug(message)
        else:
            self.logger.info(message)  # Default to info
