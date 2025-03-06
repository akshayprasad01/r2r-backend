import logging
import os

logger_mode = os.getenv('LOGGER_MODE', 'DEBUG').upper()

logger = logging.getLogger("aira-transfomation")
if logger_mode in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']:
    logger.setLevel(logger_mode)
else:
    logger.setLevel(logging.DEBUG)

console_handler = logging.StreamHandler()
console_handler.setLevel(logger.level)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(funcName)s - %(message)s'
)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)