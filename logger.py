import sys
import logging

# logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger()
if logger.handlers:
    for handler in logger.handlers:
        logger.removeHandler(handler)
logging.basicConfig(format='%(asctime)s %(message)s',level=logging.INFO)