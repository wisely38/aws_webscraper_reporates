from logger  import logger
import requests

class Crawler:
    def __init__(self, url):
        session = requests.session()
        logger.info("INFO - fetching url - %s"%url)
        self.result = session.get(url)

    def get_crawled_result(self):
        return self.result

    def is_crawled_result_valid(self):
        return self.result.status_code >= 200 and self.result.status_code < 300