import logging

from com.gwngames.pubscraper.Context import Context
from com.gwngames.pubscraper.scraper.scraper.SeleniumDriver import SeleniumDriver, SeleniumDriverManager


class GeneralScraper:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.ctx = Context()
        self.driver_manager: SeleniumDriver = SeleniumDriverManager.get_instance(self.__class__.__name__)
