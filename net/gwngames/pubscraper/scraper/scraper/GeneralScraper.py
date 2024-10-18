import logging
import re

from net.gwngames.pubscraper.Context import Context
from net.gwngames.pubscraper.constants.CaptchaConstants import CaptchaConstants
from net.gwngames.pubscraper.scraper.scraper.SeleniumDriver import SeleniumDriver


class GeneralScraper:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.ctx = Context()
        self.driver_manager = SeleniumDriver.get_instance()

