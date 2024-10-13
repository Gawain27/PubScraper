import logging
import random
import threading

from scholarly import ProxyGenerator, scholarly

from net.gwngames.pubscraper.Context import Context
from net.gwngames.pubscraper.constants.ConfigConstants import ConfigConstants
from net.gwngames.pubscraper.constants.ProxyConstants import ProxyConstants


class ProxyRunner:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, 'initialized'):
            self.initialized = True
            self.logger = logging.getLogger('Proxy Runner')
            self.ctx = Context()
            self._proxy_choice: str = "NONE"
            self._timeout: int = 0

    def init_proxy_choice(self):
        self._proxy_choice = self.ctx.get_config().get_value(ConfigConstants.PROXY_TYPE)
        self._timeout = self.ctx.get_config().get_value(ConfigConstants.MIN_WAIT_TIME)
        self.refresh_proxy()

    def refresh_proxy(self, is_failure: bool = False):
        with ProxyRunner._lock:
            if is_failure:
                self._timeout *= 2

            if self._proxy_choice == ProxyConstants.PROXY_TYPE_FREE:
                self._refresh_scholarly_proxy(self._proxy_choice)
            elif self._proxy_choice == ProxyConstants.PROXY_TYPE_SCRAPERAPI:
                self._refresh_scholarly_proxy(self._proxy_choice)
            elif self._proxy_choice == ProxyConstants.PROXY_TYPE_LUMINATI:
                # Not implemented
                void = None
            else:
                self._refresh_scholarly_proxy(self._proxy_choice)
                self.logger.debug("No proxy used")

    def _refresh_scholarly_proxy(self, proxy_type: str):
        pg = ProxyGenerator()
        if proxy_type == ProxyConstants.PROXY_TYPE_FREE:
            success = pg.FreeProxies(self._timeout,
                                     random.uniform(self.ctx.get_config().get_value(ConfigConstants.MIN_WAIT_TIME),
                                                    self.ctx.get_config().get_value(ConfigConstants.MAX_WAIT_TIME)))
        elif proxy_type == ProxyConstants.PROXY_TYPE_SCRAPERAPI:
            key: str = self.ctx.get_config().get_value(ConfigConstants.SCRAPERAPI_KEY)
            if key is None:
                raise Exception("A Scraper API key must be provided for scholarly to work!")
            else:
                success = pg.ScraperAPI(key)
        else:  # No proxy
            success = True

        if not success:
            raise Exception("Couldn't set up proxy for scraping")

        scholarly.use_proxy(pg)
