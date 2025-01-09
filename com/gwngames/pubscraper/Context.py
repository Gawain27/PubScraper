import logging
import os.path
import threading

from couchdb import Server

from com.gwngames.pubscraper.constants.ConfigConstants import ConfigConstants
from com.gwngames.pubscraper.utils.StringUtils import StringUtils


class Context:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super().__new__(cls)
        return cls._instance

    # Add attributes as needed
    def __init__(self):
        if not hasattr(self, 'initialized'):
            self._drivers = {}
            self.initialized = True
            self.logger = logging.getLogger('Context')
            self._current_dir = None
            self._config = None
            self._message_stats = None
            self._client: Server = Server()

    def build_path(self, path: str):
        return os.path.join(self.get_current_dir(), path)

    def get_current_dir(self):
        with self._lock:
            return self._current_dir

    def set_current_dir(self, current_dir):
        with self._lock:
            self._current_dir = current_dir
            self.logger.info("Context added: current active directory: " + current_dir)

    def get_config(self):
        with self._lock:
            from com.gwngames.pubscraper.utils.JsonReader import JsonReader
            _dir: JsonReader = self._config
            return _dir

    def set_config(self, config):
        with self._lock:
            self._config = config
            self.logger.info("Context added: Set current config: " + config.file)

    def get_message_data(self):
        with self._lock:
            from com.gwngames.pubscraper.utils.JsonReader import JsonReader
            _dir: JsonReader = self._message_stats
            return _dir

    def set_message_data(self, message_stats):
        with self._lock:
            self._message_stats = message_stats
            self.logger.info("Context added: Set current config: " + message_stats.file)

    def get_dbclient(self) -> Server:
        with self._lock:
            return self._client

    def set_client(self, client: Server):
        with self._lock:
            self._client = client
            self.logger.info("Context added: Set current DB client")

    def get_active_interfaces(self) -> list[str]:
        main = self.get_main_interfaces()
        return main

    def get_main_interfaces(self) -> list:
        main = self.get_config().get_value(ConfigConstants.INTERFACES_ENABLED)
        return StringUtils.process_string(main)

    def get_max_requests(self) -> int:
        reqs = self.get_config().get_value(ConfigConstants.MAX_IFACE_REQUESTS)
        return reqs

    def is_iface_enabled(self, iface_name) -> bool:
        return iface_name in self.get_config().get_value(ConfigConstants.INTERFACES_ENABLED)
