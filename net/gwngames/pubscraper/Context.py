import logging
import os.path
import threading


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
            self.initialized = True
            self.logger = logging.getLogger('Context')
            self._current_dir = None

    def build_path(self, path: str):
        return os.path.join(self.get_current_dir(), path)

    def get_current_dir(self):
        with self._lock:
            return self._current_dir

    def set_current_dir(self, current_dir):
        with self._lock:
            self._current_dir = current_dir
            self.logger.info("Set current active directory: "+current_dir)
