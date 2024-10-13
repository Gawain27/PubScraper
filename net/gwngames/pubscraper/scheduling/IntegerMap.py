import threading

from net.gwngames.pubscraper.Context import Context


class IntegerMap:
    _ctx = None
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        """Ensure only one instance of the class (Singleton)."""
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        """Initialize the singleton instance only once."""
        with IntegerMap._lock:
            if not self._initialized:
                self.map = {}
                self._ctx = Context()
                self._lock = threading.Lock()  # Lock for thread-safe operations
                self._initialized = True

    def get_next_id(self, key: str) -> int:
        """Generate and return the next integer ID for the string key."""
        with self._lock:
            if key not in self.map:
                # If the key is not in the map, assign the next ID and increment the counter
                next_id = self._ctx.get_message_data().get_value(key)
                if next_id is None:
                    next_id = 0
            else:
                next_id = self.map[key]
            # Return the existing or newly created ID
            self.map[key] = next_id + 1
            return next_id

    def store_all_on_failure(self):
        for key in self.map:
            self._ctx.get_message_data().set_value(key, self.map[key])


