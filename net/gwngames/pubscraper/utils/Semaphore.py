import threading


class SingletonSemaphore:
    _instances = {}

    def __new__(cls, name, initial_count=1):
        if name not in cls._instances:
            cls._instances[name] = super().__new__(cls)
            cls._instances[name].__init__(name, initial_count)
        return cls._instances[name]

    def __init__(self, name, initial_count=1):
        self.name = name
        self.semaphore = threading.Semaphore(initial_count)

    def acquire(self):
        self.semaphore.acquire()

    def release(self):
        self.semaphore.release()

    def __repr__(self):
        return f"<SingletonSemaphore(name={self.name}, count={self.semaphore._value})>"