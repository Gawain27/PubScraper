import logging
import threading
from queue import Queue

from com.gwngames.pubscraper.Context import Context
from com.gwngames.pubscraper.comm.SynchroSocket import SynchroSocket
from com.gwngames.pubscraper.constants.ConfigConstants import ConfigConstants
from com.gwngames.pubscraper.msg.comm.SendEntity import SendEntity
from com.gwngames.pubscraper.scraper.buffer.DatabaseHandler import DatabaseHandler
from com.gwngames.pubscraper.utils.JsonReader import JsonReader


class OutSender:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(OutSender, cls).__new__(cls)
                cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self._initialized = True
        server_port = JsonReader(JsonReader.CONFIG_FILE_NAME).get_value(ConfigConstants.SERVER_ENTITY_PORT)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.socket_instance = SynchroSocket(server_port)
        self.ctx = Context()
        self._queue = Queue()
        self._worker_thread = threading.Thread(target=self._process_queue, daemon=True)
        self._worker_thread.start()

    def send_data(self, data: SendEntity):
        """
        Enqueue data to be sent.

        :param data: SendEntity containing the entity data to send.
        """
        self.logger.info("Queueing data to send for: %s", data.entity_id)
        self._queue.put(data)

    def _process_queue(self):
        while True:
            try:
                data = self._queue.get()
                self.logger.info("Processing data to send for: %s", data.entity_id)
                self._send_data_internal(data)
            except Exception as e:
                self.logger.error("Error processing queue: %s", e)

    def _send_data_internal(self, data: SendEntity):
        """
        Handles the actual sending of data.

        :param data: SendEntity containing the entity data to send.
        """
        data_source: DatabaseHandler = DatabaseHandler(self.ctx.get_dbclient(), data.entity_db)
        self.logger.info("Retrieving entity to send: %s", data.entity_id)
        entity = data_source.db.get(data.entity_id)

        if entity is None:
            self.logger.error("Entity found none for message %s", data.content)
            raise Exception("Entity found none for message %s", data.content)
        else:
            self.logger.info("Sending message %s", data.content)

        self.socket_instance.send_message(data.entity)
        self.logger.info("Message successfully sent: %s", data.content)

        entity['sent'] = True
        data_source.insert_or_update_document(data.content, data.entity_id, entity)


