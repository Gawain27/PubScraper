import json
import logging
import os
import threading
from typing import Any

import psutil

from net.gwngames.pubscraper.comm.SynchroSocket import SynchroSocket
from net.gwngames.pubscraper.constants.ConfigConstants import ConfigConstants
from net.gwngames.pubscraper.constants.JsonConstants import JsonConstants
from net.gwngames.pubscraper.constants.PriorityConstants import PriorityConstants
from net.gwngames.pubscraper.constants.QueueConstants import QueueConstants
from net.gwngames.pubscraper.msg.system.SystemStatusReq import SystemStatusReq
from net.gwngames.pubscraper.scheduling.MessageRouter import MessageRouter
from net.gwngames.pubscraper.utils.FileReader import FileReader
from net.gwngames.pubscraper.utils.LoadState import LoadState


class StatusServlet:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(StatusServlet, cls).__new__(cls)
                    logging.debug("StatusServlet instance created")
        return cls._instance

    def __init__(self):
        if not hasattr(self, '_initialized'):
            self._initialized = True
            self.thread = None
            self.config = FileReader(FileReader.CONFIG_FILE_NAME)
            self.port = self.config.get_value(ConfigConstants.SERVER_STATUS_PORT)
            self.socket = SynchroSocket(self.port)
            logging.debug("StatusServlet instance initialized")

    @staticmethod
    def process_status(server_msg: Any):
        try:
            # Parse the JSON message
            server_load = json.loads(server_msg)

            # Extract the required fields
            cpu_load = server_load.get(JsonConstants.TAG_CPU_LOAD)
            db_load = server_load.get(JsonConstants.TAG_DB_LOAD)
            keepdown = server_load.get(JsonConstants.TAG_KEEPDOWN)

            LoadState().keepdown = keepdown

            # Calculate load percentage with db_load slightly more important
            if cpu_load is not None and db_load is not None:
                # Adjust weights as needed; here db_load is given more importance
                total_load = 0.4 * cpu_load + 0.6 * db_load  # Example weighting
                load_perc = min(total_load, 100)  # Cap at 100% if necessary
            elif cpu_load is not None:
                load_perc = min(cpu_load, 100)
            elif db_load is not None:
                load_perc = min(db_load, 100)
            else:
                load_perc = None  # Handle case where both values are None

            LoadState().load_perc = load_perc
            logging.info(f'Server Status {load_perc}% - CPU Load: {cpu_load}%, DB Load: {db_load}%, Keepdown: {keepdown}')

        except json.JSONDecodeError as e:
            logging.error(f'Error decoding JSON message: {server_msg}, Error: {e}')

    def _thread_listen(self):
        StatusServlet.set_highest_priority()
        logging.info(f"StatusServlet is listening on port: {self.port}")
        while True:
            for server_msg in self.socket.receive_message():
                load_req = SystemStatusReq("StatusServlet", server_msg)
                MessageRouter.get_instance().send_message(load_req, QueueConstants.SYSTEM_QUEUE, PriorityConstants.SYSTEM_REQ)

    def start(self):
        if self.thread is None:
            self.thread = threading.Thread(target=self._thread_listen(), daemon=True)
            self.thread.start()
            logging.info("Status servlet started")

    @staticmethod
    def set_highest_priority():
        try:
            p = psutil.Process(os.getpid())
            p.nice(psutil.HIGH_PRIORITY_CLASS)
            logging.info("Process priority set to high for Status servlet")
        except Exception as e:
            logging.error(f"Failed to set process priority: {e}")
            raise
