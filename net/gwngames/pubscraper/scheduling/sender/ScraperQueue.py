from datetime import datetime
from typing import Final

from net.gwngames.pubscraper.constants.ConfigConstants import ConfigConstants
from net.gwngames.pubscraper.constants.QueueConstants import QueueConstants
from net.gwngames.pubscraper.msg.BaseMessage import BaseMessage
from net.gwngames.pubscraper.msg.scraper.FetchCoreEduData import FetchCoreEduData
from net.gwngames.pubscraper.msg.scraper.FetchDblpData import FetchDblpData
from net.gwngames.pubscraper.msg.scraper.FetchGeneralData import FetchGeneralData
from net.gwngames.pubscraper.msg.scraper.FetchScholarData import FetchScholarlyData
from net.gwngames.pubscraper.msg.scraper.FetchScimagoData import FetchScimagoData
from net.gwngames.pubscraper.scheduling.sender.AsyncQueue import AsyncQueue
from net.gwngames.pubscraper.scraper.ifaces.CoreEduDataFetcher import CoreEduDataFetcher
from net.gwngames.pubscraper.scraper.ifaces.DblpDataFetcher import DblpDataFetcher
from net.gwngames.pubscraper.scraper.ifaces.ScholarDataFetcher import ScholarDataFetcher
from net.gwngames.pubscraper.scraper.ifaces.ScimagoDataFetcher import ScimagoDataFetcher


import threading
from concurrent.futures import ThreadPoolExecutor
from queue import Queue, PriorityQueue
from typing import Dict, Type

class ScraperQueue(AsyncQueue):
    QUEUE: Final = QueueConstants.SCRAPER_QUEUE

    def __init__(self):
        super().__init__()
        self.is_queue_depth_limited = True
        # Subqueues for each message type
        self.subqueues: Dict[Type[BaseMessage], Queue] = {
            FetchScholarlyData: PriorityQueue(),
            FetchDblpData: Queue(),
            FetchScimagoData: Queue(),
            FetchCoreEduData: Queue(),
        }
        max_req = self.ctx.get_config().get_value(ConfigConstants.MAX_IFACE_REQUESTS)
        self.executors: Dict[Type[BaseMessage], ThreadPoolExecutor] = {
            FetchScholarlyData: ThreadPoolExecutor(max_workers=max_req),
            FetchDblpData: ThreadPoolExecutor(max_workers=max_req),
            FetchScimagoData: ThreadPoolExecutor(max_workers=max_req),
            FetchCoreEduData: ThreadPoolExecutor(max_workers=max_req),
        }
        # Start processing threads
        self.start_processing_threads()

    def start_processing_threads(self):
        for message_type, queue in self.subqueues.items():
            threading.Thread(target=self.process_subqueue, args=(message_type, queue), daemon=True).start()

    def process_subqueue(self, message_type: Type[BaseMessage], queue: Queue):
        while True:
            msg = queue.get()
            if msg is None:  # None signals shutdown
                break
            try:
                self.process_scraper_message(message_type, msg)
            except Exception as e:
                self.logger.error(f"Error processing message in {message_type.__name__}: {e}")
            finally:
                queue.task_done()

    def process_scraper_message(self, message_type: Type[BaseMessage], msg: FetchGeneralData):
        if message_type == FetchScholarlyData:
            ScholarDataFetcher().fetch_general_data(msg)
        elif message_type == FetchDblpData:
            DblpDataFetcher().fetch_general_data(msg)
        elif message_type == FetchScimagoData:
            ScimagoDataFetcher().fetch_general_data(msg)
        elif message_type == FetchCoreEduData:
            CoreEduDataFetcher().fetch_general_data(msg)
        else:
            self.logger.error("ScraperQueue - Received undefined message type: %s", type(msg).__name__)
            raise Exception("ScraperQueue - Received undefined message type: %s", type(msg).__name__)

    def register_me(self) -> type:
        return ScraperQueue

    def on_message(self, msg: BaseMessage) -> None:
        self.logger.info("Received message: %s", msg)

        update_data: list = self.message_stats.get_value(msg.content)
        update_index = 0 if update_data is None else int(update_data[0])
        self.logger.info("Setting last update data for %s from: %s", msg.content, update_index)
        self.message_stats.set_and_save(msg.content, [str(update_index+1), datetime.today().isoformat()])

        # Forward to the appropriate subqueue
        message_type = type(msg)
        if message_type in self.subqueues:
            self.subqueues[message_type].put(msg)
        else:
            self.logger.error("ScraperQueue - Unsupported message type: %s", message_type.__name__)

    def shutdown(self):
        for queue in self.subqueues.values():
            queue.put(None)  # Signal shutdown for each processing thread
        for executor in self.executors.values():
            executor.shutdown(wait=True)

