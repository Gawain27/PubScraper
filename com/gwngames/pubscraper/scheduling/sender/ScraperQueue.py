import traceback
from datetime import datetime
from typing import Final

from com.gwngames.pubscraper.constants.QueueConstants import QueueConstants
from com.gwngames.pubscraper.exception.IgnoreCaptchaException import IgnoreCaptchaException
from com.gwngames.pubscraper.exception.UninplementedCaptchaException import UninmplementedCaptchaException
from com.gwngames.pubscraper.msg.BaseMessage import BaseMessage
from com.gwngames.pubscraper.msg.scraper.FetchCoreEduData import FetchCoreEduData
from com.gwngames.pubscraper.msg.scraper.FetchDblpData import FetchDblpData
from com.gwngames.pubscraper.msg.scraper.FetchScholarData import FetchScholarlyData
from com.gwngames.pubscraper.msg.scraper.FetchScimagoData import FetchScimagoData
from com.gwngames.pubscraper.scheduling.sender.AsyncQueue import AsyncQueue
from com.gwngames.pubscraper.scraper.ifaces.CoreEduDataFetcher import CoreEduDataFetcher
from com.gwngames.pubscraper.scraper.ifaces.DblpDataFetcher import DblpDataFetcher
from com.gwngames.pubscraper.scraper.ifaces.ScholarDataFetcher import ScholarDataFetcher
from com.gwngames.pubscraper.scraper.ifaces.ScimagoDataFetcher import ScimagoDataFetcher

class ScraperQueue(AsyncQueue):
    QUEUE: Final = QueueConstants.SCRAPER_QUEUE

    def __init__(self):
        super().__init__()
        self.is_queue_depth_limited = True

    def register_me(self) -> type:
        return ScraperQueue

    def on_message(self, msg: BaseMessage) -> None:
        self.logger.info("Received message: %s", msg)

        update_data: list = self.message_stats.get_value(msg.content)
        update_index = 0 if update_data is None else int(update_data[0])
        self.logger.info("Setting last update data for %s from: %s", msg.content, update_index)
        self.message_stats.set_and_save(msg.content, [str(update_index+1), datetime.today().isoformat()])

        try:
            self.logger.info(f"Processing message {msg.message_id} of type {msg.message_type}: {msg.content}"
                         f" - with depth {msg.depth if msg.depth is not None else 'None'}")
            if isinstance(msg, FetchScholarlyData):
                ScholarDataFetcher().fetch_general_data(msg)
            elif isinstance(msg, FetchDblpData):
                DblpDataFetcher().fetch_general_data(msg)
            elif isinstance(msg, FetchScimagoData):
                ScimagoDataFetcher().fetch_general_data(msg)
            elif isinstance(msg, FetchCoreEduData):
                CoreEduDataFetcher().fetch_general_data(msg)
            else:
                self.logger.error("ScraperQueue - Received undefined message type: %s", type(msg).__name__)
                raise Exception("ScraperQueue - Received undefined message type: %s", type(msg).__name__)

        except Exception as e:
            full_exception = traceback.format_exc()
            if isinstance(e, IgnoreCaptchaException) or isinstance(e, UninmplementedCaptchaException):
                self.logger.warning(full_exception)
            elif isinstance(e, StopIteration):
                self.logger.error("Reached end of iteration for %s - %s", msg.message_type, msg.content)
            elif isinstance(e, KeyError):
                # ignore error as the entity cannot be processed
                self.logger.error("Entity not processable for %s - %s", msg.message_type, msg.content)
                self.logger.error(full_exception)
            else:
                raise e
        self.logger.info(f"Processed message {msg.message_id} of type {msg.message_type}: {msg.content}"
                         f" - with depth {msg.depth if msg.depth is not None else 'None'}")

        return