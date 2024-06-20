from datetime import datetime
from typing import Final

from net.gwngames.pubscraper.constants.PriorityConstants import PriorityConstants
from net.gwngames.pubscraper.constants.QueueConstants import QueueConstants
from net.gwngames.pubscraper.msg.BaseMessage import BaseMessage
from net.gwngames.pubscraper.msg.comm.SerializeJSONData import SerializeJSONData
from net.gwngames.pubscraper.msg.scraper.GetGoogleScholarData import GetGoogleScholarData
from net.gwngames.pubscraper.msg.scraper.core.ScrapeLeaf import ScrapeLeaf
from net.gwngames.pubscraper.msg.scraper.core.ScrapeTopic import ScrapeTopic
from net.gwngames.pubscraper.scheduling.MessageRouter import MessageRouter
from net.gwngames.pubscraper.scheduling.sender.AsyncQueue import AsyncQueue
from net.gwngames.pubscraper.scraper.ifaces.ScholarlyDataFetcher import ScholarlyDataFetcher


class ScraperQueue(AsyncQueue):
    QUEUE: Final = QueueConstants.SCRAPER_QUEUE

    def register_me(self) -> type:
        return ScraperQueue

    def on_message(self, msg: BaseMessage) -> None:
        self.logger.info("Received message: %s", msg)

        if msg.is_first_run:
            filter_date = datetime.min
            self.logger.info("Setting filter date from the minimum possible date: %s", filter_date)
        else:
            filter_date = self.message_stats.get_value(msg.content)
            self.logger.info("Setting filter date from: %s", filter_date)
        self.message_stats.set_and_save(msg.content, datetime.today().isoformat())

        scraped_info_file: str = ""

        if isinstance(msg, ScrapeTopic):
            self.logger.info("Processing ScrapeTopic message for paper: %s", msg.content)
            ScholarlyDataFetcher().scrape_paper(msg)
            self.logger.info("Successfully completed ScrapeTopic message for paper: %s", msg.content)
            return
        elif isinstance(msg, GetGoogleScholarData):
            self.logger.info("Processing GetGoogleScholarData message with query: %s", msg.query)
            try:
                scraped_info_file = ScholarlyDataFetcher().get_new_data_since(msg.query, filter_date)
                self.logger.info("Successfully fetched new data since %s for query: %s", filter_date, msg.query)
            except Exception as e:
                self.logger.error("Failed to fetch data for query %s: %s", msg.query, str(e))
                raise e
        else:
            self.logger.error("ScraperQueue - Received undefined message type: %s", type(msg).__name__)
            raise Exception("ScraperQueue - Received undefined message type: %s", type(msg).__name__)

        json_serialize_req = SerializeJSONData(msg.content, scraped_info_file)
        MessageRouter.get_instance().send_message(json_serialize_req, QueueConstants.OUTSENDER_QUEUE,
                                                  priority=PriorityConstants.SERIALIZATION_REQ)
