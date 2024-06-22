from datetime import datetime
from typing import Final

from net.gwngames.pubscraper.constants.PriorityConstants import PriorityConstants
from net.gwngames.pubscraper.constants.QueueConstants import QueueConstants
from net.gwngames.pubscraper.msg.BaseMessage import BaseMessage
from net.gwngames.pubscraper.msg.comm.SerializeJSONData import SerializeJSONData
from net.gwngames.pubscraper.msg.scraper.scholarly.GetGoogleScholarData import GetGoogleScholarData
from net.gwngames.pubscraper.msg.scraper.core.ScrapeTopic import ScrapeTopic
from net.gwngames.pubscraper.msg.scraper.scholarly.GetScholarlyAuthor import GetScholarlyAuthor
from net.gwngames.pubscraper.msg.scraper.scholarly.GetScholarlyPublication import GetScholarlyPublication
from net.gwngames.pubscraper.scheduling.MessageRouter import MessageRouter
from net.gwngames.pubscraper.scheduling.sender.AsyncQueue import AsyncQueue
from net.gwngames.pubscraper.scraper.ifaces.ScholarlyDataFetcher import ScholarlyDataFetcher


class ScraperQueue(AsyncQueue):
    QUEUE: Final = QueueConstants.SCRAPER_QUEUE

    def register_me(self) -> type:
        return ScraperQueue

    def on_message(self, msg: BaseMessage) -> None:
        self.logger.info("Received message: %s", msg)

        filter_date = self.message_stats.get_value(msg.content)
        if filter_date is None:
            self.logger.info("Setting filter date from: %s", filter_date)
            self.message_stats.set_and_save(msg.content, datetime.today().isoformat())

        scraped_info_file: str = ""
        if isinstance(msg, ScrapeTopic):
            self.logger.info("Processing ScrapeTopic message for paper: %s", msg.content)
            ScholarlyDataFetcher().scrape_paper(msg)
            self.logger.info("Successfully completed ScrapeTopic message for paper: %s", msg.content)
            return
        elif isinstance(msg, GetScholarlyAuthor):
            self.logger.info("Processing message %s of type GetScholarlyAuthor", msg.message_id)
            scraped_info_file = ScholarlyDataFetcher().fetch_author_data(msg.author)
            self.logger.info("Processed message %s of type GetScholarlyAuthor", msg.message_id)
        elif isinstance(msg, GetScholarlyPublication):
            scraped_info_file = ScholarlyDataFetcher().fetch_author_publication(msg.pub)
            self.logger.info("Processed message %s of type GetScholarlyPublication", msg.message_id)
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

        return
        json_serialize_req = SerializeJSONData(msg.content, scraped_info_file)
        self.logger.info("Sending message %s for %s of type %s", json_serialize_req.message_id,
                         json_serialize_req.content, json_serialize_req.message_type)
        MessageRouter.get_instance().send_message(json_serialize_req,
                                                  priority=PriorityConstants.SERIALIZATION_REQ)
