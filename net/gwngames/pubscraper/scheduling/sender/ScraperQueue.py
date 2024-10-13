from datetime import datetime
from typing import Final

from scholarly import MaxTriesExceededException

from net.gwngames.pubscraper.constants.PriorityConstants import PriorityConstants
from net.gwngames.pubscraper.constants.QueueConstants import QueueConstants
from net.gwngames.pubscraper.msg.BaseMessage import BaseMessage
from net.gwngames.pubscraper.msg.comm.SerializeJSONData import SerializeJSONData
from net.gwngames.pubscraper.msg.scraper.scholarly.FetchScholarlyData import FetchScholarlyData
from net.gwngames.pubscraper.msg.scraper.scholarly.GetAllScholarlyAuthors import GetAllScholarlyAuthors
from net.gwngames.pubscraper.msg.scraper.scholarly.GetScholarlyAuthor import GetScholarlyAuthor
from net.gwngames.pubscraper.msg.scraper.scholarly.GetScholarlyOrg import GetScholarlyOrg
from net.gwngames.pubscraper.msg.scraper.scholarly.GetScholarlyPubCitations import GetScholarlyPubCitations
from net.gwngames.pubscraper.msg.scraper.scholarly.GetScholarlyPubRelatedArticles import GetScholarlyPubRelatedArticles
from net.gwngames.pubscraper.msg.scraper.scholarly.GetScholarlyPublication import GetScholarlyPublication
from net.gwngames.pubscraper.scheduling.MessageRouter import MessageRouter
from net.gwngames.pubscraper.scheduling.sender.AsyncQueue import AsyncQueue
from net.gwngames.pubscraper.scraper.ifaces.ScholarlyDataFetcher import ScholarlyDataFetcher
from net.gwngames.pubscraper.utils.StringUtils import StringUtils


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

        scraped_info_file: str = ""
        try:
            self.logger.info(f"Processing message {msg.message_id} of type {msg.message_type}: {msg.content}"
                         f" - with depth {msg.depth if msg.depth is not None else 'None'}")
            if isinstance(msg, FetchScholarlyData):
                ScholarlyDataFetcher().fetch_general_data(msg)
            elif isinstance(msg, GetAllScholarlyAuthors):
                scraped_info_file = ScholarlyDataFetcher(proxy=True).fetch_multiple_authors(msg.authors, 0)
            elif isinstance(msg, GetScholarlyOrg):
                scraped_info_file = ScholarlyDataFetcher(proxy=True).fetch_org_data(msg)
            elif isinstance(msg, GetScholarlyPublication):
                scraped_info_file = ScholarlyDataFetcher(proxy=True).fetch_author_publication(msg)
            elif isinstance(msg, GetScholarlyPubCitations):
                msg.pub_id = StringUtils.sanitize_string(msg.pub_id)
                scraped_info_file = ScholarlyDataFetcher(proxy=True).fetch_pub_citations(msg)
            elif isinstance(msg, GetScholarlyPubRelatedArticles):
                msg.pub_id = StringUtils.sanitize_string(msg.pub_id)
                scraped_info_file = ScholarlyDataFetcher(proxy=True).fetch_related_articles(msg)
            else:
                self.logger.error("ScraperQueue - Received undefined message type: %s", type(msg).__name__)
                raise Exception("ScraperQueue - Received undefined message type: %s", type(msg).__name__)

        except Exception as e:
            if isinstance(e, StopIteration):
                self.logger.error("Reached end of iteration for %s - %s", msg.message_type, msg.content)
            elif isinstance(e, MaxTriesExceededException):
                ScholarlyDataFetcher().refresh_proxy()
            elif isinstance(e, KeyError):
                # ignore error as the entity cannot be processed
                self.logger.error("Entity not processable for %s - %s", msg.message_type, msg.content)
                return
            else:
                raise e
        self.logger.info(f"Processed message {msg.message_id} of type {msg.message_type}: {msg.content}"
                         f" - with depth {msg.depth if msg.depth is not None else 'None'}")

        return
        if scraped_info_file == JsonReader.DEV_NULL:
            return
        json_serialize_req = SerializeJSONData(msg.content, scraped_info_file)
        self.logger.info("Sending message %s for %s of type %s", json_serialize_req.message_id,
                         json_serialize_req.content, json_serialize_req.message_type)
        MessageRouter.get_instance().send_message(json_serialize_req,
                                                  priority=PriorityConstants.SERIALIZATION_REQ)
