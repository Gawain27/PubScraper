from datetime import datetime
from typing import Final

from net.gwngames.pubscraper.constants.PriorityConstants import PriorityConstants
from net.gwngames.pubscraper.constants.QueueConstants import QueueConstants
from net.gwngames.pubscraper.msg.BaseMessage import BaseMessage
from net.gwngames.pubscraper.msg.comm.SerializeJSONData import SerializeJSONData
from net.gwngames.pubscraper.msg.scraper.scholarly.GetScholarlyAuthor import GetScholarlyAuthor
from net.gwngames.pubscraper.msg.scraper.scholarly.GetScholarlyPubCitations import GetScholarlyPubCitations
from net.gwngames.pubscraper.msg.scraper.scholarly.GetScholarlyPubRelatedArticles import GetScholarlyPubRelatedArticles
from net.gwngames.pubscraper.msg.scraper.scholarly.GetScholarlyPublication import GetScholarlyPublication
from net.gwngames.pubscraper.scheduling.MessageRouter import MessageRouter
from net.gwngames.pubscraper.scheduling.sender.AsyncQueue import AsyncQueue
from net.gwngames.pubscraper.scraper.ifaces.ScholarlyDataFetcher import ScholarlyDataFetcher
from net.gwngames.pubscraper.utils.StringUtils import StringUtils


class ScraperQueue(AsyncQueue):
    QUEUE: Final = QueueConstants.SCRAPER_QUEUE

    def register_me(self) -> type:
        return ScraperQueue

    def on_message(self, msg: BaseMessage) -> None:
        self.logger.info("Received message: %s", msg)

        update_data: list = self.message_stats.get_value(msg.content)
        update_index = 0 if update_data is None else int(update_data[0])
        self.logger.info("Setting last update data for %s from: %s", msg.content, update_index)
        self.message_stats.set_and_save(msg.content, [str(update_index+1), datetime.today().isoformat()])

        scraped_info_file: str = ""
        if isinstance(msg, GetScholarlyAuthor):
            self.logger.info("Processing message %s of type GetScholarlyAuthor for %s", msg.message_id, msg.author)
            scraped_info_file = ScholarlyDataFetcher(proxy=True).fetch_author_data(msg.author)
            self.logger.info("Processed message %s of type GetScholarlyAuthor for %s", msg.message_id, msg.author)
        elif isinstance(msg, GetScholarlyPublication):
            scraped_info_file = ScholarlyDataFetcher(proxy=True).fetch_author_publication(msg.pub)
            self.logger.info("Processed message %s of type GetScholarlyPublication", msg.message_id)
        elif isinstance(msg, GetScholarlyPubCitations):
            scraped_info_file = ScholarlyDataFetcher(proxy=True).fetch_pub_citations(msg.content, msg.citations, StringUtils.sanitize_string(msg.pub_id), msg.pub)
            self.logger.info("Processed message %s of type GetScholarlyPubCitations", msg.content)
        elif isinstance(msg, GetScholarlyPubRelatedArticles):
            scraped_info_file = ScholarlyDataFetcher(proxy=True).fetch_related_articles(msg.content, msg.articles, StringUtils.sanitize_string(msg.pub_id), msg.pub)
            self.logger.info("Processed message %s of type GetScholarlyPubRelatedArticles", msg.content)
        else:
            self.logger.error("ScraperQueue - Received undefined message type: %s", type(msg).__name__)
            raise Exception("ScraperQueue - Received undefined message type: %s", type(msg).__name__)

        return
        if scraped_info_file == "/dev/null":
            return
        json_serialize_req = SerializeJSONData(msg.content, scraped_info_file)
        self.logger.info("Sending message %s for %s of type %s", json_serialize_req.message_id,
                         json_serialize_req.content, json_serialize_req.message_type)
        MessageRouter.get_instance().send_message(json_serialize_req,
                                                  priority=PriorityConstants.SERIALIZATION_REQ)
