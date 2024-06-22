import datetime

from net.gwngames.pubscraper.constants.MessageConstants import MessageConstants
from net.gwngames.pubscraper.constants.QueueConstants import QueueConstants
from net.gwngames.pubscraper.msg.BaseMessage import BaseMessage


class ScrapeLeaf(BaseMessage):
    def __init__(self, content: str, term: str, number_of_terms: int, timestamp: datetime = None) -> None:
        super().__init__(MessageConstants.MSG_SCRAPE_LEAF, content, timestamp)
        self.number_of_terms = number_of_terms
        self.term = term
        self.destination_queue = QueueConstants.SCRAPER_QUEUE
