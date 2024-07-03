import datetime

from net.gwngames.pubscraper.constants.MessageConstants import MessageConstants
from net.gwngames.pubscraper.constants.QueueConstants import QueueConstants
from net.gwngames.pubscraper.msg.BaseMessage import BaseMessage


class GetAllScholarlyAuthors(BaseMessage):
    def __init__(self, content: str, authors: list, timestamp: datetime = None) -> None:
        super().__init__(MessageConstants.MSG_ALL_SCHOLARLY_AUTHORS, content, timestamp)
        self.authors: list = authors
        self.destination_queue = QueueConstants.SCRAPER_QUEUE
