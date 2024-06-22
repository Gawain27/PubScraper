from datetime import datetime

from net.gwngames.pubscraper.constants.MessageConstants import MessageConstants
from net.gwngames.pubscraper.msg.BaseMessage import BaseMessage


class GetScholarlyAuthor(BaseMessage):
    def __init__(self, content: str, author: str, timestamp: datetime = None) -> None:
        super().__init__(MessageConstants.MSG_SCHOLARLY_AUTHOR, content, timestamp)
        self.author: str = author
