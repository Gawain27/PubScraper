from datetime import datetime
from typing import Any

from net.gwngames.pubscraper.constants.MessageConstants import MessageConstants
from net.gwngames.pubscraper.constants.QueueConstants import QueueConstants
from net.gwngames.pubscraper.msg.BaseMessage import BaseMessage


class GetScholarlyPubCitations(BaseMessage):
    def __init__(self, content: str, pub_id: str, citations: Any, pub: Any = None, timestamp: datetime = None) -> None:
        super().__init__(MessageConstants.MSG_SCHOLARLY_CITATIONS, content, timestamp)
        self.pub_id: str = pub_id
        self.citations: Any = citations
        self.pub = pub
        self.destination_queue = QueueConstants.SCRAPER_QUEUE
