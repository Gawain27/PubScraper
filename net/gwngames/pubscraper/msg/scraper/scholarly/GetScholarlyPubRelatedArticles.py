from datetime import datetime
from typing import Any

from net.gwngames.pubscraper.constants.MessageConstants import MessageConstants
from net.gwngames.pubscraper.constants.QueueConstants import QueueConstants
from net.gwngames.pubscraper.msg.BaseMessage import BaseMessage


class GetScholarlyPubRelatedArticles(BaseMessage):
    def __init__(self, content: str, pub_id: str, articles: Any, pub: Any = None, timestamp: datetime = None) -> None:
        super().__init__(MessageConstants.MSG_SCHOLARLY_REL_ARTICLES, content, timestamp)
        self.pub_id: str = pub_id
        self.pub: Any = pub
        self.articles: Any = articles
        self.destination_queue = QueueConstants.SCRAPER_QUEUE
