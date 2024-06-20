from datetime import datetime
from typing import Any

from net.gwngames.pubscraper.constants.MessageConstants import MessageConstants
from net.gwngames.pubscraper.msg.BaseMessage import BaseMessage


class ScrapeTopic(BaseMessage):
    def __init__(self, content: str, number_of_terms: int,  paper: Any, timestamp: datetime = None) -> None:
        super().__init__(MessageConstants.MSG_SCRAPE_TOPIC, content, timestamp)
        self.number_of_terms = number_of_terms
        self.paper: Any = paper
