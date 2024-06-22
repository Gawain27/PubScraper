import datetime
from typing import Any

from net.gwngames.pubscraper.constants.MessageConstants import MessageConstants
from net.gwngames.pubscraper.msg.BaseMessage import BaseMessage


class GetScholarlyPublication(BaseMessage):
    def __init__(self, content: str, pub: Any, timestamp: datetime = None) -> None:
        super().__init__(MessageConstants.MSG_SCHOLARLY_PUB, content, timestamp)
        self.pub: str = pub
