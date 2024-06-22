from datetime import datetime
from typing import Any

from net.gwngames.pubscraper.constants.MessageConstants import MessageConstants
from net.gwngames.pubscraper.constants.QueueConstants import QueueConstants
from net.gwngames.pubscraper.msg.BaseMessage import BaseMessage


class SystemStatusReq(BaseMessage):
    def __init__(self, content: str, server_msg: Any, timestamp: datetime = None) -> None:
        super().__init__(MessageConstants.MSG_UPDATE_LOAD_STATE, content, timestamp)
        self.server_msg: Any = server_msg
        self.destination_queue = QueueConstants.SYSTEM_QUEUE
