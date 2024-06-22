import datetime

from net.gwngames.pubscraper.constants.MessageConstants import MessageConstants
from net.gwngames.pubscraper.constants.QueueConstants import QueueConstants
from net.gwngames.pubscraper.msg.BaseMessage import BaseMessage


class SendEntity(BaseMessage):  # TODO logging stuff
    def __init__(self, content: str, entity: bytes, cid: int, timestamp: datetime = None) -> None:
        super().__init__(MessageConstants.MSG_SEND_ENTITY, content, timestamp)
        self.entity: bytes = entity
        self.entity_cid: int = cid
        self.destination_queue = QueueConstants.OUTSENDER_QUEUE
