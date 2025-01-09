import datetime

from com.gwngames.pubscraper.constants.MessageConstants import MessageConstants
from com.gwngames.pubscraper.constants.QueueConstants import QueueConstants
from com.gwngames.pubscraper.msg.BaseMessage import BaseMessage


class SendEntity(BaseMessage):  # TODO logging stuff
    def __init__(self, content: str, entity: bytes, entity_id: str, entity_db: str, timestamp: datetime = None) -> None:
        super().__init__(MessageConstants.MSG_SEND_ENTITY, content, timestamp)
        self.entity: bytes = entity
        self.entity_id: str = entity_id
        self.system_message = True
        self.entity_db: str = entity_db
        self.destination_queue = QueueConstants.OUTSENDER_QUEUE
