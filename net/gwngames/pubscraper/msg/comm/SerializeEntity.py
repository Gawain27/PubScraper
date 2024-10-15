from datetime import datetime

from net.gwngames.pubscraper.constants.MessageConstants import MessageConstants
from net.gwngames.pubscraper.constants.QueueConstants import QueueConstants
from net.gwngames.pubscraper.msg.BaseMessage import BaseMessage


class SerializeEntity(BaseMessage):
    def __init__(self, content: str, entity_id: str, entity_db: str,
                 entity_class: int, entity_variant: int, timestamp: datetime = None) -> None:
        super().__init__(MessageConstants.MSG_SERIALIZE_ENTITY, content, timestamp)
        self.entity_id: str = entity_id
        self.entity_db: str = entity_db
        self.entity_class: int = entity_class
        self.entity_variant: int = entity_variant
        self.destination_queue = QueueConstants.OUTSENDER_QUEUE
