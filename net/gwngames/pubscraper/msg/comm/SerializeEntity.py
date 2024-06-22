from datetime import datetime

from net.gwngames.pubscraper.constants.MessageConstants import MessageConstants
from net.gwngames.pubscraper.constants.QueueConstants import QueueConstants
from net.gwngames.pubscraper.msg.BaseMessage import BaseMessage


class SerializeEntity(BaseMessage): # TODO logging stuff
    def __init__(self, content: str, entity_loc: str, timestamp: datetime = None) -> None:
        super().__init__(MessageConstants.MSG_SERIALIZE_ENTITY, content, timestamp)
        self.entity_loc: str = entity_loc
        self.destination_queue = QueueConstants.OUTSENDER_QUEUE
