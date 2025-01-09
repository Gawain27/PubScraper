from datetime import datetime

from com.gwngames.pubscraper.constants.MessageConstants import MessageConstants
from com.gwngames.pubscraper.constants.QueueConstants import QueueConstants
from com.gwngames.pubscraper.msg.BaseMessage import BaseMessage


class PackageEntity(BaseMessage):
    def __init__(self, content: str, entity_id: str, entity_db: str, timestamp: datetime = None) -> None:
        super().__init__(MessageConstants.MSG_PACKAGE_ENTITY, content, timestamp)
        self.entity_id: str = entity_id
        self.system_message = True
        self.entity_db: str = entity_db
        self.destination_queue = QueueConstants.OUTSENDER_QUEUE
