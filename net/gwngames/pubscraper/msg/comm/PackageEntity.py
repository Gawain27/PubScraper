from datetime import datetime

from net.gwngames.pubscraper.constants.MessageConstants import MessageConstants
from net.gwngames.pubscraper.msg.BaseMessage import BaseMessage


class PackageEntity(BaseMessage):  # TODO logging stuff
    def __init__(self, content: str, entity: bytes, cid: int, timestamp: datetime = None) -> None:
        super().__init__(MessageConstants.MSG_PACKAGE_ENTITY, content, timestamp)
        self.entity: bytes = entity
        self.entity_cid: int = cid