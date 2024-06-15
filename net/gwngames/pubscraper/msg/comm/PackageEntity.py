from datetime import datetime

from net.gwngames.pubscraper.comm.entity.EntityBase import EntityBase
from net.gwngames.pubscraper.msg.BaseMessage import BaseMessage
from net.gwngames.pubscraper.scheduling.sender.OutSenderQueue import OutSenderQueue


class PackageEntity(BaseMessage):  # TODO logging stuff
    def __init__(self, content: str, entity: bytes, cid: int, timestamp: datetime = None) -> None:
        super().__init__(OutSenderQueue.MSG_PACKAGE_ENTITY, content, timestamp)
        self.entity: bytes = entity
        self.entity_cid: int = cid
