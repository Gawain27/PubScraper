from datetime import datetime

from net.gwngames.pubscraper.comm.entity.EntityBase import EntityBase
from net.gwngames.pubscraper.msg.BaseMessage import BaseMessage
from net.gwngames.pubscraper.scheduling.sender.OutSenderQueue import OutSenderQueue


class ValidateEntity(BaseMessage):  # TODO logging stuff
    def __init__(self, content: str, entity: EntityBase, timestamp: datetime = None) -> None:
        super().__init__(OutSenderQueue.MSG_VALIDATE_ENTITY, content, timestamp)
        self.entity: EntityBase = entity
