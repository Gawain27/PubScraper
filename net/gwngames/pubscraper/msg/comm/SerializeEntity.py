from datetime import datetime

from net.gwngames.pubscraper.msg.BaseMessage import BaseMessage
from net.gwngames.pubscraper.scheduling.sender.OutSenderQueue import OutSenderQueue


class SerializeEntity(BaseMessage): # TODO logging stuff
    def __init__(self, content: str, entity_loc: str, timestamp: datetime = None) -> None:
        super().__init__(OutSenderQueue.MSG_SERIALIZE_ENTITY, content, timestamp)
        self.entity_loc: str = entity_loc
