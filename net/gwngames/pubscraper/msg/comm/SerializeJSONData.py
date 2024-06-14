from datetime import datetime

from net.gwngames.pubscraper.msg.BaseMessage import BaseMessage
from net.gwngames.pubscraper.scheduling.sender.OutSenderQueue import OutSenderQueue


class SerializeJSONData(BaseMessage): # TODO logging stuff
    def __init__(self, content: str, json_loc: str, timestamp: datetime = None) -> None:
        super().__init__(OutSenderQueue.MSG_SERIALIZE_DATA, content, timestamp)
        self.json_loc: str = json_loc
