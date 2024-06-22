from datetime import datetime

from net.gwngames.pubscraper.constants.MessageConstants import MessageConstants
from net.gwngames.pubscraper.constants.QueueConstants import QueueConstants
from net.gwngames.pubscraper.msg.BaseMessage import BaseMessage


class SerializeJSONData(BaseMessage): # TODO logging stuff
    def __init__(self, content: str, json_loc: str, timestamp: datetime = None) -> None:
        super().__init__(MessageConstants.MSG_SERIALIZE_DATA, content, timestamp)
        self.json_loc: str = json_loc
        self.destination_queue = QueueConstants.OUTSENDER_QUEUE
