import datetime

from com.gwngames.pubscraper.constants.QueueConstants import QueueConstants
from com.gwngames.pubscraper.msg.BaseMessage import BaseMessage
from com.gwngames.pubscraper.scraper.adapter.AdapterPropertiesConstants import AdapterPropertiesConstants
from com.gwngames.pubscraper.scraper.adapter.GeneralDataAdapter import GeneralDataAdapter


class FetchGeneralData(BaseMessage):
    def __init__(self, msg_type: str, adapter: GeneralDataAdapter, timestamp: datetime = None, depth: int = 0) -> None:
        super().__init__(msg_type,
                         adapter.get_property(AdapterPropertiesConstants.PHASE_REF), timestamp=timestamp,
                         depth=depth)
        self.adapter: GeneralDataAdapter = adapter
        self.destination_queue = QueueConstants.SCRAPER_QUEUE
        self.depth = 0

    def __str__(self) -> str:
        expected_id = self.adapter.get_property(AdapterPropertiesConstants.EXPECTED_ID, can_fail=False)
        if expected_id is None:
            expected_id = "None"
            return f"Message Type: {self.message_type}, Expected ID: {expected_id}, Timestamp: {self.timestamp}"
        else:
            return f"Message Type: {self.message_type}, Expected ID: {str(expected_id)}"


