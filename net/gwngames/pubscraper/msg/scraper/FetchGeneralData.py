import datetime

from net.gwngames.pubscraper.constants.QueueConstants import QueueConstants
from net.gwngames.pubscraper.msg.BaseMessage import BaseMessage
from net.gwngames.pubscraper.scraper.adapter.AdapterPropertiesConstants import AdapterPropertiesConstants
from net.gwngames.pubscraper.scraper.adapter.GeneralDataAdapter import GeneralDataAdapter


class FetchGeneralData(BaseMessage):
    def __init__(self, msg_type: str, adapter: GeneralDataAdapter, timestamp: datetime = None) -> None:
        super().__init__(msg_type,
                         adapter.get_property(AdapterPropertiesConstants.PHASE_REF), timestamp)
        self.adapter: GeneralDataAdapter = adapter
        self.destination_queue = QueueConstants.SCRAPER_QUEUE
        self.depth = 0

    def get_group_key(self) -> str:
        return str(self.adapter.get_property(AdapterPropertiesConstants.PHASE_REF))

    def __str__(self) -> str:
        expected_id = self.adapter.get_property(AdapterPropertiesConstants.EXPECTED_ID, can_fail=False)
        if expected_id is None:
            expected_id = "None"
            return f"Message Type: {self.message_type}, Expected ID: {expected_id}, Timestamp: {self.timestamp}"
        else:
            return f"Message Type: {self.message_type}, Expected ID: {str(expected_id)}"


