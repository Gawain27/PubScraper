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
