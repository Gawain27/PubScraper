import datetime

from net.gwngames.pubscraper.msg.scraper.FetchGeneralData import FetchGeneralData
from net.gwngames.pubscraper.scraper.adapter.AdapterPropertiesConstants import AdapterPropertiesConstants
from net.gwngames.pubscraper.scraper.adapter.GeneralDataAdapter import GeneralDataAdapter


class FetchScholarlyData(FetchGeneralData):
    def __init__(self, msg_type: str, adapter: GeneralDataAdapter, timestamp: datetime = None) -> None:
        super().__init__(msg_type, adapter, timestamp)
