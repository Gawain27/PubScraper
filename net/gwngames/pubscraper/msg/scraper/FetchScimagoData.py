from datetime import datetime

from net.gwngames.pubscraper.msg.scraper.FetchGeneralData import FetchGeneralData
from net.gwngames.pubscraper.scraper.adapter.GeneralDataAdapter import GeneralDataAdapter


class FetchScimagoData(FetchGeneralData):
    def __init__(self, msg_type: str, adapter: GeneralDataAdapter, timestamp: datetime = None) -> None:
        super().__init__(msg_type, adapter, timestamp)
