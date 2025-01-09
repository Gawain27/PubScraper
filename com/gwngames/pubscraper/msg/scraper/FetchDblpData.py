from datetime import datetime

from com.gwngames.pubscraper.msg.scraper.FetchGeneralData import FetchGeneralData
from com.gwngames.pubscraper.scraper.adapter.GeneralDataAdapter import GeneralDataAdapter


class FetchDblpData(FetchGeneralData):
    def __init__(self, msg_type: str, adapter: GeneralDataAdapter, timestamp: datetime = None, depth: int = 0) -> None:
        super().__init__(msg_type, adapter, timestamp=timestamp, depth=depth)
