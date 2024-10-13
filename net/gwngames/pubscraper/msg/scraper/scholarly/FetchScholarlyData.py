import datetime

from net.gwngames.pubscraper.constants.MessageConstants import MessageConstants
from net.gwngames.pubscraper.msg.scraper.scholarly.FetchGeneralData import FetchGeneralData
from net.gwngames.pubscraper.scraper.adapter.GeneralDataAdapter import GeneralDataAdapter


class FetchScholarlyData(FetchGeneralData):
    def __init__(self, adapter: GeneralDataAdapter, timestamp: datetime = None) -> None:
        super().__init__(MessageConstants.MSG_ALL_SCHOLARLY_AUTHORS, adapter, timestamp)
