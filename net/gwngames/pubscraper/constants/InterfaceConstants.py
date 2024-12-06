from typing import Final
from net.gwngames.pubscraper.scraper.ifaces.DblpDataFetcher import DblpDataFetcher
from net.gwngames.pubscraper.scraper.ifaces.ScholarDataFetcher import ScholarDataFetcher


class InterfaceConstants:
    # Main interfaces
    GOOGLE_SCHOLAR: Final = ScholarDataFetcher.INTERFACE_ID
    DBLP: Final = DblpDataFetcher.INTERFACE_ID
