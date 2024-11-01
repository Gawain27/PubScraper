from typing import Final
from net.gwngames.pubscraper.scraper.ifaces.CoreEduDataFetcher import CoreEduDataFetcher
from net.gwngames.pubscraper.scraper.ifaces.DblpDataFetcher import DblpDataFetcher
from net.gwngames.pubscraper.scraper.ifaces.ScholarDataFetcher import ScholarDataFetcher
from net.gwngames.pubscraper.scraper.ifaces.ScimagoDataFetcher import ScimagoDataFetcher


class InterfaceConstants:
    # Main interfaces
    GOOGLE_SCHOLAR: Final = ScholarDataFetcher.INTERFACE_ID
    DBLP: Final = DblpDataFetcher.INTERFACE_ID

    # Sub interfaces
    CORE_EDU: Final = CoreEduDataFetcher.INTERFACE_ID
    SCIMAGO_JR: Final = ScimagoDataFetcher.INTERFACE_ID
