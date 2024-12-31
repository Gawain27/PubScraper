import logging
from typing import Final

from couchdb import Document

from net.gwngames.pubscraper.constants.EntityCidConstants import EntityCidConstants
from net.gwngames.pubscraper.constants.EntityVidConstants import EntityVidConstants
from net.gwngames.pubscraper.constants.LoggingConstants import LoggingConstants
from net.gwngames.pubscraper.constants.MessageConstants import MessageConstants
from net.gwngames.pubscraper.constants.PriorityConstants import PriorityConstants
from net.gwngames.pubscraper.msg.scraper.FetchDblpData import FetchDblpData
from net.gwngames.pubscraper.scheduling.MessageRouter import MessageRouter
from net.gwngames.pubscraper.scraper.adapter.AdapterPropertiesConstants import AdapterPropertiesConstants
from net.gwngames.pubscraper.scraper.adapter.GeneralDataAdapter import GeneralDataAdapter
from net.gwngames.pubscraper.scraper.ifaces.GeneralDataFetcher import GeneralDataFetcher
from net.gwngames.pubscraper.scraper.scraper.DblpScraper import DblpScraper
from net.gwngames.pubscraper.utils.JsonReader import JsonReader


class DblpDataFetcher(GeneralDataFetcher):
    INTERFACE_ID: Final = 'dblp'
    authors_seen = []
    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(self.__class__.__name__)
        logging.basicConfig(level=LoggingConstants.DBLP_DATA_FETCHER)
        self.config = JsonReader(JsonReader.CONFIG_FILE_NAME, parent=self.INTERFACE_ID)
        self.db = self.get_or_create_db(self.ctx.get_dbclient(), self.INTERFACE_ID)

    def generate_fetch_adapter(self, adapter_code: int, opt_arg: list = None) -> GeneralDataAdapter:
        adapter: GeneralDataAdapter = GeneralDataAdapter()
        adapter.add_property(AdapterPropertiesConstants.IFACE_REF, self.INTERFACE_ID)
        adapter.add_property(AdapterPropertiesConstants.PHASE_REF, adapter_code)

        if adapter_code == EntityCidConstants.PUB:
            adapter.add_property(AdapterPropertiesConstants.IFACE_FX, DblpScraper().get_author_publications)
            adapter.add_property(AdapterPropertiesConstants.MULTI_RESULT, True)
        else:
            raise Exception("Dblp - unknown adapter: " + str(adapter_code))

        if opt_arg is not None:
            adapter.add_property(AdapterPropertiesConstants.ALT_ITERABLE, opt_arg)
        return adapter

    def prepare_next_phase(self, phase_ref: int, current_entity: Document, phase_depth: int, prev_adapter: GeneralDataAdapter) -> tuple[list[GeneralDataAdapter], dict]:
        self.adapter_list, self.priorities_map = ([], {})
        if phase_ref == EntityCidConstants.PUB:
            pubs = current_entity.get("publications", [])
            for pub in pubs:
                for author in pub.get("authors", []):
                    if author not in DblpDataFetcher.authors_seen:
                        DblpDataFetcher.authors_seen.append(author)
                        self.generate_adapter_with_prio(EntityCidConstants.PUB,
                                                        PriorityConstants.PUB_REQ * 5, author, author)
                        self.logger.debug("Processing Dblp Authors next phase")

        return super(DblpDataFetcher, self).prepare_next_phase(phase_ref, current_entity, phase_depth=phase_depth, prev_adapter=prev_adapter)

    def _start_interface_collectors(self, opt_arg: list | int = None):
        for author_name in opt_arg:
            adapter = self.generate_fetch_adapter(EntityCidConstants.PUB)
            adapter.add_property(AdapterPropertiesConstants.IFACE_FX_PARAM, author_name)
            adapter.add_property(AdapterPropertiesConstants.EXPECTED_ID, author_name)
            MessageRouter.later_in(FetchDblpData(MessageConstants.MSG_ALL_DBLP_AUTHORS, adapter),
                                    PriorityConstants.PUB_REQ, 0)

    def get_interface_id(self) -> str:
        return self.INTERFACE_ID

    def get_key_fields(self, entity_cid: int) -> list[str]:
        raise Exception("Dblp - unknown iter cid: " + str(entity_cid))

    def get_variant_type(self) -> int:
        return EntityVidConstants.DBLP_VID

