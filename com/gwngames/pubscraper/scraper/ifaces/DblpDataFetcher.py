import logging
from typing import Final

from couchdb import Document

from com.gwngames.pubscraper.constants.EntityCidConstants import EntityCidConstants
from com.gwngames.pubscraper.constants.EntityVidConstants import EntityVidConstants
from com.gwngames.pubscraper.constants.MessageConstants import MessageConstants
from com.gwngames.pubscraper.constants.PriorityConstants import PriorityConstants
from com.gwngames.pubscraper.msg.scraper.FetchDblpData import FetchDblpData
from com.gwngames.pubscraper.scheduling.MessageRouter import MessageRouter
from com.gwngames.pubscraper.scraper.adapter.AdapterPropertiesConstants import AdapterPropertiesConstants
from com.gwngames.pubscraper.scraper.adapter.GeneralDataAdapter import GeneralDataAdapter
from com.gwngames.pubscraper.scraper.ifaces.GeneralDataFetcher import GeneralDataFetcher
from com.gwngames.pubscraper.scraper.scraper.DblpScraper import DblpScraper
from com.gwngames.pubscraper.utils.JsonReader import JsonReader


class DblpDataFetcher(GeneralDataFetcher):
    INTERFACE_ID: Final = 'dblp'
    authors_seen = []
    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.config = JsonReader(JsonReader.CONFIG_FILE_NAME, parent=self.INTERFACE_ID)
        self.db = self.get_or_create_db(self.ctx.get_dbclient(), self.INTERFACE_ID)

    def generate_fetch_adapter(self, adapter_code: int) -> GeneralDataAdapter:
        adapter: GeneralDataAdapter = GeneralDataAdapter()
        adapter.add_property(AdapterPropertiesConstants.IFACE_REF, self.INTERFACE_ID)
        adapter.add_property(AdapterPropertiesConstants.PHASE_REF, adapter_code)

        if adapter_code == EntityCidConstants.PUB:
            adapter.add_property(AdapterPropertiesConstants.ROLL_OVER_DEPTH, False)
            adapter.add_property(AdapterPropertiesConstants.IFACE_FX, DblpScraper().get_author_publications)
            adapter.add_property(AdapterPropertiesConstants.MULTI_RESULT, True)
        else:
            raise Exception("Dblp - unknown adapter: " + str(adapter_code))

        return adapter

    def prepare_next_phase(self, phase_ref: int, current_entity: Document, phase_depth: int, prev_adapter: GeneralDataAdapter) -> tuple[list[GeneralDataAdapter], dict]:
        self.adapter_list, self.priorities_map = ([], {})
        self.logger.debug("Processing next phase from: %s", prev_adapter.get_property(AdapterPropertiesConstants.EXPECTED_ID))

        if phase_ref == EntityCidConstants.PUB:
            pubs = current_entity.get("publications", [])
            for pub in pubs:
                for author in pub.get("authors", []):
                    if author not in DblpDataFetcher.authors_seen:
                        DblpDataFetcher.authors_seen.append(author)
                        self.generate_adapter_with_prio(EntityCidConstants.PUB,
                                                        PriorityConstants.PUB_REQ, [author], author)

        self.logger.debug("Completed processing next phase from: %s", prev_adapter.get_property(AdapterPropertiesConstants.EXPECTED_ID))

        return super(DblpDataFetcher, self).prepare_next_phase(phase_ref, current_entity, phase_depth=phase_depth, prev_adapter=prev_adapter)

    def _start_interface_collectors(self, opt_arg: list):
        for author_name in opt_arg:
            adapter = self.generate_fetch_adapter(EntityCidConstants.PUB)
            adapter.add_property(AdapterPropertiesConstants.IFACE_FX_PARAM_LIST, [author_name])
            adapter.add_property(AdapterPropertiesConstants.EXPECTED_ID, author_name)
            MessageRouter.get_instance().send_message(FetchDblpData(MessageConstants.MSG_DBLP_AUTHOR, adapter),
                                    PriorityConstants.PUB_REQ)

    def get_interface_id(self) -> str:
        return self.INTERFACE_ID

    def get_variant_type(self) -> int:
        return EntityVidConstants.DBLP_VID

