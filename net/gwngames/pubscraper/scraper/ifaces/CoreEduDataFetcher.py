import logging
from typing import Final

from couchdb import Document

from net.gwngames.pubscraper.constants.EntityCidConstants import EntityCidConstants
from net.gwngames.pubscraper.constants.EntityVidConstants import EntityVidConstants
from net.gwngames.pubscraper.constants.JsonConstants import JsonConstants
from net.gwngames.pubscraper.constants.LoggingConstants import LoggingConstants
from net.gwngames.pubscraper.constants.PriorityConstants import PriorityConstants
from net.gwngames.pubscraper.scraper.adapter.AdapterPropertiesConstants import AdapterPropertiesConstants
from net.gwngames.pubscraper.scraper.adapter.GeneralDataAdapter import GeneralDataAdapter
from net.gwngames.pubscraper.scraper.ifaces.GeneralDataFetcher import GeneralDataFetcher
from net.gwngames.pubscraper.scraper.scraper.CoreEduScraper import CoreEduScraper
from net.gwngames.pubscraper.utils.JsonReader import JsonReader


class CoreEduDataFetcher(GeneralDataFetcher):
    INTERFACE_ID: Final = 'core_edu'

    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(self.__class__.__name__)
        logging.basicConfig(level=LoggingConstants.CORE_EDU_DATA_FETCHER)
        self.config = JsonReader(JsonReader.CONFIG_FILE_NAME, parent=self.INTERFACE_ID)
        self.db = self.get_or_create_db(self.ctx.get_dbclient(), self.INTERFACE_ID)

    def generate_fetch_adapter(self, adapter_code: int, opt_arg: list = None) -> GeneralDataAdapter:
        adapter: GeneralDataAdapter = GeneralDataAdapter()
        adapter.add_property(AdapterPropertiesConstants.IFACE_REF, self.INTERFACE_ID)
        adapter.add_property(AdapterPropertiesConstants.PHASE_REF, adapter_code)

        if adapter_code == EntityCidConstants.CORE_MAIN_CONFERENCE_RANK:
            adapter.add_property(AdapterPropertiesConstants.IFACE_FX, CoreEduScraper().get_conference_details)
            adapter.add_property(AdapterPropertiesConstants.MULTI_RESULT, True)
        elif adapter_code == EntityCidConstants.CORE_CONFERENCE_RANK:
            adapter.add_property(AdapterPropertiesConstants.IFACE_FX, CoreEduScraper().get_conference_year_details)
            adapter.add_property(AdapterPropertiesConstants.MULTI_RESULT, True)
        else:
            raise Exception("Core edu - unknown adapter: " + str(adapter_code))

        if opt_arg is not None:
            adapter.add_property(AdapterPropertiesConstants.ALT_ITERABLE, opt_arg)
        return adapter

    def _start_interface_collectors(self, opt_arg: list = None):
        return

    def get_interface_id(self) -> str:
        return self.INTERFACE_ID

    def get_key_fields(self, entity_cid: int) -> list[str]:
        raise Exception("Core edu - unknown iter cid: " + str(entity_cid))

    def prepare_next_phase(self, phase_ref: int, current_entity: Document) -> tuple[list[GeneralDataAdapter], dict]:
        self.adapter_list, self.priorities_map = ([], {})

        self.logger.info(f"Preparing next phase for phase_ref: {phase_ref}")

        if phase_ref == EntityCidConstants.CORE_MAIN_CONFERENCE_RANK:
            self.logger.debug("Processing Core edu conference phase")
            conferences = current_entity.get(JsonConstants.TAG_CONFERENCES, [])

            for conf in conferences:
                self.generate_adapter_with_prio(EntityCidConstants.CORE_CONFERENCE_RANK,
                                                PriorityConstants.CONFERENCE_RANK_REQ, conf["conference_url"], conf["conference_url"])

        self.logger.info("Completed preparing next phase for phase_ref: {phase_ref}")
        return super(CoreEduDataFetcher, self).prepare_next_phase(phase_ref, current_entity)

    def get_variant_type(self) -> int:
        return EntityVidConstants.CORE_EDU_VID

