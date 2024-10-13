import logging
import threading
from abc import abstractmethod
from datetime import datetime
from typing import List, Set, Any, Final

from couchdb import Database, Document

from net.gwngames.pubscraper.Context import Context
from net.gwngames.pubscraper.msg.scraper.scholarly.FetchGeneralData import FetchGeneralData
from net.gwngames.pubscraper.scheduling.IntegerMap import IntegerMap
from net.gwngames.pubscraper.scheduling.MessageRouter import MessageRouter
from net.gwngames.pubscraper.scraper.adapter.AdapterPropertiesConstants import AdapterPropertiesConstants
from net.gwngames.pubscraper.scraper.adapter.GeneralDataAdapter import GeneralDataAdapter
from net.gwngames.pubscraper.scraper.buffer.DatabaseHandler import DatabaseHandler
from net.gwngames.pubscraper.scraper.ifaces.ProxyRunner import ProxyRunner
from net.gwngames.pubscraper.utils.ClassUtils import ClassUtils
from net.gwngames.pubscraper.utils.JsonReader import JsonReader
from net.gwngames.pubscraper.utils.StringUtils import StringUtils


class GeneralDataFetcher:
    def __init__(self):
        self.ctx = Context()
        self._proxy = ProxyRunner()
        self.logger = logging.getLogger(self.__class__.__name__)
        # todo here we will add next entity for the adapter, first is author with the list (see two phase param in const)
        self.adapter_list = []
        self.priorities_map = {}
        self._proxy.init_proxy_choice()

    def generate_fetch_adapter(self, adapter_code: int, opt_arg: list = None) -> GeneralDataAdapter:
        pass

    def start_interface_fetching(self, opt_arg: list = None):
        threading.Thread(
            target=self._start_interface_collectors,
            args=(opt_arg,),
            daemon=True
        ).start()

    # methods must implement loop for scraping for each object, stop signal is given by queue
    def _start_interface_collectors(self, opt_arg: list = None):
        pass

    def get_interface_id(self) -> str:
        pass

    def get_key_fields(self, entity_cid: int) -> list[str]:
        pass

    def fetch_general_data(self, data: FetchGeneralData):
        # Step 1 - Prepare the adapter and the data source
        adapter: GeneralDataAdapter = data.adapter
        if adapter is None:
            raise Exception(f"Null adapter:  [{self.get_interface_id()}] - [{data.content}]")

        data_source: DatabaseHandler = DatabaseHandler(self.ctx.get_dbclient(),
                                                       adapter.get_property(AdapterPropertiesConstants.IFACE_REF))
        database: Database = data_source.get_or_create_db()

        # Step 2 - Obtain id of the entity to process
        existing_data_id = None
        fetch_iterator: list = adapter.get_property(AdapterPropertiesConstants.ALT_ITERABLE, False)

        if fetch_iterator is not False:
            # Alt Iterator exists, this is the initial fetching phase
            existing_data_id = fetch_iterator.pop()
            adapter.add_property(AdapterPropertiesConstants.IFACE_FX_PARAM, existing_data_id)
            adapter.add_property(AdapterPropertiesConstants.ALT_ITERABLE, fetch_iterator)
        else:
            existing_data_id = adapter.get_property(AdapterPropertiesConstants.NEXT_PHASE_ID)

        # Step 3 - Fetch the related entity through the interface or from the data source
        existing_object = database.get(existing_data_id)
        interface_fx = adapter.get_property(AdapterPropertiesConstants.IFACE_FX)
        interface_fx_param = adapter.get_property(AdapterPropertiesConstants.IFACE_FX_PARAM)
        interface_fx_ref = adapter.get_property(AdapterPropertiesConstants.PHASE_REF)
        interface_iter_idx = adapter.get_property(AdapterPropertiesConstants.IFACE_IDX, False)
        fetched_entity = None
        entity_none_or_outdated = self.is_outdated(existing_object)

        if entity_none_or_outdated:

            # Step 3.5 - Differentiate between iterators and simple objects
            if interface_iter_idx is False:  # Does not require index
                method_iter = interface_fx(interface_fx_param)
                fetched_entity = next(method_iter)
                self.logger.info("Fetched entity: %s - %s", data.content, existing_data_id)
            else:  # Requires index
                if adapter.get_property(AdapterPropertiesConstants.IFACE_CACHED_ITER) is None:
                    iter_coll = interface_fx(interface_fx_param)
                    adapter.add_property(AdapterPropertiesConstants.IFACE_CACHED_ITER, iter_coll)
                    adapter.add_property(AdapterPropertiesConstants.NEXT_PHASE_ID, JsonReader.DEV_NULL)

                iterMap = adapter.get_property(AdapterPropertiesConstants.IFACE_CACHED_ITER)
                next_id: int = IntegerMap().get_next_id(self.generate_unique_key(interface_fx_param, self.get_key_fields(interface_fx_ref), limit=True))
                fetched_entity = next(iterMap, next_id)
                self.logger.info("Loaded entity: %s - %s - for id: %s", data.content, existing_data_id, next_id)

            additional_fx = adapter.get_property(AdapterPropertiesConstants.IFACE_ADDITIONAL_FX, True)
            if additional_fx is not False:
                fetched_entity = additional_fx(fetched_entity)
                self.logger.info("Enriched entity: %s - %s", data.content, existing_data_id)

            data_source.insert_or_update_document(data.content, existing_data_id, fetched_entity)
        else:
            fetched_entity = existing_object

        # Step 4 - Request serialization for new object
        if entity_none_or_outdated:
            # todo serialization
            None

        # Step 5 - Prepare adapters for the next phases

        reuse_adapter = ((fetch_iterator is not False and fetch_iterator.__len__() > 0)
                         or interface_iter_idx is not False)
        next_adapters, next_prio = self.prepare_next_phase(interface_fx_ref, fetched_entity)

        if reuse_adapter:
            # adapter is still needed (it's an iterable), reuse it
            next_adapters.append(adapter)
            next_prio[adapter] = data.priority
        for next_adapter, prio in zip(next_adapters, next_prio):
            nextMessage: FetchGeneralData = FetchGeneralData(next_adapter, prio)
            MessageRouter.later_in(nextMessage, priority=prio)

    def prepare_next_phase(self, phase_ref: int, current_entity: Document) -> tuple[list[GeneralDataAdapter], dict]:
        return self.adapter_list, self.priorities_map

    @abstractmethod
    def generate_all_relevant_authors(self, authors_list: str):
        """
                Generate all relevant authors by extracting relevant links from the search results.

                :param authors_list: The base author's list (e.g. '<NAME>, etc...')'
                :return: None, everything is handled with files for less ram complexity
                """
        pass

    @abstractmethod
    def fetch_author_data(self, author: str) -> str:
        """
        :param author:
        :return: the file name of the author
        """
        pass

    @abstractmethod
    def fetch_author_publication(self, publication: Any) -> str:
        """
        :param publication:
        :return: the file name of the publication
        """
        pass

    @staticmethod
    def get_data_fetcher_class(interface_id: str) -> type:
        for cls in ClassUtils.get_all_subclasses(GeneralDataFetcher):
            if getattr(cls, 'INTERFACE_ID', None) == interface_id:
                logging.info('Found interface %s', interface_id)
                return cls
        logging.warning('Interface ID %s not found in GeneralDataFetcher', interface_id)

    def generate_unique_key(self, obj: Any, key_fields: list[str], limit: bool = False) -> str:
        """
        Generates a unique key for a given object based on specified fields,
        including nested fields. If the field value is a string, it includes
        up to the first three words.

        Parameters:
        obj (dict): The input dictionary object.

        Returns:
        str: A unique key generated from the specified fields.
        """

        key_components = []

        for field in key_fields:
            value = self.get_field_value(obj, field)
            if isinstance(value, list):
                # Join list elements with an underscore if the field is a list
                value = '_'.join(value)
            elif isinstance(value, str):
                # Get up to the first three words if the field is a string
                value = StringUtils.sanitize_string(value)
                if limit:
                    value = '_'.join(value.split()[:3])
            elif isinstance(value, int):
                value = str(value)
            key_components.append(value)

        # Combine all components into a single string with underscores
        if limit:
            unique_key = '_'.join(key_components)
        else:
            unique_key = ' '.join(key_components)
        return unique_key

    @classmethod
    def get_field_value(cls, nested_obj: Any, field_path: str):
        """
        Retrieves the value from a nested dictionary based on the field path.

        Parameters:
        nested_obj (dict): The nested dictionary object.
        field_path (str): The dot-separated path to the field.

        Returns:
        str: The value of the field, or an empty string if not found.
        """
        fields = field_path.split('.')
        current_value = nested_obj
        for field in fields:
            if isinstance(current_value, dict):
                current_value = current_value.get(field, '')
            else:
                return ''
        return current_value

    def is_outdated(self, entity: Document):
        # Placeholder function for checking if a record is outdated
        if entity is None:
            return True
        # todo the logic with lambda functions in subclasses
        return False

    def generate_adapter_with_prio(self, ref: int, prio: int, param: Any):
        tmp_adapter = self.generate_fetch_adapter(ref)

        if param is None:
            self.logger.warning(f"None FX parameter found: {ref} - {prio}")
            return

        if isinstance(param, list):
            if len(param) == 0:
                return
            tmp_adapter.add_property(AdapterPropertiesConstants.ALT_ITERABLE, param)
        else:
            tmp_adapter.add_property(AdapterPropertiesConstants.IFACE_FX_PARAM, param)
            tmp_adapter.add_property(AdapterPropertiesConstants.NEXT_PHASE_ID, param)

        self.adapter_list.append(tmp_adapter)
        self.priorities_map[tmp_adapter] = prio
