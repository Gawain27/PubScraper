import json
import logging
import threading
import traceback
from abc import abstractmethod
from datetime import datetime, timedelta
from typing import Any

from couchdb import Database, Document, Server, ResourceNotFound, Unauthorized, ServerError

from net.gwngames.pubscraper.Context import Context
from net.gwngames.pubscraper.constants.ConfigConstants import ConfigConstants
from net.gwngames.pubscraper.constants.PriorityConstants import PriorityConstants
from net.gwngames.pubscraper.msg.comm.SerializeEntity import SerializeEntity
from net.gwngames.pubscraper.msg.scraper.FetchGeneralData import FetchGeneralData
from net.gwngames.pubscraper.scheduling.IntegerMap import IntegerMap
from net.gwngames.pubscraper.scheduling.MessageRouter import MessageRouter
from net.gwngames.pubscraper.scraper.adapter.AdapterPropertiesConstants import AdapterPropertiesConstants
from net.gwngames.pubscraper.scraper.adapter.GeneralDataAdapter import GeneralDataAdapter
from net.gwngames.pubscraper.scraper.buffer.DatabaseHandler import DatabaseHandler
from net.gwngames.pubscraper.utils.ClassUtils import ClassUtils
from net.gwngames.pubscraper.utils.JsonReader import JsonReader
from net.gwngames.pubscraper.utils.StringUtils import StringUtils


class GeneralDataFetcher:
    def __init__(self):
        self.ctx = Context()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.adapter_list = []
        self.priorities_map = {}

    def get_or_create_db(self, client: Server, db_name):
        try:
            db = client[db_name]
        except ResourceNotFound:
            self.logger.info(f"Database {db_name} not found. Creating new database.")
            db = client.create(db_name)
        except Unauthorized:
            self.logger.error("Unauthorized access to CouchDB. Check your credentials.")
            raise
        except ServerError as e:
            self.logger.error(f"Server error: {e}")
            raise
        return db

    @abstractmethod
    def generate_fetch_adapter(self, adapter_code: int, opt_arg: list = None) -> GeneralDataAdapter:
        pass

    def start_interface_fetching(self, opt_arg: list | int = None):
        threading.Thread(
            target=self._start_interface_collectors,
            args=(opt_arg,),
            daemon=True
        ).start()

    # methods must implement loop for scraping for each object, stop signal is given by queue
    @abstractmethod
    def _start_interface_collectors(self, opt_arg: list | int = None):
        pass

    @abstractmethod
    def get_interface_id(self) -> str:
        pass

    @abstractmethod
    def get_key_fields(self, entity_cid: int) -> list[str]:
        pass

    def fetch_general_data(self, data: FetchGeneralData):
        try:
            # Step 1 - Prepare the adapter and the data source
            adapter: GeneralDataAdapter = data.adapter
            if adapter is None:
                raise Exception(f"Null adapter: [{self.get_interface_id()}] - [{data.content}]")

            data_source: DatabaseHandler = DatabaseHandler(self.ctx.get_dbclient(),
                                                           adapter.get_property(AdapterPropertiesConstants.IFACE_REF))
            database: Database = data_source.get_or_create_db()

            # Step 2 - Obtain id of the entity to process
            fetch_iterator: list | bool = adapter.get_property(AdapterPropertiesConstants.ALT_ITERABLE, False)

            if fetch_iterator is not False:
                existing_data_id = fetch_iterator.pop()
                adapter.add_property(AdapterPropertiesConstants.IFACE_FX_PARAM, existing_data_id)
                adapter.add_property(AdapterPropertiesConstants.ALT_ITERABLE, fetch_iterator)
            else:
                existing_data_id = adapter.get_property(AdapterPropertiesConstants.EXPECTED_ID)

            # Step 3 - Fetch the related entity through the interface or from the data source
            existing_object = database.get(existing_data_id)
            interface_fx = adapter.get_property(AdapterPropertiesConstants.IFACE_FX)
            interface_fx_param = adapter.get_property(AdapterPropertiesConstants.IFACE_FX_PARAM)
            interface_fx_ref = adapter.get_property(AdapterPropertiesConstants.PHASE_REF)
            interface_iter_idx = adapter.get_property(AdapterPropertiesConstants.IFACE_IDX, False)
            entity_none_or_outdated = self.is_outdated(existing_object)

            if entity_none_or_outdated:
                self.logger.info("Entity outdated or not found for ID: %s", existing_data_id)

                # Step 3.5 - Differentiate between iterators and simple objects
                if not adapter.get_property(AdapterPropertiesConstants.IFACE_IS_ITERATOR, failable=False):
                    fetched_entity = interface_fx(interface_fx_param)
                    self.logger.info("Fetched simple entity: %s - ID: %s", data.content, existing_data_id)
                else:
                    if interface_iter_idx is False:  # Does not require index
                        fetched_entity = interface_fx(interface_fx_param)
                        self.logger.info("Fetched entity from iterator: %s - ID: %s", data.content, existing_data_id)
                    else:  # Requires index
                        if adapter.get_property(AdapterPropertiesConstants.IFACE_CACHED_ITER) is None:
                            self.logger.debug("Initializing new cached iterator for content: %s", data.content)
                            iter_coll = interface_fx(interface_fx_param)
                            adapter.add_property(AdapterPropertiesConstants.IFACE_CACHED_ITER, iter_coll)
                            adapter.add_property(AdapterPropertiesConstants.EXPECTED_ID, JsonReader.DEV_NULL)

                        iter_map = adapter.get_property(AdapterPropertiesConstants.IFACE_CACHED_ITER)
                        next_id: int = IntegerMap().get_next_id(
                            self.generate_unique_key(interface_fx_param, self.get_key_fields(interface_fx_ref),
                                                     limit=True))
                        fetched_entity = next(iter_map, next_id)
                        self.logger.info("Loaded entity from cached iterator: %s - ID: %s - Next ID: %s", data.content,
                                         existing_data_id, next_id)

                additional_fx = adapter.get_property(AdapterPropertiesConstants.IFACE_ADDITIONAL_FX, failable=False)
                if additional_fx is not False:
                    fetched_entity = additional_fx(fetched_entity)
                    self.logger.info("Enriched entity for content: %s - ID: %s", data.content, existing_data_id)

                if fetched_entity is not None and fetched_entity is not False:  # falsy conversion for empty lists, etc...
                    if isinstance(fetched_entity, dict):
                        fetched_entity["serialized"] = False
                    elif isinstance(fetched_entity, str):
                        fetched_entity = json.loads(fetched_entity)
                        fetched_entity["serialized"] = False
                    else:
                        raise Exception(fetched_entity.__class__.__name__ + " not serializable")

                    if adapter.get_property(AdapterPropertiesConstants.MULTI_RESULT, failable=False) is True:
                        fetched_entity[AdapterPropertiesConstants.MULTI_RESULT] = True

                    self.logger.debug("Inserting or updating document in data source for content: %s - %s", data.content, existing_data_id)
                    data_source.insert_or_update_document(data.content, existing_data_id, fetched_entity)
            else:
                self.logger.info("Entity is up-to-date: %s - ID: %s", data.content, existing_data_id)
                fetched_entity = existing_object

            # Step 4 - Request serialization for new object
            if entity_none_or_outdated and fetched_entity is not None:
                self.logger.debug("Requesting serialization for content: %s - ID: %s", data.content, existing_data_id)
                serialize_entity_msg = SerializeEntity(data.content,
                                                       entity_id=existing_data_id,
                                                       entity_db=adapter.get_property(
                                                           AdapterPropertiesConstants.IFACE_REF),
                                                       entity_class=int(
                                                           adapter.get_property(AdapterPropertiesConstants.PHASE_REF)),
                                                       entity_variant=self.get_variant_type())
                serialize_entity_msg.system_message = True
                MessageRouter.get_instance().send_message(serialize_entity_msg, priority=PriorityConstants.ENTITY_SERIAL_REQ)

            # Step 5 - Prepare adapters for the next phases
            reuse_adapter = ((fetch_iterator is not False and fetch_iterator.__len__() > 0)
                             or interface_iter_idx is not False)
            next_adapters, next_prio = ([], {})
            if fetched_entity is not None:
                next_adapters, next_prio = self.prepare_next_phase(interface_fx_ref, fetched_entity, data.depth)

            if reuse_adapter:
                self.logger.debug("Reusing adapter for content: %s - ID: %s", data.content, existing_data_id)
                # adapter is still needed (it's an iterable), reuse it
                next_adapters.append(adapter)
                next_prio[adapter] = data.priority

            for next_adapter, prio in zip(next_adapters, next_prio.values()):
                next_message = data.__class__(data.__class__.__name__, adapter=next_adapter)
                next_message.depth = data.depth + 1

                if next_adapter.get_property(AdapterPropertiesConstants.IFACE_IDX, failable=False) is not False:
                    next_message.depth -= 1 # Re-iterable should not decrease, they are multi root

                MessageRouter.later_in(next_message, priority=prio)

        except Exception as e:
            self.logger.error("Error fetching general data for content: %s - Error: %s", data.content, str(e))
            self.logger.error(traceback.format_exc())
            raise e

    @abstractmethod
    def prepare_next_phase(self, phase_ref: int, current_entity: Document, phase_depth: int) -> tuple[list[GeneralDataAdapter], dict]:
        return self.adapter_list, self.priorities_map

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
        if entity is None or entity.get("update_date") is None:
            return True
        decoded_date = datetime.strptime(entity.get("update_date"), "%Y-%m-%d %H:%M:%S")
        time_difference = datetime.now() - decoded_date
        if abs(time_difference) > timedelta(seconds=self.ctx.get_config().get_value(ConfigConstants.MIN_SECONDS_BEWTWEEN_UPDATES)):
            self.logger.info("[General] Entity outdated: " + str(entity.get("_id")))
            return True
        return False

    def generate_adapter_with_prio(self, ref: int, prio: int, param: Any, expected_id: str, fetcher=None):
        if param is None:
            self.logger.warning(f"None FX parameter found: {ref} - {prio}")
            return

        if fetcher is not None:
            tmp_adapter = fetcher.generate_fetch_adapter(ref)
        else:
            tmp_adapter = self.generate_fetch_adapter(ref)

        if isinstance(param, list):
            if len(param) == 0:
                return
            tmp_adapter.add_property(AdapterPropertiesConstants.ALT_ITERABLE, param)
        else:
            tmp_adapter.add_property(AdapterPropertiesConstants.IFACE_FX_PARAM, param)
            tmp_adapter.add_property(AdapterPropertiesConstants.EXPECTED_ID, expected_id)

        self.adapter_list.append(tmp_adapter)
        self.priorities_map[tmp_adapter] = prio
        
        return tmp_adapter

    @abstractmethod
    def get_variant_type(self) -> int:
        pass
