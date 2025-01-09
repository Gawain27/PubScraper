import json
import logging
import threading
import traceback
from abc import abstractmethod
from datetime import datetime, timedelta
from typing import Any

from couchdb import Database, Document, Server, ResourceNotFound, Unauthorized, ServerError

from com.gwngames.pubscraper.Context import Context
from com.gwngames.pubscraper.constants.ConfigConstants import ConfigConstants
from com.gwngames.pubscraper.constants.PriorityConstants import PriorityConstants
from com.gwngames.pubscraper.msg.comm.SerializeEntity import SerializeEntity
from com.gwngames.pubscraper.msg.scraper.FetchGeneralData import FetchGeneralData
from com.gwngames.pubscraper.scheduling.MessageRouter import MessageRouter
from com.gwngames.pubscraper.scraper.adapter.AdapterPropertiesConstants import AdapterPropertiesConstants
from com.gwngames.pubscraper.scraper.adapter.GeneralDataAdapter import GeneralDataAdapter
from com.gwngames.pubscraper.scraper.buffer.DatabaseHandler import DatabaseHandler
from com.gwngames.pubscraper.utils.ClassUtils import ClassUtils


class GeneralDataFetcher:
    duplicate_lock = threading.Lock()
    seen_ids = []

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
    def generate_fetch_adapter(self, adapter_code: int) -> GeneralDataAdapter:
        pass

    def start_interface_fetching(self, opt_arg: list):
        threading.Thread(
            target=self._start_interface_collectors,
            args=(opt_arg,),
            daemon=True
        ).start()

    # methods must implement loop for scraping for each object, stop signal is given by queue
    @abstractmethod
    def _start_interface_collectors(self, opt_arg: list):
        pass

    @abstractmethod
    def get_interface_id(self) -> str:
        pass


    def fetch_general_data(self, data: FetchGeneralData):
        try:
            self.logger.info("Starting fetch_general_data for content: %s", data.content)

            # Step 1 - Prepare the adapter and the data source
            self.logger.debug("Preparing adapter and data source for content: %s", data.content)
            adapter: GeneralDataAdapter = data.adapter
            if adapter is None:
                error_message = f"Null adapter: [{self.get_interface_id()}] - [{data.content}]"
                self.logger.error(error_message)
                raise Exception(error_message)

            data_source: DatabaseHandler = DatabaseHandler(self.ctx.get_dbclient(),
                                                           adapter.get_property(AdapterPropertiesConstants.IFACE_REF))
            database: Database = data_source.get_or_create_db()

            # Step 2 - Obtain id of the entity to process
            self.logger.info("Obtaining entity ID to process for content: %s", data.content)

            existing_data_id = adapter.get_property(AdapterPropertiesConstants.EXPECTED_ID)

            # Step 3 - Fetch the related entity through the interface or from the data source
            self.logger.info("Fetching entity from database or interface for content: %s", data.content)
            existing_object = database.get(existing_data_id)
            interface_fx = adapter.get_property(AdapterPropertiesConstants.IFACE_FX)
            interface_fx_params = adapter.get_property(AdapterPropertiesConstants.IFACE_FX_PARAM_LIST)
            interface_fx_ref = adapter.get_property(AdapterPropertiesConstants.PHASE_REF)

            entity_none_or_outdated = self.is_outdated(existing_object)

            if entity_none_or_outdated:
                self.logger.info("Entity outdated or not found for ID: %s", existing_data_id)

                self.logger.info("Fetching simple entity for content: %s", data.content)
                fetched_entity = interface_fx(*interface_fx_params)

                self.logger.info("Fetched simple entity: %s - ID: %s", data.content, existing_data_id)


                additional_fx = adapter.get_property(AdapterPropertiesConstants.IFACE_ADDITIONAL_FX, can_fail=False)
                if additional_fx is not None:
                    self.logger.info("Applying additional function for content: %s", data.content)
                    fetched_entity = additional_fx(fetched_entity)
                    self.logger.info("Enriched entity for content: %s - ID: %s", data.content, existing_data_id)

                if fetched_entity is not None and fetched_entity is not False:  # falsy conversion for empty lists, etc...
                    if isinstance(fetched_entity, dict):
                        fetched_entity["serialized"] = False
                    elif isinstance(fetched_entity, str):
                        fetched_entity = json.loads(fetched_entity)
                        fetched_entity["serialized"] = False
                    else:
                        error_message = f"{fetched_entity.__class__.__name__} not serializable"
                        self.logger.error(error_message)
                        raise Exception(error_message)

                    if adapter.get_property(AdapterPropertiesConstants.MULTI_RESULT, can_fail=False) is True:
                        fetched_entity[AdapterPropertiesConstants.MULTI_RESULT] = True

                    self.logger.info("Inserting or updating document in data source for content: %s - %s",
                                      data.content, existing_data_id)
                    data_source.insert_or_update_document(data.content, existing_data_id, fetched_entity)
            else:
                self.logger.info("Entity is up-to-date: %s - ID: %s", data.content, existing_data_id)
                fetched_entity = existing_object

            # Step 4 - Request serialization for new object
            if entity_none_or_outdated and fetched_entity is not None:
                self.logger.info("Requesting serialization for content: %s - ID: %s", data.content, existing_data_id)
                serialize_entity_msg = SerializeEntity(data.content,
                                                       entity_id=existing_data_id,
                                                       entity_db=adapter.get_property(
                                                           AdapterPropertiesConstants.IFACE_REF),
                                                       entity_class=int(
                                                           adapter.get_property(AdapterPropertiesConstants.PHASE_REF)),
                                                       entity_variant=self.get_variant_type())
                serialize_entity_msg.system_message = True
                MessageRouter.get_instance().send_message(serialize_entity_msg,
                                                          priority=PriorityConstants.ENTITY_SERIAL_REQ)

            # Step 5 - Prepare adapters for the next phases

            next_adapters, next_prio = ([], {})
            if fetched_entity is not None:
                self.logger.info("Preparing next phase adapters for content: %s", data.content)
                next_adapters, next_prio = self.prepare_next_phase(interface_fx_ref, fetched_entity, data.depth,
                                                                   data.adapter)

            for next_adapter, prio in zip(next_adapters, next_prio.values()):
                next_message = data.__class__(data.__class__.__name__, adapter=next_adapter, depth=data.depth)
                next_message.depth = data.depth

                if next_adapter.get_property(AdapterPropertiesConstants.ROLL_OVER_DEPTH, can_fail=False) is True:
                    self.logger.info("Rolling over: %s - %s - depth %s -> %s", adapter.get_property(AdapterPropertiesConstants.PHASE_REF),
                                     next_adapter.get_property(AdapterPropertiesConstants.IFACE_FX_PARAM_LIST, can_fail=False),
                                     next_message.depth, next_message.depth-1)
                    next_message.depth = next_message.depth - 1
                MessageRouter.later_in(next_message, priority=prio)

        except Exception as e:
            self.logger.error("Error fetching general data for content: %s - Error: %s", data.content, str(e))
            self.logger.error(traceback.format_exc())
            raise e

    @abstractmethod
    def prepare_next_phase(self, phase_ref: int, current_entity: Document, phase_depth: int, prev_adapter: GeneralDataAdapter) -> tuple[list[GeneralDataAdapter], dict]:
        return self.adapter_list, self.priorities_map

    @staticmethod
    def get_data_fetcher_class(interface_id: str) -> type:
        for cls in ClassUtils.get_all_subclasses(GeneralDataFetcher):
            if getattr(cls, 'INTERFACE_ID', None) == interface_id:
                logging.info('Found interface %s', interface_id)
                return cls
        logging.warning('Interface ID %s not found in GeneralDataFetcher', interface_id)


    def is_outdated(self, entity: Document):
        # Placeholder function for checking if a record is outdated
        if entity is None or entity.get("update_date") is None:
            return True
        if entity.get("serialized") is None or entity.get("serialized") is False:
            return True
        decoded_date = datetime.strptime(entity.get("update_date"), "%Y-%m-%d %H:%M:%S")
        time_difference = datetime.now() - decoded_date
        if abs(time_difference) > timedelta(seconds=self.ctx.get_config().get_value(ConfigConstants.MIN_SECONDS_BEWTWEEN_UPDATES)):
            self.logger.info("[General] Entity outdated: " + str(entity.get("_id")))
            return True
        return False

    def generate_adapter_with_prio(self, ref: int, prio: int, param_list: list, expected_id: str):
        if param_list is None:
            self.logger.warning(f"None FX parameters found: {ref} - {prio}")
            return

        if expected_id is not None:
            with GeneralDataFetcher.duplicate_lock:
                if expected_id in GeneralDataFetcher.seen_ids:
                    return
                else:
                    GeneralDataFetcher.seen_ids.append(expected_id)

        tmp_adapter = self.generate_fetch_adapter(ref)

        tmp_adapter.add_property(AdapterPropertiesConstants.IFACE_FX_PARAM_LIST, param_list)
        tmp_adapter.add_property(AdapterPropertiesConstants.EXPECTED_ID, expected_id)

        self.adapter_list.append(tmp_adapter)
        self.priorities_map[tmp_adapter] = prio
        
        return tmp_adapter

    @abstractmethod
    def get_variant_type(self) -> int:
        pass
