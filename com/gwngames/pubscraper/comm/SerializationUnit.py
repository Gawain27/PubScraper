from couchdb import Database

from com.gwngames.pubscraper.Context import Context
from com.gwngames.pubscraper.constants.PriorityConstants import PriorityConstants
from com.gwngames.pubscraper.msg.comm.PackageEntity import PackageEntity
from com.gwngames.pubscraper.msg.comm.SerializeEntity import SerializeEntity
from com.gwngames.pubscraper.scheduling.MessageRouter import MessageRouter
from com.gwngames.pubscraper.scraper.buffer.DatabaseHandler import DatabaseHandler

import logging


class SerializationUnit:
    def __init__(self):
        self.ctx = Context()
        self.data = None
        self.logger = logging.getLogger(SerializationUnit.__name__)

    def execute(self, msg: SerializeEntity):
        self.logger.info(
            f"Starting serialization process for message {msg.content} with entity ID {msg.entity_id} in database {msg.entity_db}.")

        try:
            data_source: DatabaseHandler = DatabaseHandler(self.ctx.get_dbclient(), msg.entity_db)
            database: Database = data_source.get_or_create_db()

            entity = database.get(msg.entity_id)
            if entity is None:
                self.logger.error(f"Entity not found for message {msg.content}, entity ID {msg.entity_id}.")
                raise Exception(f"Entity found none for message {msg.content}")

            # Check if the entity is already serialized
            if entity.get('serialized') is True:
                self.logger.info(f"Entity {msg.entity_id} is already serialized, skipping re-serialization.")
                return  # Avoid re-serialization

            if msg.entity_variant is None or msg.entity_class is None:
                self.logger.error(
                    f"Entity mapping missing for entity ID {msg.entity_id} in database {msg.entity_db}. Variant: {msg.entity_variant}, Class: {msg.entity_class}")
                raise Exception(f"No Entity mapping found for entity {msg.entity_id} of {msg.entity_db}")

            self.logger.debug(
                f"Serializing entity {msg.entity_id} with class {msg.entity_class} and variant {msg.entity_variant}.")
            entity['class_id'] = msg.entity_class
            entity['variant_id'] = msg.entity_variant
            entity['serialized'] = True
            entity['sent'] = False

            data_source.insert_or_update_document(msg.content, msg.entity_id, entity)
            self.logger.info(f"Entity {msg.entity_id} serialized and updated in database {msg.entity_db}.")

            entity_package_req: PackageEntity = PackageEntity(msg.content, msg.entity_id, msg.entity_db)
            entity_package_req.system_message = True
            self.logger.debug(f"Sending ENTITY_PACKAGE_REQ for entity ID {msg.entity_id}.")
            MessageRouter.get_instance().send_message(entity_package_req, PriorityConstants.ENTITY_PACKAGE_REQ)

            self.logger.info(f"Serialization process completed successfully for entity ID {msg.entity_id}.")

        except Exception as e:
            self.logger.exception(f"Error occurred during serialization for entity ID {msg.entity_id}: {str(e)}")
            raise
