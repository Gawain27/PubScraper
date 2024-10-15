import gzip
import json
import logging
import os
import pickle
from typing import Any

from couchdb import Database
from lxml.parser import entity

from net.gwngames.pubscraper.Context import Context
from net.gwngames.pubscraper.comm.entity.EntityBase import EntityBase
from net.gwngames.pubscraper.constants.JsonConstants import JsonConstants
from net.gwngames.pubscraper.constants.PriorityConstants import PriorityConstants
from net.gwngames.pubscraper.constants.QueueConstants import QueueConstants
from net.gwngames.pubscraper.msg.comm.PackageEntity import PackageEntity
from net.gwngames.pubscraper.msg.comm.SerializeEntity import SerializeEntity
from net.gwngames.pubscraper.scheduling.MessageRouter import MessageRouter
from net.gwngames.pubscraper.scraper.buffer.DatabaseHandler import DatabaseHandler
from net.gwngames.pubscraper.utils.JsonReader import JsonReader

logger = logging.getLogger(__name__)

class SerializationUnit:
    def __init__(self):
        self.ctx = Context()
        self.data = None

    def execute(self, msg: SerializeEntity):

        data_source: DatabaseHandler = DatabaseHandler(self.ctx.get_dbclient(), msg.entity_db)
        database: Database = data_source.get_or_create_db()

        entity = database.get(msg.entity_id)
        if entity is None:
            raise Exception("Entity found none for message %s", msg.content)

        if entity['serialized'] is True:
            return # Avoid re-serialization

        if msg.entity_variant is None or msg.entity_class is None:
            raise Exception("No Entity mapping found for entity %s of %s", msg.entity_id, msg.entity_db)

        entity['class_id'] = msg.entity_class
        entity['variant_id'] = msg.entity_variant
        entity['serialized'] = True
        entity['sent'] = False

        data_source.insert_or_update_document(msg.content, msg.entity_id, entity)

        entity_package_req: PackageEntity = PackageEntity(msg.content, msg.entity_id, msg.entity_db)
        MessageRouter.get_instance().send_message(entity_package_req,
                                                  PriorityConstants.ENTITY_PACKAGE_REQ)