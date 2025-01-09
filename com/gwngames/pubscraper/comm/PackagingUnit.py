import json
import logging

from couchdb import Database

from com.gwngames.pubscraper.Context import Context
from com.gwngames.pubscraper.constants.PriorityConstants import PriorityConstants
from com.gwngames.pubscraper.msg.comm.PackageEntity import PackageEntity
from com.gwngames.pubscraper.msg.comm.SendEntity import SendEntity
from com.gwngames.pubscraper.scheduling.MessageRouter import MessageRouter
from com.gwngames.pubscraper.scraper.buffer.DatabaseHandler import DatabaseHandler


class PackagingUnit:
    def __init__(self):
        """
        Initialize the PackagingUnit.
        This actually acts as the buffer master for the outsender, separating the storing from the sending logic
        """
        self.ctx = Context()
        self.logger = logging.getLogger('PackagingUnit')

    @staticmethod
    def compress_json(obj: dict) -> bytes:
        """
        Compress a Python dictionary as JSON.
        Args:
            obj: A Python dictionary to be serialized to JSON and compressed.
        Returns:
            The compressed byte stream.
        """
        # Serialize the object to JSON
        json_data = json.dumps(obj).encode('utf-8')

        return json_data

    def package_based_on_load(self, msg: PackageEntity):
        """
        Make the thread sleep based on the load percentage in the configuration.
        Retry with decreasing sleep durations up to `max_retries` times.
        """

        # Bufferize for sender
        data_source: DatabaseHandler = DatabaseHandler(self.ctx.get_dbclient(), msg.entity_db)
        database: Database = data_source.get_or_create_db()

        entity = database.get(msg.entity_id)
        if entity is None:
            raise Exception("Entity found none for message %s", msg.content)

        if entity['sent'] is True:
            return  # Avoid re-sending

        compressed_entity = self.compress_json(entity)

        entity_send_req = SendEntity(msg.content, compressed_entity, msg.entity_id, msg.entity_db)
        entity_send_req.system_message = True
        MessageRouter.get_instance().send_message(entity_send_req, PriorityConstants.ENTITY_SEND_REQ)
