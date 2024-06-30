import logging
import os
import time
from typing import Final

from net.gwngames.pubscraper.comm.OutSender import OutSender
from net.gwngames.pubscraper.comm.PackagingUnit import PackagingUnit
from net.gwngames.pubscraper.comm.SerializationUnit import SerializationUnit
from net.gwngames.pubscraper.constants.ConfigConstants import ConfigConstants
from net.gwngames.pubscraper.constants.EntityCidConstants import EntityCidConstants
from net.gwngames.pubscraper.constants.JsonConstants import JsonConstants
from net.gwngames.pubscraper.constants.PriorityConstants import PriorityConstants
from net.gwngames.pubscraper.constants.QueueConstants import QueueConstants
from net.gwngames.pubscraper.msg.BaseMessage import BaseMessage
from net.gwngames.pubscraper.msg.comm import PackageEntity
from net.gwngames.pubscraper.msg.comm.SendEntity import SendEntity
from net.gwngames.pubscraper.msg.comm.SerializeEntity import SerializeEntity
from net.gwngames.pubscraper.msg.comm.SerializeJSONData import SerializeJSONData
from net.gwngames.pubscraper.msg.comm.PackageEntity import PackageEntity
from net.gwngames.pubscraper.scheduling.MessageRouter import MessageRouter
from net.gwngames.pubscraper.scheduling.sender.AsyncQueue import AsyncQueue
from net.gwngames.pubscraper.utils.JsonReader import JsonReader
from net.gwngames.pubscraper.utils.LoadState import LoadState


class OutSenderQueue(AsyncQueue):

    QUEUE: Final = QueueConstants.OUTSENDER_QUEUE

    def register_me(self) -> type:
        return OutSenderQueue

    def on_message(self, msg: BaseMessage) -> None:
        if isinstance(msg, SerializeJSONData):
            logging.info("Processing SerializeJSONData message with file: %s", msg.json_loc)
            try:
                config = JsonReader(JsonReader.CONFIG_FILE_NAME)
                entity_data = JsonReader(msg.json_loc, msg.json_loc.split(',')[0])
                #TODO
                entity_path = msg.json_loc.split(',')[0]+"_entity"
                entity_name = msg.json_loc+"_entity"
                entity_file = JsonReader(entity_name, entity_path)
                # TODO: logic for automatic entity association
                entity_serialize_req = SerializeEntity(msg.content, "the entity file name here" + "_" + msg.json_loc)
                MessageRouter().send_message(entity_serialize_req,
                                                PriorityConstants.ENTITY_SERIAL_REQ)
                logging.info("Successfully requested serialization for file: %s", msg.json_loc)
            except Exception as e:
                logging.error("Failed to read entities for file %s: %s", msg.json_loc, str(e))
                raise e
        elif isinstance(msg, SerializeEntity):
            logging.info("Processing SerializeEntity message with file: %s", msg.entity_loc)
            #  entity gets handed over to the packager right away, compress during serialization
            SerializationUnit().execute(msg)
            logging.info("Serialized and sent for validation message with file: %s", msg.entity_loc)
        elif isinstance(msg, PackageEntity):
            logging.info("Processing Entity %s with message id: %s", msg.entity_cid, msg.message_id)
            PackagingUnit().sleep_based_on_load(msg)
            logging.info("Packaged Entity %s with message id: %s", msg.entity_cid, msg.message_id)
        elif isinstance(msg, SendEntity):
            logging.info("Sending bufferized Entity %s with message id: %s", msg.entity_cid, msg.message_id)
            OutSender().send_data(msg.entity)
            logging.info("Entity %s with message id: %s successfully delivered to server", msg.entity_cid,
                         msg.message_id)
        else:
            logging.error("OutSenderQueue - Received undefined message type: %s", type(msg).__name__)
