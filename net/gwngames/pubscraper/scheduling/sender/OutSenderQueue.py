import logging
from typing import Final

from net.gwngames.pubscraper.comm.OutSender import OutSender
from net.gwngames.pubscraper.comm.PackagingUnit import PackagingUnit
from net.gwngames.pubscraper.comm.SerializationUnit import SerializationUnit
from net.gwngames.pubscraper.constants.EntityCidConstants import EntityCidConstants
from net.gwngames.pubscraper.constants.JsonConstants import JsonConstants
from net.gwngames.pubscraper.constants.PriorityConstants import PriorityConstants
from net.gwngames.pubscraper.msg.BaseMessage import BaseMessage
from net.gwngames.pubscraper.msg.comm import PackageEntity
from net.gwngames.pubscraper.msg.comm.SendEntity import SendEntity
from net.gwngames.pubscraper.msg.comm.SerializeEntity import SerializeEntity
from net.gwngames.pubscraper.msg.comm.SerializeJSONData import SerializeJSONData
from net.gwngames.pubscraper.msg.comm.PackageEntity import PackageEntity
from net.gwngames.pubscraper.scheduling.MessageRouter import MessageRouter
from net.gwngames.pubscraper.scheduling.sender.AsyncQueue import AsyncQueue
from net.gwngames.pubscraper.utils.FileReader import FileReader


class OutSenderQueue(AsyncQueue):
    MSG_SERIALIZE_DATA: Final = "serializeJsonData"
    MSG_SERIALIZE_ENTITY: Final = "serializeEntity"
    MSG_PACKAGE_ENTITY: Final = "validateEntity"
    MSG_SEND_ENTITY: Final = "sendEntity"

    def on_message(self, msg: BaseMessage) -> None:
        if isinstance(msg, SerializeJSONData):
            logging.info("Processing SerializeJSONData message with file: %s", msg.json_loc)
            try:
                entity_data = FileReader(msg.json_loc)
                entity_num = 0
                for paper in entity_data.data:
                    paper_file = FileReader(str(entity_num)+"_"+msg.json_loc)
                    paper_file.set_and_save(JsonConstants.TAG_ENTITY_CID, EntityCidConstants.GOOGLE_SCHOLAR_PUB)
                    paper_file.set_and_save(JsonConstants.TAG_ENTITY, paper)
                    entity_num += 1
                    entity_serialize_req = SerializeEntity(msg.content, str(entity_num)+"_"+msg.json_loc)
                    MessageRouter().send_message(entity_serialize_req, OutSenderQueue(), PriorityConstants.ENTITY_SERIAL_REQ)
                logging.info("Successfully requested serialization for file: %s", msg.json_loc)
            except Exception as e:
                logging.error("Failed to read entities for file %s: %s", msg.json_loc, str(e))
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
            logging.info("Entity %s with message id: %s successfully delivered to server", msg.entity_cid, msg.message_id)
        else:
            logging.error("OutSenderQueue - Received undefined message type: %s", type(msg).__name__)
