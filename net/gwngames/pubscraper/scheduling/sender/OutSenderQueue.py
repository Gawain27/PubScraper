import logging
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
from net.gwngames.pubscraper.utils.FileReader import FileReader
from net.gwngames.pubscraper.utils.LoadState import LoadState


class OutSenderQueue(AsyncQueue):

    QUEUE: Final = QueueConstants.OUTSENDER_QUEUE

    def register_me(self) -> type:
        return OutSenderQueue

    def on_message(self, msg: BaseMessage) -> None:
        if isinstance(msg, SerializeJSONData):
            logging.info("Processing SerializeJSONData message with file: %s", msg.json_loc)
            try:
                config = FileReader(FileReader.CONFIG_FILE_NAME)
                entity_data = FileReader(msg.json_loc)
                entity_num = 0
                for paper in entity_data.data:
                    paper_file = FileReader(str(entity_num) + "_" + msg.json_loc)
                    paper_file.set_and_save(JsonConstants.TAG_ENTITY_CID, EntityCidConstants.GOOGLE_SCHOLAR_PUB)
                    paper_file.set_and_save(JsonConstants.TAG_ENTITY, paper)
                    while LoadState().keepdown:
                        keepdown_time = config.get_value(ConfigConstants.KEEPDOWN_TIME) / 1000
                        logging.info("Keeping down serialization for file: %s for %s ms", msg.json_loc, keepdown_time)
                        time.sleep(keepdown_time)
                    entity_num += 1
                    entity_serialize_req = SerializeEntity(msg.content, str(entity_num) + "_" + msg.json_loc)
                    MessageRouter().send_message(entity_serialize_req, QueueConstants.OUTSENDER_QUEUE,
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
