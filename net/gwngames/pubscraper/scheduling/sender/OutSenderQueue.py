import logging
from typing import Final

from net.gwngames.pubscraper.comm.OutSender import OutSender
from net.gwngames.pubscraper.comm.PackagingUnit import PackagingUnit
from net.gwngames.pubscraper.comm.SerializationUnit import SerializationUnit
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


class OutSenderQueue(AsyncQueue):

    QUEUE: Final = QueueConstants.OUTSENDER_QUEUE

    def register_me(self) -> type:
        return OutSenderQueue

    def on_message(self, msg: BaseMessage) -> None:
        if isinstance(msg, SerializeEntity):
            logging.info("Processing SerializeEntity message with id: %s - %s", msg.entity_id, msg.entity_db)
            #  entity gets handed over to the packager right away, quick serialization only
            SerializationUnit().execute(msg)
            logging.info("Serialized and sent for validation message entity: %s - %s", msg.entity_id, msg.entity_db)
        elif isinstance(msg, PackageEntity):
            logging.info("Processing Entity with id: %s - %s", msg.entity_id, msg.entity_db)
            PackagingUnit().package_based_on_load(msg)
            logging.info("Packaged Entity with  id: %s - %s", msg.entity_id, msg.entity_db)
        elif isinstance(msg, SendEntity):
            logging.info("Sending bufferized Entity %s - %s", msg.entity_id, msg.entity_db)
            OutSender().send_data(msg)
            logging.info("Entity %s successfully delivered to server", msg.entity_id, msg.entity_db,)
        else:
            logging.error("OutSenderQueue - Received undefined message type: %s", type(msg).__name__)
