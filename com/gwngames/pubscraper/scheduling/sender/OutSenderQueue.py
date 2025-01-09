from typing import Final

from com.gwngames.pubscraper.comm.OutSender import OutSender
from com.gwngames.pubscraper.comm.PackagingUnit import PackagingUnit
from com.gwngames.pubscraper.comm.SerializationUnit import SerializationUnit
from com.gwngames.pubscraper.constants.QueueConstants import QueueConstants
from com.gwngames.pubscraper.msg.BaseMessage import BaseMessage
from com.gwngames.pubscraper.msg.comm import PackageEntity
from com.gwngames.pubscraper.msg.comm.PackageEntity import PackageEntity
from com.gwngames.pubscraper.msg.comm.SendEntity import SendEntity
from com.gwngames.pubscraper.msg.comm.SerializeEntity import SerializeEntity
from com.gwngames.pubscraper.scheduling.sender.AsyncQueue import AsyncQueue


class OutSenderQueue(AsyncQueue):

    QUEUE: Final = QueueConstants.OUTSENDER_QUEUE

    def register_me(self) -> type:
        return OutSenderQueue

    def on_message(self, msg: BaseMessage) -> None:
        if isinstance(msg, SerializeEntity):
            #  entity gets handed over to the packager right away, quick serialization only
            SerializationUnit().execute(msg)
            self.logger.info("Serialized and sent for validation message entity: %s - %s", msg.entity_id, msg.entity_db)
        elif isinstance(msg, PackageEntity):
            PackagingUnit().package_based_on_load(msg)
            self.logger.info("Packaged Entity with  id: %s - %s", msg.entity_id, msg.entity_db)
        elif isinstance(msg, SendEntity):
            OutSender().send_data(msg)
            self.logger.info("Entity %s - %s successfully delivered to server", msg.entity_id, msg.entity_db)
        else:
            self.logger.error("OutSenderQueue - Received undefined message type: %s", type(msg).__name__)
        return
