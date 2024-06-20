import logging
from typing import Final

from net.gwngames.pubscraper.constants.QueueConstants import QueueConstants
from net.gwngames.pubscraper.msg.BaseMessage import BaseMessage
from net.gwngames.pubscraper.msg.system.SystemStatusReq import SystemStatusReq
from net.gwngames.pubscraper.scheduling.sender.AsyncQueue import AsyncQueue
from net.gwngames.pubscraper.servlet.StatusServlet import StatusServlet


class SystemQueue(AsyncQueue):

    QUEUE: Final = QueueConstants.SYSTEM_QUEUE

    def register_me(self) -> type:
        return SystemQueue

    def on_message(self, msg: BaseMessage) -> None:
        if isinstance(msg, SystemStatusReq):
            logging.info("Processing SystemStatusReq message with id: %s", msg.message_id)
            StatusServlet.process_status(msg.server_msg)
            logging.info("Processed SystemStatusReq message with id: %s", msg.message_id)
        else:
            logging.error("SystemQueue - Received undefined message type: %s", type(msg).__name__)
            raise Exception("SystemQueue - Received undefined message type: %s", type(msg).__name__)
