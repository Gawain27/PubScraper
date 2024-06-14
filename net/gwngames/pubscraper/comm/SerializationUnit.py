import json
import logging

from net.gwngames.pubscraper.comm.entity.EntityBase import EntityBase
from net.gwngames.pubscraper.constants.PriorityConstants import PriorityConstants
from net.gwngames.pubscraper.msg.comm.SerializeEntity import SerializeEntity
from net.gwngames.pubscraper.msg.comm.ValidateEntity import ValidateEntity
from net.gwngames.pubscraper.scheduling.MessageRouter import MessageRouter
from net.gwngames.pubscraper.scheduling.sender.OutSenderQueue import OutSenderQueue

logger = logging.getLogger(__name__)


class SerializationUnit:
    def __init__(self):
        self.entity_data = None
        self.data = None
        self.entity_cid = None

    def execute(self, msg: SerializeEntity):
        self.load_data(msg.entity_loc)
        entity_instance = self.find_entity_instance()

        if entity_instance:
            entity_instance.fill_properties(self.entity_data)
            entity_validate_req: ValidateEntity = ValidateEntity(msg.content, entity_instance)
            MessageRouter.get_instance().send_message(entity_validate_req, OutSenderQueue(),
                                                      PriorityConstants.ENTITY_VALIDATE_REQ)
        else:
            logger.error("Failed to find or instantiate entity instance.")

    def load_data(self, file_path: str):
        try:
            with open(file_path, 'r') as f:
                self.data = json.load(f)
                self.entity_cid = self.data.get("entity_cid")
                if self.entity_cid is None:
                    raise ValueError("No 'entity_cid' found in the JSON.")
                self.entity_data = self.data.get("entity")
                if self.entity_data is None:
                    raise ValueError("No 'entity' found in the JSON.")
        except FileNotFoundError:
            logger.error(f"Error: File '{file_path}' not found.")
        except json.JSONDecodeError:
            logger.error(f"Error: Invalid JSON format in file '{file_path}'.")
        except ValueError as e:
            logger.error(str(e))

    def find_entity_instance(self):
        try:
            entity_instance = EntityBase.from_cid(self.entity_cid)
        except ValueError as e:
            logger.error(str(e))
            return None

        return entity_instance

    def get_entity_cid(self):
        return self.entity_cid

    def get_entity_data(self):
        return self.entity_data
