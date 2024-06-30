import gzip
import json
import logging
import os
import pickle
from typing import Any

from net.gwngames.pubscraper.comm.entity.EntityBase import EntityBase
from net.gwngames.pubscraper.constants.JsonConstants import JsonConstants
from net.gwngames.pubscraper.constants.PriorityConstants import PriorityConstants
from net.gwngames.pubscraper.constants.QueueConstants import QueueConstants
from net.gwngames.pubscraper.msg.comm.PackageEntity import PackageEntity
from net.gwngames.pubscraper.msg.comm.SerializeEntity import SerializeEntity
from net.gwngames.pubscraper.scheduling.MessageRouter import MessageRouter
from net.gwngames.pubscraper.utils.JsonReader import JsonReader

logger = logging.getLogger(__name__)


def compress_object(obj: Any) -> bytes:
    """
        Compress a Python object.
    Returns:
        The compressed byte stream.
    """
    # Serialize the object using pickle
    serialized_data = pickle.dumps(obj)
    # Compress the serialized data using gzip
    compressed_data = gzip.compress(serialized_data)

    return compressed_data


class SerializationUnit:
    def __init__(self):
        self.entity_data = None
        self.data = None
        self.entity_cid = None

    def execute(self, msg: SerializeEntity):
        entity_file = JsonReader(msg.entity_loc, msg.entity_loc.split(',')[0])

        cid = entity_file.get_value(JsonConstants.TAG_ENTITY_CID)
        if cid is None:
            raise Exception("Cid found none for message %s", msg.content)

        entity_instance = self.find_entity_instance()

        if entity_instance:
            entity_instance.fill_properties(self.entity_data)

            if not self._check_attribute_count(entity_instance):
                raise Exception(f"Entity {entity_instance} is not serializable, check error logs")

            serialized_entity = compress_object(entity_instance)
            entity_package_req: PackageEntity = PackageEntity(msg.content, serialized_entity, entity_instance.cid)
            MessageRouter.get_instance().send_message(entity_package_req,
                                                      PriorityConstants.ENTITY_PACKAGE_REQ)
        else:
            logger.error("Failed to find or instantiate entity instance %s", msg.content)

    def find_entity_instance(self):
        try:
            entity_instance = EntityBase.from_cid(self.entity_cid)
        except ValueError as e:
            logger.error(str(e))
            return None

        return entity_instance

    def _check_attribute_count(self, entity_instance: EntityBase) -> bool:
        # Get the attributes of entity_instance
        entity_attributes = vars(entity_instance)

        # Track missing attributes
        missing_attributes = []

        # Check each key in self.entity_data
        for key in self.entity_data:
            if key not in entity_attributes:
                missing_attributes.append(key)

        # Log missing attributes if any
        if missing_attributes:
            logger.error(f"Missing attributes in entity {entity_instance.cid}: {', '.join(missing_attributes)}")
            return False

        return True

    def get_entity_cid(self):
        return self.entity_cid

    def get_entity_data(self):
        return self.entity_data
