import json
from typing import Dict

from net.gwngames.pubscraper.utils.FileReader import FileReader


class AbstractMessage:
    """
    Initializes a new AbstractMessage object.

    :param message_type: The type of the message.
    """

    def __init__(self, message_type: str) -> None:
        self.stats: FileReader = FileReader(FileReader.MESSAGE_STAT_FILE_NAME)
        self.message_type: str = message_type
        self.message_id: str = self.generate_message_id()
        # TODO: loading of message types from file, define constant enum

    def generate_message_id(self) -> str:
        """
        Generate a unique message ID for the current message type.

        :return: A unique message ID in the format "<message_type>_<counter>".
        """
        if self.stats.get_value(self.message_type) is None:
            self.stats.set_and_save(self.message_type, 0)
        else:
            self.stats.increment(self.message_type)
        return f"{self.message_type}_{self.stats.get_value(self.message_type)}"

    def __str__(self) -> str:
        """
        Return a string representation of the object.

        :return: A string representation of the object.
        """
        return f"Message Type: {self.message_type}, Message ID: {self.message_id}"

    def to_dict(self) -> Dict[str, str]:
        """
        Convert the object to a dictionary representation.

        :return: A dictionary representation of the object.
        """
        return {
            'message_type': self.message_type,
            'message_id': self.message_id
        }

    @classmethod
    def from_dict(cls, data: Dict[str, str]) -> 'AbstractMessage':
        """
        Create an object instance from a dictionary.

        :param data: Dictionary containing the object data.
        :return: An instance of AbstractMessage.
        """
        message_type: str = data['message_type']
        instance: AbstractMessage = cls(message_type)
        instance.message_id = data['message_id']
        return instance

    def to_json(self) -> str:
        """
        Convert the object to a JSON string.

        :return: A JSON representation of the object.
        """
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str: str) -> 'AbstractMessage':
        """
        Create an object instance from a JSON string.

        :param json_str: JSON string containing the object data.
        :return: An instance of AbstractMessage.
        """
        data: Dict[str, str] = json.loads(json_str)
        return cls.from_dict(data)
