import json
from datetime import datetime
from typing import Dict

from com.gwngames.pubscraper.msg.AbstractMessage import AbstractMessage


class BaseMessage(AbstractMessage):
    """
    Initializes a new TextMessage object.

    :param message_type: The type of the message.
    :param content: The content of the text message.
    :param timestamp: The time the message was created.
    """

    def __init__(self, message_type: str, content: str, timestamp: datetime = None, depth: int = 0) -> None:
        super().__init__(message_type, timestamp=timestamp, depth=depth)  # Initialize the parent class
        self.content: str = content

    def __str__(self) -> str:
        """
        Return a string representation of the object.

        :return: A string representation of the object.
        """
        parent_str = super().__str__()  # Get the string representation from the parent class
        return f"{parent_str}, Content: {self.content}, Timestamp: {self.timestamp}"

    def to_dict(self) -> Dict[str, str]:
        """
        Convert the object to a dictionary representation.

        :return: A dictionary representation of the object.
        """
        parent_dict = super().to_dict()  # Get the dictionary representation from the parent class
        parent_dict.update({
            'content': self.content,
            'timestamp': self.timestamp.isoformat()
        })
        return parent_dict

    @classmethod
    def from_dict(cls, data: Dict[str, str]) -> 'BaseMessage':
        """
        Create an object instance from a dictionary.

        :param data: Dictionary containing the object data.
        :return: An instance of TextMessage.
        """
        message_type: str = data['message_type']
        content: str = data['content']
        timestamp: datetime = datetime.fromisoformat(data['timestamp'])
        instance: BaseMessage = cls(message_type, content, timestamp)
        instance.message_id = data['message_id']
        return instance

    def to_json(self) -> str:
        """
        Convert the object to a JSON string.

        :return: A JSON representation of the object.
        """
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str: str) -> 'BaseMessage':
        """
        Create an object instance from a JSON string.

        :param json_str: JSON string containing the object data.
        :return: An instance of TextMessage.
        """
        data: Dict[str, str] = json.loads(json_str)
        return cls.from_dict(data)
