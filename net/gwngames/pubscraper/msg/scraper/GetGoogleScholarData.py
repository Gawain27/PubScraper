from datetime import datetime
from typing import Dict

from net.gwngames.pubscraper.msg.BaseMessage import BaseMessage


class GetGoogleScholarData(BaseMessage):
    def __init__(self, content: str, query: str, timestamp: datetime = None) -> None:
        super().__init__('GetGoogleScholarData', content, timestamp)
        self.query: str = query

    def __str__(self) -> str:
        parent_str = super().__str__()
        return f"{parent_str}, Query: {self.query}"

    def to_dict(self) -> Dict[str, str]:
        parent_dict = super().to_dict()
        parent_dict.update({
            'query': self.query
        })
        return parent_dict

    @classmethod
    def from_dict(cls, data: Dict[str, str]) -> 'GetGoogleScholarData':
        content: str = data['content']
        query: str = data['query']
        timestamp: datetime = datetime.fromisoformat(data['timestamp'])
        instance: GetGoogleScholarData = cls(content, query, timestamp)
        instance.message_type = data['message_type']
        instance.message_id = data['message_id']
        return instance
