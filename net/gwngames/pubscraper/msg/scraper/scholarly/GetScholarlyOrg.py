import datetime

from net.gwngames.pubscraper.constants.MessageConstants import MessageConstants
from net.gwngames.pubscraper.constants.QueueConstants import QueueConstants
from net.gwngames.pubscraper.msg.BaseMessage import BaseMessage


class GetScholarlyOrg(BaseMessage):
    def __init__(self, content: str, org_name: str, timestamp: datetime = None) -> None:
        super().__init__(MessageConstants.MSG_SCHOLARLY_ORG, content, timestamp)
        self.org_name: str = org_name
        self.destination_queue = QueueConstants.SCRAPER_QUEUE
