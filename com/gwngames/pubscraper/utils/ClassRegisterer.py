#
#  Various registerers are created here, they are not many
#
from com.gwngames.pubscraper.utils.DataRegisterer import DataRegisterer


class QueueRegisterer(DataRegisterer):

    def register_queues(self):
        from com.gwngames.pubscraper.scheduling.sender.OutSenderQueue import OutSenderQueue
        from com.gwngames.pubscraper.scheduling.sender.ScraperQueue import ScraperQueue
        self.add_all([
            OutSenderQueue.__class__.__name__,
            ScraperQueue.__class__.__name__
        ])


class TopicRegisterer(DataRegisterer):
    def register_topic(self, topics: set):
        self.add_all(topics)
