#
#  Various registerers are created here, they are not many
#

from net.gwngames.pubscraper.utils.DataRegisterer import DataRegisterer


class QueueRegisterer(DataRegisterer):

    def register_queues(self):
        from net.gwngames.pubscraper.scheduling.sender.OutSenderQueue import OutSenderQueue
        from net.gwngames.pubscraper.scheduling.sender.ScraperQueue import ScraperQueue
        from net.gwngames.pubscraper.scheduling.sender.SystemQueue import SystemQueue
        self.add_all([
            OutSenderQueue.__class__.__name__,
            ScraperQueue.__class__.__name__,
            SystemQueue.__class__.__name__
        ])


class TopicRegisterer(DataRegisterer):
    def register_topic(self, topics: set):
        self.add_all(topics)
