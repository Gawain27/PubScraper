import logging
from datetime import datetime
from typing import Final, Dict

from net.gwngames.pubscraper.msg.BaseMessage import BaseMessage
from net.gwngames.pubscraper.msg.scraper.GetGoogleScholarData import GetGoogleScholarData
from net.gwngames.pubscraper.scheduling.sender.AsyncQueue import AsyncQueue
from net.gwngames.pubscraper.scraper.ifaces.ScholarlyDataFetcher import ScholarlyDataFetcher


class ScraperQueue(AsyncQueue):
    MSG_GOOGLE_SCHOLAR_QUERY: Final = "getGoogleScholarEntity"

    def on_message(self, msg: BaseMessage) -> None:
        logging.info("Received message: %s", msg)

        if msg.is_first_run:
            filter_date = datetime.today()
            logging.info("Setting filter date to today: %s", filter_date)
        else:
            filter_date = datetime.min
            logging.info("Setting filter date to the minimum possible date: %s", filter_date)

        new_data: list[Dict] = []

        if isinstance(msg, GetGoogleScholarData):
            logging.info("Processing GetGoogleScholarData message with query: %s", msg.query)
            try:
                new_data = ScholarlyDataFetcher().get_new_data_since(msg.query, filter_date)
                logging.info("Successfully fetched new data since %s for query: %s", filter_date, msg.query)
            except Exception as e:
                logging.error("Failed to fetch data for query %s: %s", msg.query, str(e))
        else:
            logging.error("Received undefined message type: %s", type(msg).__name__)
        # TODO: take the data and send it to the serialization unit via queue, when ready
