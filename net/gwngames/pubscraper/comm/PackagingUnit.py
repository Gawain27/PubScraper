import gzip
import logging
import math
import pickle
import random
import time
from typing import Any

from couchdb import Database

from net.gwngames.pubscraper.Context import Context
from net.gwngames.pubscraper.constants.ConfigConstants import ConfigConstants
from net.gwngames.pubscraper.constants.PriorityConstants import PriorityConstants
from net.gwngames.pubscraper.msg.comm.PackageEntity import PackageEntity
from net.gwngames.pubscraper.msg.comm.SendEntity import SendEntity
from net.gwngames.pubscraper.scheduling.MessageRouter import MessageRouter
from net.gwngames.pubscraper.scraper.buffer.DatabaseHandler import DatabaseHandler
from net.gwngames.pubscraper.utils.LoadState import LoadState


class PackagingUnit:
    def __init__(self):
        """
        Initialize the PackagingUnit.
        This actually acts as the buffer master for the outsender, separating the storing from the sending logic
        """
        self.load_state = LoadState()
        self.ctx = Context()

    @staticmethod
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

    def calculate_sleep_duration(self, load_percentage, threshold, retries):  # TODO: to rework
        """
        Calculate the sleep duration based on an exponential function.

        Args:
            load_percentage (float): The current load percentage of the server.
            threshold (float): The threshold percentage for the load.
            retries (int): Reduction factor

        Returns:
            float: The calculated sleep duration.
        """
        # Define base sleep durations
        base_sleep_short = self.ctx.get_config().get_value(ConfigConstants.MIN_LOAD_WAIT)  # Base sleep duration for low load
        base_sleep_long = self.ctx.get_config().get_value(ConfigConstants.MAX_LOAD_WAIT)  # Maximum sleep duration for high load

        # Normalize the load percentage to a range [0, 1]
        normalized_load = min(max(load_percentage / 100, 0), 1)

        # Calculate sleep duration using an exponential function
        if normalized_load <= threshold / 100:
            # Below or at the threshold, use shorter sleep
            sleep_duration = base_sleep_short
        else:
            # Above the threshold, calculate exponential sleep duration
            excess_load = normalized_load - (threshold / 100)
            sleep_duration = base_sleep_short + (base_sleep_long - base_sleep_short) * (math.exp(excess_load) - 1) / (
                        math.e - 1)

        # Add a random component to the sleep duration
        random_factor = random.uniform(0.5, 1.5)  # Random factor between 0.5 and 1.5
        sleep_duration *= random_factor

        adjusted_duration = sleep_duration * (0.5 ** retries)

        return adjusted_duration

    def package_based_on_load(self, msg: PackageEntity):
        """
        Make the thread sleep based on the load percentage in the configuration.
        Retry with decreasing sleep durations up to `max_retries` times.
        """
        acceptable_load = self.ctx.get_config().get_value(ConfigConstants.ACCEPTABLE_LOAD)
        max_retries = self.ctx.get_config().get_value(ConfigConstants.MAX_BUFFER_RETRIES)
        retries = 0

        while retries < max_retries:
            load_percentage = self.load_state.load_perc
            if load_percentage < acceptable_load:
                logging.debug(f"Message: {msg.content} - Load is {load_percentage}%. No need to sleep.")
                break

            threshold = self.ctx.get_config().get_value(ConfigConstants.DELAY_THRESHOLD)
            sleep_duration = self.calculate_sleep_duration(load_percentage, threshold, retries)
            logging.debug(f"Message: {msg.content} - Load is {load_percentage}%. Sleeping for {sleep_duration:.2f} seconds.")
            time.sleep(sleep_duration)
            retries += 1

        # Can finally be bufferized for sender
        data_source: DatabaseHandler = DatabaseHandler(self.ctx.get_dbclient(), msg.entity_db)
        database: Database = data_source.get_or_create_db()

        entity = database.get(msg.entity_id)
        if entity is None:
            raise Exception("Entity found none for message %s", msg.content)

        if entity['sent'] is True:
            return  # Avoid re-sending

        compressed_entity = self.compress_object(entity)

        entity_send_req = SendEntity(msg.content, compressed_entity, msg.entity_id, msg.entity_db)
        MessageRouter.get_instance().send_message(entity_send_req, PriorityConstants.ENTITY_SEND_REQ)
