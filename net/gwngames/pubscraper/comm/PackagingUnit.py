import logging
import math
import random
import time

from net.gwngames.pubscraper.constants.ConfigConstants import ConfigConstants
from net.gwngames.pubscraper.constants.PriorityConstants import PriorityConstants
from net.gwngames.pubscraper.msg.comm.PackageEntity import PackageEntity
from net.gwngames.pubscraper.msg.comm.SendEntity import SendEntity
from net.gwngames.pubscraper.scheduling.MessageRouter import MessageRouter
from net.gwngames.pubscraper.scheduling.sender.OutSenderQueue import OutSenderQueue
from net.gwngames.pubscraper.utils.FileReader import FileReader
from net.gwngames.pubscraper.utils.LoadState import LoadState


class PackagingUnit:
    def __init__(self):
        """
        Initialize the PackagingUnit.
        This actually acts as the buffer master for the outsender, separating the storing from the sending logic
        """
        self.load_state = LoadState()
        self.config = FileReader(FileReader.CONFIG_FILE_NAME)

    @staticmethod
    def calculate_sleep_duration(load_percentage, threshold, retries):
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
        base_sleep_short = 1.0  # Base sleep duration for low load
        base_sleep_long = 10.0  # Maximum sleep duration for high load

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

    def sleep_based_on_load(self, msg: PackageEntity):
        """
        Make the thread sleep based on the load percentage in the configuration.
        Retry with decreasing sleep durations up to `max_retries` times.
        """
        acceptable_load = self.config.get_value(ConfigConstants.ACCEPTABLE_LOAD)
        max_retries = self.config.get_value(ConfigConstants.MAX_BUFFER_RETRIES)
        retries = 0

        while retries < max_retries:
            load_percentage = self.load_state.value
            if load_percentage < acceptable_load:
                logging.debug(f"Message: {msg.content} - Load is {load_percentage}%. No need to sleep.")
                break

            threshold = self.config.get_value(ConfigConstants.DELAY_THRESHOLD)
            sleep_duration = self.calculate_sleep_duration(load_percentage, threshold, retries)
            logging.debug(f"Message: {msg.content} - Load is {load_percentage}%. Sleeping for {sleep_duration:.2f} seconds.")
            time.sleep(sleep_duration)
            retries += 1

        # Can finally be bufferized for sender
        entity_send_req = SendEntity(msg.content, msg.entity, msg.entity_cid)
        MessageRouter.get_instance().send_message(entity_send_req, OutSenderQueue, PriorityConstants.ENTITY_SEND_REQ)