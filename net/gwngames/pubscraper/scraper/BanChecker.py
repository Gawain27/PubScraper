import logging
import threading
import time
from random import Random

from bs4 import BeautifulSoup

from net.gwngames.pubscraper.constants.ConfigConstants import ConfigConstants


class BanChecker:
    def __init__(self, ctx):
        self.ctx = ctx

    def has_ban_phrase(self, html_content: str, phrase: str = "We're sorry...") -> bool:
        """
        Check if a specific ban phrase is present in the HTML content.
        Args:
            html_content (str): The HTML content of the page as a string.
            phrase (str): The specific phrase to check for. Default is Google's "We're sorry...".

        Returns:
            bool: True if the phrase is found, False otherwise.
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        text_content = soup.get_text(separator=' ', strip=True)

        if phrase in text_content:
            if (Random().random() > 0.5
                    and (self.ctx.get_config().get_value(ConfigConstants.MAX_WAIT_TIME) > self.ctx.get_config().get_value(ConfigConstants.MIN_WAIT_TIME))):
                self.ctx.get_config().set_and_save(ConfigConstants.MIN_WAIT_TIME,
                    self.ctx.get_config().get_value(ConfigConstants.MIN_WAIT_TIME) + 1)
            else:
                self.ctx.get_config().set_and_save(ConfigConstants.MAX_WAIT_TIME,
                    self.ctx.get_config().get_value(ConfigConstants.MAX_WAIT_TIME) + 1)

            self.ctx.get_message_data().set_and_save("was_banned", True)
            return True

        return False

    def reverse_logic(self):
        """
        Perform the opposite of the logic specified in `has_ban_phrase`.
        """
        if (Random().random() > 0.5
                and (self.ctx.get_config().get_value(ConfigConstants.MAX_WAIT_TIME) <= self.ctx.get_config().get_value(
                    ConfigConstants.MIN_WAIT_TIME))):
            self.ctx.get_config().set_and_save(ConfigConstants.MIN_WAIT_TIME,
                self.ctx.get_config().get_value(ConfigConstants.MIN_WAIT_TIME) - 1)
        else:
            self.ctx.get_config().set_and_save(ConfigConstants.MAX_WAIT_TIME,
                self.ctx.get_config().get_value(ConfigConstants.MAX_WAIT_TIME) - 1)

    def monitor_ban_state(self):
        """
        Periodically check the `was_banned` flag every hour.
        If the flag is `True`, reset it to `False` and do nothing.
        If the flag is `False`, perform the reverse logic.
        """
        while True:
            was_banned = self.ctx.get_message_data().get_value("was_banned")
            if was_banned:
                self.ctx.get_message_data().set_and_save("was_banned", False)
            else:
                self.reverse_logic()
            time.sleep(3600)

    def start_monitoring(self):
        """
        Start the thread to check the `was_banned` flag.
        """
        thread = threading.Thread(target=self.monitor_ban_state, daemon=True)
        thread.start()
