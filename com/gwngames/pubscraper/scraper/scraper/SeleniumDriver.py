import logging
import threading
import time

from fake_useragent import UserAgent
from selenium import webdriver
from selenium.common import NoAlertPresentException, UnexpectedAlertPresentException
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options as FirefoxOptions, Options
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.support.wait import WebDriverWait

from com.gwngames.pubscraper.Context import Context
from com.gwngames.pubscraper.constants.ConfigConstants import ConfigConstants
from com.gwngames.pubscraper.scraper.scraper.CaptchaHandler import CaptchaHandler
from com.gwngames.pubscraper.utils.JsonReader import JsonReader
from com.gwngames.pubscraper.utils.ThreadUtils import ThreadUtils


class SeleniumDriver:

    def __init__(self, interface_name: str):
        self.interface_name = interface_name
        self.logger = logging.getLogger(f"SeleniumDriver-{interface_name}")
        self.logger.info("Initializing SeleniumDriver instance.")
        self.ctx = Context()
        self.config = JsonReader(JsonReader.CONFIG_FILE_NAME)
        self.number_of_tabs = self.ctx.get_max_requests()
        self.available_tabs = {i: True for i in range(self.number_of_tabs)}
        self.window_handles = {}
        self._condition = threading.Condition()
        self._captcha_condition = threading.Condition()
        self.timeout = self.config.get_value(ConfigConstants.URL_TIMEOUT)
        self.logger.debug(f"Timeout set to {self.timeout} seconds.")
        self.driver = self._initialize_driver()
        self._initialize_tabs()
        self.logger.info("Driver initialization complete. Sleeping for 5 seconds.")
        time.sleep(5)  # Initialization time

    def _initialize_driver(self):
        browser_type = self.config.get_value(ConfigConstants.BROWSER_TYPE)
        self.logger.info(f"Initializing browser driver for {browser_type}.")
        browser_driver_path = self.config.get_value(ConfigConstants.BROWSER_DRIVER_PATH)

        try:
            if browser_type.lower() == 'chrome':
                chrome_options = ChromeOptions()
                self.user_agent = UserAgent().random
                chrome_options.add_argument(f"user-agent={self.user_agent}")
                chrome_options.add_argument(f"user-data-dir={self.config.get_value(ConfigConstants.BROWSER_DATA_PATH)}")
                chrome_options.add_argument(f"profile-directory=Default")
                chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
                chrome_options.add_experimental_option('useAutomationExtension', False)
                chrome_options.binary_location = browser_driver_path
                driver = webdriver.Chrome(options=chrome_options)
            elif browser_type.lower() == 'firefox':
                firefox_options = FirefoxOptions()
                self.user_agent = UserAgent().random
                profile = webdriver.FirefoxProfile(
                    f"{self.config.get_value(ConfigConstants.BROWSER_DATA_PATH)}")
                profile.set_preference("general.useragent.override", self.user_agent)
                profile.set_preference("dom.webdriver.enabled", False)
                firefox_options.profile = profile
                firefox_options.binary_location = browser_driver_path
                driver = webdriver.Firefox(options=firefox_options)
            elif browser_type.lower() == 'embedded':
                options = Options()

                self.user_agent = UserAgent().random
                profile = webdriver.FirefoxProfile(
                    f"{self.config.get_value(ConfigConstants.BROWSER_DATA_PATH)}")
                profile.set_preference("general.useragent.override", self.user_agent)
                profile.set_preference("dom.webdriver.enabled", False)
                options.profile = profile

                options.binary_location = self.config.get_value(ConfigConstants.BROWSER_DRIVER_PATH)
                service = Service(executable_path=self.config.get_value("geckodriver"))

                driver = webdriver.Firefox(
                    options=options,
                    service=service
                )
            else:
                raise ValueError(f"Unsupported browser type: {browser_type}")

            self.logger.info(f"Browser driver for {browser_type} initialized successfully.")
            return driver

        except Exception as e:
            self.logger.error(f"Failed to initialize browser driver: {e}")
            raise

    def _initialize_tabs(self):
        self.logger.info("Initializing browser tabs.")
        for tab in range(1, self.number_of_tabs):
            self.driver.execute_script("window.open('about:blank', '_blank');")
            time.sleep(2)
            self.logger.debug(f"Opened tab {tab}.")
            if tab == 1 and self.config.get_value(ConfigConstants.BROWSER_EMBEDDED):
                self.logger.debug("Clicking 'always connect automatically' button.")
                self.click_always_connect_automatically()
                time.sleep(5)

        for i, handle in enumerate(self.driver.window_handles):
            self.window_handles[i] = handle
        self.logger.info(f"Initialized {self.number_of_tabs} tabs.")

    def click_always_connect_automatically(self):
        try:
            self.logger.info("Attempting to click 'always connect automatically' button.")
            connect_button = self.driver.find_element(By.ID, "connectButton")
            connect_button.click()
            self.logger.info("Connection was successful.")
        except Exception as e:
            self.logger.error(f"Error clicking 'always connect automatically': {e}")

    def restart_driver(self):
        self.logger.info("Restarting browser driver.")
        with self._condition:
            urls_to_reload = {}
            i = 0
            for tab in self.driver.window_handles:
                self.driver.switch_to.window(tab)
                urls_to_reload[str(i)] = self.driver.current_url
                i+=1

            self.close_driver()
            self.driver = self._initialize_driver()
            self._initialize_tabs()
            time.sleep(2)

            i = 0
            for tab in self.driver.window_handles:
                self.driver.switch_to.window(tab)
                self.driver.get(urls_to_reload[str(i)])
                time.sleep(5)
                i+=1
            time.sleep(5)

    def obtain_tab(self, url_search: str) -> int:
        self.logger.info(f"Attempting to obtain a tab for URL search: {url_search}.")
        index_tab = None
        while True:
            with self._condition:
                for tab_id, is_available in self.available_tabs.items():
                    if is_available:
                        index_tab = tab_id
                        self.available_tabs[tab_id] = False
                        self.logger.info(f"Tab[{tab_id}] assigned for URL search: {url_search}.")
                        break
                if index_tab is None:
                    self._condition.wait()
                    continue

                self._condition.notify_all()
                return index_tab

    def load_url_from_tab(self, index_tab: int, url: str):
        self.logger.info(f"Loading URL: {url} in tab[{index_tab}].")
        if index_tab is not None and (0 <= index_tab < self.number_of_tabs):
            with self._condition:
                self.driver.switch_to.window(self.window_handles[index_tab])
                self.driver.get(url)

                try:
                    WebDriverWait(self.driver, self.timeout).until(
                        lambda x: self.driver.execute_script("return document.readyState") == "complete"
                    )
                    self.logger.info(f"URL {url} loaded successfully in tab[{index_tab}].")
                except UnexpectedAlertPresentException:
                    self.logger.warning("Unexpected alert detected; dismissing.")
                    try:
                        alert = self.driver.switch_to.alert
                        alert.dismiss()
                        self.logger.info("Alert dismissed.")
                    except NoAlertPresentException:
                        self.logger.warning("No alert found during dismissal attempt.")

                self._condition.notify_all()
        else:
            error_msg = f"Invalid tab index: {index_tab} for URL: {url}"
            self.logger.error(error_msg)
            raise Exception(error_msg)

    def obtain_html_from_tab(self, index_tab: int, specific_wait_time: int = 0, possible_captcha: str = None) -> str:
        self.logger.info(f"Obtaining HTML from tab[{index_tab}].")
        if index_tab is not None and (0 <= index_tab < self.number_of_tabs):
            if specific_wait_time > 0:
                self.logger.debug(f"Waiting for {specific_wait_time} seconds before fetching HTML.")
                time.sleep(specific_wait_time)
            else:
                self.logger.debug("Using default wait time.")
                ThreadUtils.sleep_for(self.ctx.get_config().get_value(ConfigConstants.MIN_WAIT_TIME),
                                      self.ctx.get_config().get_value(ConfigConstants.MAX_WAIT_TIME),
                                      self.logger,
                                      str(index_tab))

            with self._condition:
                self.driver.switch_to.window(self.window_handles[index_tab])

                if possible_captcha is not None:
                    with self._captcha_condition:
                        captcha_handler = CaptchaHandler(self.driver, index_tab, self.timeout, self.user_agent,
                                                         possible_captcha)

                        if captcha_handler.check_for_captcha():
                            self.logger.info("Captcha detected. Attempting to solve.")
                            captcha_handler.solve_captcha()
                            self.refresh_all_tabs()

                self.logger.info(f"HTML obtained from tab[{index_tab}].")
                self._condition.notify_all()
                return self.driver.page_source

        else:
            error_msg = f"Invalid tab index: {index_tab}"
            self.logger.error(error_msg)
            raise Exception(error_msg)

    def release_tab(self, index_tab: int, url_search: str):
        self.logger.info(f"Releasing tab[{index_tab}] for URL search: {url_search}.")
        if index_tab is not None and (0 <= index_tab < self.number_of_tabs):
            with self._condition:
                self.available_tabs[index_tab] = True
                self.logger.info(f"Tab[{index_tab}] released successfully.")
                self._condition.notify_all()
        else:
            error_msg = f"Invalid tab index: {index_tab} during release for URL search: {url_search}"
            self.logger.error(error_msg)
            raise Exception(error_msg)

    def refresh_all_tabs(self):
        for tab in self.driver.window_handles:
            self.driver.switch_to.window(tab)
            self.driver.get(self.driver.current_url)
            time.sleep(5)
        time.sleep(5)

    def close_driver(self):
        self.logger.info("Closing browser driver.")
        with self._condition:
            try:
                self.driver.quit()
                self.logger.info(f"Driver for interface {self.interface_name} closed successfully.")
            except Exception as e:
                self.logger.error(f"Error closing driver: {e}")

class SeleniumDriverManager:
    _instances = {}
    _lock = threading.Lock()

    @classmethod
    def get_instance(cls, interface_name: str) -> SeleniumDriver:
        with cls._lock:
            if interface_name not in cls._instances:
                cls._instances[interface_name] = SeleniumDriver(interface_name)
            return cls._instances[interface_name]



