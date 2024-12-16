import logging
import re
import threading
import time

from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from selenium import webdriver
from selenium.common import NoSuchElementException
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.support.wait import WebDriverWait
from twocaptcha import TwoCaptcha
from typing_extensions import Final

from net.gwngames.pubscraper.Context import Context
from net.gwngames.pubscraper.constants.CaptchaConstants import CaptchaActionConstants, CaptchaConstants
from net.gwngames.pubscraper.constants.ConfigConstants import ConfigConstants
from net.gwngames.pubscraper.exception.IgnoreCaptchaException import IgnoreCaptchaException
from net.gwngames.pubscraper.exception.UninplementedCaptchaException import UninmplementedCaptchaException
from net.gwngames.pubscraper.utils.JsonReader import JsonReader
from net.gwngames.pubscraper.utils.ThreadUtils import ThreadUtils


class SeleniumDriver:
    ONE_PER_REQ: Final = "one-per-req"
    FREE: Final = "free-tabs"

    def __init__(self, interface_name: str):
        self.interface_name = interface_name
        self.logger = logging.getLogger(f"SeleniumDriver-{interface_name}")
        self.ctx = Context()
        self.config = JsonReader(JsonReader.CONFIG_FILE_NAME)
        self.number_of_tabs = self.ctx.get_max_requests()
        self.available_tabs = {i: True for i in range(self.number_of_tabs)}
        self.window_handles = {}
        self.tab_to_url_type = {}
        self._condition = threading.Condition()
        self.timeout = self.config.get_value(ConfigConstants.URL_TIMEOUT)
        self.mode = SeleniumDriver.FREE
        self.driver = self._initialize_driver()
        self._initialize_tabs()
        time.sleep(5)  # Initialization time

    def _initialize_driver(self):
        browser_type = self.config.get_value(ConfigConstants.BROWSER_TYPE)
        self.logger.info(f"Initializing browser driver for {browser_type}.")
        browser_driver_path = self.config.get_value(ConfigConstants.BROWSER_DRIVER_PATH)

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
            firefox_options.headless = True
            self.user_agent = UserAgent().random
            profile = webdriver.FirefoxProfile(
                f"{self.config.get_value(ConfigConstants.BROWSER_DATA_PATH)}\\profile.default")
            profile.set_preference("general.useragent.override", self.user_agent)
            profile.set_preference("dom.webdriver.enabled", False)
            firefox_options.profile = profile
            firefox_options.binary_location = browser_driver_path
            driver = webdriver.Firefox(options=firefox_options)
        else:
            raise ValueError(f"Unsupported browser type: {browser_type}")

        self.logger.info(f"Browser driver for {browser_type} initialized.")
        return driver

    def _initialize_tabs(self):
        for _ in range(1, self.number_of_tabs):
            self.driver.execute_script("window.open('about:blank', '_blank');")
            time.sleep(1)
        for i, handle in enumerate(self.driver.window_handles):
            self.window_handles[i] = handle
        self.logger.info(f"Initialized {self.number_of_tabs} tabs.")

    def restart_driver(self):
        with self._condition:
            self.close_driver()
            self.driver = self._initialize_driver()
            self._initialize_tabs()

    def retrieve_html_in_tab(self, url, url_type, possible_captcha=None):
        index_tab = None
        html_content = None
        try:
            while True:
                with self._condition:
                    if self.mode == SeleniumDriver.ONE_PER_REQ:
                        # Lock one tab for each url_type
                        if url_type not in self.tab_to_url_type.values():
                            for tab_id, is_available in self.available_tabs.items():
                                if is_available:
                                    index_tab = tab_id
                                    self.available_tabs[tab_id] = False
                                    self.tab_to_url_type[tab_id] = url_type
                                    self.logger.debug(f"TAB[{tab_id}] assigned to URL type: {url_type}")
                                    break
                            if index_tab is None:
                                self._condition.wait()
                                continue
                        else:
                            self._condition.wait()
                            continue
                    elif self.mode == SeleniumDriver.FREE:
                        # Use any available tab
                        for tab_id, is_available in self.available_tabs.items():
                            if is_available:
                                index_tab = tab_id
                                self.available_tabs[tab_id] = False
                                self.tab_to_url_type[tab_id] = url_type
                                self.logger.debug(f"TAB[{tab_id}] assigned in FREE mode to URL type: {url_type}")
                                break
                        if index_tab is None:
                            self._condition.wait()
                            continue

                    if index_tab is not None:
                        self.driver.switch_to.window(self.window_handles[index_tab])
                        self.driver.get(url)
                        WebDriverWait(self.driver, self.timeout).until(
                            lambda x: self.driver.execute_script("return document.readyState") == "complete"
                        )

                        self.check_for_captcha(index_tab, possible_captcha)
                        html_content = self.driver.page_source
                        self._condition.notify_all()
                        return index_tab, html_content
                self._condition.wait()
        except Exception as e:
            if index_tab is not None:
                self.release_tab(index_tab)
            self.logger.error(f"Error retrieving HTML in tab: {e}")
            raise e

    def check_for_captcha(self, tab_id, captcha_div_id):
        if captcha_div_id:
            soup = BeautifulSoup(self.driver.page_source, "html.parser")
            for script in soup.find_all('script'):
                if 'grecaptcha.render' in (script.string or ''):
                    self.logger.warning("Captcha found! Handling...")
                    sitekey = re.search(r'"sitekey":"(.*?)"', script.string).group(1)
                    iframe = soup.find('iframe', {'title': 'reCAPTCHA'})
                    captcha_url = iframe['src']
                    self.solve_captcha(tab_id, CaptchaConstants.RECAPTCHA_V2, sitekey, captcha_url, captcha_div_id)

    def release_tab(self, tab_id):
        ThreadUtils.sleep_for(self.ctx.get_config().get_value(ConfigConstants.MIN_WAIT_TIME),
                              self.ctx.get_config().get_value(ConfigConstants.MAX_WAIT_TIME),
                              self.logger,
                              tab_id)
        with self._condition:
            self.available_tabs[tab_id] = True
            self.tab_to_url_type.pop(tab_id, None)
            self._condition.notify_all()
        self.logger.info(f"Released tab {tab_id}.")

    def close_driver(self):
        with self._condition:
            self.driver.quit()
            self.logger.info(f"Closed driver for interface {self.interface_name}.")

    def solve_captcha(self, tab_id, captcha_type: int, captcha_key: str, captcha_url: str, captcha_div_id: str):
        action_type: int = self.config.get_value(ConfigConstants.CAPTCHA_ACTION)

        if action_type == CaptchaActionConstants.IGNORE:
            raise IgnoreCaptchaException(
                f"TAB[{tab_id}] - Captcha ignored for: " + str(captcha_type) + " - " + captcha_key)

        elif action_type == CaptchaActionConstants.WAIT_USER:
            self.driver.switch_to.window(self.window_handles[tab_id])
            while True:
                try:
                    element = self.driver.find_element(By.ID, captcha_div_id)
                    if not element.is_displayed():
                        break  # Exit the loop if the captcha element is no longer visible
                    self.logger.info(f"TAB[{tab_id}] - Waiting for captcha to be solved...")
                    time.sleep(10)
                except NoSuchElementException:
                    break
            self.logger.info(
                f"TAB[{tab_id}] - Captcha manually solved: " + str(captcha_type) + " - " + captcha_key)

        elif action_type == CaptchaActionConstants.BYPASS:
            # Here we solve the various types of captcha
            self.driver.switch_to.window(self.window_handles[tab_id])
            captcha_api_key = self.config.get_value(ConfigConstants.CAPTCHA_API_KEY)
            if captcha_api_key is None or captcha_api_key == "":
                raise IgnoreCaptchaException(
                    f"TAB[{tab_id}] - Captcha API key is invalid, check your configuration")

            solver = TwoCaptcha(apiKey=captcha_api_key)
            cookies = self.driver.get_cookies()
            cookie_string = ";".join([f"{cookie['name']}:{cookie['value']}" for cookie in cookies])

            if captcha_type == CaptchaConstants.RECAPTCHA_V2:
                captcha_result = solver.recaptcha(sitekey=captcha_key, url=captcha_url, userAgent=self.user_agent,
                                                  cookies=cookie_string)
                self.logger.info("zyz" + str(captcha_result))
                textarea = self.driver.find_element(By.ID, "g-recaptcha-response")

                self.driver.execute_script(
                    f"arguments[0].style.display = 'block'; arguments[0].value = '{captcha_result['code']}';",
                    textarea)
                form = self.driver.find_element(By.ID, 'gs_captcha_f')
                form.submit()
                time.sleep(2)
                self.driver.refresh()
                WebDriverWait(self.driver, self.timeout).until(
                    lambda x: self.driver.execute_script("return document.readyState") == "complete")
            else:
                raise UninmplementedCaptchaException(
                    f"TAB[{tab_id}] - Captcha resolution not implemented for: " + str(
                        captcha_type) + " - " + captcha_key)

            self.logger.info(
                f"TAB[{tab_id}] - Captcha automatically solved: " + str(captcha_type) + " - " + captcha_key)
        else:
            raise Exception(f"TAB[{tab_id}] - Invalid captcha action")

class SeleniumDriverManager:
    _instances = {}
    _lock = threading.Lock()

    @classmethod
    def get_instance(cls, interface_name: str) -> SeleniumDriver:
        with cls._lock:
            if interface_name not in cls._instances:
                cls._instances[interface_name] = SeleniumDriver(interface_name)
            return cls._instances[interface_name]



