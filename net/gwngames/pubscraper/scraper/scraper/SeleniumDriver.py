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

from net.gwngames.pubscraper.Context import Context
from net.gwngames.pubscraper.constants.CaptchaConstants import CaptchaActionConstants, CaptchaConstants
from net.gwngames.pubscraper.constants.ConfigConstants import ConfigConstants
from net.gwngames.pubscraper.exception.IgnoreCaptchaException import IgnoreCaptchaException
from net.gwngames.pubscraper.exception.UninplementedCaptchaException import UninmplementedCaptchaException
from net.gwngames.pubscraper.utils.JsonReader import JsonReader
from net.gwngames.pubscraper.utils.ThreadUtils import ThreadUtils


class SeleniumDriver:
    _instance = None
    _lock = threading.Lock()

    def __init__(self):
        if SeleniumDriver._instance is not None:
            raise Exception("This class is a singleton! Use 'get_instance()' instead.")

        self.logger = logging.getLogger(SeleniumDriver.__name__)
        self.ctx = Context()
        self.config = JsonReader(JsonReader.CONFIG_FILE_NAME)
        self.number_of_tabs = self.ctx.get_max_requests()
        self.available_tabs = {}
        for i in range(self.number_of_tabs):
            self.available_tabs[i] = True
        self.window_handles = {}
        self.tab_to_url_type = {}
        self._condition = threading.Condition()
        self.timeout = self.config.get_value(ConfigConstants.URL_TIMEOUT)

        # Initialize one driver with multiple tabs
        self.driver = self._initialize_driver()
        self._initialize_tabs()

    @classmethod
    def get_instance(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

    def lock_driver(self):
        self._condition.acquire()

    def unlock_driver(self):
        self._condition.release()

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

    def _initialize_driver(self):
        self.logger.info(f"Init for the browser driver for {self.config.get_value(ConfigConstants.BROWSER_TYPE)}.")
        browser_driver_path = self.config.get_value(ConfigConstants.BROWSER_DRIVER_PATH)
        if str(self.config.get_value(ConfigConstants.BROWSER_TYPE)).lower() == 'chrome':
            chrome_options = ChromeOptions()

            self.user_agent = UserAgent().random
            chrome_options.add_argument(f"user-agent={self.user_agent}")

            chrome_options.add_argument(f"user-data-dir={self.config.get_value(ConfigConstants.BROWSER_DATA_PATH)}")
            chrome_options.add_argument(f"profile-directory=Default")

            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            chrome_options.binary_location = browser_driver_path

            driver = webdriver.Chrome(options=chrome_options)

        elif str(self.config.get_value(ConfigConstants.BROWSER_TYPE)).lower() == 'firefox':
            firefox_options = FirefoxOptions()
            firefox_options.headless = True
            self.user_agent = UserAgent().random

            profile = webdriver.FirefoxProfile(
                f"{self.config.get_value(ConfigConstants.BROWSER_DATA_PATH)}\\profile.default")

            profile.set_preference("general.useragent.override", self.user_agent)
            profile.set_preference("dom.webdriver.enabled", False)  # Hide WebDriver
            profile.set_preference("useAutomationExtension", False)
            profile.set_preference("media.navigator.permission.disabled", True)  # Hide media permissions
            profile.set_preference("media.peerconnection.enabled", False)  # Disable WebRTC leaks
            profile.set_preference("dom.webnotifications.enabled", False)  # Disable notifications
            profile.set_preference("datareporting.healthreport.uploadEnabled", False)
            profile.set_preference("datareporting.policy.dataSubmissionEnabled", False)
            profile.set_preference("geo.enabled", False)
            profile.set_preference("geo.provider.use_corelocation", False)
            profile.set_preference("geo.prompt.testing", False)
            profile.set_preference("geo.prompt.testing.allow", False)

            firefox_options.profile = profile
            firefox_options.binary_location = browser_driver_path

            driver = webdriver.Firefox(options=firefox_options)

        else:
            raise ValueError(f"Unsupported browser type: {self.config.get_value(ConfigConstants.BROWSER_TYPE)}")

        self.logger.info(f"Initialized the browser driver for {self.config.get_value(ConfigConstants.BROWSER_TYPE)}.")
        return driver

    def _initialize_tabs(self):
        # Open new tabs using JavaScript and store the window handles
        for i in range(1, self.number_of_tabs):
            self.driver.execute_script("window.open('about:blank', '_blank');")
            time.sleep(1)  # Ensure the tab opens completely

        i = 0
        for handle in self.driver.window_handles:
            self.window_handles[i] = handle
            i = i + 1

        self.logger.info(
            f"Initialized {self.number_of_tabs} tabs for {self.config.get_value(ConfigConstants.BROWSER_TYPE)}.")

    def _load_url(self, index_tab, possible_captcha, url):
        self.driver.switch_to.window(self.window_handles[index_tab])
        self.driver.get(url)
        WebDriverWait(self.driver, self.timeout).until(
            lambda x: self.driver.execute_script("return document.readyState") == "complete")
        self.check_for_captcha(index_tab, possible_captcha)
        self._condition.notify_all()  # Notify other threads

    def load_url_in_available_tab(self, url, url_type, possible_captcha=None, prev_ind=None):
        index_tab = None
        try:
            while True:
                with self._condition:
                    # Check if a tab is already assigned for this url_type
                    if prev_ind is None and url_type in self.tab_to_url_type.values():
                        self._condition.wait()
                        continue  # Retry after waiting

                    # Critical section to ensure only one thread can select an available tab
                    if prev_ind is None:
                        for i, is_available in self.available_tabs.items():
                            if is_available:
                                # Mark tab as unavailable and assign the url_type to this tab
                                self.available_tabs[i] = False
                                self.tab_to_url_type[i] = url_type
                                index_tab = i
                                break  # Exit the loop once a tab is assigned

                    if prev_ind is not None:
                        index_tab = prev_ind
                        self._load_url(prev_ind, possible_captcha, url)
                        break  # Exit the while loop once a tab is resynched

                    if index_tab is not None:
                        self._load_url(index_tab, possible_captcha, url)
                        break

                    # If no tabs are available, wait for a tab to be released
                    self._condition.wait()

            if index_tab is not None:
                self.logger.info(f"Loaded {url} in tab {index_tab} for url_type {url_type}")
                return index_tab

        except Exception as e:
            if index_tab is not None:
                self.release_tab(index_tab)
            raise e

    def get_html_of_tab(self, tab_id):
        with self._condition:
            if 0 <= tab_id < len(self.window_handles):
                self.driver.switch_to.window(self.window_handles[tab_id])
                html_content = self.driver.page_source
                self.logger.info(f"TAB[{tab_id}] - Retrieved HTML content from tab {tab_id}")
                return html_content
            else:
                raise Exception(f"TAB[{tab_id}] - Invalid tab ID: {tab_id}")

    def check_for_captcha(self, tab_id, captcha_div_id: str):
        if captcha_div_id is None:
            return
        try:
            soup = BeautifulSoup(self.driver.page_source, "html.parser")
            for script in soup.find_all('script'):
                if script.string and 'grecaptcha.render' in script.string:
                    # Use regex to extract the sitekey from the script content
                    sitekey_match = re.search(r'"sitekey":"(.*?)"', script.string)
                    if sitekey_match:
                        sitekey = sitekey_match.group(1)
                        iframe = soup.find('iframe', {'title': 'reCAPTCHA'})
                        captcha_url = iframe['src']
                        self.logger.warning(f"TAB[{tab_id}] - reCAPTCHA v2 detected. Sitekey: {sitekey}")
                        self.solve_captcha(tab_id, CaptchaConstants.RECAPTCHA_V2, sitekey, captcha_url, captcha_div_id)

        except Exception as e:
            raise e

    def release_tab(self, tab_id):
        if 0 <= tab_id < len(self.available_tabs):

            self.logger.info(f"Releasing TAB[{tab_id}]...")
            ThreadUtils.random_sleep(self.config.get_value(ConfigConstants.MIN_WAIT_TIME),
                                     self.config.get_value(ConfigConstants.MAX_WAIT_TIME), self.logger,
                                     f"TAB[{tab_id}]")
            with self._condition:
                self.tab_to_url_type[tab_id] = ""
                self.available_tabs[tab_id] = True
                self._condition.notify_all()
        else:
            raise Exception(f"TAB[{tab_id}] - Invalid tab ID: {tab_id}")

    def close_driver(self):
        with self._condition:
            self.driver.quit()
            self.logger.info(f"Closed all {self.number_of_tabs} tabs.")
