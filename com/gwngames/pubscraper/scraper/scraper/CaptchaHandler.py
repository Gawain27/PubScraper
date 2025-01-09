import logging
import re
import time

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from twocaptcha import TwoCaptcha

from com.gwngames.pubscraper.Context import Context
from com.gwngames.pubscraper.constants.CaptchaConstants import CaptchaActionConstants, CaptchaConstants
from com.gwngames.pubscraper.constants.ConfigConstants import ConfigConstants
from com.gwngames.pubscraper.exception.IgnoreCaptchaException import IgnoreCaptchaException
from com.gwngames.pubscraper.exception.UninplementedCaptchaException import UninmplementedCaptchaException


class CaptchaHandler:

    def __init__(self, driver: webdriver, tab_id: int, timeout: int, user_agent: str, captcha_div_id: str):
        if not captcha_div_id:
            raise ValueError("Captcha div ID cannot be empty.")

        self.driver = driver
        self.tab_id = tab_id
        self.timeout = timeout
        self.logger = logging.getLogger('CaptchaHandler')
        self.ctx = Context()
        self.user_agent = user_agent
        self.captcha_div_id = captcha_div_id
        self.site_key = None
        self.captcha_url = None
        self.captcha_type = None

    def check_for_captcha(self) -> bool:
        try:
            soup = BeautifulSoup(self.driver.page_source, "html.parser")
            for script in soup.find_all('script'):
                script_content = script.string or ''
                if 'grecaptcha.render' in script_content:
                    self.logger.warning("Captcha found! Attempting to handle...")
                    self.site_key = re.search(r'"sitekey":"(.*?)"', script_content).group(1)
                    iframe = soup.find('iframe', {'title': 'reCAPTCHA'})
                    if iframe:
                        self.captcha_url = iframe['src']
                        self.captcha_type = CaptchaConstants.RECAPTCHA_V2
                        self.logger.info(f"Captcha detected: type={self.captcha_type}, site_key={self.site_key}")
                        return True
                    else:
                        self.logger.error("Captcha iframe not found.")
                        return False
            self.logger.info("No captcha detected.")
            return False
        except Exception as e:
            self.logger.error(f"Error while checking for captcha: {str(e)}")
            return False

    def solve_captcha(self):
        try:
            action_type = self.ctx.get_config().get_value(ConfigConstants.CAPTCHA_ACTION)
            self.logger.info("Determining captcha action type.")

            if action_type == CaptchaActionConstants.IGNORE:
                raise IgnoreCaptchaException(
                    f"TAB[{self.tab_id}] - Ignoring captcha: type={self.captcha_type}, site_key={self.site_key}")

            elif action_type == CaptchaActionConstants.WAIT_USER:
                self._wait_for_manual_resolution()

            elif action_type == CaptchaActionConstants.BYPASS:
                self._bypass_captcha()

            else:
                self.logger.error("Invalid captcha action specified.")
                raise Exception(f"TAB[{self.tab_id}] - Invalid captcha action.")

        except Exception as e:
            self.logger.error(f"Failed to solve captcha: {str(e)}")
            raise

    def _wait_for_manual_resolution(self):
        self.logger.info(f"TAB[{self.tab_id}] - Waiting for manual captcha resolution.")
        try:
            while True:
                element = self.driver.find_element(By.ID, self.captcha_div_id)
                if not element.is_displayed():
                    self.logger.info("Captcha element no longer visible. Assuming manual resolution completed.")
                    break
                time.sleep(10)
        except NoSuchElementException:
            self.logger.info("Captcha element not found. Assuming manual resolution completed.")
        except Exception as e:
            self.logger.error(f"Error during manual captcha resolution: {str(e)}")
            raise

    def _bypass_captcha(self):
        try:
            captcha_api_key = self.ctx.get_config().get_value(ConfigConstants.CAPTCHA_API_KEY)
            if not captcha_api_key:
                raise IgnoreCaptchaException("Captcha API key is not configured.")

            solver = TwoCaptcha(apiKey=captcha_api_key)
            cookies = self.driver.get_cookies()
            cookie_string = "; ".join([f"{cookie['name']}={cookie['value']}" for cookie in cookies])

            if self.captcha_type == CaptchaConstants.RECAPTCHA_V2:
                self._solve_recaptcha_v2(solver, self.site_key, self.captcha_url, cookie_string)
            else:
                raise UninmplementedCaptchaException(
                    f"TAB[{self.tab_id}] - Captcha type not supported: {self.captcha_type}")

        except Exception as e:
            self.logger.error(f"Failed to bypass captcha: {str(e)}")
            raise

    def _solve_recaptcha_v2(self, solver, site_key: str, captcha_url: str, cookie_string: str):
        try:
            self.logger.info("Attempting to solve reCAPTCHA v2.")
            captcha_result = solver.recaptcha(sitekey=site_key, url=captcha_url, userAgent=self.user_agent,
                                              cookies=cookie_string)
            self.logger.info(f"Captcha solved: {captcha_result}")

            textarea = self.driver.find_element(By.ID, "g-recaptcha-response")
            self.driver.execute_script(
                f"arguments[0].style.display = 'block'; arguments[0].value = '{captcha_result['code']}';", textarea)

            form = self.driver.find_element(By.ID, 'gs_captcha_f')
            form.submit()
            WebDriverWait(self.driver, self.timeout).until(
                lambda x: self.driver.execute_script("return document.readyState") == "complete")
            self.logger.info("Captcha successfully submitted and page reloaded.")

        except Exception as e:
            self.logger.error(f"Error while solving reCAPTCHA v2: {str(e)}")
            raise
