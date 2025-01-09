import threading

from bs4 import BeautifulSoup

from com.gwngames.pubscraper.Context import Context
from com.gwngames.pubscraper.constants.ConfigConstants import ConfigConstants
from com.gwngames.pubscraper.scraper.BanChecker import BanChecker
from com.gwngames.pubscraper.scraper.scraper.GeneralScraper import GeneralScraper
import urllib.parse


def extract_conference_acronym(text):
    """
    Extracts the conference acronym by taking the first word (likely the acronym)
    from sanitized conference text.
    """
    if not text:
        return "Unknown"
    words = text.split()
    if words:
        return words[0]
    return "Unknown"


def sanitize_text(text):
    """
    Sanitizes text to remove unwanted characters, including parentheses, commas, points, and
    numbers that are not valid years (length != 4).
    """
    import re
    if not text:
        return "Unknown"
    text = re.sub(r"\(.*?\)|[.,]", "", text)
    text = re.sub(r"\b\d{1,3}\b|\b\d{5,}\b", "", text)
    return text.strip()


def extract_year(item):
    """
    Extracts the publication year from the item's sibling elements.
    """
    if not item:
        return "Unknown"
    year_element = item.find_previous_sibling("li", class_="year")
    if year_element:
        year = year_element.get_text(strip=True)
        return year if len(year) == 4 and year.isdigit() else "Unknown"
    return "Unknown"


class DblpScraper(GeneralScraper):
    journal_names = {}
    journal_lock = threading.Lock()

    def get_author_publications(self, author_name):
        self.logger.info("Starting to fetch publications for author: %s", author_name)
        base_url = "https://dblp.org/search?q="
        search_url = base_url + urllib.parse.quote(author_name)

        self.logger.debug("Loading search URL: %s", search_url)
        i = self.driver_manager.obtain_tab(author_name)
        self.driver_manager.load_url_from_tab(i, search_url)
        search_content = self.driver_manager.obtain_html_from_tab(i, self.ctx.get_config().get_value(ConfigConstants.MIN_WAIT_TIME))

        if BanChecker(Context()).has_ban_phrase(search_content, "Too Many Requests"):
            self.driver_manager.restart_driver()

        search_soup = BeautifulSoup(search_content, "html.parser")

        author_link_element = search_soup.select_one("div#completesearch-authors .result-list li a")

        if not author_link_element:
            self.logger.error("No author profile found for %s", author_name)
            self.driver_manager.release_tab(i, author_name)
            return {"publications": []}

        author_profile_link = author_link_element.get("href")
        if not author_profile_link:
            self.driver_manager.release_tab(i, author_name)
            self.logger.error("Author profile link not found for %s", author_name)
            return {"publications": []}

        self.logger.info("Found author profile link: %s", author_profile_link)

        self.driver_manager.load_url_from_tab(i, author_profile_link)
        profile_content = self.driver_manager.release_tab(i, author_name)
        profile_soup = BeautifulSoup(profile_content, "html.parser")
        publications = []

        publ_section = profile_soup.find(id="publ-section")
        if not publ_section:
            self.logger.warning("Publication section not found for %s", author_name)
            self.driver_manager.release_tab(i, author_name)
            return {"publications": []}

        publ_items = publ_section.find_all("li", class_="entry")
        if not publ_items:
            self.driver_manager.release_tab(i, author_name)
            self.logger.warning("No publications found for %s", author_name)
            return {"publications": []}

        self.logger.info("Extracting publications for %s", author_name)
        for item in publ_items:
            if not item or not item.get("class"):
                self.logger.info("Skipping invalid item in publications list: %S", item)
                continue

            # Determine the publication type
            if "article" in item.get("class", []):
                pub_type = "Journal"
            elif "inproceedings" in item.get("class", []):
                pub_type = "Conference"
            else:
                self.logger.info("Skipping unknown publication type in item: %s", item)
                continue

            title_element = item.find(class_="title")
            if not title_element:
                self.logger.info("Skipping item with missing title: %s", item)
                continue

            title = title_element.get_text(strip=True)
            authors = [
                author.get_text(strip=True)
                for author in item.select("span[itemprop='author'] span[itemprop='name']")
            ]
            if not authors:
                self.logger.info("Skipping item with missing authors: %s", item)
                continue

            extra_info = {}
            if pub_type == "Journal":
                journal_name = None
                journal_info = item.select_one("cite span[itemprop='isPartOf'] span[itemprop='name']")

                journal_short = sanitize_text(journal_info.get_text(strip=True))
                with DblpScraper.journal_lock:
                    if journal_short and journal_short in DblpScraper.journal_names.keys():
                        journal_name = DblpScraper.journal_names[journal_short]
                        extra_info["journal_name"] = sanitize_text(journal_name)


                cite_element = item.find('cite', class_='data tts-content')
                journal_link_element = None
                if cite_element:
                    for child in cite_element.contents:
                        if child.name == 'a' and child.has_attr('href'):  # Check for direct <a> tags
                            journal_link_element = child
                            break
                if journal_link_element and journal_link_element.has_attr("href") and journal_name is None:
                    journal_link = journal_link_element["href"]
                    self.logger.info("Found journal link: %s", journal_link)
                    self.driver_manager.load_url_from_tab(i, journal_link)
                    journal_page_content = self.driver_manager.obtain_html_from_tab(i, 5)
                    journal_soup = BeautifulSoup(journal_page_content, "html.parser")

                    journal_header = journal_soup.select_one("header#headline h1")
                    if journal_header:
                        journal_name = journal_header.get_text(strip=True).split(",")[0]
                        with DblpScraper.journal_lock:
                            DblpScraper.journal_names[journal_short] = journal_name
                        extra_info["journal_name"] = sanitize_text(journal_name)
                    self.logger.info("Found journal name: %s", journal_name)

            elif pub_type == "Conference":
                conference_info = item.select_one("cite span[itemprop='isPartOf'] span[itemprop='name']")
                if conference_info:
                    conference_text = sanitize_text(conference_info.get_text(strip=True))
                    acronym = extract_conference_acronym(conference_text)
                    extra_info["conference_acronym"] = acronym

            if not extra_info:
                self.logger.debug("Skipping item with missing required details: %s", item)
                continue

            publications.append({
                "title": title,
                "type": pub_type,
                "authors": authors,
                **extra_info
            })

        self.driver_manager.release_tab(i, author_name)

        self.logger.info("Completed fetching publications for author: %s", author_name)
        return {"publications": publications}

