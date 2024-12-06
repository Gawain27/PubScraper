from bs4 import BeautifulSoup

from net.gwngames.pubscraper.scraper.scraper.GeneralScraper import GeneralScraper
import urllib.parse


class DblpScraper(GeneralScraper):

    def get_author_publications(self, author_name):
        self.logger.info("Starting to fetch publications for author: %s", author_name)
        base_url = "https://dblp.org/search?q="
        search_url = base_url + urllib.parse.quote(author_name)

        self.logger.debug("Loading search URL: %s", search_url)
        i = self.driver_manager.load_url_in_available_tab(search_url, 'dblp_pubs')
        search_content = self.driver_manager.get_html_of_tab(i)
        search_soup = BeautifulSoup(search_content, "html.parser")

        author_link_element = search_soup.select_one("div#completesearch-authors .result-list li a")
        self.driver_manager.release_tab(i)

        if not author_link_element:
            self.logger.warning("No author profile found for %s", author_name)
            return {}

        author_profile_link = author_link_element.get("href")
        if not author_profile_link:
            self.logger.error("Author profile link not found for %s", author_name)
            return {}

        self.logger.info("Found author profile link: %s", author_profile_link)

        i = self.driver_manager.load_url_in_available_tab(author_profile_link, 'dblp_pubs')
        profile_content = self.driver_manager.get_html_of_tab(i)
        profile_soup = BeautifulSoup(profile_content, "html.parser")
        publications = []

        publ_section = profile_soup.find(id="publ-section")
        if not publ_section:
            self.logger.warning("Publication section not found for %s", author_name)
            self.driver_manager.release_tab(i)
            return {"publications": []}

        publ_items = publ_section.find_all("li", class_="entry")
        if not publ_items:
            self.logger.warning("No publications found for %s", author_name)
            self.driver_manager.release_tab(i)
            return {"publications": []}

        self.logger.info("Extracting publications for %s", author_name)
        for item in publ_items:
            if not item or not item.get("class"):
                self.logger.debug("Skipping invalid item in publications list")
                continue

            # Determine the publication type
            if "article" in item.get("class", []):
                pub_type = "Journal"
            elif "inproceedings" in item.get("class", []):
                pub_type = "Conference"
            else:
                self.logger.debug("Skipping unknown publication type in item: %s", item)
                continue

            # Extract publication details
            title_element = item.find(class_="title")
            if not title_element:
                self.logger.debug("Skipping item with missing title: %s", item)
                continue

            title = title_element.get_text(strip=True)
            authors = [
                author.get_text(strip=True)
                for author in item.select("span[itemprop='author'] span[itemprop='name']")
            ]
            if not authors:
                self.logger.debug("Skipping item with missing authors: %s", item)
                continue

            extra_info = {}
            if pub_type == "Journal":
                journal_info = item.select_one("cite span[itemprop='isPartOf'] span[itemprop='name']")
                if journal_info:
                    journal_name = self.sanitize_text(journal_info.get_text(strip=True))
                    year = self.extract_year(item)
                    if year:
                        extra_info["journal_name"] = journal_name
                        extra_info["publication_year"] = year
                        self.logger.debug("Journal publication found: %s", journal_name)
            elif pub_type == "Conference":
                conference_info = item.select_one("cite span[itemprop='isPartOf'] span[itemprop='name']")
                if conference_info:
                    conference_text = self.sanitize_text(conference_info.get_text(strip=True))
                    year = self.extract_year(item)
                    if year:
                        acronym = self.extract_conference_acronym(conference_text)
                        extra_info["conference_acronym"] = acronym
                        extra_info["conference_year"] = year
                        self.logger.debug("Conference publication found: %s", conference_text)

            if not extra_info:
                self.logger.debug("Skipping item with missing required details: %s", item)
                continue

            publications.append({
                "title": title,
                "type": pub_type,
                "authors": authors,
                **extra_info
            })

        self.driver_manager.release_tab(i)
        self.logger.info("Completed fetching publications for author: %s", author_name)
        return {"publications": publications}

    def sanitize_text(self, text):
        """
        Sanitizes text to remove unwanted characters, including parentheses, commas, points, and
        numbers that are not valid years (length != 4).
        """
        import re
        if not text:
            return "Unknown"
        text = re.sub(r"\(.*?\)|[.,]", "", text)  # Remove parentheses and punctuation
        text = re.sub(r"\b\d{1,3}\b|\b\d{5,}\b", "", text)  # Remove invalid numbers
        return text.strip()

    def extract_year(self, item):
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

    def extract_conference_acronym(self, text):
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