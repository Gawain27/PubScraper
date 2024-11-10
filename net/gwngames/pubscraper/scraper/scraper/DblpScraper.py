from bs4 import BeautifulSoup

from net.gwngames.pubscraper.constants.JsonConstants import JsonConstants
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
        self.logger.info("Found author profile link: %s", author_profile_link)

        i = self.driver_manager.load_url_in_available_tab(author_profile_link, 'dblp_pubs')
        profile_content = self.driver_manager.get_html_of_tab(i)
        profile_soup = BeautifulSoup(profile_content, "html.parser")
        publications = []

        publ_section = profile_soup.find(id="publ-section")
        publ_items = publ_section.find_all("li", class_="entry")

        self.logger.info("Extracting publications for %s", author_name)
        for item in publ_items:
            # Determine the publication type (Journal or Conference)
            if "article" in item.get("class", []):
                pub_type = "Journal"
            elif "inproceedings" in item.get("class", []):
                pub_type = "Conference"
            else:
                self.logger.debug("Skipping unknown publication type in item: %s", item)
                continue

            # Extract publication details
            title = item.find(class_="title").get_text(strip=True)
            year = item.find_previous_sibling("li", class_="year").get_text(strip=True)
            link = item.select_one("cite > a:last-of-type").get("href")
            authors = [author.get_text(strip=True) for author in
                       item.select("span[itemprop='author'] span[itemprop='name']")]

            extra_info = {}
            if pub_type == "Journal":
                journal_name = item.select_one("cite span[itemprop='isPartOf'] span[itemprop='name']").get_text(
                    strip=True)
                extra_info["journal_full_name"] = journal_name
                self.logger.debug("Journal publication found: %s", journal_name)
            elif pub_type == "Conference":
                conference_name = item.select_one("cite span[itemprop='isPartOf'] span[itemprop='name']").get_text(
                    strip=True)
                conference_parts = conference_name.split("/")
                extra_info["conference_parts"] = conference_parts
                self.logger.debug("Conference publication found: %s", conference_name)

            publications.append({
                "title": title,
                "type": pub_type,
                "year": year,
                "link": link,
                "authors": authors,
                **extra_info
            })

        self.driver_manager.release_tab(i)
        self.logger.info("Completed fetching publications for author: %s", author_name)
        return {JsonConstants.TAG_PUBLICATIONS: publications}

    def get_journal_volume_data(self, volume_url):
        self.logger.info("Starting to fetch journal volume data from: %s", volume_url)
        i = self.driver_manager.load_url_in_available_tab(volume_url, 'dblp_journal')
        journal_page = self.driver_manager.get_html_of_tab(i)
        soup = BeautifulSoup(journal_page, 'html.parser')

        collection_title = soup.find('h1').text.strip() if soup.find('h1') else "Unknown Collection Title"

        self.logger.debug("Collection title found: %s", collection_title)

        journals = []
        volume_number = collection_title.split(", Volume ")[
            -1] if ", Volume " in collection_title else "Unknown Volume Number"

        collection_title = collection_title.split(',')[0]

        for entry in soup.select('li.entry.article'):
            title = entry.find(class_='title').get_text(strip=True) if entry.find(class_='title') else "Unknown Title"
            authors = [author.get_text(strip=True) for author in
                       entry.select('span[itemprop="author"] span[itemprop="name"]')]
            doi_link = entry.select_one('nav.publ li.ee a')
            doi = doi_link.get('href') if doi_link else None

            volume = entry.find('meta', {'itemprop': 'volume'}).get('content', 'Unknown Volume') if entry.find('meta', {
                'itemprop': 'volume'}) else 'Unknown Volume'
            issue = entry.find('meta', {'itemprop': 'issue'}).get('content', 'Unknown Issue') if entry.find('meta', {
                'itemprop': 'issue'}) else 'Unknown Issue'
            year = entry.find('meta', {'itemprop': 'datePublished'}).get('content', 'Unknown Year') if entry.find(
                'meta', {'itemprop': 'datePublished'}) else 'Unknown Year'
            pages = entry.find('span', {'itemprop': 'pagination'}).get_text(strip=True) if entry.find('span', {
                'itemprop': 'pagination'}) else "Unknown Pages"

            journals.append({
                "title": title,
                "authors": authors,
                "collection_title": collection_title,
                "volume": volume,
                "issue": issue,
                "year": year,
                "pages": pages,
                "doi": doi,
                "volume_number": volume_number
            })

        self.driver_manager.release_tab(i)
        self.logger.info("Completed fetching journal volume data for: %s", volume_url)
        return {JsonConstants.TAG_JOURNALS: journals}

    def extract_articles(self, conference_url):
        self.logger.info("Starting to extract articles from conference: %s", conference_url)
        i = self.driver_manager.load_url_in_available_tab(conference_url, 'dblp_conference')
        html_content = self.driver_manager.get_html_of_tab(i)
        soup = BeautifulSoup(html_content, 'html.parser')
        articles_data = []

        breadcrumb_elements = soup.find('div', id='breadcrumbs').find_all('span', itemprop='name')
        conferences = [element.text.strip() for element in breadcrumb_elements if len(element.text.strip()) <= 5 and not element.text.__contains__('Home')]
        self.logger.debug("Conferences found: %s", conferences)

        conference_title = soup.find('h1').text if soup.find('h1') else 'Unknown Conference'
        self.logger.debug("Conference title found: %s", conference_title)

        workshop_sections = soup.find_all('header', class_='h2')
        for workshop in workshop_sections:
            workshop_name = workshop.find('h2').text.strip()
            articles = workshop.find_next('ul', class_='publ-list').find_all('li', class_='entry inproceedings')

            for article in articles:
                title_tag = article.find('span', class_='title')
                article_title = title_tag.text.strip() if title_tag else 'Unknown Title'

                authors = [author.text.strip() for author in article.find_all('span', itemprop='name')]
                year_tag = article.find('meta', itemprop='datePublished')
                year = year_tag['content'] if year_tag else 'Unknown Year'

                article_data = {
                    'conference_title': conference_title,
                    'authors': ', '.join(authors),
                    'article_title': article_title,
                    'year': year,
                    'workshop_name': workshop_name,
                    'conferences': conferences  # Add the dynamically extracted list of conference acronyms
                }
                articles_data.append(article_data)

        self.driver_manager.release_tab(i)
        self.logger.info("Completed extracting articles from conference: %s", conference_url)
        return {JsonConstants.TAG_CONFERENCES: articles_data}
