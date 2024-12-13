import re

from bs4 import BeautifulSoup

from net.gwngames.pubscraper.scraper.scraper.GeneralScraper import GeneralScraper


class CoreEduScraper(GeneralScraper):

    def get_conferences_data(self, page_number):
        self.logger.info("Fetching conferences data from page: %s", page_number)
        base_url = "https://portal.core.edu.au/conf-ranks/?search=&by=all&source=all&sort=atitle&page="
        target_url = base_url + str(page_number)

        if page_number is False:
            return {}

        self.logger.debug("Loading URL: %s", target_url)
        i, page_content = self.driver_manager.retrieve_html_in_tab(target_url, 'core_conferences')
        page_soup = BeautifulSoup(page_content, "html.parser")

        self.driver_manager.release_tab(i)

        container = page_soup.find("div", id="container")
        if not container:
            self.logger.warning("No container found in page %s", page_number)
            return {"page_number": str(int(page_number)+1)}

        table = container.find("table")
        if not table:
            self.logger.warning("No table found in page %s", page_number)
            return {"page_number": str(int(page_number)+1)}

        rows = table.find_all("tr")[1:]  # Skip the header row
        if not rows:
            self.logger.warning("No rows found in the table on page %s", page_number)
            return {"page_number": str(int(page_number)+1)}

        conferences = []
        self.logger.info("Extracting conference data from table rows.")

        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 9:
                self.logger.debug("Skipping incomplete row: %s", row)
                continue

            # Extract conference details
            title = cells[0].get_text(strip=True)
            acronym = cells[1].get_text(strip=True)
            source = cells[2].get_text(strip=True)
            rank = cells[3].get_text(strip=True)
            note = cells[4].get_text(strip=True)
            dblp_link_element = cells[5].find("a")
            dblp_link = dblp_link_element["href"] if dblp_link_element else "N/A"
            primary_for = cells[6].get_text(strip=True)
            comments = cells[7].get_text(strip=True)
            avg_rating = cells[8].get_text(strip=True)

            match = re.search(r'\d{4}', source)
            year = match.group(0) if match else 0

            conference_data = {
                "title": title,
                "acronym": acronym,
                "source": source,
                "rank": rank,
                "note": note,
                "dblp_link": dblp_link,
                "primary_for": primary_for,
                "comments": comments,
                "average_rating": avg_rating,
                "year": year
            }

            self.logger.debug("Extracted conference: %s", conference_data)
            conferences.append(conference_data)

        self.logger.info("Completed fetching conference data from page: %s", page_number)
        return {"page_number": str(int(page_number)+1), "conferences": conferences}
