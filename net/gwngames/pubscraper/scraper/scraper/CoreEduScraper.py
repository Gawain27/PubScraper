import urllib.parse

from bs4 import BeautifulSoup

from net.gwngames.pubscraper.constants.JsonConstants import JsonConstants
from net.gwngames.pubscraper.scraper.scraper.GeneralScraper import GeneralScraper


class CoreEduScraper(GeneralScraper):

    def get_conference_details(self, acronym):
        base_url = "https://portal.core.edu.au/conf-ranks/"
        search_url = f"{base_url}?search={urllib.parse.quote(acronym)}&by=acronym&source=all&sort=atitle&page=1"
        self.logger.info("Built search URL: %s", search_url)

        i = self.driver_manager.load_url_in_available_tab(search_url, 'conference_details')
        html = self.driver_manager.get_html_of_tab(i)

        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find("table")

        if not table:
            self.logger.warning("No conference table found on page.")
            self.driver_manager.release_tab(i)
            return []

        conference_details = []

        for row in table.find_all("tr", class_="evenrow"):
            conference_data = {}

            cells = row.find_all("td")
            if len(cells) >= 9:  # Ensure there are enough cells
                conference_data["title"] = cells[0].get_text(strip=True)
                conference_data["acronym"] = cells[1].get_text(strip=True)
                conference_data["source"] = cells[2].get_text(strip=True)
                conference_data["rank"] = cells[3].get_text(strip=True)
                conference_data["note"] = cells[4].get_text(strip=True)

                dblp_link = cells[5].select_one("a[target='_blank']")
                conference_data["dblp_link"] = dblp_link.get("href") if dblp_link else None

                conference_data["primary_for"] = cells[6].get_text(strip=True)
                conference_data["comments"] = cells[7].get_text(strip=True)
                conference_data["average_rating"] = cells[8].get_text(strip=True)

                # Extract and construct the conference-specific URL from the onclick attribute
                onclick = row.get("onclick")
                if onclick:
                    # Extract path, remove redundant '/conf-ranks/' if present
                    relative_path = onclick.split("'")[1]
                    if relative_path.startswith("/conf-ranks/"):
                        relative_path = relative_path.replace("/conf-ranks/", "", 1)
                    conference_data["conference_url"] = f"{base_url}/conf-ranks/{relative_path}"
                    self.logger.info("Constructed conference URL: %s", conference_data["conference_url"])
                else:
                    self.logger.debug("No onclick attribute found in row: %s", row)
                    conference_data["conference_url"] = ""

                self.logger.info("Extracted conference data: %s", conference_data)
                conference_details.append(conference_data)
            else:
                self.logger.debug("Row skipped due to insufficient cells: %s", row)

        self.driver_manager.release_tab(i)
        self.logger.info("Extracted details for %d conferences with acronym %s", len(conference_details), acronym)
        return {JsonConstants.TAG_CONFERENCES, conference_details}

    def get_conference_year_details(self, conference_url):
        self.logger.info("Fetching conference details from URL: %s", conference_url)
        i = self.driver_manager.load_url_in_available_tab(conference_url, 'conference_year_details')
        html = self.driver_manager.get_html_of_tab(i)

        soup = BeautifulSoup(html, 'html.parser')

        acronym_row = soup.find("div", class_="row evenrow", text=lambda x: x and "Acronym:" in x)
        acronym = acronym_row.get_text(strip=True).split(":")[-1].strip() if acronym_row else "Unknown"
        self.logger.info("Extracted acronym: %s", acronym)

        years = {}

        details_sections = soup.find_all("div", class_="detail")
        for section in details_sections:
            source = None
            rank = None
            field_of_research_code = "Unknown"
            field_of_research_description = "Unknown"

            for row in section.find_all("div", class_="row"):
                text = row.get_text(strip=True)

                if "Source:" in text:
                    source = text.split("Source:")[-1].strip()
                    self.logger.debug("Found source year: %s", source)

                elif "Rank:" in text:
                    rank = text.split("Rank:")[-1].strip()
                    self.logger.debug("Found rank: %s", rank)

                elif "Field Of Research:" in text:
                    for_text = text.split("Field Of Research:")[-1].strip()
                    field_of_research_code = for_text.split("-")[0].strip()
                    field_of_research_description = for_text.split("-")[1].strip() if "-" in for_text else "Unknown"
                    self.logger.debug("Found Field of Research - Code: %s, Description: %s",
                                      field_of_research_code, field_of_research_description)

            if source:
                years[source] = {
                    "acronym": acronym,
                    "rank": rank if rank else "Unknown",
                    "field_of_research": {
                        "code": field_of_research_code,
                        "description": field_of_research_description
                    }
                }
                self.logger.info("Added data for source year %s: %s", source, years[source])
            else:
                self.logger.warning("Skipping section due to missing source year information: %s", section)

        self.driver_manager.release_tab(i)
        self.logger.info("Completed extraction for conference URL: %s", conference_url)
        return {"years": years}
