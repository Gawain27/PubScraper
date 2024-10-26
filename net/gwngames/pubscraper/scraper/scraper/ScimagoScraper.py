from bs4 import BeautifulSoup

from net.gwngames.pubscraper.scraper.scraper.GeneralScraper import GeneralScraper


class ScimagoScraper(GeneralScraper):

    def get_journal_details(self, journal_url):
        self.logger.info("Fetching journal details from URL: %s", journal_url)
        i = self.driver_manager.load_url_in_available_tab(journal_url, 'journal_details')
        html = self.driver_manager.get_html_of_tab(i)

        soup = BeautifulSoup(html, 'html.parser')
        journal_details = {"title": soup.find("h1").get_text(strip=True)}

        country_section = soup.find("h2", text="Country")
        journal_details["country"] = country_section.find_next("p").get_text(
            strip=True) if country_section else "Unknown"

        subject_area_section = soup.find("h2", text="Subject Area and Category")
        subject_areas = []
        if subject_area_section:
            for category in subject_area_section.find_next("ul").find_all("li", recursive=False):
                area = category.find("a").get_text(strip=True)
                categories = [cat.get_text(strip=True) for cat in category.find_all("li")]
                subject_areas.append({"area": area, "categories": categories})
        journal_details["subject_areas"] = subject_areas

        publisher_section = soup.find("h2", text="Publisher")
        journal_details["publisher"] = publisher_section.find_next("p").get_text(
            strip=True) if publisher_section else "Unknown"

        hindex_section = soup.find("h2", text="H-Index")
        journal_details["h_index"] = hindex_section.find_next("p", class_="hindexnumber").get_text(
            strip=True) if hindex_section else "Unknown"

        publication_type_section = soup.find("h2", text="Publication type")
        journal_details["publication_type"] = publication_type_section.find_next("p").get_text(
            strip=True) if publication_type_section else "Unknown"

        issn_section = soup.find("h2", text="ISSN")
        journal_details["issn"] = issn_section.find_next("p").get_text(strip=True) if issn_section else "Unknown"

        coverage_section = soup.find("h2", text="Coverage")
        journal_details["coverage_years"] = coverage_section.find_next("p").get_text(
            strip=True) if coverage_section else "Unknown"

        information_section = soup.find("h2", text="Information")
        links = information_section.find_next_siblings("p") if information_section else []
        journal_details["information_links"] = {
            "homepage": "None",
            "submission": "None",
            "contact_email": "None"
        }
        for link in links:
            if "Homepage" in link.get_text(strip=True):
                journal_details["information_links"]["homepage"] = link.find_next("a")["href"]
            elif "How to publish in this journal" in link.get_text(strip=True):
                journal_details["information_links"]["submission"] = link.find_next("a")["href"]
            elif "@" in link.get_text(strip=True):
                journal_details["information_links"]["contact_email"] = link.get_text(strip=True)

        scope_section = soup.find("h2", text="Scope")
        journal_details["scope"] = scope_section.find_next("div", class_="fullwidth").get_text(
            strip=True) if scope_section else "Unknown"

        quartile_data = []
        quartile_table = soup.find("table")
        if quartile_table:
            for row in quartile_table.find("tbody").find_all("tr"):
                cells = row.find_all("td")
                category = cells[0].get_text(strip=True)
                year = cells[1].get_text(strip=True)
                quartile = cells[2].get_text(strip=True)
                quartile_data.append({"category": category, "year": year, "quartile": quartile})
        journal_details["quartiles"] = quartile_data

        self.driver_manager.release_tab(i)
        self.logger.info("Completed extraction for journal URL: %s", journal_url)
        return journal_details
