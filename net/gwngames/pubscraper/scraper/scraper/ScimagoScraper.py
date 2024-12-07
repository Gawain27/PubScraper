from net.gwngames.pubscraper.constants.ConfigConstants import ConfigConstants
from net.gwngames.pubscraper.constants.JsonConstants import JsonConstants
from net.gwngames.pubscraper.scraper.scraper.GeneralScraper import GeneralScraper


class ScimagoScraper(GeneralScraper):
    year = 1999  # Static years, every publication beging at this point, incremental only

    def get_journals_from_page(self, area):
        """
        Retrieves journal data for the given year, area, and page number from SCImago Journal Rank.
        """
        import re
        from bs4 import BeautifulSoup

        base_url = "https://www.scimagojr.com/journalrank.php"
        target_url = f"{base_url}?year={ScimagoScraper.year}&area={area}&page={self.ctx.get_config().get_value(ConfigConstants.SCIMAGO_STARTING_PAGE)}"
        self.logger.info("Fetching journals from URL: %s", target_url)

        try:
            i = self.driver_manager.load_url_in_available_tab(target_url, 'scimago_journals')
            page_content = self.driver_manager.get_html_of_tab(i)
            self.driver_manager.release_tab(i)
        except Exception as e:
            self.logger.error("Error loading or releasing tab: %s", e)
            return {"journals": [], "end": False}

        try:
            page_soup = BeautifulSoup(page_content, "html.parser")
        except Exception as e:
            self.logger.error("Error parsing page content with BeautifulSoup: %s", e)
            return {"journals": [], "end": False}

        # Extract pagination information
        pagination_text = None
        try:
            pagination_div = page_soup.find("div", class_="pagination")
            if pagination_div:
                pagination_text = pagination_div.get_text(strip=True)
            self.logger.debug("Pagination text: %s", pagination_text)
        except Exception as e:
            self.logger.error("Error extracting pagination information: %s", e)

        is_end = False
        if pagination_text:
            try:
                match = re.match(r"(\d+)\s*-\s*(\d+)\s*of\s*(\d+)", pagination_text)
                if match:
                    start, end, total = map(int, match.groups())
                    is_end = (end == total)
                else:
                    self.logger.warning("Could not parse pagination text: %s", pagination_text)
            except Exception as e:
                self.logger.error("Error parsing pagination text: %s", e)

        # Extract journal data
        journals = []
        try:
            table = page_soup.find("div", class_="table_wrap").find("table")
            if not table:
                self.logger.warning("No table found on the page")
                return {"journals": [], "end": is_end}

            rows = table.find("tbody").find_all("tr")
            self.logger.info("Found %d journal entries on the page", len(rows))

            for row in rows:
                try:
                    cells = row.find_all("td")
                    if len(cells) < 13:
                        self.logger.warning("Skipping row with insufficient columns: %s", row)
                        continue

                    title_cell = cells[1] if len(cells) > 1 else None
                    title = title_cell.get_text(strip=True) if title_cell else "N/A"
                    link_element = title_cell.find("a") if title_cell else None
                    link = link_element["href"] if link_element and link_element.has_attr("href") else "N/A"

                    pub_type = cells[2].get_text(strip=True) if len(cells) > 2 else "N/A"
                    sjr = cells[3].get_text(strip=True) if len(cells) > 3 else "N/A"
                    h_index = cells[4].get_text(strip=True) if len(cells) > 4 else "N/A"
                    total_docs_2008 = cells[5].get_text(strip=True) if len(cells) > 5 else "N/A"
                    total_docs_3years = cells[6].get_text(strip=True) if len(cells) > 6 else "N/A"
                    total_refs_2008 = cells[7].get_text(strip=True) if len(cells) > 7 else "N/A"
                    total_cites_3years = cells[8].get_text(strip=True) if len(cells) > 8 else "N/A"
                    citable_docs_3years = cells[9].get_text(strip=True) if len(cells) > 9 else "N/A"
                    cites_per_doc_2years = cells[10].get_text(strip=True) if len(cells) > 10 else "N/A"
                    refs_per_doc_2008 = cells[11].get_text(strip=True) if len(cells) > 11 else "N/A"
                    female_percent_2008 = cells[12].get_text(strip=True) if len(cells) > 12 else "N/A"

                    # Extract Q rank (e.g., Q1) if present in SJR cell
                    q_rank_element = cells[3].find("span", class_="q1") if len(cells) > 3 else None
                    q_rank = q_rank_element.get_text(strip=True) if q_rank_element else sjr[-2:] if sjr[-2] == 'q' else "N/A"

                    match = re.search(r'year=(\d{4})', url)
                    year = match.group(1) if match else 0
                    if year is 0:
                        continue

                    journal_data = {
                        "title": title,
                        "link": link,
                        "year": year,
                        "type": pub_type,
                        "sjr": sjr,
                        "q_rank": q_rank,
                        "h_index": h_index,
                        "total_docs_2008": total_docs_2008,
                        "total_docs_3years": total_docs_3years,
                        "total_refs_2008": total_refs_2008,
                        "total_cites_3years": total_cites_3years,
                        "citable_docs_3years": citable_docs_3years,
                        "cites_per_doc_2years": cites_per_doc_2years,
                        "refs_per_doc_2008": refs_per_doc_2008,
                        "female_percent_2008": female_percent_2008,
                    }
                    journals.append(journal_data)
                except Exception as e:
                    self.logger.error("Error parsing journal row: %s", e)
        except Exception as e:
            self.logger.error("Error extracting journal data: %s", e)

        if is_end:
            ScimagoScraper.year = ScimagoScraper.year + 1
        return {JsonConstants.TAG_JOURNALS: journals, JsonConstants.TAG_IS_END: is_end, "area": area}
