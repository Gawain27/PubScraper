import re

from bs4 import BeautifulSoup, Tag

from com.gwngames.pubscraper.scraper.scraper.GeneralScraper import GeneralScraper


class ScimagoScraper(GeneralScraper):

    def get_journals_from_page(self, journal_year, page):
        """
        Retrieves journal data for the given year, area, and page number from SCImago Journal Rank.
        """
        if page is False:
            return {}

        base_url = "https://www.scimagojr.com/journalrank.php"
        target_url = f"{base_url}?year={journal_year}&page={page}"
        self.logger.info("Fetching journals from URL: %s", target_url)

        try:
            i = self.driver_manager.obtain_tab(journal_year+"-"+page)
            self.driver_manager.load_url_from_tab(i, target_url)
            page_content = self.driver_manager.obtain_html_from_tab(i)
        except Exception as e:
            self.logger.error("Error loading or releasing tab: %s", e)
            return {"journals": [], "is_end": False}

        try:
            page_soup = BeautifulSoup(page_content, "html.parser")
        except Exception as e:
            self.logger.error("Error parsing page content with BeautifulSoup: %s", e)
            return {"journals": [], "is_end": False}

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
                raise Exception("No table found on page: " + str(page) + " for year: " + str(journal_year))

            rows = table.find("tbody").find_all("tr")
            self.logger.info("Found %d journal entries on the page", len(rows))

            thead = table.find("thead")
            # Check if thead exists and extract titles or headers
            if thead:
                titles = [th['title'] for th in thead.find_all('th', title=True)]
                combined_titles = ' '.join(titles)

                match = re.search(r'\b(\d{4})\b', combined_titles)  # Matches years like 2020, 2021, etc.
                year = int(match.group(1)) if match else None
                if year is None:
                    raise Exception("Cannot save journal without date")
            else:
                raise Exception("No table header found on the page")

            for row in rows:
                try:
                    cells = row.find_all("td")
                    if not cells or len(cells) < 13:
                        self.logger.warning("Skipping row with insufficient columns: %s", row)
                        continue

                    title_cell = cells[1] if len(cells) > 1 else None
                    title = title_cell.get_text(strip=True) if title_cell and isinstance(title_cell, Tag) else "N/A"
                    link_element = title_cell.find("a") if title_cell and isinstance(title_cell, Tag) else None
                    link = link_element["href"] if link_element and link_element.has_attr("href") else "N/A"

                    pub_type = cells[2].get_text(strip=True) if len(cells) > 2 else "N/A"
                    sjr = cells[3].get_text(strip=True) if len(cells) > 3 else "N/A"
                    sjr = sjr[:5]

                    # Extract Q rank (e.g., Q1) if present in SJR cell
                    if len(cells) > 3:
                        q_rank_element = cells[3].find("span", class_="q1") if cells[3].find("span", class_="q1") \
                            else cells[3].find("span", class_="q2") if cells[3].find("span", class_="q2") \
                            else cells[3].find("span", class_="q3") if cells[3].find("span", class_="q3") \
                            else cells[3].find("span", class_="q4") if cells[3].find("span", class_="q4") else None
                    else:
                        q_rank_element = None

                    q_rank = q_rank_element.get_text(strip=True) if q_rank_element and isinstance(q_rank_element,
                                                                                                  Tag) else "N/A"

                    h_index = cells[4].get_text(strip=True) if len(cells) > 4 else "N/A"
                    total_docs_2008 = cells[5].get_text(strip=True) if len(cells) > 5 else "N/A"
                    total_docs_3years = cells[6].get_text(strip=True) if len(cells) > 6 else "N/A"
                    total_refs_2008 = cells[7].get_text(strip=True) if len(cells) > 7 else "N/A"
                    total_cites_3years = cells[8].get_text(strip=True) if len(cells) > 8 else "N/A"
                    citable_docs_3years = cells[9].get_text(strip=True) if len(cells) > 9 else "N/A"
                    cites_per_doc_2years = cells[10].get_text(strip=True) if len(cells) > 10 else "N/A"
                    refs_per_doc_2008 = cells[11].get_text(strip=True) if len(cells) > 11 else "N/A"
                    female_percent_2008 = cells[12].get_text(strip=True) if len(cells) > 12 else "N/A"

                    journal_data = {
                        "title": title,
                        "link": link,
                        "type": pub_type,
                        "sjr": sjr,
                        "q_rank": q_rank,
                        "h_index": h_index,
                        "total_docs": total_docs_2008,
                        "total_docs_3years": total_docs_3years,
                        "total_refs": total_refs_2008,
                        "total_cites_3years": total_cites_3years,
                        "citable_docs_3years": citable_docs_3years,
                        "cites_per_doc_2years": cites_per_doc_2years,
                        "refs_per_doc": refs_per_doc_2008,
                        "female_percent": female_percent_2008,
                        "year": year
                    }
                    journals.append(journal_data)
                except Exception as e:
                    self.logger.error("Error parsing journal row: %s. Row content: %s", e, row)
        except Exception as e:
            self.logger.error("Error extracting journal data: %s", e)

        self.driver_manager.release_tab(i, journal_year+"-"+page)
        return {"journals": journals, "is_end": is_end}

