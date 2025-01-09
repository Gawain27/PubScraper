import json
import re
import traceback

from bs4 import BeautifulSoup

from com.gwngames.pubscraper.scraper.BanChecker import BanChecker
from com.gwngames.pubscraper.scraper.scraper.GeneralScraper import GeneralScraper


class ScholarScraper(GeneralScraper):
    def __init__(self):
        super().__init__()

    def get_author_profile_data(self, profile_id):
        self.logger.info(f"Starting profile data extraction for: {profile_id}")

        author_base_url = "https://scholar.google.com/citations?hl=it&user=" + profile_id
        i = None
        try:
            i = self.driver_manager.obtain_tab(profile_id)
            self.driver_manager.load_url_from_tab(i, author_base_url)

            page_source = self.driver_manager.obtain_html_from_tab(i)

            soup = BeautifulSoup(page_source, 'html.parser')

            profile_section = soup.find('div', id='gsc_prf_w')
            if not profile_section:
                self.logger.error(f"TAB[{i}] - Profile data not found.")
                self.driver_manager.release_tab(i, profile_id)
                return {}

            name = profile_section.find('div', id='gsc_prf_in').text if profile_section.find('div',
                                                                                             id='gsc_prf_in') else "Name not available"
            self.logger.info(f"TAB[{i}] - Extracted name: {name}")

            author_id = author_base_url.rsplit('=', 1)[1]

            affiliation = profile_section.find('div', class_='gsc_prf_il')
            affiliation_text = affiliation.text if affiliation else "Affiliation not available"
            affiliation_parts = affiliation_text.split('@')

            if len(affiliation_parts) == 2:
                affiliation_role = affiliation_parts[0].strip()
                affiliation_org = affiliation_parts[1].strip()
            elif len(affiliation_text.split(',')) == 2:
                affiliation_role = affiliation_text.split(',')[0].strip()
                affiliation_org = affiliation_text.split(',')[1].strip()
            else:
                affiliation_role = "?"
                affiliation_org = affiliation_text

            org_link = affiliation.find('a')
            org_value = None
            if org_link and 'href' in org_link.attrs:
                org_href = org_link['href']
                match = re.search(r'org=(\d+)', org_href)
                if match:
                    org_value = match.group(1)

            email_section = profile_section.find('div', id='gsc_prf_ivh')
            email = email_section.text if email_section else "Email not available"

            image = profile_section.find('img', id='gsc_prf_pup-img')
            image_url = image['src'] if image else "Image not available"

            homepage_link = email_section.find('a', href=True)
            homepage_url = homepage_link['href'] if homepage_link else "Homepage not available"

            interests_section = profile_section.find('div', id='gsc_prf_int')
            interests = [interest.text for interest in
                         interests_section.find_all('a')] if interests_section else "Interests not available"
            self.logger.info(f"TAB[{i}] - Extracted interests: {interests}")
            self.driver_manager.release_tab(i, profile_id)
            i = None
            self.logger.info(f"TAB[{i}] - Starting publication extraction.")
            publications = self.fetch_publications(author_base_url)
            self.logger.info(f"TAB[{i}] - Successfully extracted {len(publications)} publications.")

            data_cells = soup.find_all('td', class_='gsc_rsb_std')

            h_index = -1
            i10_index = -1
            if data_cells.__len__() >= 5:
                h_index = data_cells[2].text
                i10_index = data_cells[4].text

            coauthors = self.fetch_colleagues_ids(author_id)

            author_data = {
                "author_id": author_id,
                "coauthors": coauthors,
                "name": name,
                "role": affiliation_role,
                "org": affiliation_org,
                "org_value": org_value,
                "profile_url": author_base_url,
                "verified": email,
                "image_url": image_url,
                "homepage_url": homepage_url,
                "interests": interests,
                "publications": publications,
                "h_index": h_index,
                "i10_index": i10_index
            }
            self.logger.info(f"TAB[{i}] - Profile data extraction complete for: {name}")
            return author_data
        except Exception:
            self.logger.error(f"Error extracting profile data: {str(traceback.format_exc())}")
            if i is not None:
                self.driver_manager.release_tab(i, profile_id)
            return None

    def get_scholar_profile(self, author_name):
        self.logger.info(f"Starting search for author: {author_name}")
        i = None

        try:
            formatted_name = author_name.replace(" ", "+")
            search_url = f"https://scholar.google.com/scholar?hl=it&as_sdt=0%252C5&q={formatted_name}"

            self.logger.info(f"Opening search URL: {search_url}")
            i = self.driver_manager.obtain_tab(author_name)
            self.driver_manager.load_url_from_tab(i, search_url)
            page_source = self.driver_manager.obtain_html_from_tab(i, possible_captcha='gs_captcha_ccl')

            checker = BanChecker(self.ctx)
            if checker.has_ban_phrase(page_source, "We're sorry...") or checker.has_ban_phrase(page_source, search_url):
                self.driver_manager.restart_driver()
            soup = BeautifulSoup(page_source, 'html.parser')

            author_divs = soup.find_all('h4', class_='gs_rt2')
            author_div = author_divs[-1]
            if not author_div:
                self.logger.error(f"TAB[{i}] - No author found for the name: {author_name}")
                if i is not None:
                    self.driver_manager.release_tab(i, author_name)
                return {}

            profile_link = author_div.find_next('a')['href']
            full_profile_url = f"TAB[{i}] - https://scholar.google.com{profile_link}"

            self.logger.info(f"Found profile URL: {full_profile_url}")
            self.driver_manager.release_tab(i, author_name)
            i = None

            match = re.search(r'user=([^&]+)', full_profile_url)
            user_id = match.group(1)

            author_data = self.get_author_profile_data(user_id)

            self.logger.info(f"TAB[{i}] - Author profile {author_name} found and data extracted successfully.")
            return json.dumps(author_data, indent=4)

        except Exception as e:
            self.logger.error(f"Error extracting profile for {author_name}: {str(e)}")
            if i is not None:
                self.driver_manager.release_tab(i, author_name)
            return {}

    def fetch_publications(self, profile_url):
        """
        This function takes a Selenium WebDriver instance and the Google Scholar profile URL,
        makes successive requests to retrieve all the author's publications, and returns the data.
        """
        self.logger.info(f"Starting publication extraction from: {profile_url}")

        base_url = profile_url
        publications = []
        cstart = 0
        pagesize = 100
        more_results = True
        total_pages = 0

        i = self.driver_manager.obtain_tab(profile_url)

        while more_results:
            paginated_url = f"{base_url}&cstart={cstart}&pagesize={pagesize}"
            self.logger.info(f"Loading page with start index {cstart} and page size {pagesize}")
            try:
                self.driver_manager.load_url_from_tab(i, paginated_url)
                page_source = self.driver_manager.obtain_html_from_tab(i)

                soup = BeautifulSoup(page_source, 'html.parser')

                # Find the publication table content
                table_body = soup.find('tbody', id='gsc_a_b')
                if not table_body:
                    self.logger.warning(f"TAB[{i}] - No data found on this page. Stopping publication extraction...")
                    break

                rows = table_body.find_all('tr', class_='gsc_a_tr')
                if not rows or len(rows) == 1:
                    self.logger.info(f"TAB[{i}] - No more rows found. Stopping extraction.")
                    break

                # Log the number of publications found on this page
                self.logger.info(f"TAB[{i}] - Found {len(rows)} publications on page with start index {cstart}")

                # For each row, extract publication details
                for row in rows:
                    title_tag = row.find('a', class_='gsc_a_at')
                    title = title_tag.text if title_tag else "N/A"
                    pub_url = f"https://scholar.google.com{title_tag['href']}" if title_tag else "N/A"

                    publication = {
                        "title": title,
                        "url": pub_url,
                        "publication_id": pub_url.rsplit('=', 1)[1]
                    }
                    publications.append(publication)

                cstart += pagesize
                total_pages += 1
            except Exception as e:
                self.logger.error(f"Error during publication extraction on page {total_pages + 1}: {str(e)}")
                if i is not None:
                    self.driver_manager.release_tab(i, profile_url)
                break

        self.driver_manager.release_tab(i, profile_url)

        # Log the total number of pages loaded and total publications extracted
        self.logger.info(
            f"Publication extraction complete. Loaded {total_pages} pages and extracted {len(publications)} publications.")
        return publications

    def fetch_publication_data(self, publication_url):
        """
        Extracts publication data from a Google Scholar publication URL using Selenium and BeautifulSoup.
        Returns the data in JSON format.
        """
        self.logger.info(f"Fetching publication data from: {publication_url}")
        i = None
        try:
            i = self.driver_manager.obtain_tab(publication_url)
            self.driver_manager.load_url_from_tab(i, publication_url)
            page_source = self.driver_manager.obtain_html_from_tab(i)

            if not page_source:
                self.logger.error(f"TAB[{i}] - Failed to retrieve page source for: {publication_url}")
                if i is not None:
                    self.driver_manager.release_tab(i, publication_url)
                return {}

            soup = BeautifulSoup(page_source, 'html.parser')

            publication_id_match = re.search(r'citation_for_view=([^&]+)', publication_url)
            publication_id = publication_id_match.group(1) if publication_id_match else "Publication ID not available"
            self.logger.info(f"TAB[{i}] - Extracted publication ID: {publication_id}")
            # Find the title tag and link (if available)
            title_tag = soup.find('a', class_='gsc_oci_title_link')

            # Extract title and link
            if title_tag:
                title = title_tag.text.strip()
                title_link = title_tag['href'] if title_tag.has_attr('href') else "Title link not available"
            else:
                # If no 'gsc_oci_title_link' anchor tag, try the main title div
                title_div = soup.find('div', id='gsc_oci_title')
                title = title_div.text.strip() if title_div else "Title not available"
                title_link = "Title link not available"

            pdf_link_tag = soup.find('span', class_='gsc_vcd_title_ggt')
            pdf_link = pdf_link_tag.find_parent('a')['href'] if pdf_link_tag else "PDF link not available"

            gsc_oci_fields = soup.find_all('div', class_='gs_scl')

            authors = ""
            publication_date = 0
            conference = "Conference not available"
            pages = "Pages not available"
            publisher = "Publisher not available"
            description = "Description not available"

            for field in gsc_oci_fields:
                key_div = field.find('div', class_='gsc_oci_field')
                value_div = field.find('div', class_='gsc_oci_value')

                if not key_div or not value_div:
                    continue

                key = key_div.get_text(strip=True)

                datum = re.match(r'^\d{4}', value_div.get_text(strip=True))

                if "Aut" in key:
                    authors = [author.strip() for author in value_div.get_text(strip=True).split(',')]
                elif "Pag" in key:
                    pages = value_div.get_text(strip=True)
                elif "Edito" in key:
                    publisher = value_div.get_text(strip=True)
                elif "Descr" in key:
                    descr_div = field.find('div', class_='gsc_oci_value', id='gsc_oci_descr')
                    if descr_div:
                        # Remove <svg> tags (and anything else you’d like to exclude):
                        for svg_tag in descr_div.find_all('svg'):
                            svg_tag.decompose()

                        # Now safely extract all textual content:
                        description = descr_div.get_text(separator=' ', strip=True)
                elif datum is not None and len(value_div.get_text(strip=True)) < 50:
                    publication_date = datum.group(0)

            citations_tag = soup.find('a', href=True, text=lambda x: x and x.startswith("Cit"))
            if citations_tag:
                citations_number = int(re.search(r'\d+', citations_tag.text).group())
                citations_link = citations_tag['href']
                cites_id = self.extract_id_from(citations_link, "cites")
                self.logger.info(f"TAB[{i}] - Extracted total citations: {citations_number}")
            else:
                cites_id = None
                citations_number = 0
                self.logger.warning(f"TAB[{i}] - Total citations not found.")

            citation_graph = []
            graph_bars = soup.find_all('a', class_='gsc_oci_g_a')
            for bar in graph_bars:
                try:
                    year = bar['href'].split("as_ylo=")[1].split("&")[0]
                    citation_count = bar.find('span').text.strip()
                    citation_link = bar['href']
                    citation_graph.append({
                        "year": year,
                        "citations": citation_count,
                        "publication_id": publication_id,
                        "citation_link": citation_link
                    })
                except Exception as e:
                    self.logger.error(f"TAB[{i}] - Error extracting citation graph data: {str(e)}")

            self.logger.info(f"TAB[{i}] - Extracted citation graph data: {citation_graph}")

            gsc_oms_links = soup.find_all('a', class_='gsc_oms_link')
            related_articles_url = (
                gsc_oms_links[0]['href'] if len(gsc_oms_links) > 0 else "No related articles link"
            )
            all_versions_url = (
                gsc_oms_links[1]['href'] if len(gsc_oms_links) > 1 else "No all versions link"
            )

            self.logger.info(f"TAB[{i}] - Extracted related articles URL: {related_articles_url}")
            self.logger.info(f"TAB[{i}] - Extracted all versions URL: {all_versions_url}")

            publication_data = {
                "publication_id": publication_id,
                "publication_url": publication_url,
                "title": title,
                "title_link": title_link,
                "pdf_link": pdf_link,
                "authors": authors,
                "publication_date": publication_date,
                "conference": conference,
                "pages": pages,
                "publisher": publisher,
                "description": description,
                "cites_id": cites_id,
                "total_citations": citations_number,
                "citation_graph": citation_graph,
                "related_articles_url": related_articles_url,
                "all_versions_url": all_versions_url
            }

            self.driver_manager.release_tab(i, publication_url)
            self.logger.info(f"TAB[{i}] - Successfully extracted publication data.")
            return publication_data
        except Exception as e:
            self.logger.error(f"Error fetching publication data: {str(e)}")
            if i is not None:
                self.driver_manager.release_tab(i, publication_url)
            return {}

    def fetch_colleagues_ids(self, user_id):
        """
        This function takes a Selenium WebDriver instance and a Google Scholar user ID,
        modifies the colleagues URL with the user ID, retrieves the colleagues' IDs from the HTML,
        and returns them in JSON format.
        """
        self.logger.info(f"Fetching colleagues for user ID: {user_id}")

        base_url = "https://scholar.google.com/citations?view_op=list_colleagues&hl=it&json=&user={}"
        colleagues_url = base_url.format(user_id)
        i = None
        try:
            i = self.driver_manager.obtain_tab(user_id)
            self.driver_manager.load_url_from_tab(i, colleagues_url)
            page_source = self.driver_manager.obtain_html_from_tab(i)
            soup = BeautifulSoup(page_source, 'html.parser')

            author_names = [h3.get_text() for h3 in soup.find_all('h3', class_='gs_ai_name')]
            self.logger.info(f"TAB[{i}] - Extracted coauthor IDs: {len(author_names)}")
            self.driver_manager.release_tab(i, user_id)
            return author_names

        except Exception as e:
            self.logger.error(f"Error fetching colleagues for user {user_id}: {str(e)}")
            if i is not None:
                self.driver_manager.release_tab(i, user_id)
            return None

    def extract_id_from(self, url, var_name):
        match = re.search(fr'{var_name}=(\d+)', url)
        var_id = match.group(1) if match else None
        self.logger.info(f"Extracted {var_name}: {var_id} from URL: {url}")
        return var_id

    def get_citations_from_page(self, url, cites_id):
        stop = False
        self.logger.info(f"Fetching page citation page: {url}")
        i = self.driver_manager.obtain_tab(cites_id)
        self.driver_manager.load_url_from_tab(i, url)
        page_source = self.driver_manager.obtain_html_from_tab(i, possible_captcha='gs_captcha_ccl')

        checker = BanChecker(self.ctx)
        if checker.has_ban_phrase(page_source, "We're sorry...") or checker.has_ban_phrase(page_source, "That’s an error."):
            self.driver_manager.restart_driver()
        soup = BeautifulSoup(page_source, 'html.parser')

        citation_divs = soup.find_all('div', class_='gs_r')
        citation_divs = citation_divs[2:-1]
        citation_data = []

        if soup.find('span', class_='gs_ico gs_ico_nav_first') is None:
            stop = True

        for citation in citation_divs:
            title_tag = citation.find('h3', class_='gs_rt')
            title = title_tag.get_text(strip=True) if title_tag else 'No Title'
            link_tag = title_tag.find('a') if title_tag else None
            link = link_tag['href'] if link_tag else 'No Link'

            authors_tag = citation.find('div', class_='gs_a')
            author_ids = []
            profile_urls = []
            author_names = []
            if authors_tag:
                author_links = authors_tag.find_all('a')
                for author_link in author_links:
                    profile_url = author_link['href']
                    author_name = author_link.get_text(strip=True)
                    author_id_match = re.search(r'user=([a-zA-Z0-9_-]+)', profile_url)
                    if author_id_match:
                        # If there's an author id, add it to author_ids and profile_urls
                        author_ids.append(author_id_match.group(1))
                        profile_urls.append(f"https://scholar.google.com{profile_url}")
                    else:
                        # Only add the author name if no author_id is present
                        author_names.append(author_name)

            summary_tag = citation.find('div', class_='gs_rs')
            summary = summary_tag.get_text(strip=True) if summary_tag else 'No Summary'

            document_link = citation.find('div', class_='gs_or_ggsm')
            if document_link is not None:
                document_link = document_link.find('a')['href']
            else:
                document_link = "No Document link"

            self.logger.debug(
                f"TAB[{i}] - Extracted citation: Title={title}, Link={link}, Authors={author_ids}, Author Names={author_names}, Profile URLs={profile_urls}")

            citation_data.append({
                'cites_id': cites_id,
                'title': title,
                'link': link,
                'author_ids': author_ids,
                'author_names': author_names,
                'profile_urls': profile_urls,
                'summary': summary,
                'document_link': document_link
            })

        self.logger.info(f"TAB[{i}] - Finished fetching citations from page: {url}")
        self.driver_manager.release_tab(i, cites_id)
        return citation_data, stop

    def scrape_all_citations(self, base_url, pub_id):
        start = 0
        all_citations = []
        cites_id = self.extract_id_from(base_url, "cites")

        self.logger.info(f"Starting to scrape citations for cites_id: {cites_id}")
        while True:
            url = f"{base_url}&start={start}"
            self.logger.info(f"Scraping page with start={start}")
            citations, stop = self.get_citations_from_page(url, cites_id)

            if not citations:
                self.logger.info(f"No more citations found at start={start}. Ending scraping.")
                break

            all_citations.extend(citations)
            self.logger.info(f"Collected {len(citations)} citations from page {start // 10 + 1}")
            start += 10
            if stop:
                break

        if len(all_citations) == 0:
            raise Exception("No citations found for: " + base_url)
        self.logger.info(f"Scraping complete. Total citations collected: {len(all_citations)}")
        return {"citations": all_citations, "cites_id": cites_id, "pub_id": pub_id}

    def get_versions_from_page(self, url, cluster_id):
        stop = False
        self.logger.info(f"Fetching versions page: {url}")
        i = self.driver_manager.obtain_tab(cluster_id)
        self.driver_manager.load_url_from_tab(i, url)
        page_source = self.driver_manager.obtain_html_from_tab(i, possible_captcha='gs_captcha_ccl')

        checker = BanChecker(self.ctx)
        if checker.has_ban_phrase(page_source, "We're sorry...") or checker.has_ban_phrase(page_source, "Error"):
            self.driver_manager.restart_driver()

        soup = BeautifulSoup(page_source, 'html.parser')

        extracted_data = []

        entries = soup.find_all('div', class_='gs_r gs_or gs_scl')

        for entry in entries:
            data_dict = {'id': entry.get('data-cid', '')}

            pdf_link = entry.find('a', href=True)
            if pdf_link:
                data_dict['link'] = pdf_link['href']
            else:
                data_dict['link'] = ''

            source_element = entry.find('span', class_='gs_ct2')
            if source_element:
                data_dict['source'] = source_element.text.strip()
            else:
                data_dict['source'] = ''

            description_element = entry.find('div', class_='gs_rs')
            if description_element:
                data_dict['description'] = description_element.text.strip()
            else:
                data_dict['description'] = ''

            data_dict['cluster_id'] = cluster_id

            extracted_data.append(data_dict)

        self.logger.info(f"TAB[{i}] - Finished fetching versions from page: {url}")
        self.driver_manager.release_tab(i, cluster_id)
        return extracted_data, stop

    def scrape_all_versions(self, base_url: str):
        start = 0
        all_versions = []
        cluster_id = self.extract_id_from(base_url, "cluster")

        if base_url == "No all versions link":
            return {"versions": []}

        self.logger.info(f"Starting to scrape documents for cluster_id: {cluster_id}")
        while True:
            url = f"{base_url}&start={start}"
            versions, stop = self.get_versions_from_page(url, cluster_id)

            if not versions:
                self.logger.info(f"No more versions found at start={start}. Ending scraping.")
                break

            all_versions.extend(versions)
            self.logger.info(f"Collected {len(versions)} versions from page {start // 10 + 1}")
            start += 10
            if stop:
                break

        if len(all_versions) == 0:
            raise Exception("No versions found for: " + base_url)
        self.logger.info(f"Scraping complete. Total versions collected: {len(all_versions)}")
        return {"versions": all_versions}

