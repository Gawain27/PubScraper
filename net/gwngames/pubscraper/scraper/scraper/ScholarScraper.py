import logging
import re
import time
import json
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# Set up the logger
logger = logging.getLogger("ScholarScraper")


def get_author_profile_data(profile_id):
    logger.info(f"Starting profile data extraction for: {profile_id}")

    author_base_url = "https://scholar.google.com/citations?hl=it&user="+profile_id
    driver = create_driver()
    try:
        driver.get(author_base_url)
        time.sleep(5)

        logger.info("Page loaded, parsing HTML.")
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')

        profile_section = soup.find('div', id='gsc_prf_w')
        if not profile_section:
            logger.error("Profile data not found.")
            raise Exception("Profile data not found on the page.")

        name = profile_section.find('div', id='gsc_prf_in').text if profile_section.find('div',
                                                                                         id='gsc_prf_in') else "Name not available"
        logger.info(f"Extracted name: {name}")

        author_id = author_base_url.rsplit('=', 1)[1]

        affiliation = profile_section.find('div', class_='gsc_prf_il')
        affiliation_text = affiliation.text if affiliation else "Affiliation not available"
        affiliation_parts = affiliation_text.split('@')

        if len(affiliation_parts) == 2:
            affiliation_role = affiliation_parts[0].strip()
            affiliation_org = affiliation_parts[1].strip()
        else:
            affiliation_role = "?"
            affiliation_org = affiliation_text

        logger.info(f"Extracted affiliation: {affiliation_role} at {affiliation_org}")

        org_link = affiliation.find('a')
        org_value = None
        if org_link and 'href' in org_link.attrs:
            org_href = org_link['href']
            match = re.search(r'org=([\d]+)', org_href)
            if match:
                org_value = match.group(1)
        logger.info(f"Extracted org value: {org_value}")

        email_section = profile_section.find('div', id='gsc_prf_ivh')
        email = email_section.text if email_section else "Email not available"
        logger.info(f"Extracted email: {email}")

        image = profile_section.find('img', id='gsc_prf_pup-img')
        image_url = image['src'] if image else "Image not available"
        logger.info(f"Extracted image URL: {image_url}")

        homepage_link = email_section.find('a', href=True)
        homepage_url = homepage_link['href'] if homepage_link else "Homepage not available"
        logger.info(f"Extracted homepage URL: {homepage_url}")

        interests_section = profile_section.find('div', id='gsc_prf_int')
        interests = [interest.text for interest in
                     interests_section.find_all('a')] if interests_section else "Interests not available"
        logger.info(f"Extracted interests: {interests}")

        logger.info("Starting publication extraction.")
        publications = fetch_publications(driver, author_base_url)
        logger.info(f"Successfully extracted {len(publications)} publications.")

        coauthors = fetch_colleagues_ids(driver, author_id)

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
            "publications": publications
        }

        logger.info(f"Profile data extraction complete for: {name}")
        return author_data

    except Exception as e:
        logger.error(f"Error extracting profile data: {str(e)}")
        raise Exception(f"Error extracting profile data: {str(e)}")


def create_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=chrome_options)
    return driver

def get_scholar_profile(author_name):
    logger.info(f"Starting search for author: {author_name}")

    driver = create_driver()

    try:
        formatted_name = author_name.replace(" ", "+")
        search_url = f"https://scholar.google.com/citations?view_op=search_authors&mauthors={formatted_name}&hl=en&oi=ao"

        logger.info(f"Opening search URL: {search_url}")
        driver.get(search_url)
        time.sleep(5)

        logger.info("Page loaded, parsing search results.")
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')

        author_div = soup.find('div', class_='gsc_1usr')
        if not author_div:
            logger.error(f"No author found for the name: {author_name}")
            raise Exception("No author found in search results.")

        profile_link = author_div.find('a')['href']
        full_profile_url = f"https://scholar.google.com{profile_link}"

        logger.info(f"Found profile URL: {full_profile_url}")
        author_data = get_author_profile_data(full_profile_url.rsplit('=', 1)[1])

        logger.info(f"Author profile {author_name} found and data extracted successfully.")
        return json.dumps(author_data, indent=4)

    except Exception as e:
        logger.error(f"Error extracting profile for {author_name}: {str(e)}")
        return json.dumps({"error": str(e)}, indent=4)

    finally:
        driver.quit()



def fetch_publications(driver, profile_url):
    """
    This function takes a Selenium WebDriver instance and the Google Scholar profile URL,
    makes successive requests to retrieve all the author's publications, and returns the data.
    """
    logger.info(f"Starting publication extraction from: {profile_url}")

    base_url = profile_url
    publications = []
    cstart = 0
    pagesize = 100
    more_results = True
    total_pages = 0

    while more_results:
        paginated_url = f"{base_url}&cstart={cstart}&pagesize={pagesize}"
        logger.info(f"Loading page with start index {cstart} and page size {pagesize}")

        try:
            # Load the paginated URL with Selenium
            driver.get(paginated_url)

            # Add a delay to ensure the page loads correctly
            time.sleep(2)

            # Get the page source and parse the HTML
            page_source = driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')

            # Find the publication table body
            table_body = soup.find('tbody', id='gsc_a_b')
            if not table_body:
                logger.warning("No data found on this page. Stopping publication extraction...")
                break

            # Extract all rows of publications
            rows = table_body.find_all('tr', class_='gsc_a_tr')
            if not rows or len(rows) == 1:
                logger.info("No more rows found. Stopping extraction.")
                break

            # Log the number of publications found on this page
            logger.info(f"Found {len(rows)} publications on page with start index {cstart}")

            # For each row, extract publication details
            for row in rows:
                title_tag = row.find('a', class_='gsc_a_at')
                title = title_tag.text if title_tag else "N/A"
                pub_url = f"https://scholar.google.com{title_tag['href']}" if title_tag else "N/A"

                publication = {
                    "title": title,
                    "url": pub_url
                }

                logger.debug(f"Extracted publication header: {publication}")

                publications.append(publication)

            # Increment the pagination index for the next page
            cstart += pagesize
            total_pages += 1
            time.sleep(2)

        except Exception as e:
            logger.error(f"Error during publication extraction on page {total_pages + 1}: {str(e)}")
            break

    # Log the total number of pages loaded and total publications extracted
    logger.info(
        f"Publication extraction complete. Loaded {total_pages} pages and extracted {len(publications)} publications.")
    return publications


def fetch_publication_data(publication_url):
    """
    This function takes a Selenium WebDriver instance and a Google Scholar publication URL,
    extracts all relevant publication data, and returns the data in JSON format.
    """
    logger.info(f"Fetching publication data from: {publication_url}")

    driver = create_driver()

    try:
        driver.get(publication_url)
        logger.info(f"Opened publication URL: {publication_url}")
        time.sleep(3)  # Allow the page to load completely

        # Parse the page with BeautifulSoup
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')

        # Extract publication ID from the URL
        publication_id_match = re.search(r'citation_for_view=([^&]+)', publication_url)
        publication_id = publication_id_match.group(1) if publication_id_match else "Publication ID not available"
        logger.info(f"Extracted publication ID: {publication_id}")

        # Extract publication title and title link
        title_tag = soup.find('a', class_='gsc_oci_title_link')
        title = title_tag.text.strip() if title_tag else "Title not available"
        title_link = title_tag['href'] if title_tag and title_tag.has_attr('href') else "Title link not available"
        logger.info(f"Extracted title: {title}")
        logger.info(f"Extracted title link: {title_link}")

        # Extract PDF link (if available)
        pdf_link_tag = soup.find('a', class_='gsc_vcd_title_ggt')
        pdf_link = pdf_link_tag.find_parent('a')['href'] if pdf_link_tag else "PDF link not available"
        logger.info(f"Extracted PDF link: {pdf_link}")

        # Extract authors, publication date, conference, pages, publisher, description
        # All these values share the same structure with the class 'gsc_oci_value'
        gsc_oci_values = soup.find_all('div', class_='gsc_oci_value')

        # Extract authors (first occurrence)
        authors = [author.strip() for author in gsc_oci_values[0].text.split(',')] if gsc_oci_values else ["Authors not available"]
        logger.info(f"Extracted authors: {authors}")

        # Extract publication date (second occurrence)
        publication_date = gsc_oci_values[1].text.strip() if len(gsc_oci_values) > 1 else "Date not available"
        logger.info(f"Extracted publication date: {publication_date}")

        # Extract conference (third occurrence)
        conference = gsc_oci_values[2].text.strip() if len(gsc_oci_values) > 2 else "Conference not available"
        logger.info(f"Extracted conference: {conference}")

        # Extract pages (fourth occurrence)
        pages = gsc_oci_values[3].text.strip() if len(gsc_oci_values) > 3 else "Pages not available"
        logger.info(f"Extracted pages: {pages}")

        # Extract publisher (fifth occurrence)
        publisher = gsc_oci_values[4].text.strip() if len(gsc_oci_values) > 4 else "Publisher not available"
        logger.info(f"Extracted publisher: {publisher}")

        # Extract description (sixth occurrence)
        description = gsc_oci_values[5].text.strip() if len(gsc_oci_values) > 5 else "Description not available"
        logger.info(f"Extracted description: {description}")

        # Extract total citations, only the number
        citations_tag = soup.find('a', href=True, text=lambda x: x and x.startswith("Cit"))
        if citations_tag:
            citations_number = int(re.search(r'\d+', citations_tag.text).group())
            citations_link = citations_tag['href']
            cites_id = extract_cites_id(citations_link)
            logger.info(f"Extracted total citations: {citations_number}")
        else:
            cites_id = None
            citations_number = 0
            logger.warning("Total citations not found.")

        # Extract citation graph data, including the publication ID and year-specific citation links
        citation_graph = []
        graph_bars = soup.find_all('a', class_='gsc_oci_g_a')
        for bar in graph_bars:
            year = bar['href'].split("as_ylo=")[1].split("&")[0]
            citation_count = bar.find('span').text.strip()
            citation_link = f"https://scholar.google.com{bar['href']}"
            citation_graph.append({
                "year": year,
                "citations": citation_count,
                "publication_id": publication_id,
                "citation_link": citation_link
            })
        logger.info(f"Extracted citation graph data: {citation_graph}")

        # Extract related articles and versions using the 'gsc_oms_link' class
        gsc_oms_links = soup.find_all('a', class_='gsc_oms_link')

        # Assume the first link is related articles and the second is all versions
        related_articles_url = f"https://scholar.google.com{gsc_oms_links[0]['href']}" if len(
            gsc_oms_links) > 0 else "No related articles link"
        all_versions_url = f"https://scholar.google.com{gsc_oms_links[1]['href']}" if len(
            gsc_oms_links) > 1 else "No all versions link"

        logger.info(f"Extracted related articles URL: {related_articles_url}")
        logger.info(f"Extracted all versions URL: {all_versions_url}")

        # Construct the publication data dictionary
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

        logger.info("Successfully extracted publication data.")
        return json.dumps(publication_data, indent=4)
    except Exception as e:
        logger.error(f"Error fetching publication data: {str(e)}")
        raise e


def fetch_colleagues_ids(driver, user_id):
    """
    This function takes a Selenium WebDriver instance and a Google Scholar user ID,
    modifies the colleagues URL with the user ID, retrieves the colleagues' IDs from the HTML,
    and returns them in JSON format.
    """
    logger.info(f"Fetching colleagues for user ID: {user_id}")

    # Replace the "user" parameter in the base URL with the given user_id
    base_url = "https://scholar.google.com/citations?view_op=list_colleagues&hl=it&json=&user={}"
    colleagues_url = base_url.format(user_id)

    try:
        # Use Selenium to fetch the HTML content
        driver.get(colleagues_url)
        logger.info(f"Opened URL: {colleagues_url}")

        # Allow the page to load completely
        time.sleep(3)

        # Parse the page with BeautifulSoup
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')

        author_names = [h3.get_text() for h3 in soup.find_all('h3', class_='gs_ai_name')]
        logger.info(f"Extracted coauthor IDs: {len(author_names)}")

        # Convert the list of author IDs to JSON format
        return author_names

    except Exception as e:
        logger.error(f"Error fetching colleagues for user {user_id}: {str(e)}")
        return json.dumps({"error": str(e)}, indent=4)


def extract_cites_id(url):
    match = re.search(r'cites=(\d+)', url)
    cites_id = match.group(1) if match else None
    logger.info(f"Extracted cites_id: {cites_id} from URL: {url}")
    return cites_id


def get_citations_from_page(url, cites_id):
    logger.info(f"Fetching page: {url}")
    driver = webdriver.Chrome()
    driver.get(url)
    time.sleep(3)

    page_source = driver.page_source
    soup = BeautifulSoup(page_source, 'html.parser')

    citation_divs = soup.find_all('div', class_='gs_r')
    citation_divs = citation_divs[2:-1]
    citation_data = []

    if citation_divs:
        logger.info(f"Found {len(citation_divs)} citations on the page.")
    else:
        logger.info(f"No citations found on the page.")

    for citation in citation_divs:
        title_tag = citation.find('h3', class_='gs_rt')
        title = title_tag.get_text(strip=True) if title_tag else 'No Title'
        link_tag = title_tag.find('a') if title_tag else None
        link = link_tag['href'] if link_tag else 'No Link'

        # Extract authors' profile URLs and author IDs
        authors_tag = citation.find('div', class_='gs_a')
        author_ids = []
        profile_urls = []
        if authors_tag:
            author_links = authors_tag.find_all('a')
            for author_link in author_links:
                profile_url = author_link['href']
                author_id_match = re.search(r'user=([a-zA-Z0-9_-]+)', profile_url)
                if author_id_match:
                    author_ids.append(author_id_match.group(1))
                    profile_urls.append(f"https://scholar.google.com{profile_url}")

        summary_tag = citation.find('div', class_='gs_rs')
        summary = summary_tag.get_text(strip=True) if summary_tag else 'No Summary'

        logger.debug(
            f"Extracted citation: Title={title}, Link={link}, Authors={author_ids}, Profile URLs={profile_urls}")

        citation_data.append({
            'cites_id': cites_id,
            'title': title,
            'link': link,
            'author_ids': author_ids,
            'profile_urls': profile_urls,
            'summary': summary
        })

    driver.quit()
    logger.info(f"Finished fetching citations from page: {url}")
    return citation_data


def scrape_all_citations(base_url):
    start = 0
    all_citations = []
    cites_id = extract_cites_id(base_url)

    logger.info(f"Starting to scrape citations for cites_id: {cites_id}")

    while True:
        url = f"{base_url}&start={start}"
        logger.info(f"Scraping page with start={start}")
        citations = get_citations_from_page(url, cites_id)

        if not citations:
            logger.info(f"No more citations found at start={start}. Ending scraping.")
            break

        all_citations.extend(citations)
        logger.info(f"Collected {len(citations)} citations from page {start // 10 + 1}")
        start += 10

    logger.info(f"Scraping complete. Total citations collected: {len(all_citations)}")
    return all_citations


