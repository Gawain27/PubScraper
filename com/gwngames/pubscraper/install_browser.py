import logging
import os
import platform
import stat
import sys
import tarfile

import requests
from bs4 import BeautifulSoup

def download_file(url, destination):
    """Download a file from a URL to a specified destination."""
    if os.path.exists(destination):
        logging.info(f"File already exists at {destination}, skipping download.")
        return
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(destination, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        logging.info(f"Downloaded file to {destination}")
    except Exception as e:
        logging.info(f"Failed to download file: {e}")
        sys.exit(1)


def modify_torrc_file(destination):
    """Modify the torrc file with specific configurations."""
    torrc_path = os.path.join(destination, "tor-browser", "Browser", "TorBrowser", "Data", "Tor", "torrc")
    try:
        with open(torrc_path, 'a') as torrc:
            torrc.write("\n")
            torrc.write("MaxCircuitDirtiness 3600\n")
            torrc.write("NewCircuitPeriod 3600\n")
            torrc.write("NumEntryGuards 1\n")
            torrc.write("StrictNodes 0\n")
        logging.info("torrc file modified successfully.")
    except Exception as e:
        logging.info(f"Failed to modify torrc file: {e}")
        sys.exit(1)


def install_geckodriver(destination):
    """
    Download and install geckodriver (v0.35.0) for Linux 64-bit
    into the same 'destination' folder (tor_download).
    """
    gecko_url = "https://github.com/mozilla/geckodriver/releases/download/v0.35.0/geckodriver-v0.35.0-linux64.tar.gz"
    tar_name = os.path.join(destination, "geckodriver-v0.35.0-linux64.tar.gz")

    download_file(gecko_url, tar_name)

    # Extract geckodriver
    try:
        with tarfile.open(tar_name, "r:gz") as tar:
            tar.extractall(destination)
        logging.info("Geckodriver extracted successfully.")
    except Exception as e:
        logging.info(f"Failed to extract geckodriver: {e}")
        sys.exit(1)

    # Verify the geckodriver binary was extracted
    gecko_binary = os.path.join(destination, "geckodriver")
    if not os.path.exists(gecko_binary):
        logging.info("Extraction failed: geckodriver not found.")
        sys.exit(1)

    # Make geckodriver executable within destination
    try:
        st = os.stat(gecko_binary)
        os.chmod(gecko_binary, st.st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        logging.info(f"Geckodriver installed successfully to {gecko_binary}.")
        logging.info("If you'd like to use it system-wide, consider adding this folder to your PATH.")
    except Exception as e:
        logging.info(f"Failed to make geckodriver executable: {e}")
        sys.exit(1)


def install_tor_browser_on_linux(download_url, destination):
    """Install Tor Browser on Linux and modify torrc."""
    file_name = os.path.join(destination, "tor-browser.tar.xz")
    download_file(download_url, file_name)

    # Extract the tor-browser tar.xz
    try:
        with tarfile.open(file_name, "r:xz") as tar:
            tar.extractall(destination)
        logging.info("Tor Browser extracted successfully.")
    except Exception as e:
        logging.info(f"Failed to extract Tor Browser: {e}")
        sys.exit(1)

    # Verify extraction
    extracted_dir = os.path.join(destination, "tor-browser")
    if os.path.exists(extracted_dir):
        logging.info(f"Tor Browser installed at {extracted_dir}")
        modify_torrc_file(destination)
    else:
        logging.info("Extraction failed: tor-browser directory not found.")
        sys.exit(1)

def get_tor_download_link(os_name):
    # Define the base URL
    base_url = "https://www.torproject.org"
    page_url = f"{base_url}/download/"

    # Send a GET request to the page
    response = requests.get(page_url)
    response.raise_for_status()  # Raise an error for HTTP issues

    # Parse the page content with BeautifulSoup
    soup = BeautifulSoup(response.text, "html.parser")

    # Initialize the dictionary to store links
    os_links = {}

    # Find all anchor tags with download links
    for a_tag in soup.find_all("a", class_="downloadLink"):
        href = a_tag.get("href")
        if href:
            if "windows" in href:
                os_links["Windows"] = base_url + href
            elif "macos" in href:
                os_links["macOS"] = base_url + href
            elif "linux" in href:
                os_links["Linux"] = base_url + href
            elif "android" in href:
                os_links["Android"] = base_url + href

    # Return the link for the specified OS
    return os_links.get(os_name)

def install_browser():
    # Determine the operating system
    system = platform.system()
    download_dir = os.path.join(os.getcwd(), "tor_download")

    # Create download directory if it doesn't exist
    os.makedirs(download_dir, exist_ok=True)
    logging.info(f"Operating system: {system}")

    if system == "Linux":
        logging.info("Detected Linux system.")
        linux_url = get_tor_download_link("Linux")
        install_tor_browser_on_linux(linux_url, download_dir)
        install_geckodriver(download_dir)
    else:
        logging.info("Unsupported operating system.")
        sys.exit(1)



