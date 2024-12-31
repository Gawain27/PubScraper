import logging
import os
import platform
import stat
import sys
import tarfile

import requests

logging.basicConfig(level=logging.INFO)


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
            torrc.write(
                "EntryNodes {at},{be},{ch},{cz},{de},{dk},{es},{fi},{fr},{gb},{hu},{ie},{is},{it},{lu},{nl},{no},{pl},{pt},{ro},{se}\n")
            torrc.write(
                "ExitNodes {at},{be},{ch},{cz},{de},{dk},{es},{fi},{fr},{gb},{hu},{ie},{is},{it},{lu},{nl},{no},{pl},{pt},{ro},{se}\n")
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

    logging.info(f"Downloading geckodriver from {gecko_url}...")
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

    # Cleanup the tar.gz file
    os.remove(tar_name)


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


def install_browser():
    # Determine the operating system
    system = platform.system()
    download_dir = os.path.join(os.getcwd(), "tor_download")

    # Create download directory if it doesn't exist
    os.makedirs(download_dir, exist_ok=True)
    logging.info(f"Operating system: {system}")

    if system == "Linux":
        logging.info("Detected Linux system.")
        linux_url = "https://www.torproject.org/dist/torbrowser/14.0.3/tor-browser-linux-x86_64-14.0.3.tar.xz"
        install_tor_browser_on_linux(linux_url, download_dir)
        install_geckodriver(download_dir)
    else:
        logging.info("Unsupported operating system.")
        sys.exit(1)



