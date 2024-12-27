import logging
import os
import platform
import tarfile
import requests
import shutil

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
        exit(1)

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
            torrc.write("EntryNodes {at},{be},{ch},{cz},{de},{dk},{es},{fi},{fr},{gb},{hu},{ie},{is},{it},{lu},{nl},{no},{pl},{pt},{ro},{se}\n")
            torrc.write("ExitNodes {at},{be},{ch},{cz},{de},{dk},{es},{fi},{fr},{gb},{hu},{ie},{is},{it},{lu},{nl},{no},{pl},{pt},{ro},{se}\n")
        logging.info("torrc file modified successfully.")
    except Exception as e:
        logging.info(f"Failed to modify torrc file: {e}")
        exit(1)

def install_on_linux(download_url, destination):
    """Install Tor Browser on Linux."""
    file_name = os.path.join(destination, "tor-browser.tar.xz")
    download_file(download_url, file_name)

    # Extract the tar.xz file
    try:
        with tarfile.open(file_name, "r:xz") as tar:
            tar.extractall(destination)
        logging.info("Tor Browser extracted successfully.")
    except Exception as e:
        logging.info(f"Failed to extract Tor Browser: {e}")
        exit(1)

    # Verify extraction
    extracted_dir = os.path.join(destination, "tor-browser")
    if os.path.exists(extracted_dir):
        logging.info(f"Tor Browser installed at {extracted_dir}")
        modify_torrc_file(destination)
    else:
        logging.info("Extraction failed.")
        exit(1)

def install_browser():
    # Determine the operating system
    system = platform.system()
    download_dir = os.path.join(os.getcwd(), "tor_download")

    # Create download directory if it doesn't exist
    os.makedirs(download_dir, exist_ok=True)
    logging.info("Operating system: " + system)
    if system == "Linux":
        logging.info("Detected Linux system.")
        linux_url = "https://www.torproject.org/dist/torbrowser/14.0.3/tor-browser-linux-x86_64-14.0.3.tar.xz"
        install_on_linux(linux_url, download_dir)
    else:
        logging.info("Unsupported operating system.")
        exit(1)



