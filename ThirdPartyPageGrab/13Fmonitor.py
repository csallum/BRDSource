"""
By : Craig Allum
Date : 24/02/2026
Version : v1.0
Purpose :

SEC 13F Securities List Monitor
Downloads new PDFs when changes are detected.

"""

import os
import json
import hashlib
import requests
from datetime import datetime
from pathlib import Path
from bs4 import BeautifulSoup
import time

# Configuration Information
SEC_URL = "https://www.sec.gov/rules-regulations/staff-guidance/division-investment-management-frequently-asked-questions/official-list-section-13f-securities"
DATA_DIR = Path("sec_13f_data")
TRACKING_FILE = DATA_DIR / "tracking.json"
LOG_FILE = DATA_DIR / "check_log.txt"
DOWNLOADS_DIR = DATA_DIR / "downloads"

# User-Agent header (SEC requires identification to be able to do the download)
HEADERS = {
    'User-Agent': 'Kaizenreporting/1.0 (csallum@gmail.com)',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}


def setup_directories():
    """
    Creates the necessary directories if they don't exist.
    """
    DATA_DIR.mkdir(exist_ok=True)
    DOWNLOADS_DIR.mkdir(exist_ok=True)


def get_page_hash(url):
    """
    Fetch the webpage and return a hash of its content.
    This in needed to detect if the page has changed.
    """
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        
        # Parse the page to extract content
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Look for PDF links ie 13F lists
        pdf_links = []
        for link in soup.find_all('a', href=True):
            href = link['href']
            if href.endswith('.pdf') or '13f' in href.lower():
                pdf_links.append(href)
        
        # Create a hash based on PDF links and main content on that page
        content_str = str(pdf_links) + soup.get_text()
        content_hash = hashlib.sha256(content_str.encode()).hexdigest()
        
        return content_hash, pdf_links, response.content
    
    except Exception as e:
        log_message(f"ERROR: Failed to fetch page: {str(e)}")
        return None, None, None


def find_pdf_url(page_content):
    """Extract the most recent 13F securities list PDF URL from the page."""
    soup = BeautifulSoup(page_content, 'html.parser')
    
    # Look for PDF links
    for link in soup.find_all('a', href=True):
        href = link['href']
        text = link.get_text().lower()
        
        # Look for PDF links related to 13F securities list
        if href.endswith('.pdf') and ('13f' in text or '13f' in href.lower()):
            # Make absolute URL if needed
            if href.startswith('/'):
                return f"https://www.sec.gov{href}"
            elif not href.startswith('http'):
                return f"https://www.sec.gov/{href}"
            return href
    
    return None


def download_pdf(url, filename):
    """Download the PDF file from the given URL."""
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        
        filepath = DOWNLOADS_DIR / filename
        with open(filepath, 'wb') as f:
            f.write(response.content)
        
        log_message(f"SUCCESS: Downloaded new PDF to {filepath}")
        return str(filepath)
    
    except Exception as e:
        log_message(f"ERROR: Failed to download PDF: {str(e)}")
        return None


def load_tracking_data():
    """Load tracking data from JSON file."""
    if TRACKING_FILE.exists():
        with open(TRACKING_FILE, 'r') as f:
            return json.load(f)
    return {
        'last_hash': None,
        'last_check': None,
        'last_download': None,
        'check_count': 0
    }


def save_tracking_data(data):
    """Save tracking data to JSON file."""
    with open(TRACKING_FILE, 'w') as f:
        json.dump(data, f, indent=2)


def log_message(message):
    """Append a timestamped message to the log file."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] {message}\n"
    
    print(log_entry.strip())
    
    with open(LOG_FILE, 'a') as f:
        f.write(log_entry)


def check_for_updates():
    """
    This is the Main function to check for updates to the 13F securities list.
    Returns True if a new version was downloaded, False otherwise.
    """
    setup_directories()
    
    log_message("=" * 60)
    log_message("Starting 13F securities list check")
    
    # Load tracking data
    tracking = load_tracking_data()
    tracking['check_count'] += 1
    tracking['last_check'] = datetime.now().isoformat()
    
    # Get current page hash
    current_hash, pdf_links, page_content = get_page_hash(SEC_URL)
    
    if current_hash is None:
        log_message("ERROR: Could not retrieve page content")
        save_tracking_data(tracking)
        return False
    
    # Check if content has changed
    if tracking['last_hash'] == current_hash:
        log_message("No changes detected - page content unchanged")
        log_message(f"Total checks performed: {tracking['check_count']}")
        save_tracking_data(tracking)
        return False
    
    # Content has changed
    log_message("CHANGE DETECTED: Page content has been updated!")
    
    # Find and download the PDF
    pdf_url = find_pdf_url(page_content)
    
    if pdf_url:
        log_message(f"Found PDF URL: {pdf_url}")
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"13f_securities_list_{timestamp}.pdf"
        
        downloaded_path = download_pdf(pdf_url, filename)
        
        if downloaded_path:
            tracking['last_download'] = datetime.now().isoformat()
            tracking['last_hash'] = current_hash
            log_message(f"Successfully downloaded and saved: {filename}")
            save_tracking_data(tracking)
            return True
    else:
        log_message("WARNING: Page changed but no PDF link found")
        # Still update hash to avoid repeated warnings
        tracking['last_hash'] = current_hash
    
    save_tracking_data(tracking)
    return False


def show_status():
    """Display current tracking status."""
    if not TRACKING_FILE.exists():
        print("No tracking data found. Run a check first.")
        return
    
    tracking = load_tracking_data()
    
    print("\n" + "=" * 60)
    print("SEC 13F Securities List Monitor - Status")
    print("=" * 60)
    print(f"Total checks performed: {tracking['check_count']}")
    print(f"Last check: {tracking['last_check']}")
    print(f"Last download: {tracking['last_download']}")
    print(f"Current hash: {tracking['last_hash'][:16]}..." if tracking['last_hash'] else "None")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "status":
        show_status()
    else:
        check_for_updates()