#!/usr/bin/env python3
"""
================================================================================
Module      : 13Fmonitor.py
Author      : Craig Allum
Date        : 24/02/2026
Version     : v1.1
Description :
    SEC Section 13F Securities List Monitor.

    Monitors the SEC's official Section 13F securities list page for content
    changes.  When a change is detected the latest PDF is downloaded and saved
    to the local downloads directory.

    Change detection is performed by hashing the page content (PDF link hrefs
    plus visible text) and comparing it to the previously stored hash.  If the
    hash differs, a new PDF download is triggered automatically.

    The script is designed to be called periodically via cron or the
    accompanying scheduler (sechduler.py).  It maintains a small JSON tracking
    file so that state is preserved between invocations.

    Note: The SEC requires a descriptive ``User-Agent`` header on all automated
    requests.  Update the ``HEADERS`` constant if deploying under a different
    organisation.

Usage:
    python 13Fmonitor.py           # check for updates and download if changed
    python 13Fmonitor.py status    # display current tracking state

Dependencies:
    requests, beautifulsoup4
================================================================================
"""

import hashlib
import json
import os
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SEC_URL: str = (
    "https://www.sec.gov/rules-regulations/staff-guidance/"
    "division-investment-management-frequently-asked-questions/"
    "official-list-section-13f-securities"
)

DATA_DIR:      Path = Path("sec_13f_data")
TRACKING_FILE: Path = DATA_DIR / "tracking.json"
LOG_FILE:      Path = DATA_DIR / "check_log.txt"
DOWNLOADS_DIR: Path = DATA_DIR / "downloads"

# The SEC requires requests to include an identifying User-Agent header.
HEADERS: dict[str, str] = {
    "User-Agent": "Kaizenreporting/1.0 (csallum@gmail.com)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


# ---------------------------------------------------------------------------
# Directory setup
# ---------------------------------------------------------------------------

def setup_directories() -> None:
    """Create required data and download directories if they do not exist."""
    DATA_DIR.mkdir(exist_ok=True)
    DOWNLOADS_DIR.mkdir(exist_ok=True)


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def log_message(message: str) -> None:
    """Append a timestamped entry to the log file and print to stdout.

    Args:
        message: The message text to log.
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] {message}\n"
    print(log_entry.strip())

    with open(LOG_FILE, "a") as fh:
        fh.write(log_entry)


# ---------------------------------------------------------------------------
# Tracking state
# ---------------------------------------------------------------------------

def load_tracking_data() -> dict:
    """Load the persisted tracking state from the JSON tracking file.

    Returns a default structure with ``None`` values if the tracking file
    does not yet exist (i.e. on first run).

    Returns:
        A dictionary containing ``last_hash``, ``last_check``,
        ``last_download``, and ``check_count`` keys.
    """
    if TRACKING_FILE.exists():
        with open(TRACKING_FILE, "r") as fh:
            return json.load(fh)

    return {
        "last_hash":     None,
        "last_check":    None,
        "last_download": None,
        "check_count":   0,
    }


def save_tracking_data(data: dict) -> None:
    """Persist the tracking state to the JSON tracking file.

    Args:
        data: Dictionary of tracking state to serialise.
    """
    with open(TRACKING_FILE, "w") as fh:
        json.dump(data, fh, indent=2)


# ---------------------------------------------------------------------------
# Page hashing
# ---------------------------------------------------------------------------

def get_page_hash(url: str) -> tuple[str | None, list[str], bytes | None]:
    """Fetch the SEC page and compute a SHA-256 hash of its relevant content.

    The hash is computed over the list of PDF link hrefs and the full visible
    page text, which ensures that both structural link changes and text edits
    are detected.

    Args:
        url: The URL of the SEC 13F securities list page.

    Returns:
        A ``(content_hash, pdf_links, raw_page_content)`` tuple.  All three
        values are ``None`` / empty if the request fails.
    """
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, "html.parser")

        # Collect all PDF or 13F-related links
        pdf_links: list[str] = [
            link["href"]
            for link in soup.find_all("a", href=True)
            if link["href"].endswith(".pdf") or "13f" in link["href"].lower()
        ]

        content_hash = hashlib.sha256(
            (str(pdf_links) + soup.get_text()).encode()
        ).hexdigest()

        return content_hash, pdf_links, response.content

    except Exception as exc:
        log_message(f"ERROR: Failed to fetch page: {exc}")
        return None, [], None


# ---------------------------------------------------------------------------
# PDF discovery and download
# ---------------------------------------------------------------------------

def find_pdf_url(page_content: bytes) -> str | None:
    """Extract the URL of the most prominent 13F PDF from the page content.

    Scans anchor tags for links whose href ends in ``.pdf`` and whose
    visible text or href contains ``13f``.  Relative URLs are converted to
    absolute SEC URLs.

    Args:
        page_content: Raw HTML bytes of the SEC page.

    Returns:
        The absolute URL of the PDF, or ``None`` if no matching link is found.
    """
    soup = BeautifulSoup(page_content, "html.parser")

    for link in soup.find_all("a", href=True):
        href = link["href"]
        text = link.get_text().lower()

        if href.endswith(".pdf") and ("13f" in text or "13f" in href.lower()):
            if href.startswith("/"):
                return f"https://www.sec.gov{href}"
            if not href.startswith("http"):
                return f"https://www.sec.gov/{href}"
            return href

    return None


def download_pdf(url: str, filename: str) -> str | None:
    """Download a PDF from the given URL and save it to the downloads directory.

    Args:
        url:      Absolute URL of the PDF to download.
        filename: Target filename within :data:`DOWNLOADS_DIR`.

    Returns:
        The string path of the saved file on success, or ``None`` on failure.
    """
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()

        filepath = DOWNLOADS_DIR / filename
        with open(filepath, "wb") as fh:
            fh.write(response.content)

        log_message(f"SUCCESS: Downloaded new PDF to {filepath}")
        return str(filepath)

    except Exception as exc:
        log_message(f"ERROR: Failed to download PDF: {exc}")
        return None


# ---------------------------------------------------------------------------
# Main check logic
# ---------------------------------------------------------------------------

def check_for_updates() -> bool:
    """Check the SEC page for changes and download the PDF if updated.

    Compares the current page hash against the stored hash.  If the hashes
    differ the function locates the PDF link on the page and downloads it.
    Tracking state is always persisted at the end of the run.

    Returns:
        ``True`` if a new PDF was successfully downloaded, ``False`` otherwise.
    """
    setup_directories()

    log_message("=" * 60)
    log_message("Starting 13F securities list check")

    tracking = load_tracking_data()
    tracking["check_count"] += 1
    tracking["last_check"]   = datetime.now().isoformat()

    current_hash, pdf_links, page_content = get_page_hash(SEC_URL)

    if current_hash is None:
        log_message("ERROR: Could not retrieve page content")
        save_tracking_data(tracking)
        return False

    # No change detected
    if tracking["last_hash"] == current_hash:
        log_message("No changes detected - page content unchanged")
        log_message(f"Total checks performed: {tracking['check_count']}")
        save_tracking_data(tracking)
        return False

    # Change detected - attempt PDF download
    log_message("CHANGE DETECTED: Page content has been updated!")

    pdf_url = find_pdf_url(page_content)

    if pdf_url:
        log_message(f"Found PDF URL: {pdf_url}")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename  = f"13f_securities_list_{timestamp}.pdf"

        downloaded_path = download_pdf(pdf_url, filename)

        if downloaded_path:
            tracking["last_download"] = datetime.now().isoformat()
            tracking["last_hash"]     = current_hash
            log_message(f"Successfully downloaded and saved: {filename}")
            save_tracking_data(tracking)
            return True
    else:
        log_message("WARNING: Page changed but no PDF link was found")
        # Update hash to avoid generating repeated warnings on subsequent runs
        tracking["last_hash"] = current_hash

    save_tracking_data(tracking)
    return False


# ---------------------------------------------------------------------------
# Status display
# ---------------------------------------------------------------------------

def show_status() -> None:
    """Print the current tracking state to stdout.

    Displays check count, last check timestamp, last download timestamp, and
    a truncated version of the stored page hash.
    """
    if not TRACKING_FILE.exists():
        print("No tracking data found. Run a check first.")
        return

    tracking = load_tracking_data()

    print("\n" + "=" * 60)
    print("SEC 13F Securities List Monitor - Status")
    print("=" * 60)
    print(f"Total checks performed : {tracking['check_count']}")
    print(f"Last check             : {tracking['last_check']}")
    print(f"Last download          : {tracking['last_download']}")

    if tracking["last_hash"]:
        print(f"Current hash           : {tracking['last_hash'][:16]}...")
    else:
        print("Current hash           : None")

    print("=" * 60 + "\n")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "status":
        show_status()
    else:
        check_for_updates()
