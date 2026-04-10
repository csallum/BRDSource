#!/usr/bin/env python3
"""
================================================================================
Module      : BrazilBDRList.py
Author      : Craig Allum
Date        : 24/02/2026
Version     : v1.1
Description :
    ANBIMA / B3 Instruments Consolidated Downloader - Scheduled Version.

    Connects to the B3 public API to retrieve a time-limited download token
    for the InstrumentsConsolidatedFile, downloads the CSV, and then invokes
    the instruments_extractor module to filter and process the data.

    The download job runs automatically at 21:00 (9pm) UK time every day.
    On first launch, an immediate download is also triggered so that the
    deployment can be validated without waiting for the scheduled window.

    This module is intended to run as a long-lived process (e.g. via systemd
    or screen/tmux).  For cron-based execution use BrazilBDRListOnce.py.

Usage:
    python BrazilBDRList.py

Dependencies:
    requests, schedule, pytz, instruments_extractor (local module)
================================================================================
"""

import logging
import time
from datetime import datetime, timedelta
from pathlib import Path

import pytz
import requests
import schedule

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DOWNLOAD_DIR: Path = Path("downloads")
LOG_DIR:      Path = Path("logs")
BASE_URL:     str  = "https://arquivos.b3.com.br/api/download"
UK_TIMEZONE         = pytz.timezone("Europe/London")

# ---------------------------------------------------------------------------
# Directory setup
# ---------------------------------------------------------------------------

DOWNLOAD_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

_log_file = LOG_DIR / f"anbima_download_{datetime.now().strftime('%Y%m')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(_log_file),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Optional extractor import
# ---------------------------------------------------------------------------

try:
    from instruments_extractor import (
        COLUMNS_TO_EXTRACT,
        EXTRACT_ALL_COLUMNS,
        extract_columns,
    )
    EXTRACTOR_AVAILABLE = True
    logger.info("Instruments extractor module loaded successfully")
except ImportError as exc:
    logger.warning(
        "instruments_extractor module not found - extraction will be skipped: %s", exc
    )
    EXTRACTOR_AVAILABLE = False


# ---------------------------------------------------------------------------
# Token & file discovery
# ---------------------------------------------------------------------------

def get_download_token(date_str: str) -> tuple[str | None, str | None]:
    """Request a one-time download token from the B3 API for a given date.

    The B3 API requires a ``fileName`` and ``date`` parameter and returns a
    short-lived token that must be used immediately to download the file.

    Args:
        date_str: ISO-formatted date string (``YYYY-MM-DD``) for which the
            token should be requested.

    Returns:
        A ``(token, filename)`` tuple on success, or ``(None, None)`` if the
        file is not available for that date or the request fails.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
    }
    params = {
        "fileName": "InstrumentsConsolidatedFile",
        "date": date_str,
        "recaptchaToken": "",
    }

    try:
        logger.info("Requesting download token for date: %s", date_str)
        response = requests.get(
            f"{BASE_URL}/requestname",
            headers=headers,
            params=params,
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()

        if "token" in data and "file" in data:
            token    = data["token"]
            filename = data["file"]["name"] + data["file"]["extension"]
            logger.info("Received token for file: %s", filename)
            return token, filename

        logger.warning("No token received for date: %s", date_str)
        return None, None

    except requests.RequestException as exc:
        logger.error("Failed to get download token: %s", exc)
        return None, None
    except Exception as exc:
        logger.error("Unexpected error processing token response: %s", exc)
        return None, None


def get_latest_file_token() -> tuple[str | None, str | None, str | None]:
    """Attempt to obtain a download token, falling back up to five previous days.

    B3 files are not always published on weekends or public holidays, so this
    function walks backwards through the last six calendar days until a valid
    token is returned.

    Returns:
        A ``(token, filename, date_str)`` tuple on success, or
        ``(None, None, None)`` if no file is available within the window.
    """
    try:
        uk_now = datetime.now(UK_TIMEZONE)

        for days_back in range(6):
            target_date = uk_now - timedelta(days=days_back)
            date_str    = target_date.strftime("%Y-%m-%d")
            token, filename = get_download_token(date_str)

            if token:
                logger.info("Found available file for date: %s", date_str)
                return token, filename, date_str

            logger.info("File not available for %s", date_str)

        logger.warning("No file found for the last 6 days")
        return None, None, None

    except Exception as exc:
        logger.error("Error getting latest file token: %s", exc)
        return None, None, None


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def download_file(token: str, filename: str) -> Path | None:
    """Download the instruments file using a previously obtained token.

    A timestamp suffix is appended to the filename to prevent overwriting
    previous downloads.  After download the file is sanity-checked to ensure
    it is not an HTML error page masquerading as the data file.

    Args:
        token:    B3 API download token obtained from :func:`get_download_token`.
        filename: Original filename returned by the token endpoint.

    Returns:
        The :class:`~pathlib.Path` to the saved file on success, or ``None``
        if the download fails or the file fails validation.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

    try:
        download_url = f"{BASE_URL}/?token={token}"
        logger.info("Downloading file: %s", filename)
        logger.info("Download URL: %s", download_url)

        response = requests.get(
            download_url, headers=headers, timeout=120, stream=True
        )
        response.raise_for_status()

        # Append a timestamp to avoid overwriting previous downloads
        timestamp  = datetime.now().strftime("%Y%m%d_%H%M%S")
        name_parts = filename.rsplit(".", 1)
        if len(name_parts) == 2:
            timestamped_name = f"{name_parts[0]}_{timestamp}.{name_parts[1]}"
        else:
            timestamped_name = f"{filename}_{timestamp}"

        filepath = DOWNLOAD_DIR / timestamped_name

        with open(filepath, "wb") as fh:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    fh.write(chunk)

        file_size = filepath.stat().st_size
        logger.info(
            "Successfully downloaded: %s (%s bytes)",
            timestamped_name,
            f"{file_size:,}",
        )

        # Sanity check: reject suspiciously small files and HTML error pages
        if file_size < 1_000:
            logger.warning(
                "File size is suspiciously small (%s bytes) - checking content", file_size
            )
            with open(filepath, "r", encoding="utf-8", errors="ignore") as fh:
                preview = fh.read(500)
            if "<html" in preview.lower() or "<!doctype" in preview.lower():
                logger.error(
                    "Downloaded file appears to be an HTML error page, not the data file"
                )
                return None

        return filepath

    except requests.RequestException as exc:
        logger.error("Failed to download file: %s", exc)
        return None
    except Exception as exc:
        logger.error("Error saving downloaded file: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------

def run_extraction(filepath: Path) -> bool:
    """Invoke the column extractor on a downloaded instruments file.

    Delegates to :func:`instruments_extractor.extract_columns` using the
    column list and flags defined in that module's configuration section.

    Args:
        filepath: Path to the downloaded CSV file to be processed.

    Returns:
        ``True`` if extraction completed successfully, ``False`` otherwise.
    """
    if not EXTRACTOR_AVAILABLE:
        logger.warning("Extractor module not available - skipping extraction")
        return False

    try:
        logger.info("=" * 80)
        logger.info("Starting automatic column extraction")
        logger.info("=" * 80)

        output_file = extract_columns(
            filepath,
            columns_to_extract=COLUMNS_TO_EXTRACT,
            extract_all=EXTRACT_ALL_COLUMNS,
        )

        if output_file:
            logger.info("Extraction completed successfully: %s", output_file)
            return True

        logger.error("Extraction failed - check extractor logs for details")
        return False

    except Exception as exc:
        logger.error("Unexpected error during extraction: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Job orchestration
# ---------------------------------------------------------------------------

def run_download_job() -> None:
    """Orchestrate a full download-and-extract cycle.

    This function is registered with the ``schedule`` library and is also
    called once on startup.  It coordinates token acquisition, file download,
    and post-download extraction, logging the outcome of each stage.
    """
    uk_time = datetime.now(UK_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S %Z")
    logger.info("=" * 80)
    logger.info("Starting B3 Instruments download job at %s", uk_time)
    logger.info("=" * 80)

    try:
        token, filename, date_str = get_latest_file_token()

        if not token:
            logger.error("Failed to obtain a download token")
            logger.info("Job completed with FAILURE")
            return

        downloaded_filepath = download_file(token, filename)

        if downloaded_filepath:
            logger.info("Download completed SUCCESSFULLY")

            if run_extraction(downloaded_filepath):
                logger.info("Full job completed SUCCESSFULLY (download + extraction)")
            else:
                logger.warning("Download succeeded but extraction failed")
        else:
            logger.error("Download failed")
            logger.info("Job completed with FAILURE")

    except Exception as exc:
        logger.error("Unexpected error during job execution: %s", exc)
        logger.info("Job completed with FAILURE")

    logger.info("=" * 80)


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------

def schedule_job() -> None:
    """Initialise the scheduler and begin the run loop.

    Registers :func:`run_download_job` to fire at 21:00 UK time each day,
    then immediately executes an initial run before entering the polling loop.
    The loop checks for pending jobs every 60 seconds.

    Raises:
        KeyboardInterrupt: Caught by the caller to allow a clean shutdown.
    """
    schedule.every().day.at("21:00").do(run_download_job)

    logger.info("Scheduler initialised")
    logger.info("Download job scheduled for 21:00 (9pm) UK time daily")
    logger.info("Press Ctrl+C to stop")

    # Run immediately on startup so the deployment can be validated
    logger.info("Running initial download on startup...")
    run_download_job()

    while True:
        schedule.run_pending()
        time.sleep(60)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    try:
        schedule_job()
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user")
    except Exception as exc:
        logger.error("Fatal error: %s", exc)
        raise
