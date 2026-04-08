#!/usr/bin/env python3
"""
By : Craig Allum
Date : 24/02/2026
Version : v1.0
Purpose :

ANBIMA Cadastro de Instrumentos Downloader (Scheduled version)
Downloads the most recent Cadastro de Instrumentos (Listado) daily at 9pm UK time
Automatically extracts columns after successful download
"""

import os
import logging
import requests
from datetime import datetime, timedelta
from pathlib import Path
import schedule
import time
import pytz

# Configuration Information
DOWNLOAD_DIR = Path("downloads")
LOG_DIR = Path("logs")
BASE_URL = "https://arquivos.b3.com.br/api/download"
UK_TIMEZONE = pytz.timezone('Europe/London')

# Setup directories
DOWNLOAD_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)

# Setup logging
log_file = LOG_DIR / f"anbima_download_{datetime.now().strftime('%Y%m')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Try to import the extractor module
try:
    from instruments_extractor import extract_columns, COLUMNS_TO_EXTRACT, EXTRACT_ALL_COLUMNS
    EXTRACTOR_AVAILABLE = True
    logger.info("Instruments extractor module loaded successfully")
except ImportError as e:
    logger.warning(f"instruments_extractor module not found - extraction will be skipped: {e}")
    EXTRACTOR_AVAILABLE = False


def get_download_token(date_str):
    """
    Requests a download token from B3 API for a specific date
    Returns (token, filename) tuple or (None, None) if not available
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
        }
        
        # Request token from B3 API
        token_url = f"{BASE_URL}/requestname"
        params = {
            'fileName': 'InstrumentsConsolidatedFile',
            'date': date_str,
            'recaptchaToken': ''  
        }
        
        logger.info(f"Requesting download token for date: {date_str}")
        response = requests.get(token_url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        if 'token' in data and 'file' in data:
            token = data['token']
            filename = data['file']['name'] + data['file']['extension']
            logger.info(f"Received token for file: {filename}")
            return token, filename
        else:
            logger.warning(f"No token received for date: {date_str}")
            return None, None
            
    except requests.RequestException as e:
        logger.error(f"Failed to get download token: {e}")
        return None, None
    except Exception as e:
        logger.error(f"Error processing token response: {e}")
        return None, None


def get_latest_file_token():
    """
    Tries to get a download token for today's file, falling back to previous days
    Returns (token, filename, date_str) tuple or (None, None, None) if not found
    """
    try:
        # Use UK timezone for consistency with schedule
        uk_now = datetime.now(UK_TIMEZONE)
        
        # Try today and up to 5 previous days
        for days_back in range(6):
            target_date = uk_now - timedelta(days=days_back)
            date_str = target_date.strftime('%Y-%m-%d')
            
            token, filename = get_download_token(date_str)
            
            if token:
                logger.info(f"Found available file for date: {date_str}")
                return token, filename, date_str
            else:
                logger.info(f"File not available for {date_str}")
        
        logger.warning("No file found for the last 6 days")
        return None, None, None
        
    except Exception as e:
        logger.error(f"Error getting latest file token: {e}")
        return None, None, None


def download_file(token, filename):
    """
    Downloads the file using the provided token
    Returns the filepath if successful, None otherwise
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        # Construct download URL with token
        download_url = f"{BASE_URL}/?token={token}"
        
        logger.info(f"Downloading file: {filename}")
        logger.info(f"Download URL: {download_url}")
        response = requests.get(download_url, headers=headers, timeout=120, stream=True)
        response.raise_for_status()
        
        # Add timestamp to filename to avoid overwriting
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        name_parts = filename.rsplit('.', 1)
        if len(name_parts) == 2:
            filename_with_timestamp = f"{name_parts[0]}_{timestamp}.{name_parts[1]}"
        else:
            filename_with_timestamp = f"{filename}_{timestamp}"
        
        filepath = DOWNLOAD_DIR / filename_with_timestamp
        
        # Download file
        total_size = 0
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    total_size += len(chunk)
        
        file_size = filepath.stat().st_size
        logger.info(f"Successfully downloaded: {filename_with_timestamp} ({file_size:,} bytes)")
        
        # Verify it's actually a CSV/data file, not HTML error page
        if file_size < 1000:
            logger.warning(f"File size is suspiciously small ({file_size} bytes), checking content...")
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read(500)
                if '<html' in content.lower() or '<!doctype' in content.lower():
                    logger.error("Downloaded file appears to be an HTML page, not the data file")
                    return None
        
        return filepath
        
    except requests.RequestException as e:
        logger.error(f"Failed to download file: {e}")
        return None
    except Exception as e:
        logger.error(f"Error saving file: {e}")
        return None


def run_extraction(filepath):
    """
    Runs the column extraction on the downloaded file
    Returns True if successful, False otherwise
    """
    if not EXTRACTOR_AVAILABLE:
        logger.warning("Extractor not available - skipping extraction")
        return False
    
    try:
        logger.info("=" * 80)
        logger.info("Starting automatic column extraction")
        logger.info("=" * 80)
        
        # Call the extractor with the specific file
        output_file = extract_columns(
            filepath,
            columns_to_extract=COLUMNS_TO_EXTRACT,
            extract_all=EXTRACT_ALL_COLUMNS
        )
        
        if output_file:
            logger.info(f"Extraction completed successfully: {output_file}")
            return True
        else:
            logger.error("Extraction failed - check extractor logs")
            return False
            
    except Exception as e:
        logger.error(f"Error during extraction: {e}")
        return False


def run_download_job():
    """
    Main job function that orchestrates the download and extraction process
    """
    uk_time = datetime.now(UK_TIMEZONE).strftime('%Y-%m-%d %H:%M:%S %Z')
    logger.info("=" * 80)
    logger.info(f"Starting B3 Instruments download job at {uk_time}")
    logger.info("=" * 80)
    
    try:
        # Get the latest file token
        token, filename, date_str = get_latest_file_token()
        
        if not token:
            logger.error("Failed to get download token")
            logger.info("Job completed with FAILURE")
            return
        
        # Download the file using the token
        downloaded_filepath = download_file(token, filename)
        
        if downloaded_filepath:
            logger.info("Download completed SUCCESSFULLY")
            
            # Run extraction on the downloaded file
            extraction_success = run_extraction(downloaded_filepath)
            
            if extraction_success:
                logger.info("Full job completed SUCCESSFULLY (download + extraction)")
            else:
                logger.warning("Download successful but extraction failed")
        else:
            logger.error("Download failed")
            logger.info("Job completed with FAILURE")
            
    except Exception as e:
        logger.error(f"Unexpected error during job execution: {e}")
        logger.info("Job completed with FAILURE")
    
    logger.info("=" * 80)


def schedule_job():
    """
    Schedules the download job for 9pm UK time daily
    """
    # Schedule for 21:00 (9pm) UK time
    schedule.every().day.at("21:00").do(run_download_job)
    
    logger.info("Scheduler initialized")
    logger.info("Download job scheduled for 21:00 (9pm) UK time daily")
    logger.info("Press Ctrl+C to stop")
    
    # Run immediately on startup
    logger.info("Running initial download ...")
    run_download_job()
    
    # Keep the scheduler running
    while True:
        # Get current UK time for display
        uk_now = datetime.now(UK_TIMEZONE)
        
        # Run pending scheduled jobs
        schedule.run_pending()
        
        # Sleep for 1 minute
        time.sleep(60)


if __name__ == "__main__":
    try:
        schedule_job()
    except KeyboardInterrupt:
        logger.info("\nScheduler stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")