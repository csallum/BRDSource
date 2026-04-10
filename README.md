# B3 / SEC Data Pipeline

A Python automation suite that downloads and processes Brazilian BDR instrument data from the B3 exchange on a daily schedule, and monitors the SEC Section 13F securities list for updates. A log monitoring module scans all job logs for errors and dispatches email alerts when issues are detected.

Built and maintained by [Kaizen Reporting](https://kaizenreporting.com).

---

## Contents

- [Overview](#overview)
- [Modules](#modules)
- [Project Structure](#project-structure)
- [Requirements](#requirements)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Scheduling](#scheduling)
- [Log Monitor](#log-monitor)
- [Instruments Extractor](#instruments-extractor)

---

## Overview

| Job | Schedule | What it does |
|-----|----------|--------------|
| B3 download | Daily 21:00 UK | Downloads the latest InstrumentsConsolidated CSV from the B3 API, filters it to BDR instruments, and writes a clean output CSV |
| Log monitor | Daily 21:30 UK | Scans log files for errors and failures; sends an HTML alert email if any are found |
| 13F monitor | 28th of each month | Checks the SEC 13F securities list page for content changes; downloads the updated PDF when detected |

---

## Modules

```
BrazilBDRListOnce.py      One-shot downloader — designed for cron
BrazilBDRList.py          Self-scheduling long-running downloader
instruments_extractor.py  Column extractor and BDR filter library
13Fmonitor.py             SEC 13F change detector and PDF downloader
sechduler.py              Long-running scheduler for 13Fmonitor
log_monitor.py            Log scanner and SMTP email alerter
```

---

## Project Structure

```
.
├── BrazilBDRListOnce.py        # One-shot B3 downloader (cron entry point)
├── BrazilBDRList.py            # Self-scheduling B3 downloader
├── instruments_extractor.py    # BDR filter and column extractor
├── 13Fmonitor.py               # SEC 13F monitor
├── sechduler.py                # Scheduler wrapper for 13Fmonitor
├── log_monitor.py              # Log scanner and email alerter
├── requirement_brazil.txt      # Python dependencies
├── cronScheduleExample.txt     # Reference cron entries
│
├── downloads/                  # Created at runtime — raw B3 CSV files
├── processed/                  # Created at runtime — filtered output CSVs
├── logs/                       # Created at runtime — job log files
└── sec_13f_data/               # Created at runtime — 13F tracking data
    └── downloads/              # Downloaded 13F PDFs
```

All runtime directories are created automatically on first run.

---

## Requirements

- Python **3.10** or later
- The packages listed in `requirement_brazil.txt`

### Why Python 3.10?

The scripts use union type hints (`str | None`) introduced in Python 3.10. Earlier versions will raise a `TypeError` at import time.

---

## Installation

**1. Clone the repository**

```bash
git clone https://github.com/your-org/b3-sec-pipeline.git
cd b3-sec-pipeline
```

**2. Create and activate a virtual environment (recommended)**

```bash
python -m venv .venv
source .venv/bin/activate        # Linux / macOS
.venv\Scripts\activate           # Windows
```

**3. Install dependencies**

```bash
pip install -r requirement_brazil.txt
```

---

## Configuration

Each script contains a clearly marked `CONFIGURATION` block near the top. The most important settings are in `log_monitor.py`:

```python
# ── log_monitor.py ──────────────────────────────────────────────────────────

# Comma-separated recipient list
ALERT_RECIPIENTS = "you@example.com, colleague@example.com"

# SMTP settings (Gmail shown — see note below)
SMTP_HOST     = "smtp.gmail.com"
SMTP_PORT     = 587
SMTP_USER     = "sender@gmail.com"
SMTP_PASSWORD = "abcdefghijklmnop"   # 16-char Gmail App Password, no spaces
FROM_ADDRESS  = "sender@gmail.com"
USE_TLS       = True
USE_SSL       = False

# Log file paths — use absolute paths in production
BRAZIL_BDR_LOG_DIR = Path("logs")
SEC_13F_LOG_DIR    = Path("sec_13f_data")
```

> **Gmail App Password** — standard Gmail passwords are rejected by SMTP. Generate a 16-character App Password at [myaccount.google.com/apppasswords](https://myaccount.google.com/apppasswords) (requires 2-Step Verification to be enabled). Remove all spaces from the generated password before pasting it into the config.

> **Security** — do not commit credentials to source control. For production deployments, load `SMTP_PASSWORD` from an environment variable or secrets manager.

---

## Usage

### Run a single B3 download cycle

```bash
python BrazilBDRListOnce.py
```

Downloads the latest InstrumentsConsolidated file, filters it to BDR instruments, and writes the output to `processed/`. Exits with code `0` on success or `1` on failure so the result can be checked by the calling process.

### Start the self-scheduling B3 downloader (long-running)

```bash
python BrazilBDRList.py
```

Fires the download job immediately on startup and then again every day at 21:00 UK time. Intended for use with `systemd`, `screen`, or `tmux` rather than cron.

### Check the SEC 13F page for updates

```bash
python 13Fmonitor.py          # check for changes
python 13Fmonitor.py status   # show tracking state without fetching the page
```

### Start the 13F scheduler (long-running)

```bash
python sechduler.py
```

### Scan logs and send alert email

```bash
python log_monitor.py
```

### Instruments extractor CLI

```bash
python instruments_extractor.py                       # process latest downloaded file
python instruments_extractor.py process <filepath>    # process a specific file
python instruments_extractor.py list    <filepath>    # list all columns in a CSV
python instruments_extractor.py diagnose <filepath>   # diagnose delimiter/structure issues
```

---

## Scheduling

For production use, `BrazilBDRListOnce.py`, `log_monitor.py`, and `13Fmonitor.py` are designed to be driven by cron. Edit your crontab with `crontab -e` and add:

```cron
# B3 download at 21:00 UK time daily
0 21 * * *  /usr/bin/python3 /opt/b3/BrazilBDRListOnce.py >> /opt/b3/logs/cron.log 2>&1

# Log monitor 30 minutes later
30 21 * * *  /usr/bin/python3 /opt/b3/log_monitor.py >> /opt/b3/logs/cron.log 2>&1

# 13F monitor on the 28th of each month at 10:00
0 10 28 * *  /usr/bin/python3 /opt/b3/13Fmonitor.py >> /opt/b3/logs/cron.log 2>&1
```

Adjust paths to match your deployment directory. Using absolute paths avoids working-directory issues that can cause cron jobs to silently fail.

---

## Log Monitor

`log_monitor.py` scans the following log files on each run:

| Log file | Written by |
|----------|------------|
| `logs/anbima_download_YYYYMM.log` | `BrazilBDRList.py` / `BrazilBDRListOnce.py` |
| `logs/extractor_YYYYMM.log` | `instruments_extractor.py` |
| `sec_13f_data/check_log.txt` | `13Fmonitor.py` |

### Scan window

Only log entries written within the last **25 hours** are considered, preventing entries from older runs generating repeated alerts. To test against a historical log file, temporarily set `SCAN_WINDOW_HOURS = 99999`.

### Error patterns detected

The monitor flags any line matching one of the following case-insensitive patterns:

```
\bERROR\b            \bFAILURE\b          \bFAILED\b
\bFATAL\b            \bEXCEPTION\b        \bCRITICAL\b
Job completed with FAILURE                Download failed
Extraction failed    could not retrieve   HTTP Error
Connection.*timed out                     Traceback (most recent call last)
```

### Alert email

If any matches are found, an HTML email is sent with:

- Summary (timestamp, scan window, issue count)
- Files scanned table
- Colour-coded issues table — red for fatal/critical/traceback, amber for error/failed/failure

No email is sent if the scan finds no issues.

---

## Instruments Extractor

`instruments_extractor.py` applies two sequential row filters before writing the output CSV:

**Filter 1 — Security Category**
Retains rows where `SctyCtgyNm` contains `BDR` (case-insensitive).

**Filter 2 — Specification Code**
Retains rows where `SpcfctnCd` starts with `DR2` or `DR3`.

Both filters can be toggled individually (`FILTER_SECURITY_CATEGORY`, `FILTER_SPECIFICATION_CODE`) or disabled together with `ENABLE_FILTERING = False`.

Output files are written to `processed/` with semicolon (`;`) delimiters and UTF-8 encoding:

```
processed/InstrumentsConsolidated_filtered_YYYYMMDD_HHMMSS.csv
```

The extractor handles B3-specific quirks automatically: metadata header detection, delimiter auto-detection, and multi-encoding fallback (UTF-8 → Latin-1 → ISO-8859-1 → CP1252).

---

## Licence

Private — Kaizen Reporting. Not for redistribution.
