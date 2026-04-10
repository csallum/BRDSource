#!/usr/bin/env python3
"""
================================================================================
Module      : log_monitor.py
Author      : Craig Allum
Date        : 10/04/2026
Version     : v1.1
Description :
    Log Monitor - Error and Failure Alerter.

    Scans the log files produced by BrazilBDRList and 13FMonitor for lines
    that match a set of error and failure patterns.  If any matching lines are
    found within the configured scan window, an alert email is sent to a
    comma-separated list of recipients.

    The email includes both a plain-text fallback and an HTML body with
    colour-coded severity rows (red for fatal/critical, amber for
    errors/failures).

    Intended to be scheduled via cron approximately 30 minutes after the main
    BrazilBDR job completes each evening.

    Cron example (run at 21:30 UK time daily):
        30 21 * * * /usr/bin/python3 /opt/b3/log_monitor.py

Dependencies:
    None beyond the Python standard library.
================================================================================
"""

import logging
import re
import smtplib
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration - edit this section before deployment
# ---------------------------------------------------------------------------

# Comma-separated list of addresses that will receive alert emails
ALERT_RECIPIENTS: str = "csallum@gmail.com, angie.huff@kaizenreporting.com"

# SMTP connection settings
SMTP_HOST:     str  = "smtp.gmail.com"
SMTP_PORT:     int  = 587
SMTP_USER:     str  = "csallum@gmail.com"
SMTP_PASSWORD: str  = "zyqurtgbhqgosknp"   # 16-char Gmail App Password (no spaces)
FROM_ADDRESS:  str  = "csallum@gmail.com"
USE_TLS:       bool = True   # STARTTLS on port 587
USE_SSL:       bool = False  # SSL on port 465 - mutually exclusive with USE_TLS

# Log file locations (relative or absolute paths)
BRAZIL_BDR_LOG_DIR: Path = Path("logs")          # BrazilBDRList log directory
SEC_13F_LOG_DIR:    Path = Path("sec_13f_data")   # 13FMonitor log directory
SEC_13F_LOG_FILE:   Path = SEC_13F_LOG_DIR / "check_log.txt"

# Only flag log lines written within the last N hours.
# Set slightly above 24 to account for timezone drift and schedule jitter.
SCAN_WINDOW_HOURS: int = 25

# Monitor's own log file (records each scan run)
MONITOR_LOG_DIR:  Path = Path("logs")
MONITOR_LOG_FILE: Path = (
    MONITOR_LOG_DIR / f"log_monitor_{datetime.now().strftime('%Y%m')}.log"
)

# ---------------------------------------------------------------------------
# Error patterns
# Each pattern is a case-insensitive regular expression.  A log line is
# flagged if it matches any one of these patterns.
# ---------------------------------------------------------------------------

ERROR_PATTERNS: list[str] = [
    r"\bERROR\b",
    r"\bFAILURE\b",
    r"\bFAILED\b",
    r"\bFATAL\b",
    r"\bEXCEPTION\b",
    r"\bCRITICAL\b",
    r"Job completed with FAILURE",
    r"Download failed",
    r"Extraction failed",
    r"could not retrieve",
    r"HTTP Error",
    r"Connection.*timed out",
    r"Traceback \(most recent call last\)",
]

# Pre-compile all patterns once at module load time for efficiency
_COMPILED_PATTERNS = [re.compile(p, re.IGNORECASE) for p in ERROR_PATTERNS]

# ---------------------------------------------------------------------------
# Logging setup for this monitor script
# ---------------------------------------------------------------------------

MONITOR_LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(MONITOR_LOG_FILE),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Timestamp parsing
# ---------------------------------------------------------------------------

def parse_log_timestamp(line: str) -> datetime | None:
    """Attempt to extract a datetime from the beginning of a log line.

    Supports two formats used across the monitored log files:

    * **Python logging format** – ``2026-04-10 21:05:34,123``
    * **13FMonitor bracket format** – ``[2026-04-10 21:05:34]``

    Args:
        line: A single line from a log file.

    Returns:
        A :class:`~datetime.datetime` object if a timestamp is found,
        otherwise ``None``.
    """
    # Python logging format: YYYY-MM-DD HH:MM:SS,mmm
    match = re.match(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", line)
    if match:
        try:
            return datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass

    # 13FMonitor bracket format: [YYYY-MM-DD HH:MM:SS]
    match = re.match(r"\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]", line)
    if match:
        try:
            return datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass

    return None


# ---------------------------------------------------------------------------
# Log scanning
# ---------------------------------------------------------------------------

def line_has_error(line: str) -> bool:
    """Return ``True`` if the line matches any configured error pattern.

    Args:
        line: A single log line to test.

    Returns:
        ``True`` if at least one pattern matches, otherwise ``False``.
    """
    return any(pattern.search(line) for pattern in _COMPILED_PATTERNS)


def scan_log_file(filepath: Path, source_label: str) -> list[dict]:
    """Scan a single log file for error and failure lines within the scan window.

    Tracks the most recently seen timestamp so that lines without an explicit
    timestamp inherit the context of the preceding timestamped line.

    Args:
        filepath:     Path to the log file to scan.
        source_label: Human-readable label used in the alert report.

    Returns:
        A list of finding dictionaries, each containing ``source``,
        ``line_no``, ``timestamp``, and ``line`` keys.  An empty list is
        returned if the file does not exist or no matching lines are found.
    """
    findings: list[dict] = []
    cutoff               = datetime.now() - timedelta(hours=SCAN_WINDOW_HOURS)

    if not filepath.exists():
        logger.warning("Log file not found, skipping: %s", filepath)
        return findings

    try:
        with open(filepath, "r", encoding="utf-8", errors="ignore") as fh:
            lines = fh.readlines()

        last_known_time: datetime | None = None

        for line_no, raw_line in enumerate(lines, start=1):
            line = raw_line.rstrip("\n")

            ts = parse_log_timestamp(line)
            if ts:
                last_known_time = ts

            # Use the last known timestamp as a proxy for lines that lack one
            effective_time = last_known_time or datetime.now()
            if effective_time < cutoff:
                continue

            if line_has_error(line):
                findings.append({
                    "source":    source_label,
                    "line_no":   line_no,
                    "timestamp": (
                        last_known_time.strftime("%Y-%m-%d %H:%M:%S")
                        if last_known_time
                        else "unknown"
                    ),
                    "line": line.strip(),
                })

        logger.info(
            "Scanned %s: %s lines checked, %s issue(s) found",
            filepath,
            len(lines),
            len(findings),
        )

    except Exception as exc:
        logger.error("Failed to scan %s: %s", filepath, exc)

    return findings


def find_brazil_bdr_logs() -> list[Path]:
    """Return BrazilBDR log files relevant to the current scan window.

    Log files follow the naming convention ``anbima_download_YYYYMM.log``.
    The current month's file is always included; the previous month's file
    is also included when running on the 1st of the month to avoid missing
    errors written near midnight on the last day of the month.

    Returns:
        A list of :class:`~pathlib.Path` objects for files that exist on disk.
    """
    log_dir = Path(BRAZIL_BDR_LOG_DIR)

    if not log_dir.exists():
        logger.warning("BrazilBDR log directory not found: %s", log_dir)
        return []

    now      = datetime.now()
    suffixes = {now.strftime("%Y%m")}

    if now.day == 1:
        prev_month = now.replace(day=1) - timedelta(days=1)
        suffixes.add(prev_month.strftime("%Y%m"))

    found = []
    for suffix in suffixes:
        candidate = log_dir / f"anbima_download_{suffix}.log"
        if candidate.exists():
            found.append(candidate)

    return found


# ---------------------------------------------------------------------------
# Email construction
# ---------------------------------------------------------------------------

def parse_recipients(recipients_str: str) -> list[str]:
    """Parse a comma-separated string of email addresses into a clean list.

    Args:
        recipients_str: Raw comma-separated string from :data:`ALERT_RECIPIENTS`.

    Returns:
        A list of stripped, non-empty email address strings.
    """
    return [addr.strip() for addr in recipients_str.split(",") if addr.strip()]


def build_email_body(
    all_findings: list[dict],
    scanned_files: list[tuple[str, str]],
) -> tuple[str, str]:
    """Build both a plain-text and an HTML version of the alert email body.

    HTML rows are colour-coded by severity:

    * **Red** – lines containing ``fatal``, ``critical``, or ``traceback``
    * **Amber** – lines containing ``error``, ``failed``, or ``failure``
    * **White** – all other matched lines

    Args:
        all_findings:  List of finding dicts produced by :func:`scan_log_file`.
        scanned_files: List of ``(filepath_str, label)`` tuples for all files
                       that were scanned during this run.

    Returns:
        A ``(plain_text, html_text)`` tuple.
    """
    now_str  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    count    = len(all_findings)

    # Group findings by source for the plain-text section
    by_source: dict[str, list[dict]] = {}
    for finding in all_findings:
        by_source.setdefault(finding["source"], []).append(finding)

    # ---- Plain text ----
    plain_lines = [
        "Log Monitor Alert",
        f"Generated    : {now_str}",
        f"Scan window  : last {SCAN_WINDOW_HOURS} hours",
        f"Total issues : {count}",
        "",
        "FILES SCANNED:",
    ]
    for fp, label in scanned_files:
        plain_lines.append(f"  [{label}]  {fp}")
    plain_lines.append("")

    for source, items in by_source.items():
        plain_lines.append("=" * 70)
        plain_lines.append(f"SOURCE: {source}  ({len(items)} issue(s))")
        plain_lines.append("=" * 70)
        for item in items:
            plain_lines.append(
                f"  Line {item['line_no']:>5} | {item['timestamp']} | {item['line']}"
            )
        plain_lines.append("")

    plain_text = "\n".join(plain_lines)

    # ---- HTML ----
    html_rows = ""
    for finding in all_findings:
        line_lower = finding["line"].lower()
        if any(x in line_lower for x in ("fatal", "critical", "traceback")):
            row_colour = "#ffe0e0"
        elif any(x in line_lower for x in ("error", "failed", "failure")):
            row_colour = "#fff3cd"
        else:
            row_colour = "#ffffff"

        html_rows += (
            f'<tr style="background-color:{row_colour};">'
            f'<td style="padding:4px 8px;border:1px solid #ccc;">{finding["source"]}</td>'
            f'<td style="padding:4px 8px;border:1px solid #ccc;">{finding["line_no"]}</td>'
            f'<td style="padding:4px 8px;border:1px solid #ccc;">{finding["timestamp"]}</td>'
            f'<td style="padding:4px 8px;border:1px solid #ccc;font-family:monospace;'
            f'font-size:12px;">{finding["line"]}</td>'
            f"</tr>"
        )

    scanned_rows = "".join(
        f'<tr>'
        f'<td style="padding:4px 8px;border:1px solid #ccc;">{label}</td>'
        f'<td style="padding:4px 8px;border:1px solid #ccc;">{fp}</td>'
        f"</tr>"
        for fp, label in scanned_files
    )

    html_text = f"""
    <html>
    <body style="font-family:Arial,sans-serif;font-size:14px;">

    <h2 style="color:#c0392b;">&#9888; Log Monitor Alert</h2>

    <table style="border-collapse:collapse;margin-bottom:16px;">
      <tr>
        <td style="padding:4px 8px;font-weight:bold;">Generated</td>
        <td style="padding:4px 8px;">{now_str}</td>
      </tr>
      <tr>
        <td style="padding:4px 8px;font-weight:bold;">Scan window</td>
        <td style="padding:4px 8px;">Last {SCAN_WINDOW_HOURS} hours</td>
      </tr>
      <tr>
        <td style="padding:4px 8px;font-weight:bold;">Total issues</td>
        <td style="padding:4px 8px;color:#c0392b;font-weight:bold;">{count}</td>
      </tr>
    </table>

    <h3>Files Scanned</h3>
    <table style="border-collapse:collapse;margin-bottom:16px;">
      <thead>
        <tr style="background:#f0f0f0;">
          <th style="padding:4px 8px;border:1px solid #ccc;">Job</th>
          <th style="padding:4px 8px;border:1px solid #ccc;">Log File</th>
        </tr>
      </thead>
      <tbody>{scanned_rows}</tbody>
    </table>

    <h3>Issues Found</h3>
    <table style="border-collapse:collapse;width:100%;">
      <thead>
        <tr style="background:#f0f0f0;">
          <th style="padding:4px 8px;border:1px solid #ccc;">Source</th>
          <th style="padding:4px 8px;border:1px solid #ccc;">Line #</th>
          <th style="padding:4px 8px;border:1px solid #ccc;">Timestamp</th>
          <th style="padding:4px 8px;border:1px solid #ccc;">Log Entry</th>
        </tr>
      </thead>
      <tbody>{html_rows}</tbody>
    </table>

    <p style="color:#888;font-size:12px;margin-top:24px;">
      Sent by log_monitor.py &mdash; Kaizen Reporting
    </p>
    </body>
    </html>
    """

    return plain_text, html_text


# ---------------------------------------------------------------------------
# Email sending
# ---------------------------------------------------------------------------

def send_alert_email(
    all_findings: list[dict],
    scanned_files: list[tuple[str, str]],
) -> bool:
    """Send the alert email to all configured recipients.

    Constructs a multipart MIME message with both plain-text and HTML parts
    and delivers it via the configured SMTP server.

    Args:
        all_findings:  List of finding dicts to include in the report.
        scanned_files: List of ``(filepath_str, label)`` tuples for context.

    Returns:
        ``True`` if the message was delivered successfully, ``False`` otherwise.
    """
    recipients = parse_recipients(ALERT_RECIPIENTS)

    if not recipients:
        logger.error("No valid recipients configured in ALERT_RECIPIENTS")
        return False

    count   = len(all_findings)
    subject = (
        f"[LOG ALERT] {count} issue(s) detected - "
        f"{datetime.now().strftime('%Y-%m-%d %H:%M')}"
    )

    plain_text, html_text = build_email_body(all_findings, scanned_files)

    msg             = MIMEMultipart("alternative")
    msg["Subject"]  = subject
    msg["From"]     = FROM_ADDRESS
    msg["To"]       = ", ".join(recipients)

    msg.attach(MIMEText(plain_text, "plain"))
    msg.attach(MIMEText(html_text,  "html"))

    try:
        if USE_SSL:
            server = smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT)
        else:
            server = smtplib.SMTP(SMTP_HOST, SMTP_PORT)

        if USE_TLS:
            server.starttls()

        server.login(SMTP_USER, SMTP_PASSWORD)
        server.sendmail(FROM_ADDRESS, recipients, msg.as_string())
        server.quit()

        logger.info("Alert email sent to: %s", ", ".join(recipients))
        return True

    except smtplib.SMTPAuthenticationError:
        logger.error(
            "SMTP authentication failed - verify SMTP_USER and SMTP_PASSWORD"
        )
    except smtplib.SMTPConnectError:
        logger.error(
            "Could not connect to SMTP server %s:%s", SMTP_HOST, SMTP_PORT
        )
    except Exception as exc:
        logger.error("Failed to send alert email: %s", exc)

    return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    """Coordinate the full scan-and-alert cycle.

    Discovers all relevant log files, scans each one for errors within the
    configured time window, and dispatches an alert email if any issues are
    found.  If no issues are detected the run completes silently with no
    email sent.
    """
    logger.info("=" * 70)
    logger.info(
        "Log Monitor starting at %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )
    logger.info("Scan window: last %s hours", SCAN_WINDOW_HOURS)
    logger.info("=" * 70)

    all_findings:  list[dict]              = []
    scanned_files: list[tuple[str, str]]   = []

    # -- Scan BrazilBDR logs --
    bdr_logs = find_brazil_bdr_logs()
    if bdr_logs:
        for log_path in bdr_logs:
            label    = f"BrazilBDRList [{log_path.name}]"
            findings = scan_log_file(log_path, label)
            all_findings.extend(findings)
            scanned_files.append((str(log_path), label))
    else:
        logger.warning("No BrazilBDR log files found to scan")

    # -- Scan 13F Monitor log --
    findings_13f = scan_log_file(SEC_13F_LOG_FILE, "SEC 13F Monitor")
    all_findings.extend(findings_13f)
    scanned_files.append((str(SEC_13F_LOG_FILE), "SEC 13F Monitor"))

    # -- Report --
    logger.info("Scan complete. Total issues found: %s", len(all_findings))

    if all_findings:
        logger.warning("%s issue(s) detected - sending alert email", len(all_findings))
        if not send_alert_email(all_findings, scanned_files):
            logger.error("Alert email could not be sent - check SMTP configuration")
    else:
        logger.info("No issues detected - no email will be sent")

    logger.info("=" * 70)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    main()
