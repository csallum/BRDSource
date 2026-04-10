#!/usr/bin/env python3
"""
================================================================================
Module      : sechduler.py
Author      : Craig Allum
Date        : 24/02/2026
Version     : v1.1
Description :
    Scheduler for the SEC 13F Securities List Monitor.

    Wraps 13Fmonitor.py in a long-running scheduler that executes the check
    on the 28th of every month at 10:00 AM.  The job is registered to fire
    daily at 10:00, but the handler function guards against execution on any
    day before the 28th.

    On startup, if the current date is on or after the 28th, an immediate
    check is run without waiting for the next scheduled window.

    This module is intended to run as a long-lived process (e.g. via systemd
    or screen/tmux).  For cron-based execution, schedule 13Fmonitor.py
    directly with a monthly cron expression instead.

Usage:
    python sechduler.py

Dependencies:
    schedule
================================================================================
"""

import subprocess
import sys
from datetime import datetime

import schedule
import time


# ---------------------------------------------------------------------------
# Job execution
# ---------------------------------------------------------------------------

def run_check() -> None:
    """Invoke 13Fmonitor.py as a subprocess and stream its output.

    Using a subprocess ensures that 13Fmonitor.py runs in a clean execution
    context and that its exit code can be captured for future alerting use.
    Any output written to stderr by the child process is printed separately
    to help distinguish errors from normal output.
    """
    print(f"\n{'=' * 60}")
    print(f"Running scheduled check at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'=' * 60}\n")

    try:
        result = subprocess.run(
            [sys.executable, "13Fmonitor.py"],
            capture_output=True,
            text=True,
        )
        print(result.stdout)

        if result.returncode != 0:
            print(f"WARNING: 13Fmonitor.py exited with code {result.returncode}")

        if result.stderr:
            print("STDERR:", result.stderr)

    except Exception as exc:
        print(f"ERROR: Failed to run 13Fmonitor.py: {exc}")


# ---------------------------------------------------------------------------
# Schedule guard
# ---------------------------------------------------------------------------

def is_end_of_month() -> bool:
    """Return ``True`` if today is the 28th or later in the current month.

    The 28th is used as the trigger date because it is the earliest day that
    is guaranteed to exist in every calendar month, ensuring the check always
    runs at least once per month regardless of month length.

    Returns:
        ``True`` if ``datetime.now().day >= 28``, otherwise ``False``.
    """
    return datetime.now().day >= 28


def monthly_check() -> None:
    """Scheduled handler - runs the check only when end-of-month criteria are met.

    This function is registered with the ``schedule`` library to fire daily
    at 10:00 AM.  The :func:`is_end_of_month` guard prevents it from doing
    any work on days 1–27.
    """
    if is_end_of_month():
        run_check()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """Initialise the scheduler and enter the polling loop.

    Registers :func:`monthly_check` to run daily at 10:00 AM, then performs
    an immediate check if today qualifies as end-of-month.  The polling loop
    checks for pending jobs every hour.

    Raises:
        KeyboardInterrupt: Caught to allow a clean shutdown message.
    """
    schedule.every().day.at("10:00").do(monthly_check)

    print("SEC 13F Monitor Scheduler Started")
    print("Schedule: checks on the 28th of every month at 10:00 AM")
    print(f"Current date: {datetime.now().strftime('%Y-%m-%d')}")

    if is_end_of_month():
        print("Today qualifies as end-of-month - running initial check now...\n")
        run_check()
    else:
        # Calculate the next trigger date for informational output
        next_check = datetime.now().replace(
            day=28, hour=10, minute=0, second=0, microsecond=0
        )
        if next_check <= datetime.now():
            # Already past the 28th this month - advance to next month
            month = next_check.month
            year  = next_check.year
            if month == 12:
                next_check = next_check.replace(year=year + 1, month=1)
            else:
                next_check = next_check.replace(month=month + 1)

        print(f"Next check scheduled for: {next_check.strftime('%Y-%m-%d at %H:%M')}\n")

    print("Scheduler running. Press Ctrl+C to stop.\n")

    while True:
        schedule.run_pending()
        time.sleep(3600)  # Poll every hour


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nScheduler stopped by user")
