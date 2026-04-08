#!/usr/bin/env python3
"""
By : Craig Allum
Date : 24/02/2026
Version : v1.0
Purpose :

Scheduler for SEC 13F Monitor
Runs the check at the end of every month (28th day at 10:00 AM)
"""

import schedule
import time
from datetime import datetime
import subprocess
import sys
import calendar

def run_check():
    print(f"\n{'='*60}")
    print(f"Running scheduled check at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")
    
    try:
        result = subprocess.run(
            [sys.executable, "sec_13f_monitor.py"],
            capture_output=True,
            text=True
        )
        print(result.stdout)
        if result.stderr:
            print("Errors:", result.stderr)
    except Exception as e:
        print(f"Error running check: {e}")

def is_end_of_month():
    today = datetime.now()
    return today.day >= 28

def monthly_check():
    if is_end_of_month():
        run_check()

# Schedule the job to run daily at 10:00 AM, but only execute at month end
schedule.every().day.at("10:00").do(monthly_check)

# Also run immediately on startup if we're at month end
print("SEC 13F Monitor Scheduler Started")
print("Schedule: Checks on the 28th of every month at 10:00 AM")
print(f"Current date: {datetime.now().strftime('%Y-%m-%d')}")

if is_end_of_month():
    print("Today is end of month - running initial check now...\n")
    run_check()
else:
    next_check = datetime.now().replace(day=28, hour=10, minute=0, second=0, microsecond=0)
    if next_check < datetime.now():
        # If we've passed the 28th this month, schedule for next month
        if datetime.now().month == 12:
            next_check = next_check.replace(year=next_check.year + 1, month=1)
        else:
            next_check = next_check.replace(month=next_check.month + 1)
    print(f"Next check will be on: {next_check.strftime('%Y-%m-%d at %H:%M')}\n")

# Keep the script running
print("Scheduler running. Press Ctrl+C to stop\n")

while True:
    schedule.run_pending()
    time.sleep(3600)  # Check every hour if a scheduled task is due