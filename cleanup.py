#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cleanup Script - Clean build artifacts and old logs.

This script only cleans build artifacts (app/, build/, dist/) and old log files.
It does NOT touch Master DB (data/store/) or exported data (exported_data/).

Usage:
    python cleanup.py              # Clean build artifacts and 7-day-old logs
    python cleanup.py --logs-days 3   # Custom log retention
    python cleanup.py --no-logs    # Skip log cleanup
"""

import os
import shutil
import argparse
from datetime import datetime, timedelta


def cleanup_build_artifacts():
    """Clean build artifacts (app/, build/, dist/)"""
    dirs_to_clean = ['app', 'build', 'dist']
    cleaned = []
    
    print("\n" + "="*60)
    print("Cleaning build artifacts...")
    print("="*60)
    
    for dir_name in dirs_to_clean:
        if os.path.exists(dir_name):
            try:
                shutil.rmtree(dir_name)
                cleaned.append(dir_name)
                print(f"[OK] Deleted: {dir_name}/")
            except Exception as e:
                print(f"[FAIL] Cannot delete {dir_name}/: {str(e)}")
    
    if not cleaned:
        print("[OK] No build directories to clean")
    
    return len(cleaned)


def cleanup_old_logs(days=7):
    """Clean log files older than N days"""
    cutoff = datetime.now() - timedelta(days=days)
    deleted = []
    
    print("\n" + "="*60)
    print(f"Cleaning log files older than {days} days...")
    print("="*60)
    
    try:
        for filename in os.listdir('.'):
            if filename.endswith('.log'):
                filepath = os.path.join('.', filename)
                try:
                    mtime = datetime.fromtimestamp(os.path.getmtime(filepath))
                    
                    if mtime < cutoff:
                        os.remove(filepath)
                        deleted.append(filename)
                        age_days = (datetime.now() - mtime).days
                        print(f"[OK] Deleted ({age_days} days old): {filename}")
                except Exception as e:
                    print(f"[FAIL] Cannot delete {filename}: {str(e)}")
        
        if not deleted:
            print(f"[OK] No log files older than {days} days")
    
    except Exception as e:
        print(f"[FAIL] Cannot scan log files: {str(e)}")
    
    return len(deleted)


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description="Clean build artifacts and old log files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python cleanup.py                 # Default cleanup (7-day logs)
  python cleanup.py --logs-days 3   # Keep only 3-day logs
  python cleanup.py --no-logs       # Skip log cleanup
  
Notes:
  - This script will NOT delete Master DB (data/store/) data
  - This script will NOT delete exported data (exported_data/)
  - Use the "Data Manager" UI to manage these data files
        """
    )
    
    parser.add_argument(
        '--logs-days',
        type=int,
        default=7,
        help='Keep logs from last N days (default: 7)'
    )
    
    parser.add_argument(
        '--no-logs',
        action='store_true',
        help='Skip log file cleanup'
    )
    
    args = parser.parse_args()
    
    print("\n" + "="*60)
    print("Quant Data Bridge - Cleanup Script")
    print("="*60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Clean build artifacts
    cleaned_dirs = cleanup_build_artifacts()
    
    # Clean logs
    cleaned_logs = 0
    if not args.no_logs:
        cleaned_logs = cleanup_old_logs(args.logs_days)
    else:
        print("\n[SKIP] Log cleanup (--no-logs)")
    
    # Summary
    print("\n" + "="*60)
    print("Cleanup Complete!")
    print("="*60)
    print(f"Deleted build directories: {cleaned_dirs}")
    print(f"Deleted log files: {cleaned_logs}")
    print("\nTIP: Use the 'Data Manager' UI to manage Master DB and exported data")
    print("="*60 + "\n")


if __name__ == '__main__':
    main()
