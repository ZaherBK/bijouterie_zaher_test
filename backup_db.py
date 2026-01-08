"""
Bijouterie Zaher - Smart Continuous Cloud Backup
===============================================
This script runs indefinitely, checking for database updates every 5 minutes.
It only saves a new backup file if the data has actually changed.

Usage:
    python backup_db.py
"""

import requests
from datetime import datetime
import os
import time
import hashlib
import glob

# ===================== CONFIGURATION =====================
CLOUD_API_URL = "https://hr-sync.onrender.com/"
CLOUD_EMAIL = "zaher@local.com"
CLOUD_PASSWORD = "5"
BACKUP_DIR = "sauve"
CHECK_INTERVAL_SECONDS = 300  # 5 minutes
# =========================================================

def login_to_cloud():
    """Login to cloud API and get access token."""
    print(f"[{datetime.now():%H:%M:%S}] Logging in to Cloud ({CLOUD_API_URL})...")
    try:
        resp = requests.post(
            f"{CLOUD_API_URL}/api/users/login",
            data={"username": CLOUD_EMAIL, "password": CLOUD_PASSWORD},
            timeout=30
        )
        if resp.status_code == 200:
            token = resp.json().get("access_token")
            # print(f"[{datetime.now():%H:%M:%S}] [OK] Login successful.") # Reduce noise
            return token
        else:
            print(f"[{datetime.now():%H:%M:%S}] [ERROR] Login failed: {resp.text}")
            return None
    except Exception as e:
        print(f"[{datetime.now():%H:%M:%S}] [ERROR] Login connection error: {e}")
        return None

def get_latest_local_hash():
    """Finds the most recent backup file and calculates its hash to avoid re-downloading duplicate state on restart."""
    if not os.path.exists(BACKUP_DIR):
        os.makedirs(BACKUP_DIR)
        return None

    files = glob.glob(os.path.join(BACKUP_DIR, "cloud_backup_*.json"))
    if not files:
        return None

    # Get latest file based on creation time
    latest_file = max(files, key=os.path.getctime)
    
    print(f"[{datetime.now():%H:%M:%S}] Calculating hash of latest local backup: {os.path.basename(latest_file)}...")
    sha256_hash = hashlib.sha256()
    try:
        with open(latest_file, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    except Exception as e:
        print(f"Error reading local file: {e}")
        return None

def check_and_backup(last_known_hash):
    """Downloads data, checks hash, and saves only if different."""
    token = login_to_cloud()
    if not token:
        return last_known_hash

    headers = {"Authorization": f"Bearer {token}"}
    
    try:
        # Check endpoint
        url = f"{CLOUD_API_URL}/api/sync/backup"
        
        # Download content into memory to check hash
        # WARNING: If DB is huge (>500MB), this approach might need streaming comparison. 
        # For typical small biz app JSON, memory is fine and safer/easier.
        resp = requests.get(url, headers=headers, timeout=120)
        
        if resp.status_code == 200:
            data = resp.content # Binary content
            current_hash = hashlib.sha256(data).hexdigest()

            if current_hash != last_known_hash:
                # Data changed! Save it.
                filename = f"cloud_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                filepath = os.path.join(BACKUP_DIR, filename)
                
                with open(filepath, 'wb') as f:
                    f.write(data)
                
                print(f"[{datetime.now():%H:%M:%S}] [CHANGE DETECTED] New backup saved: {filename}")
                return current_hash
            else:
                print(f"[{datetime.now():%H:%M:%S}] [NO CHANGE] Cloud data matches last backup. Skipping save.")
                return last_known_hash
        else:
            print(f"[{datetime.now():%H:%M:%S}] [API ERROR] Status {resp.status_code}")
            return last_known_hash

    except Exception as e:
        print(f"[{datetime.now():%H:%M:%S}] [CONNECTION ERROR] {e}")
        return last_known_hash

def main():
    print("=" * 60)
    print("  Bijouterie Zaher - Smart Continuous Backup Agent")
    print("=" * 60)
    print(f"  Check Interval: {CHECK_INTERVAL_SECONDS} seconds")
    print(f"  Target Folder:  {BACKUP_DIR}")
    print("=" * 60)

    # 1. Initialize state from disk
    last_hash = get_latest_local_hash()
    if last_hash:
        print(f"[{datetime.now():%H:%M:%S}] System resumed. Last backup hash: {last_hash[:8]}...")
    else:
        print(f"[{datetime.now():%H:%M:%S}] No local backups found. Next download will be saved.")

    # 2. Continuous Loop
    try:
        while True:
            # Perform check
            last_hash = check_and_backup(last_hash)
            
            # Wait
            # print(f"[{datetime.now():%H:%M:%S}] Sleeping for {CHECK_INTERVAL_SECONDS}s...")
            time.sleep(CHECK_INTERVAL_SECONDS)
            
    except KeyboardInterrupt:
        print("\n[STOPPED] Backup agent stopped by user.")

if __name__ == "__main__":
    main()
