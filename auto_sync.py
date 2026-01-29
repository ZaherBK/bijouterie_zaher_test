"""
Bijouterie Zaher - Automatic Local Sync Agent
==============================================
This script runs on your PC and automatically syncs data from the cloud website
to your local MySQL database (mah1303.fdepense).

It also performs a FULL CLOUD BACKUP (JSON) after every successful sync.

Run this script manually OR set it up as a Windows Scheduled Task for automatic sync.

Usage:
    python auto_sync.py
    
To run automatically every 5 minutes, create a Windows Scheduled Task.
"""

import requests
import pymysql
import pymysql.cursors
from datetime import date, datetime, timedelta
import time
import os

# ===================== CONFIGURATION =====================
CLOUD_API_URL = "https://hr-sync.onrender.com/"  # Your hosted website URL
CLOUD_EMAIL = "issam@local.com"
CLOUD_PASSWORD = "002"

LOCAL_DB_HOST = "localhost"
LOCAL_DB_USER = "root"
LOCAL_DB_PASSWORD = "6165"
LOCAL_DB_SCHEMA = "mah1303"

SYNC_USER_NAME = "BK ZAHER"  # Default Name (Fallback)

# CODDEP codes
CODDEP_EXPENSES = 2  # Dépenses
CODDEP_DEPOSITS = 1  # Avances
# =========================================================


def login_to_cloud():
    """Login to cloud API and get access token."""
    try:
        resp = requests.post(
            f"{CLOUD_API_URL}/api/users/login",
            data={"username": CLOUD_EMAIL, "password": CLOUD_PASSWORD},
            timeout=30
        )
        if resp.status_code == 200:
            token = resp.json().get("access_token")
            print(f"[{datetime.now():%H:%M:%S}] [OK] Logged in successfully")
            return token
        else:
            print(f"[{datetime.now():%H:%M:%S}] [ERROR] Login failed: {resp.text}")
            return None
    except Exception as e:
        print(f"[{datetime.now():%H:%M:%S}] [ERROR] Connection error: {e}")
        return None


def get_unsynced_data(token):
    """Fetch today's deposits and expenses from cloud API."""
    headers = {"Authorization": f"Bearer {token}"}
    today = date.today()
    start_date_obj = today - timedelta(days=30)
    start_date_str = start_date_obj.isoformat()
    
    deposits = []
    expenses = []
    payments = []
    loans = []
    
    # Fetch Deposits
    try:
        resp = requests.get(f"{CLOUD_API_URL}/api/deposits/", headers=headers, timeout=30)
        if resp.status_code == 200:
            all_deposits = resp.json()
            # MODIFIED: Check last 30 days
            deposits = [d for d in all_deposits if d.get('date') and d.get('date') >= start_date_str]
            print(f"[{datetime.now():%H:%M:%S}] Found {len(deposits)} deposits (last 30 days)")
        else:
            print(f"[{datetime.now():%H:%M:%S}] [ERROR] Deposits API Error: {resp.status_code} {resp.text}")
    except Exception as e:
        print(f"[{datetime.now():%H:%M:%S}] Error fetching deposits: {e}")
    
    # Fetch Expenses
    try:
        resp = requests.get(f"{CLOUD_API_URL}/api/expenses/", headers=headers, timeout=30)
        if resp.status_code == 200:
            all_expenses = resp.json()
            # MODIFIED: Check last 30 days
            expenses = [e for e in all_expenses if e.get('date') and e.get('date') >= start_date_str]
            print(f"[{datetime.now():%H:%M:%S}] Found {len(expenses)} expenses (last 30 days)")
        else:
            print(f"[{datetime.now():%H:%M:%S}] [ERROR] Expenses API Error: {resp.status_code} {resp.text}")
    except Exception as e:
        print(f"[{datetime.now():%H:%M:%S}] Error fetching expenses: {e}")

    # Fetch Payments (DISABLED/IGNORED per user request)
    # Payments/Primes are NOT synced to local DB fdepense.
    
    # Fetch Loans (New)
    try:
        resp = requests.get(f"{CLOUD_API_URL}/api/loans/", headers=headers, timeout=30)
        if resp.status_code == 200:
            all_loans = resp.json()
            # MODIFIED: Check last 30 days (filter by start_date)
            loans = [l for l in all_loans if l.get('start_date') and l.get('start_date') >= start_date_str]
            print(f"[{datetime.now():%H:%M:%S}] Found {len(loans)} loans (last 30 days)")
        else:
            print(f"[{datetime.now():%H:%M:%S}] [ERROR] Loans API Error: {resp.status_code} {resp.text}")
    except Exception as e:
        print(f"[{datetime.now():%H:%M:%S}] Error fetching loans: {e}")
    
    return deposits, expenses, payments, loans





def sync_to_local_mysql(token, deposits, expenses, payments, loans):
    """Insert data into local MySQL fdepense table."""
    if not deposits and not expenses and not payments and not loans:
        print(f"[{datetime.now():%H:%M:%S}] Nothing to sync")
        return 0
    
    # Collect all dates involved
    involved_dates = set()
    for item in deposits: involved_dates.add(item.get('date'))
    for item in expenses: involved_dates.add(item.get('date'))
    for item in loans: involved_dates.add(item.get('start_date'))
    
    involved_dates.discard(None) # Remove None if any
    
    # If no valid dates, return
    if not involved_dates:
        return 0

    inserted = 0
    try:
        connection = pymysql.connect(
            host=LOCAL_DB_HOST,
            user=LOCAL_DB_USER,
            password=LOCAL_DB_PASSWORD,
            database=LOCAL_DB_SCHEMA,
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=5,
            charset='latin1'  # Legacy MySQL 4.1 support
        )
        
        with connection.cursor() as cursor:
            # Get last NUMDEP
            cursor.execute("SELECT MAX(NUMDEP) as max_num FROM fdepense")
            result = cursor.fetchone()
            current_num_dep = int(result['max_num']) if result and result['max_num'] else 0
            
            # Get existing records for involved dates ONLY
            # Dynamically build query: SELECT DATDEP, LIBDEP, MONTANT FROM fdepense WHERE DATDEP IN (...)
            format_strings = ','.join(['%s'] * len(involved_dates))
            query = f"SELECT DATDEP, LIBDEP, MONTANT FROM fdepense WHERE DATDEP IN ({format_strings})"
            cursor.execute(query, tuple(involved_dates))
            
            # Key: (date_str, libelle, amount)
            existing = set(
                (str(row['DATDEP']), row['LIBDEP'], float(row['MONTANT'])) 
                for row in cursor.fetchall()
            )
            
            # Process Deposits (CODDEP = 2)
            for d in deposits:
                emp = d.get('employee') or {}
                emp_name = f"{emp.get('first_name', '')} {emp.get('last_name', '')}".strip()
                note = d.get('note', '')
                if note:
                    lib_dep = f"{emp_name} - {note}"
                else:
                    lib_dep = emp_name
                
                lib_dep = lib_dep[:45]
                amount = float(d['amount'])
                date_str = str(d['date'])
                
                if (date_str, lib_dep, amount) in existing: continue
                
                current_num_dep += 1
                creator_name = (d.get('creator') or {}).get('full_name', SYNC_USER_NAME)[:20]
                sql = """INSERT INTO fdepense 
                         (TYPE, MODREG, BANQUE, NUMPCE, DATPCE, CODDEP, NUMDEP, MONTANT, LIBDEP, DATDEP, NUM, UTIL)
                         VALUES (0, 'Espèces', '', '', %s, %s, %s, %s, %s, %s, 0, %s)"""
                cursor.execute(sql, (d['date'], CODDEP_DEPOSITS, current_num_dep, amount, lib_dep, d['date'], creator_name))
                inserted += 1
            
            # Process Expenses (CODDEP = 1)
            for e in expenses:
                lib_dep = e.get('description', 'Dépense')[:45]
                amount = float(e['amount'])
                date_str = str(e['date'])
                
                if (date_str, lib_dep, amount) in existing: continue
                
                current_num_dep += 1
                creator_name = (e.get('creator') or {}).get('full_name', SYNC_USER_NAME)[:20]
                sql = """INSERT INTO fdepense 
                         (TYPE, MODREG, BANQUE, NUMPCE, DATPCE, CODDEP, NUMDEP, MONTANT, LIBDEP, DATDEP, NUM, UTIL)
                         VALUES (0, 'Espèces', '', '', %s, %s, %s, %s, %s, %s, 0, %s)"""
                cursor.execute(sql, (e['date'], CODDEP_EXPENSES, current_num_dep, amount, lib_dep, e['date'], creator_name))
                inserted += 1
            
            # Process Payments & Primes (SKIPPED)
            # User request: "dont sync prime page".
            
            # Process Loans (CODDEP = 1)
            for l in loans:
                emp = l.get('employee') or {}
                emp_name = f"{emp.get('first_name', '')} {emp.get('last_name', '')}".strip()
                note = l.get('notes', '') or 'Prêt'
                
                lib_dep = f"{emp_name} - {note}"
                lib_dep = lib_dep[:45]
                amount = float(l['principal'])
                date_str = str(l['start_date'])
                
                if (date_str, lib_dep, amount) in existing: continue
                
                current_num_dep += 1
                creator_name = (l.get('creator') or {}).get('full_name', SYNC_USER_NAME)[:20]
                sql = """INSERT INTO fdepense 
                         (TYPE, MODREG, BANQUE, NUMPCE, DATPCE, CODDEP, NUMDEP, MONTANT, LIBDEP, DATDEP, NUM, UTIL)
                         VALUES (0, 'Espèces', '', '', %s, %s, %s, %s, %s, %s, 0, %s)"""
                cursor.execute(sql, (l['start_date'], CODDEP_DEPOSITS, current_num_dep, amount, lib_dep, l['start_date'], creator_name))
                inserted += 1

            connection.commit()
            
            if inserted > 0:
                print(f"[{datetime.now():%H:%M:%S}] [OK] Synced {inserted} new records to MySQL")
                # Backup is now handled separately by backup_db.py
            else:
                print(f"[{datetime.now():%H:%M:%S}] [INFO] All records already synced")
            
            return inserted
            
    except pymysql.MySQLError as e:
        print(f"[{datetime.now():%H:%M:%S}] [ERROR] MySQL Error: {e}")
        return 0
    except Exception as e:
        print(f"[{datetime.now():%H:%M:%S}] [ERROR] Error: {e}")
        return 0
    finally:
        if 'connection' in locals() and connection:
            connection.close()


def run_sync():
    """Run one sync cycle."""
    print(f"\n{'='*50}")
    print(f"[{datetime.now():%H:%M:%S}] Starting sync cycle...")
    
    token = login_to_cloud()
    if not token:
        return
    
    deposits, expenses, payments, loans = get_unsynced_data(token)
    # Pass token to sync function so it can use it for backup
    sync_to_local_mysql(token, deposits, expenses, payments, loans)



def ping_server():
    """Ping health endpoint to keep server awake."""
    try:
        url = f"{CLOUD_API_URL}/api/health"
        requests.get(url, timeout=10)
        print(f"[{datetime.now():%H:%M:%S}] [KEEP-ALIVE] Ping sent to keep server awake.")
    except Exception:
        pass  # Fail silently, main sync handles errors


def main():
    """Main entry point - runs continuous sync loop."""
    print("=" * 60)
    print("  Bijouterie Zaher - Automatic Local Sync Agent")
    print("=" * 60)
    print(f"  Cloud API: {CLOUD_API_URL}")
    print(f"  Local DB:  {LOCAL_DB_SCHEMA}@{LOCAL_DB_HOST}")
    print(f"  Sync as:   (Dynamically detected via API)")
    print(f"  Mode:      KEEP-ALIVE ENABLED (2 min interval)")
    print("=" * 60)
    print("\nPress Ctrl+C to stop\n")
    
    # Run initial sync
    run_sync()
    
    # Continuous loop - sync every 2 minutes to keep Render awake
    try:
        while True:
            print(f"\n[{datetime.now():%H:%M:%S}] Waiting 2 minutes until next sync...")
            time.sleep(120)  # 2 minutes (prevents Render 15min sleep)
            
            # Send Keep-Alive Ping
            ping_server()
            
            # Run Sync
            run_sync()
    except KeyboardInterrupt:
        print(f"\n\n[{datetime.now():%H:%M:%S}] Sync agent stopped by user.")


if __name__ == "__main__":
    main()
