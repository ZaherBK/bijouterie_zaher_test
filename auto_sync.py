"""
Bijouterie Zaher - Automatic Local Sync Agent
==============================================
This script runs on your PC and automatically syncs data from the cloud website
to your local MySQL database (mah1303.fdepense).

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
CLOUD_API_URL = "https://hr-sync.onrender.com"  # Your hosted website URL
CLOUD_EMAIL = "zaher@local"
CLOUD_PASSWORD = "zah1405"

LOCAL_DB_HOST = "localhost"
LOCAL_DB_USER = "root"
LOCAL_DB_PASSWORD = "6165"
LOCAL_DB_SCHEMA = "mah1303"

SYNC_USER_NAME = "BK ZAHER"  # Name that appears in UTIL column

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
    today_str = date.today().isoformat()
    
    deposits = []
    expenses = []
    
    # Fetch Deposits
    try:
        resp = requests.get(f"{CLOUD_API_URL}/api/deposits/", headers=headers, timeout=30)
        if resp.status_code == 200:
            all_deposits = resp.json()
            deposits = [d for d in all_deposits if d.get('date') == today_str]
            print(f"[{datetime.now():%H:%M:%S}] Found {len(deposits)} deposits for today")
    except Exception as e:
        print(f"[{datetime.now():%H:%M:%S}] Error fetching deposits: {e}")
    
    # Fetch Expenses
    try:
        resp = requests.get(f"{CLOUD_API_URL}/api/expenses/", headers=headers, timeout=30)
        if resp.status_code == 200:
            all_expenses = resp.json()
            expenses = [e for e in all_expenses if e.get('date') == today_str]
            print(f"[{datetime.now():%H:%M:%S}] Found {len(expenses)} expenses for today")
    except Exception as e:
        print(f"[{datetime.now():%H:%M:%S}] Error fetching expenses: {e}")

    # Fetch Payments (New)
    payments = []
    try:
        resp = requests.get(f"{CLOUD_API_URL}/api/pay/", headers=headers, timeout=30)
        if resp.status_code == 200:
            all_payments = resp.json()
            payments = [p for p in all_payments if p.get('date') == today_str]
            print(f"[{datetime.now():%H:%M:%S}] Found {len(payments)} payments for today")
    except Exception as e:
        print(f"[{datetime.now():%H:%M:%S}] Error fetching payments: {e}")

    # Fetch Loans (New)
    loans = []
    try:
        resp = requests.get(f"{CLOUD_API_URL}/api/loans/", headers=headers, timeout=30)
        if resp.status_code == 200:
            all_loans = resp.json()
            # Filter by start_date == today
            loans = [l for l in all_loans if l.get('start_date') == today_str]
            print(f"[{datetime.now():%H:%M:%S}] Found {len(loans)} loans for today")
        else:
            print(f"[{datetime.now():%H:%M:%S}] [ERROR] Loans API Error: {resp.status_code} {resp.text}")
    except Exception as e:
        print(f"[{datetime.now():%H:%M:%S}] Error fetching loans: {e}")
    
    return deposits, expenses, payments, loans


def get_existing_numdeps(cursor):
    """Get existing NUMDEP values to avoid duplicates."""
    cursor.execute("SELECT NUMDEP FROM fdepense ORDER BY NUMDEP DESC LIMIT 200")
    return set(row['NUMDEP'] for row in cursor.fetchall())


def sync_to_local_mysql(deposits, expenses, payments, loans):
    """Insert data into local MySQL fdepense table."""
    if not deposits and not expenses and not payments and not loans:
        print(f"[{datetime.now():%H:%M:%S}] Nothing to sync")
        return 0
    
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
            
            # Get existing to avoid duplicates (check by LIBDEP + DATDEP + MONTANT)
            today_str = date.today().strftime('%Y-%m-%d')
            cursor.execute(
                "SELECT LIBDEP, MONTANT FROM fdepense WHERE DATDEP = %s",
                (today_str,)
            )
            existing = set((row['LIBDEP'], float(row['MONTANT'])) for row in cursor.fetchall())
            
            inserted = 0
            
            # Process Deposits (CODDEP = 2)
            for d in deposits:
                emp = d.get('employee') or {}
                emp_name = f"{emp.get('first_name', '')} {emp.get('last_name', '')}".strip()
                note = d.get('note', '')
                if note:
                    lib_dep = f"{emp_name} - {note}"
                else:
                    lib_dep = emp_name
                
                lib_dep = lib_dep[:45]  # LIBDEP max 45 chars
                amount = float(d['amount'])
                
                # Skip if already exists
                if (lib_dep, amount) in existing:
                    continue
                
                current_num_dep += 1
                # TYPE=0, MODREG='Espèces', BANQUE='', NUMPCE='', DATPCE=DATDEP, CODDEP=2, NUMDEP=n+1, MONTANT=amount, LIBDEP=note, DATDEP=date, NUM=0, UTIL=user
                sql = """INSERT INTO fdepense 
                         (TYPE, MODREG, BANQUE, NUMPCE, DATPCE, CODDEP, NUMDEP, MONTANT, LIBDEP, DATDEP, NUM, UTIL)
                         VALUES (0, 'Espèces', '', '', %s, %s, %s, %s, %s, %s, 0, %s)"""
                cursor.execute(sql, (d['date'], CODDEP_DEPOSITS, current_num_dep, amount, lib_dep, d['date'], SYNC_USER_NAME[:20]))
                inserted += 1
            
            # Process Expenses (CODDEP = 1)
            for e in expenses:
                lib_dep = e.get('description', 'Dépense')[:45]
                amount = float(e['amount'])
                
                # Skip if already exists
                if (lib_dep, amount) in existing:
                    continue
                
                current_num_dep += 1
                # Same mapping for expenses
                sql = """INSERT INTO fdepense 
                         (TYPE, MODREG, BANQUE, NUMPCE, DATPCE, CODDEP, NUMDEP, MONTANT, LIBDEP, DATDEP, NUM, UTIL)
                         VALUES (0, 'Espèces', '', '', %s, %s, %s, %s, %s, %s, 0, %s)"""
                cursor.execute(sql, (e['date'], CODDEP_EXPENSES, current_num_dep, amount, lib_dep, e['date'], SYNC_USER_NAME[:20]))
                inserted += 1
            
            # Process Payments & Primes (CODDEP = 2)
            for p in payments:
                emp = p.get('employee') or {}
                emp_name = f"{emp.get('first_name', '')} {emp.get('last_name', '')}".strip()
                note = p.get('note', '') or ''
                if p.get('pay_type') == 'prime_rendement':
                     note = f"Prime: {note}"
                
                if note:
                    lib_dep = f"{emp_name} - {note}"
                else:
                    lib_dep = emp_name
                
                lib_dep = lib_dep[:45]
                amount = float(p['amount'])
                
                if (lib_dep, amount) in existing: continue
                
                current_num_dep += 1
                sql = """INSERT INTO fdepense 
                         (TYPE, MODREG, BANQUE, NUMPCE, DATPCE, CODDEP, NUMDEP, MONTANT, LIBDEP, DATDEP, NUM, UTIL)
                         VALUES (0, 'Espèces', '', '', %s, %s, %s, %s, %s, %s, 0, %s)"""
                cursor.execute(sql, (p['date'], CODDEP_EXPENSES, current_num_dep, amount, lib_dep, p['date'], SYNC_USER_NAME[:20]))
                inserted += 1

            # Process Loans (CODDEP = 1)
            for l in loans:
                emp = l.get('employee') or {}
                emp_name = f"{emp.get('first_name', '')} {emp.get('last_name', '')}".strip()
                note = l.get('notes', '') or 'Prêt'
                
                lib_dep = f"{emp_name} - {note}"
                lib_dep = lib_dep[:45]
                amount = float(l['principal'])
                
                if (lib_dep, amount) in existing: continue
                
                current_num_dep += 1
                sql = """INSERT INTO fdepense 
                         (TYPE, MODREG, BANQUE, NUMPCE, DATPCE, CODDEP, NUMDEP, MONTANT, LIBDEP, DATDEP, NUM, UTIL)
                         VALUES (0, 'Espèces', '', '', %s, %s, %s, %s, %s, %s, 0, %s)"""
                cursor.execute(sql, (l['start_date'], CODDEP_DEPOSITS, current_num_dep, amount, lib_dep, l['start_date'], SYNC_USER_NAME[:20]))
                inserted += 1

            connection.commit()
            
            if inserted > 0:
                print(f"[{datetime.now():%H:%M:%S}] [OK] Synced {inserted} new records to MySQL")
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
    sync_to_local_mysql(deposits, expenses, payments, loans)


def main():
    """Main entry point - runs continuous sync loop."""
    print("=" * 60)
    print("  Bijouterie Zaher - Automatic Local Sync Agent")
    print("=" * 60)
    print(f"  Cloud API: {CLOUD_API_URL}")
    print(f"  Local DB:  {LOCAL_DB_SCHEMA}@{LOCAL_DB_HOST}")
    print(f"  Sync as:   {SYNC_USER_NAME}")
    print("=" * 60)
    print("\nPress Ctrl+C to stop\n")
    
    # Run initial sync
    run_sync()
    
    # Continuous loop - sync every 5 minutes
    try:
        while True:
            print(f"\n[{datetime.now():%H:%M:%S}] Waiting 5 minutes until next sync...")
            time.sleep(300)  # 5 minutes
            run_sync()
    except KeyboardInterrupt:
        print("\n\n[{datetime.now():%H:%M:%S}] Sync agent stopped by user.")


if __name__ == "__main__":
    main()
