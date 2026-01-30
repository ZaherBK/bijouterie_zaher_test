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
CLOUD_EMAIL = "zied@local.com"
CLOUD_PASSWORD = "0"

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


def detect_local_dbs():
    """Detect ALL available store DBs (Ariana/mah1303, Nabeul/inv)."""
    options = [
        {"schema": "mah1303", "name": "Ariana", "pass": "6165"},
        {"schema": "inv", "name": "Nabeul", "pass": "6165"},
    ]
    
    found_dbs = []
    
    for opt in options:
        try:
            conn = pymysql.connect(
                host=LOCAL_DB_HOST,
                user=LOCAL_DB_USER,
                password=opt["pass"],
                database=opt["schema"],
                connect_timeout=2
            )
            conn.close()
            print(f"[{datetime.now():%H:%M:%S}] [INFO] Detected Store: {opt['name']} (DB: {opt['schema']})")
            found_dbs.append( (opt["schema"], opt["name"], opt["pass"]) )
        except:
            continue
            
    if not found_dbs:
        print(f"[{datetime.now():%H:%M:%S}] [WARN] No known database found. Defaulting to Schema: {LOCAL_DB_SCHEMA}")
        return [(LOCAL_DB_SCHEMA, "Unknown", LOCAL_DB_PASSWORD)]
        
    return found_dbs


# Global flag to ensure history is synced only once per restart
HAS_SYNCED_HISTORY = False

def sync_sales_data(token, db_schema, db_pass, store_name, full_history=False):
    """
    Calculate and Sync Sales Performance.
    If full_history=True, syncs EVERYTHING from the beginning of time.
    If full_history=False, syncs only TODAY and YESTERDAY (incremental).
    """
    mode_str = "FULL HISTORY" if full_history else "Recent (48h)"
    print(f"[{datetime.now():%H:%M:%S}] Checking Sales Data for {store_name} [{mode_str}]...")
    
    headers = {"Authorization": f"Bearer {token}"}
    
    try:
        connection = pymysql.connect(
            host=LOCAL_DB_HOST,
            user=LOCAL_DB_USER,
            password=db_pass,
            database=db_schema,
            cursorclass=pymysql.cursors.DictCursor,
            charset='latin1'
        )
        
        payload_list = []
        
        with connection.cursor() as cursor:
            # OPTIMIZED QUERY: Count Invoices (QTY) and Sum Revenue (REV)
            # "Quantity" = Number of Invoices (Factures)
            
            if full_history:
                # Sync ALL TIME
                sql = """
                    SELECT DATE(DEDATE) as sdate, UTILISATEUR, COUNT(*) as qty, SUM(TOTALPRIX) as rev 
                    FROM facture 
                    GROUP BY sdate, UTILISATEUR
                """
                cursor.execute(sql)
            else:
                # Sync Only Last 48h
                dates_to_sync = [date.today(), date.today() - timedelta(days=1)]
                
                # Using a single query with WHERE IN for efficiency
                date_strs = [d.isoformat() for d in dates_to_sync]
                format_strings = ','.join(['%s'] * len(date_strs))
                
                sql = f"""
                    SELECT DATE(DEDATE) as sdate, UTILISATEUR, COUNT(*) as qty, SUM(TOTALPRIX) as rev 
                    FROM facture 
                    WHERE DATE(DEDATE) IN ({format_strings})
                    GROUP BY sdate, UTILISATEUR
                """
                cursor.execute(sql, tuple(date_strs))
            
            # Process Results
            for row in cursor.fetchall():
                u = row['UTILISATEUR']
                d_val = row['sdate']
                
                if not u or not d_val: continue
                
                # Normalize Date
                if isinstance(d_val, bytes): d_str = d_val.decode('utf-8')[:10]
                elif isinstance(d_val, str): d_str = str(d_val)[:10]
                elif isinstance(d_val, datetime): d_str = d_val.date().isoformat()
                elif isinstance(d_val, date): d_str = d_val.isoformat()
                else: d_str = str(d_val)[:10]

                payload_list.append({
                    "date": d_str,
                    "local_user_name": u,
                    "store_name": store_name,
                    "quantity_sold": int(row['qty']), 
                    "total_revenue": float(row['rev'])
                })
        
        connection.close()
        
        if payload_list:
            # Upload to Cloud in Chunks (to avoid timeout)
            BATCH_SIZE = 100
            total_records = len(payload_list)
            print(f"[{datetime.now():%H:%M:%S}] Found {total_records} sales records for {store_name}. Syncing in batches of {BATCH_SIZE}...")
            
            for i in range(0, total_records, BATCH_SIZE):
                batch = payload_list[i:i + BATCH_SIZE]
                try:
                    resp = requests.post(f"{CLOUD_API_URL}/api/sales/sync", json=batch, headers=headers, timeout=60)
                    if resp.status_code == 201:
                        print(f"  > Batch {i//BATCH_SIZE + 1} synced ({len(batch)} records).")
                    else:
                        print(f"  [ERROR] Batch {i//BATCH_SIZE + 1} Failed: {resp.text}")
                except Exception as e:
                     print(f"  [ERROR] Batch {i//BATCH_SIZE + 1} Error: {e}")

            print(f"[{datetime.now():%H:%M:%S}] [OK] Sales Sync Complete for {store_name}.")
        else:
            print(f"[{datetime.now():%H:%M:%S}] No sales data found for {store_name}.")

    except Exception as e:
        print(f"[{datetime.now():%H:%M:%S}] [ERROR] Sales Sync Error for {store_name}: {e}")



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
        else:
            print(f"[{datetime.now():%H:%M:%S}] [ERROR] Expenses API Error: {resp.status_code} {resp.text}")
    except Exception as e:
        print(f"[{datetime.now():%H:%M:%S}] Error fetching expenses: {e}")

    # Fetch Loans (New)
    try:
        resp = requests.get(f"{CLOUD_API_URL}/api/loans/", headers=headers, timeout=30)
        if resp.status_code == 200:
            all_loans = resp.json()
            # MODIFIED: Check last 30 days (filter by start_date)
            loans = [l for l in all_loans if l.get('start_date') and l.get('start_date') >= start_date_str]
        else:
            print(f"[{datetime.now():%H:%M:%S}] [ERROR] Loans API Error: {resp.status_code} {resp.text}")
    except Exception as e:
        print(f"[{datetime.now():%H:%M:%S}] Error fetching loans: {e}")
    
    return deposits, expenses, payments, loans


def sync_to_local_mysql(token, deposits, expenses, payments, loans, db_schema, db_pass):
    """Insert data into local MySQL fdepense table."""
    if not deposits and not expenses and not payments and not loans:
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
            password=db_pass,
            database=db_schema,
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=5,
            charset='latin1'
        )
        
        with connection.cursor() as cursor:
            # Get last NUMDEP
            cursor.execute("SELECT MAX(NUMDEP) as max_num FROM fdepense")
            result = cursor.fetchone()
            current_num_dep = int(result['max_num']) if result and result['max_num'] else 0
            
            # Get existing records for involved dates ONLY
            format_strings = ','.join(['%s'] * len(involved_dates))
            query = f"SELECT DATDEP, LIBDEP, MONTANT FROM fdepense WHERE DATDEP IN ({format_strings})"
            cursor.execute(query, tuple(involved_dates))
            
            # Key: (date_str, libelle, amount)
            existing = set()
            for row in cursor.fetchall():
                d_val = row['DATDEP']
                if isinstance(d_val, datetime): d_str = d_val.date().isoformat()
                elif isinstance(d_val, date): d_str = d_val.isoformat()
                else: d_str = str(d_val)[:10]
                
                l_str = str(row['LIBDEP']) if row['LIBDEP'] else ''
                try: m_val = float(row['MONTANT'])
                except: m_val = 0.0
                
                existing.add((d_str, l_str, m_val))
            
            all_items = []
            for d in deposits: all_items.append({'type': 'deposit', 'date': d['date'], 'data': d})
            for e in expenses: all_items.append({'type': 'expense', 'date': e['date'], 'data': e})
            for l in loans: all_items.append({'type': 'loan', 'date': l['start_date'], 'data': l})
            
            all_items.sort(key=lambda x: x['date'])

            for item in all_items:
                data = item['data']
                itype = item['type']
                
                if itype == 'deposit':
                    emp = data.get('employee') or {}
                    emp_name = f"{emp.get('first_name', '')} {emp.get('last_name', '')}".strip()
                    note = data.get('note', '')
                    lib_dep = f"{emp_name} - {note}" if note else emp_name
                    amount = float(data['amount'])
                    coddep = CODDEP_DEPOSITS
                elif itype == 'expense':
                    lib_dep = data.get('description', 'Dépense')
                    amount = float(data['amount'])
                    coddep = CODDEP_EXPENSES
                elif itype == 'loan':
                    emp = data.get('employee') or {}
                    emp_name = f"{emp.get('first_name', '')} {emp.get('last_name', '')}".strip()
                    note = data.get('notes', '') or 'Prêt'
                    lib_dep = f"{emp_name} - {note}"
                    amount = float(data['principal'])
                    coddep = CODDEP_DEPOSITS

                lib_dep = lib_dep[:45]
                date_str = str(item['date'])
                
                if (date_str, lib_dep, amount) in existing:
                    continue
                
                print(f"  [INSERT] {db_schema}: {date_str} | {amount} | {lib_dep}")
                current_num_dep += 1
                creator_name = (data.get('creator') or {}).get('full_name', SYNC_USER_NAME)[:20]
                
                sql = """INSERT INTO fdepense 
                         (TYPE, MODREG, BANQUE, NUMPCE, DATPCE, CODDEP, NUMDEP, MONTANT, LIBDEP, DATDEP, NUM, UTIL)
                         VALUES (0, 'Espèces', '', '', %s, %s, %s, %s, %s, %s, 0, %s)"""
                cursor.execute(sql, (date_str, coddep, current_num_dep, amount, lib_dep, date_str, creator_name))
                inserted += 1

            connection.commit()
            if inserted > 0:
                print(f"[{datetime.now():%H:%M:%S}] [OK] Synced {inserted} new records to {db_schema}")
            
            return inserted
            
    except Exception as e:
        print(f"[{datetime.now():%H:%M:%S}] [ERROR] Sync to {db_schema} Error: {e}")
        return 0
    finally:
        if 'connection' in locals() and connection:
            connection.close()


def run_sync():
    """Run one sync cycle for ALL detected stores."""
    global HAS_SYNCED_HISTORY
    
    print(f"\n{'='*50}")
    print(f"[{datetime.now():%H:%M:%S}] Starting sync cycle...")
    
    # 1. Detect ALL Stores
    available_dbs = detect_local_dbs()
    
    token = login_to_cloud()
    if not token:
        return
    
    # 2. Sync Syncable Data (Deposits/Expenses) -> Local DB
    # Fetch Once, Apply to All detected DBs (Assuming manager/admin wants data everywhere)
    deposits, expenses, payments, loans = get_unsynced_data(token)
    
    for db_schema, store_name, db_pass in available_dbs:
        print(f"\n--- Processing Store: {store_name} ({db_schema}) ---")
        
        # Sync DOWN (Cloud -> Local)
        sync_to_local_mysql(token, deposits, expenses, payments, loans, db_schema, db_pass)
    
        # 3. Sync Sales Data (Local DB -> Cloud)
        if not HAS_SYNCED_HISTORY:
            print(f"[{datetime.now():%H:%M:%S}] [INIT] {store_name}: Performing ONE-TIME Full History Sync...")
            sync_sales_data(token, db_schema, db_pass, store_name, full_history=True)
        else:
            # Standard Incremental Sync
            sync_sales_data(token, db_schema, db_pass, store_name, full_history=False)
            
    # Mark global history sync as done after first full loop
    if not HAS_SYNCED_HISTORY:
        HAS_SYNCED_HISTORY = True


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
    print(f"  History:   FULL SYNC on first cycle")
    print("=" * 60)
    print("\nPress Ctrl+C to stop\n")
    
    # Run initial sync (Will trigger Full History)
    run_sync()
    
    # Continuous loop - sync every 2 minutes to keep Render awake
    try:
        while True:
            print(f"\n[{datetime.now():%H:%M:%S}] Waiting 2 minutes until next sync...")
            time.sleep(120)  # 2 minutes (prevents Render 15min sleep)
            
            # Send Keep-Alive Ping
            ping_server()
            
            # Run Sync (Will be incremental now)
            run_sync()
    except KeyboardInterrupt:
        print(f"\n\n[{datetime.now():%H:%M:%S}] Sync agent stopped by user.")


if __name__ == "__main__":
    main()
