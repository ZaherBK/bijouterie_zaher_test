import requests
import pymysql
import pymysql.cursors
from datetime import date
import getpass
import sys

# Configuration
CLOUD_API_URL = "https://hr-sync.onrender.com"  # URL of your hosted site
LOCAL_DB_HOST = "localhost"
LOCAL_DB_USER = "root"
LOCAL_DB_PASSWORD = "your_password" # User will input this
LOCAL_DB_SCHEMA = "mah1303" # User will input this

def get_daily_data(token):
    """Fetch today's deposits and expenses from the Cloud API."""
    headers = {"Authorization": f"Bearer {token}"}
    
    # 1. Fetch Deposits
    print("Fetching deposits...")
    try:
        resp = requests.get(f"{CLOUD_API_URL}/api/deposits/", headers=headers)
        if resp.status_code == 200:
            deposits = [d for d in resp.json() if d['date'] == date.today().isoformat()]
        else:
            print(f"Error fetching deposits: {resp.text}")
            deposits = []
    except Exception as e:
        print(f"Network error fetching deposits: {e}")
        deposits = []

    # 2. Fetch Expenses
    print("Fetching expenses...")
    try:
        resp = requests.get(f"{CLOUD_API_URL}/api/expenses/", headers=headers)
        if resp.status_code == 200:
            expenses = [e for e in resp.json() if e['date'] == date.today().isoformat()]
        else:
            print(f"Error fetching expenses: {resp.text}")
            expenses = []
    except Exception as e:
        print(f"Network error fetching expenses: {e}")
        expenses = []
        
    return deposits, expenses

def sync_to_local_db(deposits, expenses, db_password, db_schema):
    """Insert data into local MySQL."""
    try:
        connection = pymysql.connect(
            host=LOCAL_DB_HOST,
            user=LOCAL_DB_USER,
            password=db_password,
            database=db_schema,
            cursorclass=pymysql.cursors.DictCursor
        )
        
        with connection.cursor() as cursor:
            # Get last NUMDEP
            cursor.execute("SELECT MAX(NUMDEP) as max_num FROM fdepense")
            result = cursor.fetchone()
            last_num_dep = result['max_num'] if result and result['max_num'] is not None else 0
            current_num_dep = int(last_num_dep)
            
            count = 0
            
            # Process Deposits (CODDEP = 2)
            for d in deposits:
                current_num_dep += 1
                count += 1
                lib_dep = f"{d['employee']['first_name']} {d['employee']['last_name']}"
                if d.get('note'): lib_dep += f" - {d['note']}"
                
                sql = """INSERT INTO fdepense (TYPE, MODREG, BANQUE, NUMPCE, DATPCE, CODDEP, NUMDEP, MONTANT, LIBDEP, DATDEP, NUM, UTIL)
                         VALUES (0, 'Espèces', '', '', %s, 2, %s, %s, %s, %s, 0, 'SyncAgent')"""
                cursor.execute(sql, (d['date'], current_num_dep, d['amount'], lib_dep[:100], d['date']))

            # Process Expenses (CODDEP = 1)
            for e in expenses:
                current_num_dep += 1
                count += 1
                lib_dep = e['description']
                
                sql = """INSERT INTO fdepense (TYPE, MODREG, BANQUE, NUMPCE, DATPCE, CODDEP, NUMDEP, MONTANT, LIBDEP, DATDEP, NUM, UTIL)
                         VALUES (0, 'Espèces', '', '', %s, 1, %s, %s, %s, %s, 0, 'SyncAgent')"""
                cursor.execute(sql, (e['date'], current_num_dep, e['amount'], lib_dep[:100], e['date']))

            connection.commit()
            print(f"✅ Successfully synced {count} records to local database '{db_schema}'.")

    except Exception as e:
        print(f"❌ Database Error: {e}")
    finally:
        if 'connection' in locals() and connection:
            connection.close()

def main():
    print("=== Bijouterie Zaher - Local Sync Agent ===")
    print("This script syncs today's data from the Cloud to your Local PC.")
    
    email = input("Cloud Email (zaher@local): ") or "zaher@local"
    password = getpass.getpass("Cloud Password: ")
    
    # Login
    try:
        resp = requests.post(f"{CLOUD_API_URL}/api/auth/login", data={"username": email, "password": password})
        if resp.status_code != 200:
            print("❌ Login failed.")
            return
        token = resp.json()["access_token"]
        print("✅ Login successful.")
    except Exception as e:
        print(f"❌ Could not connect to cloud server: {e}")
        return

    # Get Data
    deposits, expenses = get_daily_data(token)
    print(f"Found {len(deposits)} deposits and {len(expenses)} expenses for today.")
    
    if not deposits and not expenses:
        print("Nothing to sync.")
        return

    # Local DB Info
    db_schema = input(f"Local DB Schema ({LOCAL_DB_SCHEMA}): ") or LOCAL_DB_SCHEMA
    db_password = getpass.getpass("Local DB Password: ")
    
    sync_to_local_db(deposits, expenses, db_password, db_schema)
    input("Press Enter to exit...")

if __name__ == "__main__":
    main()
