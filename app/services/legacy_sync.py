import pymysql
import pymysql.cursors
from datetime import date
from decimal import Decimal
from typing import List, Dict, Any

def sync_deposits_to_legacy(
    host: str,
    schema: str,
    password: str,
    deposits: List[Dict[str, Any]],
    user_name: str,
    coddep_override: int = 2 # Default to 2 for Avances
) -> Dict[str, Any]:
    """
    Connects to the legacy MySQL database and inserts deposit/expense records.
    """
    connection = None
    try:
        connection = pymysql.connect(
            host=host,
            user='root',
            password=password,
            database=schema,
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=5
        )

        with connection.cursor() as cursor:
            # 1. Get the last NUMDEP to increment from
            # We look for the maximum NUMDEP in the table to start incrementing
            cursor.execute("SELECT MAX(NUMDEP) as max_num FROM fdepense")
            result = cursor.fetchone()
            last_num_dep = result['max_num'] if result and result['max_num'] is not None else 0
            
            # Ensure last_num_dep is an integer
            try:
                current_num_dep = int(last_num_dep)
            except ValueError:
                current_num_dep = 0

            inserted_count = 0
            
            for deposit in deposits:
                current_num_dep += 1
                
                # Prepare data based on user rules
                # TYPE: 0
                # MODREG: "Espèces"
                # BANQUE: ""
                # NUMPCE: ""
                # DATPCE: Date of deposit (same as DATDEP)
                # CODDEP: Passed as argument (2 for Avances, 1 for Expenses usually)
                # NUMDEP: Auto-incremented
                # MONTANT: Deposit amount
                # LIBDEP: Employee Name + Note OR Description
                # DATDEP: Date of deposit
                # NUM: 0
                # UTIL: User name
                
                # Handle both Deposit (employee_name) and Expense (description) structures
                if "employee_name" in deposit:
                    lib_dep = deposit.get("employee_name", "Inconnu")
                    note = deposit.get("note", "")
                    if note:
                        lib_dep += f" - {note}"
                else:
                    lib_dep = deposit.get("description", "Dépense")
                
                # Truncate LIBDEP if necessary (assuming 100 chars limit, adjust if known)
                lib_dep = lib_dep[:100]

                sql = """
                    INSERT INTO fdepense 
                    (TYPE, MODREG, BANQUE, NUMPCE, DATPCE, CODDEP, NUMDEP, MONTANT, LIBDEP, DATDEP, NUM, UTIL)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                values = (
                    0,                  # TYPE
                    "Espèces",          # MODREG
                    "",                 # BANQUE
                    "",                 # NUMPCE
                    deposit['date'],    # DATPCE
                    coddep_override,    # CODDEP
                    current_num_dep,    # NUMDEP
                    deposit['amount'],  # MONTANT
                    lib_dep,            # LIBDEP
                    deposit['date'],    # DATDEP
                    0,                  # NUM
                    user_name[:20]      # UTIL (truncated to 20 chars)
                )
                
                cursor.execute(sql, values)
                inserted_count += 1

            connection.commit()
            return {"success": True, "count": inserted_count, "message": f"Successfully synced {inserted_count} records."}

    except pymysql.MySQLError as e:
        print(f"MySQL Error: {e}")
        return {"success": False, "message": f"Database Error: {str(e)}"}
    except Exception as e:
        print(f"General Error: {e}")
        return {"success": False, "message": f"Error: {str(e)}"}
    finally:
        if connection:
            connection.close()
