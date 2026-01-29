import pymysql
import pymysql.cursors

# Config (using current known dev credentials)
LOCAL_DB_HOST = "localhost"
LOCAL_DB_USER = "root"
LOCAL_DB_PASSWORD = "6165"
LOCAL_DB_SCHEMA = "mah1303"

def inspect_table(table_name):
    print(f"\n--- INSPECTING: {table_name} ---")
    try:
        conn = pymysql.connect(
            host=LOCAL_DB_HOST,
            user=LOCAL_DB_USER,
            password=LOCAL_DB_PASSWORD,
            database=LOCAL_DB_SCHEMA,
            charset='latin1'
        )
        with conn.cursor() as cursor:
            cursor.execute(f"DESCRIBE {table_name}")
            columns = cursor.fetchall()
            for col in columns:
                print(col)
            
            # Get a sample row
            print(f"--- SAMPLE ROW ({table_name}) ---")
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 1")
            row = cursor.fetchone()
            print(row)

    except Exception as e:
        print(f"Error inspecting {table_name}: {e}")
    finally:
        if 'conn' in locals(): conn.close()

if __name__ == "__main__":
    inspect_table("detfact")
    inspect_table("detfacts")
    inspect_table("utilisateur") # Check this for mapping 
