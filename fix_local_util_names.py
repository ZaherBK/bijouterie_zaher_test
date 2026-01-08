import pymysql

# CONFIGURATION
LOCAL_DB_HOST = "localhost"
LOCAL_DB_USER = "root"
LOCAL_DB_PASSWORD = "6165"
LOCAL_DB_SCHEMA = "mah1303"

OLD_NAME = "Zaher (Admin)"
NEW_NAME = "BK ZAHER"

def fix_local_names():
    print(f"Connecting to MySQL ({LOCAL_DB_SCHEMA})...")
    try:
        connection = pymysql.connect(
            host=LOCAL_DB_HOST,
            user=LOCAL_DB_USER,
            password=LOCAL_DB_PASSWORD,
            database=LOCAL_DB_SCHEMA,
            charset='latin1'
        )
        
        with connection.cursor() as cursor:
            # Check for records with the old name (check first 20 chars as UTIL is varchar(20))
            old_name_search = OLD_NAME[:20]
            new_name_update = NEW_NAME[:20]
            
            print(f"Checking for records with UTIL='{old_name_search}'...")
            cursor.execute("SELECT COUNT(*) FROM fdepense WHERE UTIL = %s", (old_name_search,))
            count = cursor.fetchone()[0]
            
            if count > 0:
                print(f"Found {count} records. Updating to '{new_name_update}'...")
                cursor.execute(
                    "UPDATE fdepense SET UTIL = %s WHERE UTIL = %s",
                    (new_name_update, old_name_search)
                )
                connection.commit()
                print(f"Successfully updated {cursor.rowcount} records.")
            else:
                print("No records found with old username.")
                
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'connection' in locals() and connection:
            connection.close()

if __name__ == "__main__":
    fix_local_names()
