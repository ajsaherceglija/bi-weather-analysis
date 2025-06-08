import pyodbc
import re

with open("task_log.txt", "a") as log:
    log.write("Script ran via Task Scheduler\n")


conn_str = (
    r'DRIVER={ODBC Driver 17 for SQL Server};'
    r'SERVER=DESKTOP-3IAQDFD\SQLEXPRESS;'
    r'DATABASE=geo_climate_dw;'
    r'Trusted_Connection=yes;'
)

LOAD_STG_SQL = r'C:\Users\User\Desktop\BI\sql_scripts\load_stg.sql'
LOAD_RAW_FULL_SQL = r'C:\Users\User\Desktop\BI\sql_scripts\load_raw.sql'
LOAD_RAW_INCR_SQL = r'C:\Users\User\Desktop\BI\sql_scripts\incremental_load.sql'
LOAD_STAR_SQL = r'C:\Users\User\Desktop\BI\sql_scripts\load_star.sql'

def run_sql_script(cursor, conn, file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        sql_script = f.read()

    # Robust split on "GO" (case-insensitive, line by itself)
    commands = [cmd.strip() for cmd in re.split(r'^\s*GO\s*$', sql_script, flags=re.MULTILINE)]

    for i, command in enumerate(commands):
        if command:
            print(f"Running command {i+1} from {file_path}...")
            try:
                cursor.execute(command)
            except Exception as e:
                print(f"Error executing command {i+1}: {e}")

    conn.commit()

def raw_table_exists(cursor):
    cursor.execute("""
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = 'dwh_raw' 
          AND TABLE_NAME = 'air_quality_categories'
    """)
    return cursor.fetchone()[0] == 1

def main():
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        print("Starting full load into staging layer...")
        run_sql_script(cursor, conn, LOAD_STG_SQL)
        print("Staging layer loaded.")

        if not raw_table_exists(cursor):
            print("Raw tables empty or do not exist. Running initial full load on raw...")
            run_sql_script(cursor, conn, LOAD_RAW_FULL_SQL)
        else:
            print("Raw tables exist. Running incremental load on raw...")
            run_sql_script(cursor, conn, LOAD_RAW_INCR_SQL)

        print("Loading star schema...")
        run_sql_script(cursor, conn, LOAD_STAR_SQL)
        print("Star schema loaded.")

    except Exception as e:
        print("Error:", e)
    finally:
        cursor.close()
        conn.close()

if __name__ == '__main__':
    main()
