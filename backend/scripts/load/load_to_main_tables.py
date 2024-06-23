import psycopg2

def get_sql_script(path):
    # Read the SQL file
    with open(path, 'r') as file:
        sql_script = file.read()

    if not sql_script:
        return []

    return sql_script.split(';')

def load_to_main_tables():
    """"UPSERT AGGREGATED DATA FROM TEMP TABLES INTO MAIN TABLES"""

    path = "/home/ngocthang/Documents/Code/Stock-Company-Analysis/backend/scripts/load/sql-scripts/upsert.sql"
    statements = get_sql_script(path)
    # connect to postgreSQL and perform upserting operations
    conn = psycopg2.connect(
        host="localhost",
        database="testStock",
        user="postgres",
        password="12345678"
    )
    cur = conn.cursor()

    for statement in statements:
        cur.execute(statement)
        conn.commit()

    # truncate temp tables after finishing loading operations

    for table in ['temp_companies', 'temp_regions', 'temp_exchanges', 'temp_sic_industries', 'temp_exchanges']:
        cur.execute(f"TRUNCATE TABLE {table}")
        conn.commit()
        

    cur.close()
    conn.close()

    