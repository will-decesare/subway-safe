from dependencies import start_db, close_db, time, os
from config import collisions_path, felonies_path, ridership_path, nyct_collisions_path

"""
This script:
1. Sets up a duckdb database called subway.
2. Loads a few raw data files into the database.
"""

def setup_db(conn):
    print('Starting database setup.')
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
    conn.execute("CREATE SCHEMA IF NOT EXISTS analytics")
    return conn

def load_data(conn, file_path):
    def create_table_name(file_path):
        table_name = os.path.splitext(os.path.basename(file_path))[0]
        return table_name
    table_name = create_table_name(file_path)
    query = f"CREATE OR REPLACE TABLE raw.{table_name} AS SELECT * FROM read_csv('{file_path}')"
    try:
        conn.execute(query)
        print(f"Created table: {table_name}")
    except Exception as e:
        print(f"Error creating table {table_name}: {str(e)}")

def main():

    start_time = time.time()

    conn = start_db()
    setup_db(conn)
    load_data(conn, collisions_path)
    load_data(conn, felonies_path)
    load_data(conn, ridership_path)
    load_data(conn, nyct_collisions_path)
    close_db(conn)

    end_time = time.time()
    run_time = end_time - start_time

    print(f"\nScript completed in {run_time:.2f} seconds")

if __name__ == "__main__":
    main()