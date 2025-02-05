import duckdb
import os
import time

db_name = 'collisions'
file_path = '/Users/willdecesare/Documents/GitHub/subway-safe/Collisions_20250205.csv'

def start_db():
    print('Starting database connection.')
    conn = duckdb.connect(f'{db_name}.db')
    return conn

def close_db(conn):
    print('Ending database connection.')
    conn.close()

def setup_db(conn):
    print('Starting database setup.')
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
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
    load_data(conn, file_path)
    close_db(conn)

    end_time = time.time()
    run_time = end_time - start_time

    print(f"\nScript completed in {run_time:.2f} seconds")

if __name__ == "__main__":
    main()