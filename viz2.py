from dependencies import duckdb, st, px, pd, datetime
from dependencies import start_db, run_query, close_db, execute_query

def main():
    st.title("DuckDB Explorer - Test Version")
    
    try:
        # Initialize connection
        conn = start_db()
        
        # Get list of tables
        tables = run_query(conn, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'analytics'")
        
        if not tables.empty:
            st.write("Available Tables:")
            st.write(tables)
            
            # Select a table
            selected_table = st.selectbox("Select a table to view", tables['table_name'])
            
            if selected_table:
                # Show table data
                data = run_query(conn, f"SELECT * FROM analytics.{selected_table} LIMIT 10")
                st.write(f"First 10 rows of analytics.{selected_table}:")
                st.write(data)
        else:
            st.warning("No tables found in the database")
            
    except Exception as e:
        st.error(f"Error connecting to database: {str(e)}")

if __name__ == "__main__":
    main()
