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
                data = run_query(conn, f"SELECT * FROM analytics.{selected_table} LIMIT 100")
                
                # Convert date_month column to datetime
                if "date_month" in data.columns:
                    data["date_month"] = pd.to_datetime(data["date_month"], errors="coerce")
                
                st.write(f"First 100 rows of analytics.{selected_table}:")
                st.write(data)
                
                # Visualization options
                st.subheader("Data Visualization")
                chart_type = st.selectbox("Select Chart Type", ["Bar Chart", "Stacked Bar Chart", "Line Chart"])
                
                # Select columns for visualization
                numeric_columns = data.select_dtypes(include=['number']).columns.tolist()
                categorical_columns = data.select_dtypes(include=['object', 'category']).columns.tolist()
                datetime_columns = data.select_dtypes(include=['datetime']).columns.tolist()
                
                all_x_options = categorical_columns + datetime_columns
                
                if numeric_columns and all_x_options:
                    x_axis = st.selectbox("Select X-axis", all_x_options)
                    y_axis = st.selectbox("Select Y-axis", numeric_columns)
                    
                    if chart_type == "Bar Chart":
                        fig = px.bar(data, x=x_axis, y=y_axis, title="Bar Chart")
                    elif chart_type == "Stacked Bar Chart":
                        color_col = st.selectbox("Select category for stacking", categorical_columns)
                        fig = px.bar(data, x=x_axis, y=y_axis, color=color_col, title="Stacked Bar Chart", barmode="stack")
                    elif chart_type == "Line Chart":
                        fig = px.line(data, x=x_axis, y=y_axis, title="Line Chart")
                    
                    st.plotly_chart(fig)
                else:
                    st.warning("Insufficient numeric or categorical data for visualization.")
        else:
            st.warning("No tables found in the database")
            
    except Exception as e:
        st.error(f"Error connecting to database: {str(e)}")

if __name__ == "__main__":
    main()
