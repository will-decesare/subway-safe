from dependencies import duckdb, st, px, pd, datetime
from dependencies import start_db, run_query, close_db, execute_query

def main():
    st.title("NYC Subway Safe App")
    st.write("""The following dashboard ingests and models data from the following sources:  
             \n * MTA Open Data Team: subway and bus [ridership](https://metrics.mta.info/?ridership/daybydayridershipnumbers)
             and [injuries from transport](https://metrics.mta.info/?subway/safety).
             \n * NYC Open Data Team: [collisions (cars, bikes, pedestrians)](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Person/f55k-p6yu/about_data)
             and [crime incidents (subway and bus)](https://data.ny.gov/Transportation/MTA-Major-Felonies/yeek-jhmu/about_data).   
             \n * NYC Department of Transportation's [2022 Citywide Mobility Survey Report](https://www.nyc.gov/html/dot/downloads/pdf/2022-cms-report.pdf).  

            \n `fct_safety_incidents` is an aggregate, monthly table that joins all metrics together. 
             Use this table for your exploration needs!
             
             
             """)
    
    try:
        # Initialize connection
        conn = start_db()
        
        # Select a table
        tables = run_query(conn, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'analytics'")
        if not tables.empty:
            selected_table = st.selectbox("Select a table to view", tables['table_name'])
            
            if selected_table:
                # Show table data
                data = run_query(conn, f"SELECT * FROM analytics.{selected_table}")
                
                # Convert date_month column to datetime
                if "date_month" in data.columns:
                    data["date_month"] = pd.to_datetime(data["date_month"], errors="coerce")

                    # Ensure min_date and max_date are converted to datetime.date
                    min_date = data["date_month"].min().to_period("M").to_timestamp().date() if pd.notna(data["date_month"].min()) else None
                    max_date = data["date_month"].max().to_period("M").to_timestamp().date() if pd.notna(data["date_month"].max()) else None

                    if min_date and max_date:
                        st.subheader("Data Visualization")
                        start_date, end_date = st.slider(
                            "Select Month Range", 
                            min_value=min_date, 
                            max_value=max_date, 
                            value=(min_date, max_date),
                            format="YYYY-MM"
                        )

                        # Convert start_date and end_date back to datetime for filtering
                        data = data[(data["date_month"] >= pd.to_datetime(start_date)) & (data["date_month"] <= pd.to_datetime(end_date))]
                
                # Visualization options
                chart_type = st.selectbox("Select Chart Type", ["Line Chart", "Bar Chart", "Stacked Bar Chart", "Multi-Line Chart"], index=0)
                
                # Select columns for visualization
                numeric_columns = data.select_dtypes(include=['number']).columns.tolist()
                categorical_columns = data.select_dtypes(include=['object', 'category']).columns.tolist()
                datetime_columns = data.select_dtypes(include=['datetime']).columns.tolist()
                
                if numeric_columns:
                    x_axis = "date_month" if "date_month" in data.columns else st.selectbox("Select X-axis", categorical_columns + datetime_columns)
                    if chart_type in ["Multi-Line Chart", "Line Chart"]:
                        y_axis = st.multiselect("Select Y-axis columns", numeric_columns, default=numeric_columns[:2])
                        color_col = st.selectbox("Select category for differentiation", categorical_columns) if categorical_columns else None
                    else:
                        y_axis = st.selectbox("Select Y-axis", numeric_columns)
                    
                    if chart_type == "Bar Chart":
                        fig = px.bar(data, x=x_axis, y=y_axis, title="Bar Chart")
                    elif chart_type == "Stacked Bar Chart":
                        color_col = st.selectbox("Select category for stacking", categorical_columns)
                        fig = px.bar(data, x=x_axis, y=y_axis, color=color_col, title="Stacked Bar Chart", barmode="stack")
                    elif chart_type == "Line Chart":
                        fig = px.line(data, x=x_axis, y=y_axis, color=color_col, title="Line Chart") if color_col else px.line(data, x=x_axis, y=y_axis, title="Line Chart")
                    elif chart_type == "Multi-Line Chart":
                        fig = px.line(data, x=x_axis, y=y_axis, color=color_col, title="Multi-Line Chart") if color_col else px.line(data, x=x_axis, y=y_axis, title="Multi-Line Chart")
                    
                    st.plotly_chart(fig)
                    
                    st.write(f"Filtered data from analytics.{selected_table}:")
                    st.write(data)
                else:
                    st.warning("Insufficient numeric or categorical data for visualization.")
        else:
            st.warning("No tables found in the database")
            
    except Exception as e:
        st.error(f"Error connecting to database: {str(e)}")

if __name__ == "__main__":
    main()
