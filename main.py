"""
Main File - Project Name

This file serves as the entry point for the Project Name. It coordinates the execution of various modules and
provides a central location to run all the file..

Author: Name
Version: 1.0.0
"""

import sys
import os
import streamlit as st
import snowflake.connector
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
# from dotenv import load_dotenv
from src.data.update_master_table_page import update_master_table_page
from maricovault.MaricoDB import MaricoSnowflake
import _thread

def log_file_upload(conn, file_name, status, error_message=None):
    try:
        if error_message:
            query = "INSERT INTO FILE_UPLOAD_LOG (FILE_NAME, STATUS, ERROR_MESSAGE) VALUES (%s, %s, %s)"
            conn.cursor().execute(query, (file_name, status, error_message))
        else:
            query = "INSERT INTO FILE_UPLOAD_LOG (FILE_NAME, STATUS) VALUES (%s, %s)"
            conn.cursor().execute(query, (file_name, status))
        conn.commit()
    except Exception as e:
        st.error(f"Error logging file upload: {str(e)}")

# Function to get list of databases from Snowflake
def get_databases(conn):
    try:
        query = "SHOW DATABASES"
        result = conn.cursor().execute(query)
        databases = [row[1] for row in result.fetchall()]
        return databases
    except Exception as e:
        st.error(f"Error fetching databases: {str(e)}")
        return []

# Function to get list of schemas from Snowflake
def get_schemas(conn, database):
    try:
        query = f"SHOW SCHEMAS IN {database}"
        result = conn.cursor().execute(query)
        schemas = [row[1] for row in result.fetchall()]
        return schemas
    except Exception as e:
        st.error(f"Error fetching schemas: {str(e)}")
        return []

# Function to get list of tables from Snowflake based on selected database and schema
def get_tables(conn, database, schema):
    try:
        conn.cursor().execute(f"USE DATABASE {database}")
        conn.cursor().execute(f"USE SCHEMA {schema}")
        query = "SHOW TABLES"
        result = conn.cursor().execute(query)
        tables = [row[1] for row in result.fetchall()]
        return tables
    except Exception as e:
        st.error(f"Error fetching tables: {str(e)}")
        return []

# Function to get primary keys from master table for the given table name
def get_primary_keys(conn, table_name):
    try:
        query = "SELECT PRIMARY_KEY FROM DATA_UPLOAD_MASTER_TABLE WHERE TABLE_NAME = %s"
        result = conn.cursor().execute(query, (table_name,))
        result_set = result.fetchone()
        if result_set:
            return result_set[0].split(', ')
        else:
            return None
    except Exception as e:
        st.error(f"Error fetching primary keys from master table for table {table_name}: {str(e)}")
        return None

# Function to update master table with selected table and primary keys
def update_master_table(conn, table_name, primary_keys):
    try:
        query_select = "SELECT * FROM DATA_UPLOAD_MASTER_TABLE WHERE TABLE_NAME = %s"
        result = conn.cursor().execute(query_select, (table_name,))
        existing_row = result.fetchone()
        
        if existing_row:
            query_update = "UPDATE DATA_UPLOAD_MASTER_TABLE SET PRIMARY_KEY = %s WHERE TABLE_NAME = %s"
            combined_primary_keys = ', '.join(primary_keys)
            conn.cursor().execute(query_update, (combined_primary_keys, table_name))
        else:
            query_insert = "INSERT INTO DATA_UPLOAD_MASTER_TABLE (TABLE_NAME, PRIMARY_KEY) VALUES (%s, %s)"
            combined_primary_keys = ', '.join(primary_keys)
            conn.cursor().execute(query_insert, (table_name, combined_primary_keys))
            
        conn.commit()
        return True
    except Exception as e:
        st.error(f"Error updating master table: {str(e)}")
        return False

# Function to get values from master table
def get_master_table_values(conn):
    try:
        query = "SELECT * FROM DATA_UPLOAD_MASTER_TABLE"
        result = conn.cursor().execute(query)
        rows = result.fetchall()
        return rows
    except Exception as e:
        st.error(f"Error fetching master table values: {str(e)}")
        return []

# Function to delete rows from target table based on matching primary keys
def delete_matching_rows_by_primary_keys(conn, table_name, primary_keys, file_contents):
    try:
        for _, row in file_contents.iterrows():
            where_clause = ' AND '.join([f"{key} = '{row[key]}'" for key in primary_keys])
            delete_query = f"DELETE FROM {table_name} WHERE {where_clause}"
            conn.cursor().execute(delete_query)
        conn.commit()
        return True
    except Exception as e:
        st.error(f"Error deleting rows from table {table_name}: {str(e)}")
        return False

# Function to insert data from DataFrame into Snowflake table
def insert_data_into_table(conn, df, table_name):
    try:
        write_pandas(conn, df, table_name)
        log_file_upload(conn, table_name, "Inserted Data Successfully")
        return True
    except Exception as e:
        st.error(f"Error inserting data into table {table_name}: {str(e)}")
        log_file_upload(conn, table_name, "Error", str(e))
        return False

# Function to insert data from CSV into the temporary table
def insert_data_into_temp_table(conn, file_contents, table_name):
    try:
        primary_keys = get_primary_keys(conn, table_name)
        if not primary_keys:
            st.warning(f"No primary keys found for table {table_name}. Please select target table first.")
            return False

        if insert_data_into_table(conn, file_contents, table_name):
            return True
        else:
            return False
    except Exception as e:
        st.error(f"Error inserting data into table {table_name}: {str(e)}")
        return False

# Function to fetch data from the target table
def fetch_data_from_target_table(conn, table_name):
    try:
        query = f"SELECT * FROM {table_name}"
        result = conn.cursor().execute(query)
        data = result.fetchall()
        columns = [desc[0] for desc in result.description]
        return pd.DataFrame(data, columns=columns)
    except Exception as e:
        st.error(f"Error fetching data from table {table_name}: {str(e)}")
        return None

# Function to fetch the last 10 logs from the FILE_UPLOAD_LOG table
def fetch_last_10_logs(conn):
    try:
        query = "SELECT * FROM FILE_UPLOAD_LOG ORDER BY UPLOAD_TIME DESC LIMIT 10"
        result = conn.cursor().execute(query)
        log_data = result.fetchall()
        if not log_data:
            return None
        return log_data
    except Exception as e:
        st.error(f"Error fetching last 10 logs: {str(e)}")
        return None


# Function to check if uploaded file columns match target table columns
def check_columns_match(conn, table_name, uploaded_file):
    try:
        file_columns = pd.read_excel(uploaded_file, nrows=1).columns.tolist()
        query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'"
        result = conn.cursor().execute(query)
        table_columns = [row[0].upper() for row in result.fetchall()]
        file_columns_upper = [col.upper() for col in file_columns]
        return all(col in table_columns for col in file_columns_upper)
    except Exception as e:
        st.error(f"Error checking columns: {str(e)}")
        return False
    

def get_db_credentials():
    if "snowflake_credentials" not in st.session_state:
        KEY_VAULT_NAME = "prod-pwd"
        msf = MaricoSnowflake(key_vault_name=KEY_VAULT_NAME)
        msf.get_db_credentials(db_name='dev')
        msf.connect()
        st.session_state.snowflake_credentials = msf
    return st.session_state.snowflake_credentials

def main():
    """
    Main function - Entry point for the script.

    This function is responsible for coordinating the execution of various modules and functions to perform the main
    functionality of the project.

    Inputs:
        None

    Returns:
        None
    """
    # Your code here
    st.set_page_config(
        page_title="Snowflake Data Upload Tool",
        page_icon=":floppy_disk:",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    # Set up Streamlit layout to position buttons on the left side
    home, app = st.columns([10,10])

    # Add a home button that redirects to ai.maricoapps.biz
    if home.button("Home", use_container_width=True, type='primary'):
        # Redirect to the specified URL
        home.markdown('<meta http-equiv="refresh" content="0;URL=https://ai.maricoapps.biz">', unsafe_allow_html=True)
    
    # Add an application button that redirects to ai.maricoapps.biz/application
    if app.button("Applications", use_container_width=True, type='primary'):
        # Redirect to the specified URL
        app.markdown('<meta http-equiv="refresh" content="0;URL=https://ai.maricoapps.biz/application">', unsafe_allow_html=True)
    
    st.title("Snowflake Data Upload Tool")


    tab1, tab2 = st.tabs(["Data Insertion", "Primary Key Mapping"])


    # Connect to Snowflake
    # KEY_VAULT_NAME = "prod-pwd"

    # msf = MaricoSnowflake(key_vault_name=KEY_VAULT_NAME)
    # msf.get_db_credentials(db_name='dev')
    # msf.connect()

    # Fetch the database credentials
    msf = get_db_credentials()
    msf.get_connection()

    with tab1:
        conn = msf.get_connection()
        if conn:
            st.subheader("Data Insertion")
            st.success("Connected to Snowflake!")

            # Get list of databases
            databases = get_databases(conn)
            selected_database = st.selectbox("Select Database", databases, key="database")

            # Get list of schemas
            schemas = get_schemas(conn, selected_database)
            selected_schema = st.selectbox("Select Schema", schemas, key="schema")

            # Get list of tables
            tables = get_tables(conn, selected_database, selected_schema)
            table_name = st.selectbox("Select Target Table", tables, key="table")

            # Get number of rows in the selected table
            
            current_rows_query = f"SELECT COUNT(*) FROM {table_name}"
            current_rows_result = conn.cursor().execute(current_rows_query)
            current_rows = current_rows_result.fetchone()[0]
            st.info(f"Current number of rows in the table: {current_rows}")
            # File uploader
            uploaded_file = st.file_uploader("Upload Excel File", type=["xlsx"])

            if uploaded_file is not None:
                # Log the file upload
                # log_file_upload(conn, uploaded_file.name, "Uploaded")
                
                # Force user to select a sheet from the Excel file
                selected_sheet = st.selectbox("Select a sheet", pd.ExcelFile(uploaded_file).sheet_names)
                
                # Read the selected sheet
                file_contents = pd.read_excel(uploaded_file, sheet_name=selected_sheet)

                # st.subheader("Uploaded file contents:")
                # st.dataframe(file_contents,use_container_width=True)

                if check_columns_match(conn, table_name, uploaded_file):
                    # Enable the button for deleting existing data and inserting new data
                    delete_insert_button = st.button("Delete Existing Data and Insert New Data")
                else:
                    # If columns don't match, display a message and disable the button
                    st.warning("Button will be enabled after validation of source and destination data structure")
                    delete_insert_button = st.button("Delete Existing Data and Insert New Data", disabled=True)

                # Check if uploaded file columns match target table columns
                if check_columns_match(conn, table_name, uploaded_file):
                    # Button to delete existing data from target table and insert new data
                    if delete_insert_button:
                        # Check if there is any existing data in the target table
                        existing_data = fetch_data_from_target_table(conn, table_name)
                        if existing_data is not None and not existing_data.empty:
                            # Delete existing data from the target table based on primary keys
                            primary_keys = get_primary_keys(conn, table_name)
                            if primary_keys:
                                if delete_matching_rows_by_primary_keys(conn, table_name, primary_keys, file_contents):
                                    st.success("Existing data deleted from target table successfully!")
                                else:
                                    st.error("Failed to delete existing data from target table. Please check the primary keys of selected table")
                            else:
                                st.warning(f"No primary keys found for table {table_name}. Please update primary key mapping first.")
                        else:
                            st.warning("No existing data found in target table. Skipping deletion step.")

                        # Insert new data into the target table
                        if insert_data_into_temp_table(conn, file_contents, table_name):
                            st.success("New data inserted into target table successfully!")
                        else:
                            st.error("Failed to insert new data into target table!")

                            # Display full data from the target table
                        st.subheader("Data in Target Table")
                        target_table_data = fetch_data_from_target_table(conn, table_name)
                        if target_table_data is not None:
                            # st.dataframe(target_table_data,use_container_width=True)
                            st.info(f"Number of rows after update: {len(target_table_data)}")
                        else:
                            st.warning("No data available in target table.")


            # Display last 10 logs from file_upload_log table
            st.subheader("Last 10 Logs from File Upload Log Table")
            log_data = fetch_last_10_logs(conn)
            if log_data is not None:
                st.dataframe(log_data,use_container_width=True)
            else:
                st.warning("No logs found in the file_upload_log table.")

                

    with tab2:
        msf = get_db_credentials()
        conn = msf.get_connection()
        if conn:
            st.subheader("Primary Key Mapping")
            update_master_table_page(conn)



if __name__ == "__main__":
    main()
