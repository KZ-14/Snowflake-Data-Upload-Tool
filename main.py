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
import datetime
from concurrent.futures import ThreadPoolExecutor

def log_file_upload(conn, file_name, status, error_message=None, move_data=None):
    try:
        if error_message:
            query = "INSERT INTO DATALAKE_DB.DATA_SCIENCE.TRN_FILE_UPLOAD_LOG (FILE_NAME, STATUS, ERROR_MESSAGE) VALUES (%s, %s, %s)"
            conn.cursor().execute(query, (file_name, status, error_message))
        elif move_data:
            query = "INSERT INTO DATALAKE_DB.DATA_SCIENCE.TRN_FILE_UPLOAD_LOG (FILE_NAME, STATUS, DEV_TO_PROD_MOVEMENT) VALUES (%s, %s, %s)"
            conn.cursor().execute(query, (file_name, status, move_data))
        else:
            query = "INSERT INTO DATALAKE_DB.DATA_SCIENCE.TRN_FILE_UPLOAD_LOG (FILE_NAME, STATUS) VALUES (%s, %s)"
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
        query = "SELECT PRIMARY_KEY FROM DATALAKE_DB.DATA_SCIENCE.TRN_DATA_UPLOAD_MASTER_TABLE WHERE TABLE_NAME = %s"
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
        query_select = "SELECT * FROM DATALAKE_DB.DATA_SCIENCE.TRN_DATA_UPLOAD_MASTER_TABLE WHERE TABLE_NAME = %s"
        result = conn.cursor().execute(query_select, (table_name,))
        existing_row = result.fetchone()
        
        if existing_row:
            query_update = "UPDATE DATALAKE_DB.DATA_SCIENCE.TRN_DATA_UPLOAD_MASTER_TABLE SET PRIMARY_KEY = %s WHERE TABLE_NAME = %s"
            combined_primary_keys = ', '.join(primary_keys)
            conn.cursor().execute(query_update, (combined_primary_keys, table_name))
        else:
            query_insert = "INSERT INTO DATALAKE_DB.DATA_SCIENCE.TRN_DATA_UPLOAD_MASTER_TABLE (TABLE_NAME, PRIMARY_KEY) VALUES (%s, %s)"
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
        query = "SELECT * FROM DATALAKE_DB.DATA_SCIENCE.TRN_DATA_UPLOAD_MASTER_TABLE"
        result = conn.cursor().execute(query)
        rows = result.fetchall()
        return rows
    except Exception as e:
        st.error(f"Error fetching master table values: {str(e)}")
        return []

# Function to delete rows from target table based on matching primary keys
# def delete_matching_rows_by_primary_keys(conn, table_name, primary_keys, file_contents):
#     try:
#         table_name = table_name[0]
#         for _, row in file_contents.iterrows():
#             where_clause = ' AND '.join([f"{key} = '{row[key]}'" for key in primary_keys])
#             delete_query = f"DELETE FROM DATALAKE_DB.DATA_SCIENCE.{table_name} WHERE {where_clause}"
#             print(delete_query)
#             # conn.cursor().execute(delete_query)
#         conn.commit()
#         return True
#     except Exception as e:
#         st.error(f"Error deleting rows from table {table_name}: {str(e)}")
#         return False

# def delete_matching_rows_by_primary_keys(conn, table_name, primary_keys, file_contents):
#     try:
#         table_name = table_name[0]
#         print("Table Name:", table_name)
        
#         # Fetch distinct values for each primary key from file_contents
#         distinct_values = {}
#         for key in primary_keys:
#             distinct_values[key] = file_contents[key].unique().tolist()
#             print(len(distinct_values[key]))
        
#         # Construct the IN clause with values enclosed in single quotes
#         in_clause_values = []
#         for key in primary_keys:
#             values_with_quotes = [f"'{value}'" for value in distinct_values[key]]
#             in_clause_values.append(', '.join(values_with_quotes))
        
#         # Construct the delete query
#         where_clauses = []
#         for key, in_clause_value in zip(primary_keys, in_clause_values):
#             where_clause = f"{key} IN ({in_clause_value})"
#             where_clauses.append(where_clause)
        
#         combined_where_clause = ' AND '.join(where_clauses)
#         delete_query = f"DELETE FROM DATALAKE_DB.DATA_SCIENCE.{table_name} WHERE {combined_where_clause}"
        
#         with open(f"outputs/delete_query/{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}_{table_name}.txt", "w") as text_file:
#             text_file.write("Delete Query as follows:\n%s" % delete_query)
        
#         # Execute the delete query and get the number of affected rows
#         cursor = conn.cursor()
#         cursor.execute(delete_query)
#         affected_rows = cursor.rowcount  # Get the number of deleted rows
        
#         conn.commit()
#         return affected_rows
#     except Exception as e:
#         st.error(f"Error deleting rows from table {table_name}: {str(e)}")
#         return -1  # Return -1 in case of an error

##################### BATCH WISE ################################

import datetime

def delete_matching_rows_by_primary_keys(conn, dev_table_name, prod_table_name, primary_keys, file_contents,selected_database_prod):
    try:
        
        prod_table_name = prod_table_name[0]
        print("Production Table Name:", prod_table_name)
        
        # Fetch distinct values for each primary key from file_contents
        distinct_values = {}
        for key in primary_keys:
            distinct_values[key] = file_contents[key].unique().tolist()
            print(f"Number of distinct values for {key}: {len(distinct_values[key])}")
            
        # Construct the concatenated primary key expression
        concatenated_primary_keys = " || '_' || ".join([f'"{key}"' for key in primary_keys])
        
        # Fetch primary keys from production table (prod) that match those in dev
        subquery = f"""
        SELECT {concatenated_primary_keys} AS primary_key
        FROM {selected_database_prod}.DATA_SCIENCE.{prod_table_name}
        WHERE {concatenated_primary_keys} IN (
            SELECT {concatenated_primary_keys}
            FROM DATALAKE_DB.DATA_SCIENCE.{dev_table_name}
        )
        """
        
        # Construct the delete query using the subquery
        delete_query = f"""
        DELETE FROM {selected_database_prod}.DATA_SCIENCE.{prod_table_name} 
        WHERE {concatenated_primary_keys} IN ({subquery})
        """
        
        # Save the delete query to a file
        with open(f"outputs/delete_query/{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}_{prod_table_name}.txt", "w") as text_file:
            text_file.write("Delete Query as follows:\n%s" % delete_query)
        
        # Execute the delete query and get the number of affected rows
        cursor = conn.cursor()
        cursor.execute(delete_query)
        affected_rows = cursor.rowcount  # Get the number of deleted rows
        
        conn.commit()
        
        return affected_rows
    
    except Exception as e:
        print(f"Error deleting rows from production table {prod_table_name}: {str(e)}")
        return -1  # Return -1 in case of an error

def check_for_duplicates(conn, table_name, primary_keys):
    try:
        # Quote the primary keys to handle special characters and reserved words
        quoted_primary_keys = [f'"{key}"' for key in primary_keys]
        
        # Construct the concatenated primary key
        primary_key_expr = " || '_' || ".join(quoted_primary_keys)
        
        # Construct the query to find duplicate primary keys
        duplicate_query = f"""
            SELECT {primary_key_expr} AS primary_key, 
                   COUNT({primary_key_expr}) AS primary_key_count
            FROM DEV_DB.DATA_SCIENCE.{table_name}
            GROUP BY primary_key
            HAVING primary_key_count > 1
        """
        
        print("Duplicate Query:",duplicate_query)
  
        # Execute the query to find duplicates
        cursor = conn.cursor()
        cursor.execute(duplicate_query)
        duplicates = cursor.fetchall()
        
        # Check if duplicates exist
        if duplicates:
            total_duplicates = len(duplicates)
            primary_key_list = [row[0] for row in duplicates[:100]]
            st.error(f"Total duplicates found: {total_duplicates}")
            st.error("Duplicate primary keys (showing first 100): " + ', '.join(primary_key_list))
            return True  # Duplicates exist
        else:
            st.success("No duplicates found.")
            return False  # No duplicates
    except Exception as e:
        st.error(f"Error checking for duplicates: {str(e)}")
        return True  # Assume duplicates exist in case of error

    
# Function to insert data from DataFrame into Snowflake table
# def insert_data_into_table(conn, df, table_name, movement=None):
#     try:
#         table_name = table_name[0]
#         write_pandas(conn, df, table_name,use_logical_type=True)
#         if movement:
#             log_file_upload(conn, table_name, "Inserted Data Successfully", move_data=movement)
#         else:
#             log_file_upload(conn, table_name, "Inserted Data Successfully")
#         return True
#     except Exception as e:
#         st.error(f"Error inserting data into table {table_name}: {str(e)}")
#         log_file_upload(conn, table_name, "Error", str(e))
#         return False

def insert_data_into_table(conn, df, table_name, selected_database_prod, movement=None, chunk_size=100000):
    try:
        # table_name = table_name[0]
        total_rows = len(df)
        total_chunks = (total_rows // chunk_size) + 1

        for i in range(total_chunks):
            start_idx = i * chunk_size
            end_idx = min(start_idx + chunk_size, total_rows)
            df_chunk = df.iloc[start_idx:end_idx]
            
            print(f"Inserting chunk {i + 1}/{total_chunks}, rows {start_idx} to {end_idx - 1}")
            
            try:

                success, nchunks, nrows, _ = write_pandas(conn, df_chunk, table_name, database = selected_database_prod, schema= "DATA_SCIENCE", use_logical_type=True)
                
                if not success:
                    raise Exception("write_pandas failed")

                print(f"Inserted chunk {i + 1}/{total_chunks}: {nrows} rows")
            except Exception as insert_exc:
                st.error(f"Error inserting chunk {i + 1}/{total_chunks} into table {table_name}: {str(insert_exc)}")
                log_file_upload(conn, table_name, "Error", str(insert_exc))
                return False
        
        if movement:
            log_file_upload(conn, table_name, "Inserted Data Successfully", move_data=movement)
        else:
            log_file_upload(conn, table_name, "Inserted Data Successfully")
        
        return True
    except Exception as e:
        st.error(f"Error inserting data into table {table_name}: {str(e)}")
        log_file_upload(conn, table_name, "Error", str(e))
        return False

# Function to insert data from CSV into the temporary table
def insert_data_into_temp_table(conn, file_contents, table_name,selected_database_prod, movement=None):
    try:
        print("Insertion Process Starts")
        primary_keys = get_primary_keys(conn, table_name)
        if not primary_keys:
            st.warning(f"No primary keys found for table {table_name}. Please select target table first.")
            return False

        if movement:
            if insert_data_into_table(conn, file_contents, table_name,selected_database_prod, movement=movement):
                return True
            else:
                return False
        else:
            if insert_data_into_table(conn, file_contents, table_name, selected_database_prod):
                return True
            else:
                return False
    except Exception as e:
        st.error(f"Error inserting data into table {table_name}: {str(e)}")
        return False

def fetch_table_schema(conn, table_name):
    """Fetch the schema of the table to understand the data types of each column."""
    cursor = conn.cursor()
    cursor.execute(f"DESCRIBE TABLE {table_name}")
    columns_info = cursor.fetchall()
    
    # Return column names properly quoted
    quoted_columns = [f'"{col[0]}"' for col in columns_info]
    return quoted_columns

# def fetch_column_data(conn, table_name, column_name, chunk_size=100000):
#     """Fetch data for a specific column in chunks to identify problematic rows."""
#     offset = 0
#     all_data = []

#     while True:
#         query = f'SELECT {column_name} FROM {table_name} LIMIT {chunk_size} OFFSET {offset}'
#         result = conn.cursor().execute(query)
#         data = result.fetchall()

#         if not data:
#             break

#         all_data.extend(data)
#         offset += chunk_size

#     return all_data

def fetch_data_chunk(conn, table_name, chunk_size, offset):
    """Fetch a chunk of data from the table."""
    query = f"SELECT * FROM {table_name} ORDER BY 1 LIMIT {chunk_size} OFFSET {offset}"
    result = conn.cursor().execute(query)
    data = result.fetchall()
    columns = [desc[0] for desc in result.description]
    return pd.DataFrame(data, columns=columns)


# def fetch_data_from_target_table(conn, table_name, chunk_size=100000):
#     try:
#         all_data = []
#         offset = 0

#         # Progress bar and status text
#         progress_bar = st.progress(0)
#         status_text = st.empty()

#         with st.spinner('Fetching Data In Chunks...'):
#             while True:
#                 data_chunk = fetch_data_chunk(conn, table_name, chunk_size, offset)
#                 if data_chunk.empty:
#                     break
#                 all_data.append(data_chunk)
#                 offset += chunk_size

#                 # Update progress and status
#                 progress = offset / (chunk_size * 100)  # Assuming 1 crore rows
#                 progress_bar.progress(min(progress, 1.0))
#                 status_text.text(f"Fetched {offset} rows...")

#         # Complete progress bar
#         progress_bar.progress(1.0)
#         status_text.text("Data fetching completed.")

#         if not all_data:
#             return pd.DataFrame()  # Return an empty DataFrame if no data is fetched
#         return pd.concat(all_data, ignore_index=True)
#     except Exception as e:
#         st.error(f"Error fetching data from table {table_name}: {str(e)}")
#         return None




########################## BATCH WISE #####################################
def fetch_data_from_target_table(conn, table_name, chunk_size=100000, offset=0):
    try:
        # Progress bar and status text
        progress_bar = st.progress(0)
        status_text = st.empty()

        with st.spinner('Fetching Data In Chunks...'):
            data_chunk = fetch_data_chunk(conn, table_name, chunk_size, offset)
            if data_chunk.empty:
                return pd.DataFrame()  # Return an empty DataFrame if no data is fetched

            # Update progress and status
            progress = offset / (chunk_size * 100)  # Assuming 1 crore rows
            progress_bar.progress(min(progress, 1.0))
            status_text.text(f"Fetched {offset} rows...")

        # Complete progress bar
        progress_bar.progress(1.0)
        status_text.text("Data fetching completed.")

        return data_chunk
    except Exception as e:
        st.error(f"Error fetching data from table {table_name}: {str(e)}")
        return None




# def fetch_data_from_target_table(conn, table_name, chunk_size=100000):
#     try:
#         # Fetch the schema of the table
#         columns_info = fetch_table_schema(conn, table_name)
#         print("Columns Info:", columns_info)

#         problematic_columns = []
#         all_data = []

#         for col in columns_info:
#             try:
#                 data = fetch_column_data(conn, table_name, col, chunk_size)
#                 all_data.append(pd.DataFrame(data, columns=[col.strip('"')]))
#             except Exception as col_err:
#                 problematic_columns.append(col)
#                 st.error(f"Error fetching data for column {col} in table {table_name}: {str(col_err)}")

#         if problematic_columns:
#             st.error(f"Problematic columns: {', '.join(problematic_columns)}")
#             for col in problematic_columns:
#                 try:
#                     data = fetch_column_data(conn, table_name, col, chunk_size)
#                     st.error(f"Data in column {col}: {data}")
#                 except Exception as col_err:
#                     st.error(f"Error fetching data for column {col} in table {table_name}: {str(col_err)}")

#         return pd.concat(all_data, axis=1)
#     except Exception as e:
#         st.error(f"Error fetching data from table {table_name}: {str(e)}")
#         return None



# Function to fetch the last 10 logs from the FILE_UPLOAD_LOG table
def fetch_last_10_logs(conn):
    try:
        query = "SELECT * FROM DATALAKE_DB.DATA_SCIENCE.TRN_FILE_UPLOAD_LOG ORDER BY UPLOAD_TIME DESC LIMIT 10"
        result = conn.cursor().execute(query)
        log_data = result.fetchall()
        
        # Get column names from cursor description
        columns = [desc[0] for desc in result.description]

        if not log_data:
            return None, None
        return columns, log_data
    except Exception as e:
        st.error(f"Error Fetching Last 10 Logs: {str(e)}")
        return None, None


# Function to check if uploaded file columns match target table columns
def check_columns_match(conn, table_name, uploaded_file):
    try:
        file_columns = pd.read_excel(uploaded_file, nrows=1).columns.tolist()
        query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'"
        result = conn.cursor().execute(query)
        table_columns = [row[0] for row in result.fetchall()]        
        mismatched_columns = [col for col in file_columns if col not in table_columns]
        if mismatched_columns:
            st.warning(f"Warning: The following file column(s) do not match the target table columns: {', '.join(mismatched_columns)}. Column names should be in Capital letters only and should exactly match the Target Table column name.")
        
        return all(col in table_columns for col in file_columns)
    except Exception as e:
        st.error(f"Error checking columns: {str(e)}")
        return False
    

def get_dev_credentials():
    if "snowflake_credentials" not in st.session_state:
        KEY_VAULT_NAME = "prod-pwd"
        msf = MaricoSnowflake(key_vault_name=KEY_VAULT_NAME)
        msf.get_db_credentials(db_name='dev')
        msf.connect()
        st.session_state.snowflake_credentials = msf
    return st.session_state.snowflake_credentials

def get_prod_credentials():
    if "prod_snowflake_credentials" not in st.session_state:
        KEY_VAULT_NAME = "prod-pwd"
        msf = MaricoSnowflake(key_vault_name=KEY_VAULT_NAME, write_prod=True)
        msf.get_db_credentials(db_name='prod')
        msf.connect()
        st.session_state.prod_snowflake_credentials = msf
    return st.session_state.prod_snowflake_credentials


# Function to move data from dev target table to prod target table
# def move_data_from_dev_to_prod(conn_dev, table_name_dev, conn_prod, table_name_prod):
#     try:
#         primary_keys = get_primary_keys(conn_prod, table_name_prod)
#         if primary_keys: 
#             # Fetch data from dev target table
#             dev_data = fetch_data_from_target_table(conn_dev, table_name_dev)
            
#             # Check if data was fetched successfully
#             if dev_data is not None and not dev_data.empty:
#                 st.info("Data has been fetched successfully from dev table")
#                 print(dev_data.head())
#             else:
#                 st.error("Failed to fetch data from dev table")

            
#             # dev_data.columns = dev_data.columns.str.upper()
#             # print(dev_data.columns)
            
#             # st.dataframe(dev_data)
#             if dev_data is not None:
#                 # Check for NaN values in dev_data primary keys
#                 primary_keys = get_primary_keys(conn_prod, table_name_prod)
#                 print("Primary Keys:",primary_keys)
#                 columns_with_nan = []
#                 for key in primary_keys:
#                     if dev_data[key].isnull().any():
#                         columns_with_nan.append(key)

#                 if columns_with_nan:
#                     st.warning(f"Dev data contains NaN values in primary key columns: {', '.join(columns_with_nan)}. "
#                             "Please clean the data before moving to prod target table.")
#                     return False

#                 # Delete existing data from prod target table based on primary keys
#                 primary_keys = get_primary_keys(conn_prod, table_name_prod)
                
#                 # Check for duplicate values across multiple primary keys
#                 duplicates_exist = check_for_duplicates(conn_dev, table_name_dev, primary_keys)
#                 print("Duplicates Exist",duplicates_exist)
                
#                 if duplicates_exist:
#                     st.warning("Duplicate values exist across multiple primary keys. Please resolve this issue before proceeding.")
#                     return False
                
#                 # Check if there is any existing data in the target table
#                 existing_data = fetch_data_from_target_table(conn_prod, table_name_prod[0])
            
#                 if existing_data is not None and not existing_data.empty:
#                     if primary_keys:    
#                         affected_rows = delete_matching_rows_by_primary_keys(conn_prod, table_name_prod, primary_keys, dev_data)

#                         if affected_rows > 0:
#                             st.success("Existing data deleted from prod target table successfully!")
#                             st.info(f"Number of rows deleted successfully are {affected_rows}")
#                         elif affected_rows == 0:
#                             st.warning("No matching data found to delete.")
#                         else:
#                             st.error("Failed to delete existing data from prod target table. Please check the primary keys.")
#                             return False
#                     else:
#                         st.warning(f"No primary keys found for prod target table {table_name_prod}. Please update primary key mapping.")
#                         return False
#                 else:
#                     st.warning("No existing data found in target table. Skipping deletion step.")
                    
#                 print(dev_data.head())
                
#                 # Insert data into prod target table
#                 if insert_data_into_temp_table(conn_prod, dev_data, table_name_prod[0], movement="Data has been moved Successfully"):
#                     st.success("Data moved from dev target table to prod target table successfully!")
#                     return True
#                 else:
#                     st.error("Failed to move data to prod target table.")
#                     return False
#             else:
#                 st.warning("No data available in dev target table.")
#                 return False
#         else:
#             st.warning(f"No primary keys found for prod target table {table_name_prod}. Please update primary key mapping.")
#             return False
#     except Exception as e:
#         st.error(f"Error moving data from dev to prod target table: {str(e)}")
#         return False

################################### BATCH WISE ######################################
def move_data_from_dev_to_prod(conn_dev, table_name_dev, conn_prod, table_name_prod,selected_database_prod, chunk_size=500000):
    try:
        # Fetch and compare the schemas before fetching data
        dev_schema = fetch_table_schema(conn_dev, table_name_dev)
        print("Dev Schema Columns:",dev_schema)
        
        prod_schema = fetch_table_schema(conn_prod, table_name_prod[0])
        print("Prod Schema Columns:",prod_schema)
        
        new_columns = [col for col in dev_schema if col not in prod_schema]
        
        if new_columns:
            st.warning(f"New columns found in dev table: {', '.join(new_columns)}. Please update prod table schema manually before proceeding.")
            return False
        
        primary_keys = get_primary_keys(conn_prod, table_name_prod)
        if primary_keys:
            offset = 0
            first_chunk = True
            while True:
                # Fetch data from dev target table in chunks
                dev_data = fetch_data_from_target_table(conn_dev, table_name_dev, chunk_size, offset)
                
                # Check if data was fetched successfully
                if dev_data is not None and not dev_data.empty:
                    st.info(f"Fetched {len(dev_data)} rows from dev table (Offset: {offset})")
                else:
                    st.info(f"No more data to fetch from dev table (Offset: {offset}). Process completed.")
                    break

                # Check for NaN values in dev_data primary keys
                columns_with_nan = [key for key in primary_keys if dev_data[key].isnull().any()]
                if columns_with_nan:
                    st.warning(f"Dev data contains NaN values in primary key columns: {', '.join(columns_with_nan)}. "
                            "Please clean the data before moving to prod target table.")
                    return False

                if first_chunk:
                    # Delete existing data from prod target table based on primary keys
                    duplicates_exist = check_for_duplicates(conn_dev, table_name_dev, primary_keys)
                    if duplicates_exist:
                        st.warning("Duplicate values exist across multiple primary keys. Please resolve this issue before proceeding.")
                        return False

                    existing_data = fetch_data_from_target_table(conn_prod, table_name_prod[0], chunk_size, 0)
                    if existing_data is not None and not existing_data.empty:
                        affected_rows = delete_matching_rows_by_primary_keys(conn_prod, table_name_dev, table_name_prod, primary_keys, dev_data,selected_database_prod)
                        if affected_rows > 0:
                            st.success(f"Deleted {affected_rows} matching rows from prod table")
                        elif affected_rows == 0:
                            st.warning("No matching data found to delete.")
                        else:
                            st.error("Failed to delete existing data from prod table. Please check the primary keys.")
                            return False
                    else:
                        st.info("No existing data found in target table. Skipping deletion step.")
                    
                    first_chunk = False
                
                # Insert data into prod target table
                if insert_data_into_temp_table(conn_prod, dev_data, table_name_prod[0],selected_database_prod, movement="Data has been moved Successfully"):
                    st.success(f"Data moved from dev to prod for chunk starting at offset {offset}")
                else:
                    st.error("Failed to move data to prod target table.")
                    return False
                
                offset += chunk_size
            return True
        else:
            st.warning(f"No primary keys found for prod target table {table_name_prod}. Please update primary key mapping.")
            return False
    except Exception as e:
        st.error(f"Error moving data from dev to prod target table: {str(e)}")
        return False


# Function to filter out databases from the list
def filter_databases(databases):
    allowed_databases = ["DATALAKE_DB", "PRD_DB"]
    filtered_databases = [db for db in databases if db in allowed_databases]
    return filtered_databases

def filter_dev_databases(databases):
    dev_allowed_databases = ["DEV_DB"]
    dev_filtered_databases = [db for db in databases if db in dev_allowed_databases]
    return dev_filtered_databases

def filter_prod_schemas(schemas):
    prod_allowed_schemas = ["DATA_SCIENCE"]
    prod_filtered_schemas = [schema for schema in schemas if schema in prod_allowed_schemas]
    return prod_filtered_schemas

def filter_dev_schemas(schemas):
    dev_allowed_schemas = ["DATA_SCIENCE", "TRAINING", "VENDOR"]
    dev_filtered_schemas = [schema for schema in schemas if schema in dev_allowed_schemas]
    return dev_filtered_schemas

def fetch_matching_prod_table(selected_dev_table, tables_prod):
    # Initialize an empty list to store matching production tables
    matching_tables = []
    
    # Check if the selected_dev_table exists in the tables_prod
    if selected_dev_table in tables_prod:
        matching_tables.append(selected_dev_table)
    
        # Return the list of matching tables
        return matching_tables
    else:
        # If not found, return None
        return None
    
def fetch_matching_prod_database(table_name_dev, filtered_databases_prod):
    matching_database = []
    
    if table_name_dev.startswith(('TRN', 'MST')):
        matching_database.append(filtered_databases_prod[0])
        # If the table name starts with TRN or MST, select the DATALAKE_DB
        return matching_database
    else:
        matching_database.append(filtered_databases_prod[1])
        # If the table name starts with TRN or MST, select the DATALAKE_DB
        return matching_database




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

    st.markdown(
            """<style>
            .stButton button {
                background-color: #f0f0f0; /* Light gray background color */
                color: black; /* Black text color */
                font-weight: bold; /* Make text bold */
                font-size: 16px; /* Increase text size */
            }
            .stButton button:hover {
                background-color: green; /* Green background color on hover */
            }
            .stButton button p {
                font-weight: bold; /* Make text within button bold */
                font-size: 18px; 
            }

            </style>""",
            unsafe_allow_html=True
        )

    
    # Set up Streamlit layout to position buttons on the left side
    home, app = st.columns([10,10])

    # Add a home button that redirects to ai.maricoapps.biz
    if home.button("Home", use_container_width=True, type='primary',key="home_button"):
        # Redirect to the specified URL
        home.markdown('<meta http-equiv="refresh" content="0;URL=https://ai.maricoapps.biz">', unsafe_allow_html=True)
    
    # Add an application button that redirects to ai.maricoapps.biz/application
    if app.button("Applications", use_container_width=True, type='primary', key="applications_button"):
        # Redirect to the specified URL
        app.markdown('<meta http-equiv="refresh" content="0;URL=https://ai.maricoapps.biz/application">', unsafe_allow_html=True)

    
    st.title("Snowflake Data Upload Tool")


    tab1, tab2, tab3 = st.tabs(["Data Insertion", "Primary Key Mapping", "DEV-DB To PROD-DB"])


    # Connect to Snowflake
    # KEY_VAULT_NAME = "prod-pwd"

    # msf = MaricoSnowflake(key_vault_name=KEY_VAULT_NAME)
    # msf.get_db_credentials(db_name='dev')
    # msf.connect()

    # Fetch the database credentials
    msf = get_prod_credentials()
    msf.get_connection()

    with tab1:
        msf = get_prod_credentials()
        prod_conn = msf.get_connection()
        if prod_conn:
            st.subheader("Data Insertion")
            st.success("Connected to Snowflake Prod Database!")

            # Get list of databases
            databases = get_databases(prod_conn)

            # Filter databases
            filtered_databases_prod = filter_databases(databases)
            
            selected_database = st.selectbox("Select Database", filtered_databases_prod, key="database")

            # Get list of schemas
            schemas = get_schemas(prod_conn, selected_database)
            filtered_schemas_prod = filter_prod_schemas(schemas)
            selected_schema = st.selectbox("Select Schema", filtered_schemas_prod, key="schema")

            # Get list of tables
            tables = get_tables(prod_conn, selected_database, selected_schema)
            table_name = st.selectbox("Select Target Table", tables, key="table")

            # Get number of rows in the selected table
            current_rows_query = f"SELECT COUNT(*) FROM {table_name}"
            current_rows_result = prod_conn.cursor().execute(current_rows_query)
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

                if check_columns_match(prod_conn, table_name, uploaded_file):
                    # Enable the button for deleting existing data and inserting new data
                    delete_insert_button = st.button("Delete Existing Data and Insert New Data",type="primary")
                else:
                    # If columns don't match, display a message and disable the button
                    st.warning("Button will be enabled after validation of source and destination data structure")
                    delete_insert_button = st.button("Delete Existing Data and Insert New Data", disabled=True)

            
                if delete_insert_button:
                    # Check if there is any existing data in the target table
                    existing_data = fetch_data_from_target_table(prod_conn, table_name)
                    if existing_data is not None and not existing_data.empty:
                        # Delete existing data from the target table based on primary keys
                        primary_keys = get_primary_keys(prod_conn, table_name)
                        if primary_keys:
                            if delete_matching_rows_by_primary_keys(prod_conn, table_name, primary_keys, file_contents):
                                st.success("Existing data deleted from target table successfully!")
                            else:
                                st.error("Failed to delete existing data from target table. Please check the primary keys of selected table")
                        else:
                            st.warning(f"No primary keys found for table {table_name}. Please update primary key mapping first.")
                    else:
                        st.warning("No existing data found in target table. Skipping deletion step.")

                    # Insert new data into the target table
                    if insert_data_into_temp_table(prod_conn, file_contents, table_name):
                        st.success("New data inserted into target table successfully!")
                    else:
                        st.error("Failed to insert new data into target table!")

                        # Display full data from the target table
                    st.subheader("Data in Target Table")
                    target_table_data = fetch_data_from_target_table(prod_conn, table_name)
                    if target_table_data is not None:
                        # st.dataframe(target_table_data,use_container_width=True)
                        st.info(f"Number of rows after update: {len(target_table_data)}")
                    else:
                        st.warning("No data available in target table.")


            # Display last 10 logs from file_upload_log table
            log_columns, log_data = fetch_last_10_logs(prod_conn)
            if log_data is not None:
                log_df = pd.DataFrame(log_data, columns=log_columns)
                st.dataframe(log_df, use_container_width=True)
            else:
                st.warning("No logs found in the file_upload_log table.")
                
                

    with tab2:
        # msf = get_db_credentials()
        conn = msf.get_connection()
        if conn:
            st.subheader("Primary Key Mapping")
            update_master_table_page(conn)

    with tab3:
        # msf = get_db_credentials()
        # conn = msf.get_connection()
        if conn:
            st.subheader("DEV-DB To PROD-DB")
        
        # Use columns layout manager to create two containers in one row
        col1, col2 = st.columns([1,1])

        # Container 1
        with col1:
            msf = get_dev_credentials()
            conn_dev = msf.get_connection()
            st.write("DEV_DB Configuration")

            # Get list of dev databases
            databases_dev = get_databases(conn_dev)
            # Filter dev databases
            filtered_databases_dev = filter_dev_databases(databases_dev)
            # If there's only one database available, disable the selectbox and display the database name
            if len(filtered_databases_dev) == 1:
                selected_database_dev = filtered_databases_dev[0]
                
                st.write(
    f"<div style='background-color: #f0f0f0; padding: 10px; font-weight: bold;'>Development Database:  {selected_database_dev}</div>", 
    unsafe_allow_html=True
)
                st.write("")
                
                
            else:
                # If multiple databases are available, allow selection using selectbox
                selected_database_dev = st.selectbox("Select Development Database", filtered_databases_dev, key="database1")
                # selected_database_dev = st.selectbox("Select Development Database", filtered_databases_dev, key="database1")

            # Get list of schemas
            schemas_dev = get_schemas(conn_dev, selected_database_dev)
            filtered_schemas_dev = filter_dev_schemas(schemas_dev)
            selected_schema_dev = st.selectbox("Select Schema", filtered_schemas_dev, key="schema1")


            # Get list of tables
            tables_dev = get_tables(conn_dev, selected_database_dev, selected_schema_dev)
            table_name_dev = st.selectbox("Select Dev Table", tables_dev, key="table1")
            
            # Get number of rows in the selected table
            current_rows_query = f"SELECT COUNT(*) FROM {table_name_dev}"
            current_rows_result = conn_dev.cursor().execute(current_rows_query)
            current_rows = current_rows_result.fetchone()[0]
            st.info(f"Current number of rows in the table: {current_rows}")
            
            # Display first 5 rows of the selected table
            if table_name_dev:
                select_query = f"SELECT * FROM {table_name_dev} LIMIT 5"
                select_result = conn_dev.cursor().execute(select_query)
                columns = [desc[0] for desc in select_result.description]
                rows = select_result.fetchall()
                if rows:
                    st.subheader(f"First 5 Rows of Development Table: {table_name_dev}")
                    df = pd.DataFrame(rows, columns=columns)
                    st.dataframe(df)
                else:
                    st.warning(f"No rows found in table {table_name_dev}.")
            
            


        # Container 2
        with col2:
            st.write("PROD_DB Configuration")

            msf_prod = get_prod_credentials() 
            conn_prod = msf_prod.get_connection()

            # Get list of databases
            databases_prod = get_databases(conn_prod)
            # Filter databases
            filtered_databases_prod = filter_databases(databases_prod)
            # database_name_prod = fetch_matching_prod_database(table_name_dev, filtered_databases_prod)
        
            print("Filter Databases:",filtered_databases_prod)
            if filtered_databases_prod:
                if table_name_dev.startswith("TRN") or table_name_dev.startswith("MST"):
                    selected_database_prod = filtered_databases_prod[0]
                else:
                    selected_database_prod = filtered_databases_prod[1]

            # selected_database_prod = st.selectbox("Select Production Database", filtered_databases_prod, key="database2")
            
            st.write(
    f"<div style='background-color: #f0f0f0; padding: 10px; font-weight: bold;'>Selected Production Database: {selected_database_prod}</div>", 
    unsafe_allow_html=True
)
            st.write("")
            st.write("")
            st.write("")
            
            # Get list of schemas
            schemas_prod = get_schemas(conn_prod, selected_database_prod)
            filtered_schemas_prod = filter_prod_schemas(schemas_prod)
            selected_schema_name_prod = filtered_schemas_prod[0]
            st.write(
    f"<div style='background-color: #f0f0f0; padding: 10px; font-weight: bold;'>Selected Production Schema: {selected_schema_name_prod}</div>", 
    unsafe_allow_html=True
)
            st.write("")
            st.write("")
            
            
            # selected_schema_prod = st.selectbox("Select Schema", filtered_schemas_prod, key="schema2")

            # Get list of tables
            tables_prod = get_tables(conn_prod, selected_database_prod, selected_schema_name_prod)
            table_name_prod = fetch_matching_prod_table(table_name_dev, tables_prod)
            if table_name_prod:
                # selected_table_name_prod = st.selectbox("Select Target Table", table_name_prod, key="table2")
                selected_table_name_prod = table_name_prod[0]
                st.write(
    f"<div style='background-color: #f0f0f0; padding: 10px; font-weight: bold;'>Selected Production Table: {selected_table_name_prod}</div>", 
    unsafe_allow_html=True
                )
                
                st.write("")
                
                current_rows_query = f"SELECT COUNT(*) FROM {selected_table_name_prod}"
                current_rows_result = conn_prod.cursor().execute(current_rows_query)
                current_rows = current_rows_result.fetchone()[0]
                st.info(f"Current number of rows in the table: {current_rows}")
                
                if table_name_prod[0]:
                    select_query = f"SELECT * FROM {table_name_prod[0]} LIMIT 5"
                    select_result = conn_prod.cursor().execute(select_query)
                    columns = [desc[0] for desc in select_result.description]
                    rows = select_result.fetchall()
                    if rows:
                        st.subheader(f"First 5 Rows of Production Table: {table_name_prod[0]}")
                        df = pd.DataFrame(rows, columns=columns)
                        st.dataframe(df)
                    else:
                        st.warning(f"No rows found in table {table_name_prod[0]}.")
                
                
            else:
                st.warning("No matching production table found for the selected development table.")
                
                
                

        with st.container():
            st.markdown(
        """<style>
            .element-container:nth-of-type(3) button {
                display: block;
                margin: 0 auto;
                height: 3em;
            }
            </style>""",
        unsafe_allow_html=True
    )
            # Move data button
            st.markdown('<span id="button-after"></span>', unsafe_allow_html=True)
            if st.button("Move Data from DEV to PROD",type="primary"):
                move_data_from_dev_to_prod(conn_dev, table_name_dev, conn_prod, table_name_prod,selected_database_prod)







if __name__ == "__main__":
    main()
