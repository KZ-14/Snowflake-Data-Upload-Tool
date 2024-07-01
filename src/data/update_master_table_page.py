import streamlit as st
import snowflake.connector
import pandas as pd

# Function to get list of databases from Snowflake
def get_databases(conn):
    try:
        databases_query = "SHOW DATABASES"
        databases_result = conn.cursor().execute(databases_query)
        databases = [row[1] for row in databases_result]
        return databases
    except Exception as e:
        st.error(f"Error fetching databases: {str(e)}")
        return []

# Function to get list of schemas from Snowflake
def get_schemas(conn, database):
    try:
        schemas_query = f"SHOW SCHEMAS IN {database}"
        schemas_result = conn.cursor().execute(schemas_query)
        schemas = [row[1] for row in schemas_result]
        return schemas
    except Exception as e:
        st.error(f"Error fetching schemas: {str(e)}")
        return []

# Function to get list of tables from Snowflake based on selected database and schema
def get_tables(conn, database, schema):
    try:
        use_database_query = f"USE DATABASE {database}"
        use_schema_query = f"USE SCHEMA {schema}"
        show_tables_query = "SHOW TABLES"
        
        conn.cursor().execute(use_database_query)
        conn.cursor().execute(use_schema_query)
        tables_result = conn.cursor().execute(show_tables_query)
        
        tables = [row[1] for row in tables_result]
        return tables
    except Exception as e:
        st.error(f"Error fetching tables: {str(e)}")
        return []
    
# Function to get primary keys from master table for the given table name
def get_primary_keys(conn, table_name):
    try:
        primary_keys_query = "SELECT PRIMARY_KEY FROM DATALAKE_DB.DATA_SCIENCE.TRN_DATA_UPLOAD_MASTER_TABLE WHERE TABLE_NAME = %s"
        primary_keys_result = conn.cursor().execute(primary_keys_query, (table_name,))
        
        result = primary_keys_result.fetchone()
        if result:
            return result[0].split(', ')
        else:
            return None
    except Exception as e:
        st.error(f"Error fetching primary keys from master table for table {table_name}: {str(e)}")
        return None


# Function to update master table with selected table and primary keys
def update_master_table(conn, table_name, primary_keys):
    try:
        check_table_query = "SELECT * FROM DATALAKE_DB.DATA_SCIENCE.TRN_DATA_UPLOAD_MASTER_TABLE WHERE TABLE_NAME = %s"
        check_table_result = conn.cursor().execute(check_table_query, (table_name,))
        existing_row = check_table_result.fetchone()
        
        if existing_row:
            update_query = "UPDATE DATALAKE_DB.DATA_SCIENCE.TRN_DATA_UPLOAD_MASTER_TABLE SET PRIMARY_KEY = %s WHERE TABLE_NAME = %s"
            combined_primary_keys = ', '.join(primary_keys)
            conn.cursor().execute(update_query, (combined_primary_keys, table_name))
        else:
            insert_query = "INSERT INTO DATALAKE_DB.DATA_SCIENCE.TRN_DATA_UPLOAD_MASTER_TABLE (TABLE_NAME, PRIMARY_KEY) VALUES (%s, %s)"
            combined_primary_keys = ', '.join(primary_keys)
            conn.cursor().execute(insert_query, (table_name, combined_primary_keys))
            
        return True
    except Exception as e:
        st.error(f"Error updating master table: {str(e)}")
        return False


# Function to get values from master table
def get_master_table_values(conn):
    try:
        master_table_query = "SELECT * FROM DATALAKE_DB.DATA_SCIENCE.TRN_DATA_UPLOAD_MASTER_TABLE"
        master_table_result = conn.cursor().execute(master_table_query)
        rows = master_table_result.fetchall()
        
        # Get column names from cursor description
        columns = [desc[0] for desc in master_table_result.description]

        return columns, rows
    except Exception as e:
        st.error(f"Error fetching master table values: {str(e)}")
        return [], []

def primary_keys_updated(conn, table_name):
    try:
        query = "SELECT PRIMARY_KEY FROM DATALAKE_DB.DATA_SCIENCE.TRN_DATA_UPLOAD_MASTER_TABLE WHERE TABLE_NAME = %s"
        result = conn.cursor().execute(query, (table_name,))
        row = result.fetchone()
        if row:
            return True
        else:
            return False
    except Exception as e:
        st.error(f"Error checking primary keys for table {table_name}: {str(e)}")
        return False
    
# Function to filter out databases from the list
def filter_databases(databases):
    allowed_databases = ["DATALAKE_DB", "PRD_DB"]
    filtered_databases = [db for db in databases if db in allowed_databases]
    return filtered_databases

def filter_prod_schemas(schemas):
    prod_allowed_schemas = ["DATA_SCIENCE"]
    prod_filtered_schemas = [schema for schema in schemas if schema in prod_allowed_schemas]
    return prod_filtered_schemas


def update_master_table_page(conn):

    if conn:
        st.success("Connected to Snowflake Prod Database!")

        # Get list of databases
        databases = get_databases(conn)

        # Filter databases
        filtered_databases_prod = filter_databases(databases)
        
        selected_database = st.selectbox("Select Database", filtered_databases_prod)

        # Get list of schemas
        schemas = get_schemas(conn, selected_database)
        filtered_schemas_prod = filter_prod_schemas(schemas)
        selected_schema = st.selectbox("Select Schema", filtered_schemas_prod)


        # Get list of tables
        tables = get_tables(conn, selected_database, selected_schema)
        table_name = st.selectbox("Select Target Table", tables)

        # Get list of columns for selected table
        describe_table_query = f"DESCRIBE TABLE {table_name}"
        describe_table_result = conn.cursor().execute(describe_table_query)
        columns = [row[0] for row in describe_table_result]


        if primary_keys_updated(conn, table_name):
            st.warning("Primary keys already exist for this table!")
            existing_primary_keys = get_primary_keys(conn, table_name)
            if existing_primary_keys:
                st.info("Existing Primary Keys: " + ", ".join(existing_primary_keys))
            else:
                st.info("No primary keys found.")
                
        # Select multiple primary keys
        selected_primary_keys = st.multiselect("Select Primary Keys", columns)


        # Button to update master table
        st.markdown(
        """<style>
            .element-container:nth-of-type(even) button {
                background-color: red; /* Light gray background color */
                color: white; /* Black text color */
            }
            </style>""",
        unsafe_allow_html=True
    )
        # Move data button
        st.markdown('<span id="button-after"></span>', unsafe_allow_html=True)
        if st.button("Update Primary Key Mapping",type="primary"):
            if update_master_table(conn, table_name, selected_primary_keys):
                st.success("Master table updated successfully!")
            else:
                st.error("Failed to update Primary Key Mapping table!")

        # Display master table values
        master_table_columns, master_table_values = get_master_table_values(conn)
        if master_table_values:
            st.subheader("Master Table Values")
            master_table_df = pd.DataFrame(master_table_values, columns=master_table_columns)
            # Display DataFrame with container width by default
            st.dataframe(master_table_df, use_container_width=True)
        else:
            st.warning("No data in master table.")
