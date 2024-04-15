"""
Data Ingestion Module

This module provides a class for ingesting cleaned and processed data into a database.

Author: Your Name
Version: 1.0.0
"""

import pandas as pd
from sqlalchemy import create_engine


class DataIngestion:
    def __init__(self, data, database_uri):
        """
        DataIngestion Constructor

        Initializes the DataIngestion class.

        Inputs:
            - data (pandas.DataFrame): Cleaned and processed data to be ingested.
            - database_uri (str): URI of the database to connect to.
        """
        self.data = data
        self.database_uri = database_uri

    def ingest_data(self, table_name):
        """
        Ingest Data

        This method ingests the cleaned and processed data into a database table.

        Inputs:
            - table_name (str): Name of the table to ingest the data into.
        """
        # Create database connection
        engine = create_engine(self.database_uri)

        # Ingest data into the database table
        self.data.to_sql(table_name, engine, if_exists='replace', index=False)

        # Additional steps if required
