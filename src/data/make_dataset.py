"""
Data Processor

This module provides a class for processing the data by splitting, cleaning, creating features, and pushing to a database.

Author: Your Name
Version: 1.0.0
"""

import pandas as pd
from sklearn.model_selection import train_test_split
from data_cleaning import DataCleaner
from feature_engineering import FeatureEngineer
from data_ingestion import DataIngestion


class DataProcessor:
    def __init__(self, data, database_uri):
        """
        DataProcessor Constructor

        Initializes the DataProcessor class.

        Inputs:
            - data (pandas.DataFrame): Raw data to be processed.
            - database_uri (str): URI of the database to connect to.
        """
        self.data = data
        self.database_uri = database_uri

    def process_data(self, table_name):
        """
        Process Data

        This method performs the data processing steps including splitting, cleaning, feature engineering,
        and pushing the processed data into a database.

        Inputs:
            - table_name (str): Name of the table to push the processed data into.
        """
        # Split data into train, test, and validation sets
        train_data, test_data = train_test_split(self.data, test_size=0.2, random_state=42)
        train_data, val_data = train_test_split(train_data, test_size=0.2, random_state=42)

        # Clean data
        cleaner = DataCleaner()
        train_data_cleaned = cleaner.clean_data(train_data)
        test_data_cleaned = cleaner.clean_data(test_data)
        val_data_cleaned = cleaner.clean_data(val_data)

        # Perform feature engineering
        feature_engineer = FeatureEngineer()
        train_data_processed = feature_engineer.create_features(train_data_cleaned)
        test_data_processed = feature_engineer.create_features(test_data_cleaned)
        val_data_processed = feature_engineer.create_features(val_data_cleaned)

        # Push processed data into the database
        data_ingestion = DataIngestion(database_uri=self.database_uri)
        data_ingestion.ingest_data(train_data_processed, table_name + "_train")
        data_ingestion.ingest_data(test_data_processed, table_name + "_test")
        data_ingestion.ingest_data(val_data_processed, table_name + "_validation")
