"""
Data Loader

This module provides a class for loading and preprocessing data for training an ML model.

Author: Your Name
Version: 1.0.0
"""

import pandas as pd
from .preprocessing import Preprocessing

class DataLoader:
    def __init__(self, train_file_path, test_file_path):
        """
        DataLoader Constructor

        Initializes the DataLoader class.

        Inputs:
            - train_file_path (str): Path to the training data file.
            - test_file_path (str): Path to the test data file.
        """
        self.train_file_path = train_file_path
        self.test_file_path = test_file_path

    def load_data(self, file_path):
        """
        Load Data

        This method loads the data from a specified file.

        Inputs:
            - file_path (str): Path to the data file.

        Returns:
            - data (pandas.DataFrame): Loaded data as a pandas DataFrame.
        """
        data = pd.read_csv(file_path)
        # Additional preprocessing steps if required
        return data

    def get_train_data(self):
        """
        Get Train Data

        This method retrieves the training data by calling the necessary data loading and preprocessing methods.

        Returns:
            - train_data (pandas.DataFrame): Preprocessed training data.
        """
        data = self.load_data(self.train_file_path)
        preprocessed_data = Preprocessing.preprocess_data(data)
        return preprocessed_data

    def get_test_data(self):
        """
        Get Test Data

        This method retrieves the test data by calling the necessary data loading and preprocessing methods.

        Returns:
            - test_data (pandas.DataFrame): Preprocessed test data.
        """
        data = self.load_data(self.test_file_path)
        preprocessed_data = Preprocessing.preprocess_data(data)
        return preprocessed_data
