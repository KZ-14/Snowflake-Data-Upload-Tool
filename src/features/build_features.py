"""
Feature Builder

This module provides a class for building features from the data.

Author: Name
Version: 1.0.0
"""

import pandas as pd


class FeatureBuilder:
    def __init__(self, data):
        """
        FeatureBuilder Constructor

        Initializes the FeatureBuilder class.

        Inputs:
            - data (pandas.DataFrame): Data for feature engineering.
        """
        self.data = data

    def build_features(self):
        """
        Build Features

        This method performs feature engineering on the data.

        Returns:
            - features (pandas.DataFrame): DataFrame with engineered features.
        """
        # Perform feature engineering operations
        features = self.data.copy()

        # Additional feature engineering steps if required
        features = self._add_feature_1(features)
        features = self._add_feature_2(features)

        return features

    def _add_feature_1(self, data):
        """
        Add Feature 1

        This method adds a new feature to the data.

        Inputs:
            - data (pandas.DataFrame): Data to add the feature to.

        Returns:
            - data (pandas.DataFrame): Data with the added feature.
        """
        # Add feature 1 logic
        data['feature_1'] = data['column1'] + data['column2']

        return data

    def _add_feature_2(self, data):
        """
        Add Feature 2

        This method adds another feature to the data.

        Inputs:
            - data (pandas.DataFrame): Data to add the feature to.

        Returns:
            - data (pandas.DataFrame): Data with the added feature.
        """
        # Add feature 2 logic
        data['feature_2'] = data['column3'] - data['column4']

        return data