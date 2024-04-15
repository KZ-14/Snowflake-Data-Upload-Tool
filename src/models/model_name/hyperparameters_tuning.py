"""
Hyperparameter Tuner

This module provides a class for hyperparameter tuning of an ML model.

Author: Your Name
Version: 1.0.0
"""

from sklearn.model_selection import GridSearchCV


class HyperparameterTuner:
    def __init__(self, model, param_grid):
        """
        HyperparameterTuner Constructor

        Initializes the HyperparameterTuner class.

        Inputs:
            - model: ML model object.
            - param_grid: Dictionary specifying the hyperparameters and their possible values.
        """
        self.model = model
        self.param_grid = param_grid

    def tune_hyperparameters(self, X, y):
        """
        Tune Hyperparameters

        This method performs hyperparameter tuning using GridSearchCV.

        Inputs:
            - X (array-like): Features.
            - y (array-like): Target variable.

        Returns:
            - best_params (dict): Best hyperparameters found during tuning.
        """
        grid_search = GridSearchCV(self.model, self.param_grid, cv=5)
        grid_search.fit(X, y)
        best_params = grid_search.best_params_
        return best_params
