from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV
from xgboost import XGBClassifier
from sklearn.metrics import roc_auc_score, accuracy_score

from application_logging.logger import AppLogger
import numpy as np


class ModelFinder:
    """
    Class Name : ModelFinder
    Description : This class is used to find the model with best performance
    metrics (accuray_score and roc_auc_score)
    Version : 1.0
    Revision : None
    """

    def __init__(self, file_obj, logger_obj) -> None:
        self.file_obj = file_obj
        self.logger_obj = logger_obj
        self.random_forest_clf = RandomForestClassifier()
        self.xgb_clf = XGBClassifier(objective="binary:logistic")

    def get_best_params_for_random_forest(self, X_train, y_train):
        """
        Method Name : get_best_params_for_random_forest
        Description : This method is used to get best parameters for Random
        Forest algorithm by performing hyper parameter tuning
        Output : The best random forest model with best hyper parameters
        On Failure : Raises exception
        Version : 1.0
        Revisions : None
        """

        # logging
        log_msg = """Entered the get_best_params_for_random_forest method of ModelFinder class"""
        self.logger_obj.log(self.file_obj, log_msg)

        try:
            # initializing the parameter grid with different combinations of parameters
            self.params_grid = {
                "n_estimators": [10, 50, 100, 130],
                "criterion": ['gini', 'entropy'],
                "max_depth": range(2, 4, 1),
                "max_features": ['sqrt', 'log2'],
            }

            # creating an object of the GridSearchCV class
            self.grid = GridSearchCV(estimator=self.random_forest_clf,
                                     param_grid=self.params_grid,
                                     cv=5,
                                     verbose=3)
            # finding the best parameters
            self.grid.fit(X_train, y_train)
            
            # extracting the best parameters
            self.n_estimatores = self.grid.best_params_["n_estimators"]
            self.criterion = self.grid.best_params_["criterion"]
            self.max_depth = self.grid.best_params_["max_depth"]
            self.max_features = self.grid.best_params_["max_features"]

            # creating a new model with the best parameters
            self.random_forest_clf = RandomForestClassifier(n_estimators=self.n_estimatores,
                                                            criterion=self.criterion,
                                                            max_depth=self.max_depth,
                                                            max_features=self.max_features)
            # training the model with the best parameters
            self.random_forest_clf.fit(X_train, y_train)
            
            # logging
            log_msg = f"""Random forest best parameters found: {str(self.grid.best_params_)}. 
            Exited the method get_best_params_for_random_forest of ModelFinder class"""
            self.logger_obj.log(self.file_obj, log_msg) 

            return self.random_forest_clf
        
        except Exception as e:
            error_log_msg = f"""Exception occured in get_best_params_for_random_forest method of ModelFinder class.
            Exception message: {str(e)}"""
            self.logger_obj.log(self.file_obj, error_log_msg)

            error_log_msg = f"""Random Forest hyper parameter tuning is failed.
            Exited the method get_best_params_for_random_forest of ModelFinder class"""
            self.logger_obj.log(self.file_obj, error_log_msg)
            raise Exception()
        
    def get_best_params_for_xgboost(self, X_train ,y_train):
        """
        Method Name : get_best_params_for_xgboost
        Description : This method is used to get best parameters for xgboost
        algorithm by performing hyper parameter tuning
        Output : The best xgboost model with best hyper parameters
        On Failure : Raises exception
        Version : 1.0
        Revisions : None
        """

        # logging
        log_msg = f"""Entered the get_best_params_for_xgboost method of the ModelFinder class"""
        self.logger_obj.log(self.file_obj, log_msg)

        try:
            # initializing the parameter grid with different combinations of parameters
            self.params_grid_xgboost = {
                "n_estimators": [10, 50, 100, 200],
                "max_depth": [3, 5, 10, 20],
                "learning_rate": [0.5, 0.1, 0.01, 0.001],
            }

            # creating an object of the GridSearchCV class
            self.grid = GridSearchCV(self.xgb_clf,
                                     self.params_grid_xgboost,
                                     verbose=3,
                                     cv=5)
            # finding the best parameters
            self.grid.fit(X_train, y_train)

            # extracting the best parameters
            self.n_estimators = self.grid.best_params_["n_estimators"]
            self.max_depth = self.grid.best_params_["max_depth"]
            self.learning_rate = self.grid.best_params_["learning_rate"]

            # creating new model with best parameters found
            self.xgb_clf = XGBClassifier(n_estimators=self.n_estimators,
                                         max_depth=self.max_depth,
                                         learning_rate=self.learning_rate)
            # training the new model
            self.xgb_clf.fit(X_train, y_train)

            # logging
            log_msg = f"""XGBoost best parameters found are : {str(self.grid.best_params_)}.
            Exited get_best_params_for_xgboost method of ModelFinder class"""
            self.logger_obj.log(self.file_obj, log_msg)

            return self.xgb_clf
        
        except Exception as e:
            log_msg = f"""Exception occured in get_best_params_for_xgboost method of ModelFinder class.
            Exception message : {str(e)}"""
            self.logger_obj.log(self.file_obj, log_msg)

            log_msg = f"""XGBoost parameter tuning failed. 
            Exited get_best_params_for_xgboost method of ModelFinder class"""
            self.logger_obj.log(self.file_obj, log_msg)
            raise Exception()
        
    def get_best_model(self, X_train, y_train, X_test, y_test):
        """
        Method Name : get_best_model
        Class Name : ModelFinder
        Description : Finding out the model with best AUC score
        Output : The best model and the model object
        On Failure : Raises Exception
        Version : 1.0
        Revisions : None
        """

        # logging
        log_message = f"""Entered the get_best_model method of ModelFinder class."""
        self.logger_obj.log(self.file_obj, log_message)

        try:
            # creating best model for XGBoost
            self.xgboost = self.get_best_params_for_xgboost(X_train, y_train)
            self.prediction_xgboost = self.xgboost.predict(X_test)  # predictions with XGBoost model

            # if there is only one label in y, roc_auc_score returns error
            # we will use accuracy in that case
            if len(y_test.unique()) == 1:
                self.xgboost_score = accuracy_score(y_test, self.prediction_xgboost)
                # logging
                log_msg = f"""Accuracy for XGBoost : {str(self.xgboost_score)}"""
                self.logger_obj.log(self.file_obj, log_msg)
            else:
                self.xgboost_score = roc_auc_score(y_test, self.prediction_xgboost)
                # logging
                log_msg = f"""AUC for XGBoost : {str(self.xgboost_score)}"""
                self.logger_obj.log(self.file_obj, log_msg)

            # creating best model for RandomForest
            self.random_forest = self.get_best_params_for_random_forest(X_train, y_train)
            self.prediction_random_forest = self.random_forest.predict(X_test)  # predictions with RandomForest model

            # if there is only one label in y, roc_auc_score returns error
            # we will use accuracy in that case
            if len(y_test.unique()) == 1:
                self.random_forest_score = accuracy_score(y_test, self.prediction_random_forest)
                # logging
                log_msg = f"""Accuracy for RandomForest : {str(self.random_forest_score)}"""
                self.logger_obj.log(self.file_obj, log_msg)
            else:
                self.random_forest_score = roc_auc_score(y_test, self.prediction_random_forest)
                # logging
                log_msg = f"""AUC for RandomForest : {str(self.random_forest_score)}"""
                self.logger_obj.log(self.file_obj, log_msg)

            # comparing the two models
            if(self.random_forest_score < self.xgboost_score):
                log_msg = f"""The best model is XGBoost with score of {str(self.xgboost_score)}"""
                self.logger_obj.log(self.file_obj, log_msg)
                return 'XGBoost', self.xgboost
            else:
                log_msg = f"""The best model is RandomForest with score of {str(self.random_forest_score)}"""
                self.logger_obj.log(self.file_obj, log_msg)
                return "RandomForest", self.random_forest

        except Exception as e:
            error_log_msg = f"""Exception occured in get_best_model method in ModelFinder class.
            Exception message : {str(e)}"""
            self.logger_obj.log(self.file_obj, error_log_msg)

            error_log_msg = f"""Model selection failed. Exited get_best_model method in ModelFinder class"""
            self.logger_obj.log(self.file_obj, error_log_msg)
            raise Exception()
        





