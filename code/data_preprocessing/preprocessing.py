from typing import Any
import pandas as pd
import numpy as np
from sklearn.impute import KNNImputer


class PreProcessor:
    """
    Class Name : PreProcessor
    Description : This class shall be used to pre-process the data(to clean and transform)
    before training
    Method(s) :
    Version : 1.0
    Revisions : None
    """

    def __init__(self, file_obj, logger_obj) -> None:
        self.file_obj = file_obj
        self.logger_obj = logger_obj

    def remove_columns(self, df, columns_to_remove):
        """
        Method Name : remove_columns
        Class Name : PreProcessor
        Description : This method removes the given columns from a pandas DataFrame
        Input(s):
            df : Pandas DataFrame
            columns_to_remove : the columns to remove from the dataframe
        Ouput(s) : Pandas DataFrame after removal of the specified columns
        On Failure : Raises Exception
        Version : 1.0
        Revisions : None
        """

        # logging
        log_msg = f"""Entered the remove_columns method in PreProcessing class."""
        self.logger_obj.log(self.file_obj, log_msg)

        self.df = df
        self.columns_to_remove = columns_to_remove

        try:
            self.useful_df = self.df.drop(columns=self.columns_to_remove, 
                                            axis=1)  # dropping columns from df
            log_msg = f"""column removal is successful, exiting the method remove_columns in PreProcessing class"""
            self.logger_obj.log(self.file_obj, log_msg)

            return self.useful_df
        except Exception as e:
            error_log_msg = f"""Exception occured in remove_columns method in PreProcessing class.
            Error message : {str(e)}"""
            self.logger_obj.log(self.file_obj, error_log_msg)

            error_log_msg = f"""Column removal is unsuccessful, exited remove_columns method in Preprocessing class"""
            self.logger_obj.log(self.file_obj, error_log_msg)
            raise Exception()
        
    def separate_label_feature(self, df, label_column_name):
        """
        Method Name : separate_label_feature
        Class Name : PreProcessor
        Description : This method separates the features and label columns
        Input(s) : 
            df : input dataframe
            label_coolumn_name : (str), label column name
        Output(s): returns two dataframes
            X : dataframe of features
            y : dataframe of label
        On Failure : Raises Exception
        Version : 1.0
        Revisions : None
        """

        # logging
        log_msg = f"""Entered the separate_label_feature method in PreProcessor class"""
        self.logger_obj.log(self.file_obj, log_msg)

        try:
            self.X = df.drop(columns=[label_column_name], axis=1)  # drop the label column
            # and extract features columns to variable X
            self.y = df[label_column_name]  # filtering label columns

            log_msg = f"""Label separation is successful. 
            Exited separate_label_feature method in PreProcessor class"""
            self.logger_obj.log(self.file_obj, log_msg)

            return self.X, self.y
        except Exception as e:
            error_log_msg = f"""Exception occured in separate_label_feature method in PreProcessor class.
            Exception message : {str(e)}"""
            self.logger_obj(self.file_obj, error_log_msg)

            error_log_msg = f"""Label separation is unsuccessful. Exited separate_label_feature method in PreProcessor class"""
            self.logger_obj(self.file_obj, error_log_msg)
            raise Exception()
        
    def is_null_present(self, df):
        """
        Method Name : is_null_present
        Class Name : PreProcessor
        Description : This method checks whether there are null values present in
        the input dataframe
        Input(s):
            df : input dataframe to check if it contains any null values
        Output(s): returns a boolean value
            null_present : True if null values are present in df, False if null values
            are not present in df
        On Faiure : Raises Exception
        Version : 1.0
        Revisions : None
        """
        
        # logging
        log_msg = f"""Entered the is_null_present method in PreProcessor class"""
        self.logger_obj.log(self.file_obj, log_msg)
        
        try:
            self.null_present = False
            self.null_counts = df.isna().sum()  # count of null values per column

            for i in self.null_counts:
                if i > 0:
                    self.null_present = True
                    break

            if self.null_present == True:
                df_null_count = self.null_counts.to_frame(name="null count")
                df_null_count.to_csv('preprocessing_data/null_value_counts.csv')  # storing null
                # column information to a file for further reference
            
            # logging
            log_msg = f"""Finding missing values if successful. Data written to null_value_counts.csv file in preprocessing_data folder.
            Exiting is_null_present method from PreProcessor class"""
            self.logger_obj.log(self.file_obj, log_msg)

            return self.null_present
        
        except Exception as e:
            error_log_msg = f"""Exception occured in is_null_present method of PreProcessor class.
            Exception message : {str(e)}"""
            self.logger_obj.log(self.file_obj, error_log_msg)

            error_log_msg = f"""Fiding whether the dataframe contains missing values or not? is failed.
            Exiting is_null_present method of PreProcessor class."""
            self.logger_obj(self.file_obj, error_log_msg)
            raise Exception()
        
    def impute_missing_values(self, df):
        """
        Method Name : impute_missing_values
        Class Name : PreProcessor
        Description : This method replaces all the missing values in the input dataframe
        using KNNImputer
        Input(s):
            df : input dataframe
        Output(s): 
            df_new : A dataframe with all the missing values are imputed
        Version : 1.0
        Revisions : None
        """

        # logger
        log_msg = f"""Entered the 'impute_missing_values' method of the 'PreProcessor' class"""
        self.logger_obj.log(self.file_obj, log_msg)

        try:
            self.df = df
            knn_imputer = KNNImputer(weights='uniform', missing_values=np.nan)
            self.df_new = pd.DataFrame(knn_imputer.fit_transform(self.df), columns=self.df.columns)
            
            # logging 
            log_msg = f"""Impuring missing values is successful.
            Exited impute_missing_values method on PreProcessor class"""
            self.logger_obj.log(self.file_obj, log_msg)

            return self.df_new
        
        except Exception as e:
            error_log_msg = f"""Exception occured in impute_missing_values method of PreProcessor class.
            Exception message : {str(e)}"""
            self.logger_obj.log(self.file_obj, error_log_msg)

            error_log_msg = f"""Imputing missing values is unsuccessful. 
            Exiting the impute_missing_values method of PreProcessor class"""
            self.logger_obj.log(self.file_obj, error_log_msg)
            raise Exception()
        
    def get_columns_with_zero_std_deviation(self, df):
        """
        Method Name : get_columns_with_zero_std_deviation,
        Class Name : PreProcessor
        Description : This method retuns a list of columsn which have zero standard deviation.
        If the standard deviation is zero, then the column is populated by one value. 
        So if your goal is to prepare the data for regression or classfication, 
        you can throw the column out, since it will contribute nothing to the regression or classification.
        Input(s):
            df : input dataframe
        Output(s): 
            coulumns_lst_with_zero_std_dev : returns a list of column names for which standard deviation is zero.
        Version : 1.0,
        Revisions : None
        """

        log_msg = f"""Entered the get_columns_with_zero_std_deviation method in PreProcessor class"""
        self.logger_obj.log(self.file_obj, log_msg)

        self.columns = df.columns  # list of all columns in the input dataframe
        self.df_description = df.describe()
        self.columns_lst_with_zero_std_dev = []  # creating empty list

        try:
            for col in self.columns:
                if (self.df_description[col]['std'] == 0):
                    self.columns_lst_with_zero_std_dev.append(col)
            # logging
            log_msg = f"""Columns with zero standard deviation are {self.columns_lst_with_zero_std_dev}. 
            Exited get_columns_with_zero_std_deviation method of PreProcessor class"""
            self.logger_obj.log(self.file_obj, log_msg)

            return self.columns_lst_with_zero_std_dev
        except Exception as e:
            error_log_msg = f"""Exception occures in get_columns_with_zero_std_deviation method of PreProcessor class.
            Exception message : {str(e)}"""
            self.logger_obj.log(self.file_obj, error_log_msg)

            error_log_msg = f"""Column search for columns with zero standard deviation is failed.
            Exiting get_columns_with_zero_std_deviation method of PreProcessor class"""
            self.logger_obj.log(self.file_obj, error_log_msg)
            raise Exception()


