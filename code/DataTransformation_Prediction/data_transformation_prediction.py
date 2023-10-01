import os
import pandas as pd

from application_logging.logger import AppLogger


class DataTransform:
    """
    Class Name : DataTransform
    Description : This class shall be used for transforming the Good Raw Training data
    before loading it into Database.
    Version : 1.0
    Revisions : None
    """

    def __init__(self) -> None:
        self.good_data_path = "Prediction_Raw_Files_Validated/Good_Raw"
        self.logger = AppLogger()

    def replace_missing_values_with_null(self, ):
        """
        Method Name : replace_missing_values_with_null  
        Class Name : DataTransform  
        Description : This method replaces the missing values in column with "NULL" to
        store into the table.
        Input(s) :  
        Output(s) :  
        Version : 1.0  
        Revisions : None  
        """

        try:
            log_file_path = "Prediction_Logs/dataTransformLog.txt"
            log_file = open(log_file_path, "a+")
            log_msg = f"""Entered 'replace_missing_values_with_null' method of 'DataTransform' class.
            File transformation started..."""
            self.logger.log(log_file, log_msg)

            all_items = os.listdir(self.good_data_path)  # all items in the directory
            only_files = [item for item in all_items if os.path.isfile(os.path.join(self.good_data_path, item)) and item != ".DS_Store"]

            for file in only_files:
                df = pd.read_csv(os.path.join(self.good_data_path, file))
                df = df.fillna("NULL")
                
                df.to_csv(os.path.join(self.good_data_path, file),
                          index=False,)
                
                log_msg = f"""File '{str(file)}' transformed successfully!"""
                self.logger.log(log_file, log_msg)
                log_file.close()

        except Exception as e:
            log_file_path = "Prediction_Logs/dataTransformLog.txt"
            log_file = open(log_file_path, "a+")
            error_log_msg = f"""Data transformation failed. Exception message : {str(e)}"""
            self.logger.log(log_file, error_log_msg)

            log_file.close()
