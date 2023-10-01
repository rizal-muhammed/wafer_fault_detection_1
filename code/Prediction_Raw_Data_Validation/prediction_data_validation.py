import json
import os
import shutil
import regex
from datetime import datetime
import pandas as pd

from application_logging.logger import AppLogger


class PredictionDataValidation:
    """
    Class Name : PredictionDataValidation  
    Description : This class shall be used for handling all the validation done on
    the Raw Prediction Data!  
    Version : 1.0  
    Revisions : None  
    """

    def __init__(self, path) -> None:
        self.batch_dir = path
        self.schema_path = "schema_prediction.json"
        self.logger = AppLogger()

    def values_from_schema(self,):
        """
        Method Name : values_from_schema  
        Class Name : PredictionDataValidation  
        Description : This method extract all the relevant information from the pre-defined
        "Schema" file   
        Input(s) : None  
        Output(s) : returns
            len_of_date_stamp_in_file: length of data stamp in file.  
            len_of_time_stamp_in_file: length of time stamp in file.  
            column_names: column names.   
            no_of_cols : number of columns.  
        On Failure : Raise ValueError, KeyError, Exception  
        Version : 1.0  
        Revisions : None  
        """

        try:
            with open(self.schema_path, "r") as f:
                dic = json.load(f)
                f.close()
            
            sample_file_name = dic["SampleFileName"]
            len_of_date_stamp_in_file = dic["LengthOfDateStampInFile"]
            len_of_time_stamp_in_file = dic["LengthOfTimeStampInFile"]
            no_of_cols = dic["NumberofColumns"]
            column_names = dic["ColName"]

            log_file = open("Prediction_Logs/valuesfromSchemaValidationLog.txt", "a+")
            msg = f"""LengthOfDateStampInFile : {str(len_of_date_stamp_in_file)} \t 
            LengthOfTimeStampInFile : {str(len_of_time_stamp_in_file)} \t
            NumberofColumns : {str(no_of_cols)}
            """
            self.logger.log(log_file, msg)

            log_file.close()

        except ValueError as v:
            log_file = open("Prediction_Logs/valuesfromSchemaValidationLog.txt", "a+")
            error_log_msg = f"""ValueError : Value not found in schema_training.json"""
            self.logger.log(log_file, error_log_msg)
            log_file.close()
            raise v
        
        except KeyError as k:
            log_file = open("Prediction_Logs/valuesfromSchemaValidationLog.txt", "a+")
            error_log_msg = f"""KeyError : key not found in schema_training.json. Incorrect key
            passed"""
            self.logger.log(log_file, error_log_msg)
            log_file.close()
            raise k
        
        except Exception as e:
            log_file = open("Prediction_Logs/valuesfromSchemaValidationLog.txt", "a+")
            error_log_msg = f"""Exception occured in 'values_from_schema' of 'RawDataValidation' class.
            Exception message : {str(e)}"""
            self.logger.log(log_file, error_log_msg)

            error_log_msg = f"""Relevant information from pre-defined schema could not be extracted.
            Exiting 'values_from_schema' of 'RawDataValidation' class."""
            log_file.close()
            raise e
        
        return len_of_date_stamp_in_file, len_of_time_stamp_in_file, column_names, no_of_cols
    
    def manual_regex(self,):
        """
        Method Name : manual_regex  
        Class Name : PredictionDataValidation  
        Description : This method contains manually defined regex pattern 
        based on "FileName", which can be used to validate the file name 
        of prediction data.
        Input(s) : None  
        Output(s) :  
            returns regex pattern  
        Version : 1.0  
        Revisions : None  
        """

        regex_pattern = "['wafer']+['\_'']+[\d_]+[\d]+\.csv"
        return regex_pattern
    
    def create_directory_for_good_bad_raw_data(self):
        """
        Method Name : create_dictionary_for_good_bad_raw_data  
        Class Name : PredictionDataValidation  
        Description : This method creates directories to store the Good_Data 
        and Bad_Data after validation the prediction data.  
        Input(s) : None  
        Output(s) : None  
        On Failure : Raises OSError  
        Version : 1.0  
        Revisions : None  
        """

        try:
            log_file = open("Prediction_Logs/General_Log.txt", "a+")
            msg = f"""Entered 'create_directory_for_good_bad_raw_data' method of 'RawDataValidation' class."""
            self.logger.log(log_file, msg)

            path = os.path.join("Prediction_Raw_Files_Validated/", "Good_Raw/")
            if not os.path.isdir(path):
                os.mkdir(path)
            
            path = os.path.join("Prediction_Raw_Files_Validated/", "Bad_Raw/")
            if not os.path.isdir(path):
                os.mkdir(path)

            msg = f"""Creation of Good_Raw, Bad_Raw folder is successful. 
            Exited 'create_directory_for_good_bad_raw_data' method of 'PredictionDataValidation' class."""
            self.logger.log(log_file, msg)
            log_file.close()

        except OSError as o:
            log_file = open("Training_Logs/General_Log.txt", "a+")
            error_log_msg = f"""Error while creating directory.
            Error message : {str(o)}"""
            self.logger.log(log_file, error_log_msg)

            error_log_msg = f"""Exited 'create_directory_for_good_bad_raw_data' method of 'PredictionDataValidation' class."""
            self.logger.log(log_file, error_log_msg)

            log_file.close()

            raise OSError
        
        except Exception as e:
            log_file = open("Prediction_Logs/General_Log.txt", "a+")
            error_log_msg = f"""Exception occured  at 'create_directory_for_good_bad_raw_data' method of 'PredictionDataValidation' class.
            Exception message : {str(e)}"""
            self.logger.log(log_file, error_log_msg)

            error_log_msg = f"""Exited 'create_directory_for_good_bad_raw_data' method of 'PredictionDataValidation' class."""
            self.logger.log(log_file, error_log_msg)

            log_file.close()

            raise 
        
    def delete_exising_good_data_training_folder(self):
        """
        Method Name : delete_exising_good_data_training_folder  
        Class Name : PredictionDataValidation  
        Description : This method deletes the directory made to store the Good Data, after
        loading the data into the table. Once the good files are loaded in DB,
        deleting the directory ensures space optimization.
        Input(s) : None  
        Output(s) : None  
        On Failure : Raises OSError, Exception  
        Version : 1.0  
        Revisions : None  
        """

        try:
            log_file = open("Prediction_Logs/General_Log.txt", "a+")
            log_msg = f"""Entered 'delete_exising_good_data_training_folder' method of 'PredictionDataValidation' class."""
            self.logger.log(log_file, log_msg)

            path = "Prediction_Raw_Files_Validated/"
            if os.path.isdir(path + "Good_Raw/"):
                shutil.rmtree(path + "Good_Raw/")
                log_msg = f"""GoodRaw directory deleted successfully!"""
                self.logger.log(log_file, log_msg)
            
            log_file.close()
                
        except OSError as o:
            log_file = open("Prediction_Logs/General_Log.txt", "a+")
            error_log_msg = f"""Error while deleting directory, Error message : {str(o)}"""
            self.logger.log(log_file, error_log_msg)

            error_log_msg = f"""Exited 'delete_exising_good_data_training_folder' method of 'PredictionDataValidation' class."""
            self.logger.log(log_file, error_log_msg)

            log_file.close()

        except Exception as e:
            log_file = open("Prediction_Logs/General_Log.txt", "a+")
            error_log_msg = f"""Exception while deleting directory, Exception message : {str(e)}"""
            self.logger.log(log_file, error_log_msg)

            error_log_msg = f"""Exited 'delete_exising_good_data_training_folder' method of 'PredictionDataValidation' class."""
            self.logger.log(log_file, error_log_msg)

            log_file.close()

    def delete_exising_bad_data_training_folder(self):
        """
        Method Name : delete_exising_bad_data_training_folder  
        Class Name : PredictionDataValidation  
        Description : This method deletes the directory made to store the Bad Data.
        Input(s) : None  
        Output(s) : None  
        On Failure : Raises OSError, Exception  
        Version : 1.0  
        Revisions : None  
        """

        try:
            log_file = open("Prediction_Logs/General_Log.txt", "a+")
            log_msg = f"""Entered 'delete_exising_bad_data_training_folder' method of 'PredictionDataValidation' class."""
            self.logger.log(log_file, log_msg)

            path = "Training_Raw_Files_Validated/"
            if os.path.isdir(path + "Bad_Raw/"):
                shutil.rmtree(path + "Bad_Raw/")
                log_msg = f"""BadRaw directory deleted successfully!"""
                self.logger.log(log_file, log_msg)
            
            log_file.close()
                
        except OSError as o:
            log_file = open("Training_Logs/General_Log.txt", "a+")
            error_log_msg = f"""Error while deleting directory, Error message : {str(o)}"""
            self.logger.log(log_file, error_log_msg)

            error_log_msg = f"""Exited 'delete_exising_bad_data_training_folder' method of 'PredictionDataValidations' class."""
            self.logger.log(log_file, error_log_msg)

            log_file.close()

        except Exception as e:
            log_file = open("Prediction_Logs/General_Log.txt", "a+")
            error_log_msg = f"""Exception while deleting directory, Exception message : {str(e)}"""
            self.logger.log(log_file, error_log_msg)

            error_log_msg = f"""Exited 'delete_exising_bad_data_training_folder' method of 'PredictionDataValidation' class."""
            self.logger.log(log_file, error_log_msg)

            log_file.close()

    def move_bad_files_to_archive_bad(self):
        """
        Method Name : move_bad_files_to_archive_bad  
        Class Name : PredictionDataValidation  
        Description : This method deletes the directory made to store the Bad Data,
        after moving the data in an archive folder. We archive the bad files to send them
        back to the client for invalid data issue.  
        Input(s) : None  
        Output(s) : None  
        Version : 1.0  
        Revisions : None  
        """

        now = datetime.now()
        date = now.date()
        time = now.strftime("%H:%M:%S")

        try:
            log_file_path = os.path.join("Prediction_Logs", "General_Log.txt")
            log_file = open(log_file_path, "a+")
            log_msg = f"""Entered 'move_bad_files_to_archive_bad' method of 'PredictionDataValidation' class."""
            self.logger.log(log_file, log_msg)


            source_path = "Prediction_Raw_Files_Validated/Bad_Raw/"
            if os.path.isdir(source_path):
                path = "PredictionArchivedBadData/"
                if not os.path.isdir(path):
                    os.mkdir(path)

                destination = "PredictionArchivedBadData/BadData_" + str(date) + "_" + str(time)
                if not os.path.isdir(destination):
                    os.mkdir(destination)

                files = os.listdir(source_path)
                for file in files:
                    if file not in os.listdir(destination):
                        shutil.move(source_path + file, destination)
                
                log_msg = f"""Bad files are moved to archive!"""
                self.logger.log(log_file, log_msg)

                path = "Prediction_Raw_files_validated/"
                if os.path.isdir(path + "Bad_Raw/"):
                    shutil.rmtree(path + "Bad_Raw/")

                log_msg = f"""Bad Raw Data folder deleted successfully!"""
                self.logger.log(log_file, log_msg)

                log_file.close()

        except Exception as e:
            log_file = open("Prediction_Logs/GeneralLog.txt", "a+")
            error_log_msg = f"""Exception occured while moving bad files to archive, 
            Exception message : {str(e)}"""
            self.logger.log(log_file, error_log_msg)

            error_log_msg = f"""Exited 'move_bad_files_to_archive_bad' method of 'PredictionDataValidation' class."""
            self.logger.log(log_file, error_log_msg)

            log_file.close()

            raise Exception
        
    def raw_file_name_validation(self,
                                regex_pattern,
                                len_of_date_stamp_in_file,
                                len_of_time_stamp_in_file):
        """
        Method Name : raw_file_name_validation  
        Class Name : PredictionDataValidation  
        Description : This function validates the name of the training csv files as per 
        given naming convention specified in the schema.
        regex pattern is used to validate the file name. If name format does not match,
        the file is moved to Bad Raw Data folder 
        If name format matches, then the file is moved to Good Raw Data folder  
        Input(s) :  
            regex_pattern : regex_pattern to validate the file name  
            len_of_date_stamp_in_file : lenth of date stamp in file  
            len_of_time_stamp_in_file : lenth of time stamp in file  
        Output(s) : None  
        On Failure : Raises Exception
        Version : 1.0  
        Revisions : None  
        """

        # delete the existing directories for good and bad data
        self.delete_exising_good_data_training_folder()
        self.delete_exising_bad_data_training_folder()

        # create new directories
        self.create_directory_for_good_bad_raw_data()

        # fetching only files in the batch directory
        all_items = os.listdir(self.batch_dir)  # all items in the directory
        only_files = [item for item in all_items if os.path.isfile(os.path.join(self.batch_dir, item)) and item != ".DS_Store"]

        try:
            with open("Prediction_Logs/nameValidationLog.txt", "a+") as log_file:
                log_msg = f"""Entered 'raw_file_name_validation' method of 'PredictionDataValidation' class."""
                self.logger.log(log_file, log_msg)
                for file in only_files:
                    if(regex.match(regex_pattern, file)):
                        split_at_dot = regex.split(".csv", file)
                        split_at_dot = (regex.split("_", split_at_dot[0]))
                        if (len(split_at_dot[1]) == len_of_date_stamp_in_file):
                            if (len(split_at_dot[2]) == len_of_time_stamp_in_file):
                                shutil.copy("Prediction_Batch_Files/" + file,
                                            "Prediction_Raw_Files_Validated/Good_Raw")
                                log_msg = f"""Valid file name. File {str(file)} moved to Good_Raw"""
                                self.logger.log(log_file, log_msg)
                            else:
                                shutil.copy("Prediction_Batch_Files/" + file,
                                            "Prediction_Raw_Files_Validated/Bad_Raw")
                                log_msg = f"""Not valid file name. File {str(file)} moved to Bad_Raw"""
                                self.logger.log(log_file, log_msg)
                        else:
                            shutil.copy("Prediction_Batch_Files/" + file,
                                            "Prediction_Raw_Files_Validated/Bad_Raw")
                            log_msg = f"""Invalid file name. File {str(file)} moved to Bad_Raw Folder"""
                            self.logger.log(log_file, log_msg)
                    else:
                            shutil.copy("Prediction_Batch_Files/" + file,
                                            "Prediction_Raw_Files_Validated/Bad_Raw")
                            log_msg = f"""Invalid file name. File {str(file)} moved to Bad_Raw Folder"""
                            self.logger.log(log_file, log_msg)

        except Exception as e:
            with open("Prediction_Logs/nameValidationLog.txt", "a+") as log_file:
                error_log_msg = f"""Excepton occured while validation file name,
                Exception message : {str(e)}"""
                self.logger.log(log_file, error_log_msg)

                error_log_msg = f"""Exiting from 'raw_file_name_validation' method of 'PredictionDataValidation' class."""
                self.logger.log(log_file, log_msg)
            raise Exception
        
    def validate_column_lenth(self, no_of_columns):
        """
        Method Name : validate_column_lenth  
        Class Name : PredictionDataValidation  
        Description : This method validates the number of columns in the csv file.
        If the number of columns is same as given in the schema, the file is kept inside Good_Raw,
        otherwise, if there is a mismatch between given number of columns and that is specified
        in the schema, then corresponding file is moved to Bad_Raw
        Input(s) :  
            no_of_columns : number of columns specified in the schema  
        Output(s) : None  
        On Failure : Raises Exception  
        Version : 1.0
        Revisions : None  
        """

        try:
            with open("Prediction_Logs/columnValidationLog.txt", "a+") as log_file:
                log_msg = f"""Column length validation started!"""
                self.logger.log(log_file, log_msg)

                all_items = os.listdir("Prediction_Raw_Files_Validated/Good_Raw/")  # all items in the directory
                only_files = [item for item in all_items if os.path.isfile(os.path.join("Prediction_Raw_Files_Validated/Good_Raw", item)) and item != ".DS_Store"]

                for file in only_files:
                    df = pd.read_csv("Prediction_Raw_Files_Validated/Good_Raw/" + file)
                    if df.shape[1] == no_of_columns:  # valid number of columns
                        pass  # retain the file in Good_Raw folder itelf
                    else:
                        shutil.move("Prediction_Raw_Files_Validated/Good_Raw/" + file,
                                    "Prediction_Raw_Files_Validated/Bad_Raw/")
                        log_msg = f"""file '{str(file)}' has invalid number of columns. Therefore moved into Bad_Raw folder."""
                        self.logger.log(log_file, log_msg)

                log_msg = f"""Column length validation is completed."""
                self.logger.log(log_file, log_msg)

        except OSError as o:
            with open("Prediction_Logs/columnValidationLog.txt", "a+") as log_file:
                error_log_msg = f"""Error occured while validating column length. 
                Error message : {str(o)}"""
                self.logger.log(log_file, error_log_msg)

                error_log_msg = f"""Exiting 'validate_column_lenth' method of 'PredictionDataValidation' class."""
                self.logger.log(log_file, error_log_msg)

        except Exception as e:
            with open("Prediction_Logs/columnValidationLog.txt", "a+") as log_file:
                error_log_msg = f"""Exception occured while validating column length. 
                Error message : {str(e)}"""
                self.logger.log(log_file, error_log_msg)

                error_log_msg = f"""Exiting 'validate_column_lenth' method of 'PredictionDataValidation' class."""
                self.logger.log(log_file, error_log_msg)

    def validate_missing_values_in_whole_column(self,):
        """
        Method Name : validate_missing_values_in_whole_column  
        Class Name : PredictionDataValidation  
        Description : if any column in the csv file has all the values as missing, then 
        such files are not suitable for processing. Therefore, this method moves corresponding
        files to Bad_Raw directory.
        If the first column's name, is missing, this method rename it as 'wafer'
        Input(s) : None  
        Output(s) : None  
        On Failure : Raises Exception  
        Version : 1.0
        Revisions : None
        """

        try:
            log_file = open("Prediction_Logs/missingValuesInColumn.txt", "a+")
            log_msg = f"""Entered 'validate_missing_values_in_whole_column' method in 'PredictionDataValidation' class."""
            self.logger.log(log_file, log_msg)

            log_msg = f"""Missing values for whole columns, is started !"""
            self.logger.log(log_file, log_msg)

            all_items = os.listdir("Prediction_Raw_Files_Validated/Good_Raw/")  # all items in the directory
            only_files = [item for item in all_items if os.path.isfile(os.path.join("Prediction_Raw_Files_Validated/Good_Raw", item)) and item != ".DS_Store"]

            for file in only_files:
                df = pd.read_csv(os.path.join("Prediction_Raw_Files_Validated/Good_Raw", file))

                # check if the first column name is missing
                if df.columns[0] == 'Unnamed: 0':  # if the first column name is missing, change the column name into 'wafer'
                    df = df.rename(columns={'Unnamed: 0': 'Wafer'}) 
                    df.to_csv(os.path.join("Prediction_Raw_Files_Validated/Good_Raw", file),
                                  index=False)
                    
                # Check if all values in each column are NaN
                all_nan_columns = df.isna().all()
                all_nan_columns_list = list(all_nan_columns[all_nan_columns.values == True].index)
                if len(all_nan_columns_list) > 0:
                    source_path = os.path.join("Prediction_Raw_Files_Validated/Good_Raw", file)
                    destination_path = os.path.join("Prediction_Raw_Files_Validated", "Bad_Raw")
                    if not os.path.exists(destination_path):
                        shutil.move(source_path, destination_path)
                        log_msg = f"""The file '{str(file)}' contain column with whole missing value.
                            Therefore, moved to Bad_Raw directory."""
                        self.logger.log(log_file, log_msg)
            
            log_msg = f"""Validation for Missing values for whole columns is completed!. 
            Exiting 'validate_missing_values_in_whole_column' method of 'PredictionDataValidation' class."""
            self.logger.log(log_file, log_msg)
            
            log_file.close()
                    
        except OSError as o:
            log_file = open("Prediction_Logs/missingValuesInColumn.txt", "a+")
            error_log_msg = f"""Error occured while validating whether whole columns of a file
                are missing values. Error message : {str(o)}"""
            self.logger.log(log_file, log_msg)

            error_log_msg = f"""Exiting 'validate_missing_values_in_whole_column' method of 'PredictionDataValidation' class."""
            self.logger.log(log_file, error_log_msg)

            log_file.close()
            
            raise OSError
        
        except Exception as e:
            log_file = open("Prediction_Logs/missingValuesInColumn.txt", "a+")
            error_log_msg = f"""Error occured while validating whether whole columns of a file
                are missing values. Error message : {str(e)}"""
            self.logger.log(log_file, log_msg)

            error_log_msg = f"""Exiting 'validate_missing_values_in_whole_column' method of 'PredictionDataValidation' class."""
            self.logger.log(log_file, error_log_msg)

            log_file.close()

            raise Exception
        
    def delete_prediction_file(self, ):
        """
        Method Name : 
        Class Name : 
        Description : 
        Input(s) : 
        Output(s) : 
        Version : 1.0
        Revisions : None 
        """

        try:
            if os.path.exists("Prediction_Output_File/Predictions.csv"):
                os.remove("Prediction_Output_File/Predictions.csv")
        except Exception as e:
            raise Exception

    