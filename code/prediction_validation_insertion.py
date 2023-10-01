
from Prediction_Raw_Data_Validation.prediction_data_validation import PredictionDataValidation
from DataTransformation_Prediction.data_transformation_prediction import DataTransform
from DataTypeValidation_Insertion_Prediction.data_type_validation_prediction import DBOperations
from application_logging.logger import AppLogger



class PredValidation:
    """
    Description : This class is used for Data validation for Prediction.
    
    """

    def __init__(self, path) -> None:
        self.raw_data = PredictionDataValidation(path)
        self.data_transform = DataTransform()
        self.db_operation = DBOperations()
        self.log_file = open("Prediction_Logs/Prediction_Log.txt", "a+")
        self.logger = AppLogger()

    def prediction_validation(self, ):
        try:
            log_msg = f"""Start of validation on files for prediction..."""
            self.logger.log(self.log_file, log_msg)

            # extracting values from prediction schem
            len_of_date_stamp_in_file, len_of_time_stamp_in_file, column_names, no_of_cols = self.raw_data.values_from_schema()

            # getting the regex defined to validate file name
            regex_pattern = self.raw_data.manual_regex()

            # validating file name of prediction files
            self.raw_data.raw_file_name_validation(regex_pattern,
                                                   len_of_date_stamp_in_file,
                                                   len_of_time_stamp_in_file)
            
            # validating column length in the prediction file
            self.raw_data.validate_column_lenth(no_of_cols)

            # validating whether any columns have whole missing values
            self.raw_data.validate_missing_values_in_whole_column()

            # logging, raw data validation completed
            log_msg = f"""Raw data validation completed!"""
            self.logger.log(self.log_file, log_msg)

            # logging, starting data transformation
            log_msg = f"""Starting data transformation..."""
            self.logger.log(self.log_file, log_msg)

            # replacing blanks in the csv file with "NULL"
            self.data_transform.replace_missing_values_with_null()

            # logging, data transformation is completed
            log_msg = f"""Data transformation is completed!"""
            self.logger.log(self.log_file, log_msg)
            
            # logging, creating db and tables
            log_msg = f"""Creating Prediction_Database and tables on the basis of given schema..."""
            self.logger.log(self.log_file, log_msg)

            # create database with given name, if already present, open the connection.
            # create tables with columns given in schema
            self.db_operation.create_table_db('Prediction', column_names)

            # logging, table creation is completed
            log_msg = f"""Database table creation is completed!"""
            self.logger.log(self.log_file, log_msg)

            # logging, insertion of data into db table is started
            log_msg = f"""Insertion of data into db table is started..."""
            self.logger.log(self.log_file, log_msg)
            
            # insert csv file into db table
            self.db_operation.insert_into_table_good_data('Prediction')

            # logging, insertion of data into db table is completed
            log_msg = f"""Insertion of data into db table is completed!"""
            self.logger.log(self.log_file, log_msg)

            # logging, deleting good data folder
            log_msg = f"""Deletion of Good Data folder is started..."""
            self.logger.log(self.log_file, log_msg)

            self.raw_data.delete_exising_good_data_training_folder()

            # logging, Good Data folder deletion completed
            log_msg = f"""Good Data folder deletion completed!"""
            self.logger.log(self.log_file, log_msg)

            # logging, Moving bad files to archive bad, and deleting Bad Data folder started...
            log_msg = f"""Moving bad files to archive bad, and deleting Bad Data folder started..."""
            self.logger.log(self.log_file, log_msg)

            self.raw_data.move_bad_files_to_archive_bad()

            # logging, Bad files moved to archive, and Bad Raw folder deletion is completed!
            log_msg = f"""Bad files moved to archive, and Bad Raw folder deletion is completed!"""
            self.logger.log(self.log_file, log_msg)

            # logging, Validation operation is completed
            log_msg = f"""Validation operation is completed!!!"""
            self.logger.log(self.log_file, log_msg)

            # logging, Extracting csv file from table started
            log_msg = f"""Extracting csv file from table started..."""
            self.logger.log(self.log_file, log_msg)

            # extracting csv file from table
            self.db_operation.export_good_data_to_csv('Prediction')

            # logging, extraction completed
            log_msg = f"""Exporting good data to csv from db table is completed!"""
            self.logger.log(self.log_file, log_msg)

        except Exception as e:
            error_log_msg = f"""Exception occured during prediction validation.
            Exception message : {str(e)}"""
            self.logger.log(self.log_file, error_log_msg)

            raise Exception