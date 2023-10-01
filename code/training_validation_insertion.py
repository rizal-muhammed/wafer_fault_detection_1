from Training_Raw_Data_Validation.raw_validation import RawDataValidation
from DataTransform_Training.data_transformation import DataTransform
from DataTypeValidation_Insertion_Training.data_type_validation import DBOperations
from application_logging.logger import AppLogger


class TrainValidation:
    """
    
    """

    def __init__(self, path) -> None:
        self.raw_data = RawDataValidation(path)
        self.data_transform = DataTransform()
        self.db_operation = DBOperations()
        self.log_file = open("Training_Logs/Training_Main_Log.txt", "a+")
        self.logger = AppLogger()

    def train_validation(self, ):
        try:
            log_msg = f"""Starting of validation of files..."""
            self.logger.log(self.log_file, log_msg)

            # extracting values from prediction schema
            len_of_date_stamp_in_file, len_of_time_stamp_in_file, column_names, no_of_cols = self.raw_data.values_from_schema()

            # getting the regex defined to validate the file name
            regex_pattern = self.raw_data.manual_regex()

            # validating file name of prediction files
            self.raw_data.raw_file_name_validation(regex_pattern, 
                                                   len_of_date_stamp_in_file,
                                                   len_of_time_stamp_in_file,)
            
            # validating column length in the file
            self.raw_data.validate_column_lenth(no_of_cols)

            # validating whether any column contain whole missing values
            self.raw_data.validate_missing_values_in_whole_column()

            log_msg = f"""Raw data validation completed!"""
            self.logger.log(self.log_file, log_msg)

            log_msg = f"""Starting of Data Transformation..."""
            self.logger.log(self.log_file, log_msg)

            # replacing blanks in the csv file with "NULL" vlaues to isnert in table
            self.data_transform.replace_missing_values_with_null()

            log_msg = f"""Data Transformation is completed!"""
            self.logger.log(self.log_file, log_msg)

            log_msg = f"""Creating Training_Database and tables on the basis of given schema..."""
            self.logger.log(self.log_file, log_msg)

            # create database with given name, if present, open the connection
            # create table with columns given in schema
            self.db_operation.create_table_db("Training", column_names)

            log_msg = f"""Table creation completed!"""
            self.logger.log(self.log_file, log_msg)

            log_msg = f"""Insertion of data into table started..."""
            self.logger.log(self.log_file, log_msg)

            # insert csv file into the table.
            self.db_operation.insert_into_table_good_data('Training')

            log_msg = f"""Insertion of data into table completed!"""
            self.logger.log(self.log_file, log_msg)

            log_msg = f"""Deleting Good Data Folder..."""
            self.logger.log(self.log_file, log_msg)

            # delete the good data folder after loading files in table
            self.raw_data.delete_exising_good_data_training_folder()

            log_msg = f"""Good Data folder is deleted!"""
            self.logger.log(self.log_file, log_msg)

            log_msg = f"""Moving bad files to Archine and deleteing Bad Data folder..."""
            self.logger.log(self.log_file, log_msg)

            # move the bad data files to archive folder
            self.raw_data.move_bad_files_to_archive_bad()

            log_msg = f"""Bad files moved to archive!, Bad folder deleted!"""
            self.logger.log(self.log_file, log_msg)

            log_msg = f"""Validation operation is completed!"""
            self.logger.log(self.log_file, log_msg)

            log_msg = f"""Extracting csv files from table"""
            self.logger.log(self.log_file, log_msg)

            # export data from table to csv file
            self.db_operation.export_good_data_to_csv("Training")
            self.log_file.close()



        except Exception as e:
            raise Exception
        
