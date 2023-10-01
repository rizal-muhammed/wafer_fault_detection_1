import pandas as pd


class DataGetterTrain:
    """
    Class Name : DataGetter  
    Description : This class shall be used for obtaining the data from  
    the source for training.  
    Method(s):  
        get_data : This method reads the data from source  
    Version : 1.0  
    Revisions : None  
    """

    def __init__(self, file_obj, logger_obj) -> None:
        self.training_file = "Training_FileFromDB/InputFile.csv"
        self.file_obj = file_obj
        self.logger_obj = logger_obj

    def get_data(self):
        """
        Method Name : get_data  
        Class Name : DataGetter  
        Description : This method reads the data from source  
        Input(s) :   
        Output(s) : returns a pandas dataframe contaninig data  
        On Failure : Raises Exception  
        Version : 1.0  
        Revisions : None  
        """

        # logging
        log_msg = f"""Entered 'get_data' method of 'DataGetter' class."""
        self.logger_obj.log(self.file_obj, log_msg)

        try:
            self.df = pd.read_csv(self.training_file)  # reading the data file
            # logging
            log_msg = f"""Data load successful. Exited the 'get_data' method of 'DataGetter' class."""
            self.logger_obj.log(self.file_obj, log_msg)

            return self.df
        
        except Exception as e:
            error_log_msg = f"""Exception occured in 'get_data' method of 'DataGetter' class. 
            Exception message : {str(e)}"""
            self.logger_obj.log(self.file_obj, error_log_msg)

            error_log_msg = f"""Data load unsuccessful. Exiting 'get_data' method of 'DataGetter' class."""
            self.logger_obj.log(self.file_obj, error_log_msg)

            raise Exception()