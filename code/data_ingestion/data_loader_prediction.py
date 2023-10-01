import pandas as pd


class DataGetterPred:
    """   
    Class Name : DataGetterPred   
    Description : This class shall be used for obtaining the    
    data form the source for prediction.    
    Method(s) :    
    Version : 1.0   
    Revisions : None   
    """

    def __init__(self, file_obj, logger_obj) -> None:
        self.file_obj = file_obj
        self.logger_obj = logger_obj
        self.prediction_file = "Prediction_FileFromDB/InputFile.csv"

    def get_data(self):
        """
        Method Name : get_data  
        Class Name : DataGetterPred  
        Input(s) :   
        Output(s) : returns a pandas dataframe with the data for prediction  
        Version : 1.0  
        Revisions : None  
        """

        # logging
        log_msg = f"""Entered 'get_data' method of 'DataGetterPred' class."""
        self.logger_obj.log(self.file_obj, log_msg)

        try:
            self.df = pd.read_csv(self.prediction_file)  # reading the data file
            # logging 
            log_msg = f"""Data Load is successful. 
            Exited the 'get_data' method of 'DataGetterPred' class"""
            self.logger_obj.log(self.file_obj, log_msg)

            return self.df

        except Exception as e:
            error_log_msg = f"""Exception occured in 'get_data' method of 'DataGetterPred' class. 
            Exception  message : {str(e)}"""
            self.logger_obj.log(self.file_obj, error_log_msg)

            error_log_msg = f"""Data Load unsuccessful. 
            Exiting 'get_data' method of 'DataGetterPred' class."""
            self.logger_obj.log(self.file_obj, error_log_msg)

            raise Exception()

