import pandas as pd
import json

from file_operations import file_methods
from data_preprocessing import preprocessing
from data_ingestion import data_loader_prediction
from application_logging.logger import AppLogger
from Prediction_Raw_Data_Validation.prediction_data_validation import PredictionDataValidation


class Prediction:
    """
    Class Name : 
    Description : 
    Method(s) : 
    Version : 1.0  
    Revisions : None
    """

    def __init__(self, path) -> None:
        self.log_file_obj = open("Prediction_Logs/Prediction_Log.txt", "a+")
        self.logger = AppLogger()
        if path is not None:
            self.pred_data_val = PredictionDataValidation(path)

    def prediction_from_model(self, ):
        """
        Class Name : 
        Method Name : 
        Description : 
        Input(s) : 
        Output(s) : 
        Version : 1.0 
        Revisions : None
        """ 

        try:
            self.pred_data_val.delete_prediction_file()  # deltes the existing prediction file from the last run!
            self.logger.log(self.log_file_obj, "Start of prediction...")

            # getting the data
            data_getter = data_loader_prediction.DataGetterPred(self.log_file_obj, self.logger)
            data = data_getter.get_data()
            wafer_names_series = data["Wafer"]

            # pre-processing data
            preprocessor = preprocessing.PreProcessor(self.log_file_obj, self.logger)
            data = preprocessor.remove_columns(data, columns_to_remove=['Wafer'])           

            is_null_present = preprocessor.is_null_present(data)  # is null values present in the data
            if(is_null_present):
                data = preprocessor.impute_missing_values(data)

            # cols_to_drop = preprocessor.get_columns_with_zero_std_deviation(data)
            # data = preprocessor.remove_columns(data, cols_to_drop)
            with open('columns_with_zero_std_dev.json', 'r') as file:
                cols_to_drop = json.load(file)
            
            data = preprocessor.remove_columns(data, cols_to_drop)


            # clustering
            file_loader = file_methods.FileOperations(self.log_file_obj, self.logger)
            kmeans = file_loader.load_model("KMeans")
            clusters = kmeans.predict(data) 
            data["clusters"] = clusters
            clusters = data["clusters"].unique()
            data["Wafer"] = wafer_names_series

            for i in clusters:
                cluster_data = data[data["clusters"] == i]
                wafer_names = cluster_data["Wafer"]
                cluster_data = data.drop(columns=["Wafer"], axis=1)
                cluster_data = cluster_data.drop(columns=["clusters"], axis=1)
                print(type(cluster_data))
                model_name = file_loader.find_correct_model_file(i)
                model = file_loader.load_model(model_name)

                result = list(model.predict(cluster_data))
                result = pd.DataFrame(list(zip(wafer_names, result)), columns=["Wafer", "Prediction"])
                path = "Prediction_Output_File/Predictions.csv"
                result.to_csv("Prediction_Output_File/Predictions.csv", header=True, mode="a+", 
                              index=False)
            
            self.logger.log(self.log_file_obj, "End of prediction")

        except Exception as e:
            self.logger.log(self.log_file_obj, f"""Error occured while running the prediction, Exception message : {str(e)}""")
            raise Exception
        
        return path, result.head().to_json(orient="records")
