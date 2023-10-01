import json

from application_logging.logger import AppLogger
from data_ingestion import data_loader_training
from data_preprocessing import preprocessing, clustering
from best_model_finder import tuner
from file_operations import file_methods

from sklearn.model_selection import train_test_split




class TrainModel:
    """
    Class Name : TrainModel
    Descritpion : This class is used to train the model
    Method(s) : 
        training_model
    Version : 1.0
    Revisions : None
    """

    def __init__(self) -> None:
        self.logger = AppLogger()
        self.log_file = open("Training_Logs/ModelTraininingLog.txt", "a+")

    def training_model(self, ):
        """
        Method Name : training_model
        Class Name : TrainModel
        Description : This method is used to train the model
        Input(s) : None
        Output(s) : None
        Version : 1.0
        Revisions : None
        """
        log_msg = f"""Start of training..."""
        self.logger.log(self.log_file, log_msg)

        try:
            # getting data from the source
            data_getter = data_loader_training.DataGetterTrain(self.log_file, self.logger)
            data = data_getter.get_data()

            # data pre-processing
            preprocessor = preprocessing.PreProcessor(self.log_file, self.logger)
            data = preprocessor.remove_columns(data, columns_to_remove=['Wafer'])

            # separate features and labels
            X, y = preprocessor.separate_label_feature(data, label_column_name='Output')

            # dealing with missing values
            is_null_present = preprocessor.is_null_present(X)
            if is_null_present == True:
                X = preprocessor.impute_missing_values(X)

            cols_to_drop = preprocessor.get_columns_with_zero_std_deviation(data)
            with open('columns_with_zero_std_dev.json', 'w') as file:
                json.dump(cols_to_drop, file)
                
            X = preprocessor.remove_columns(X, cols_to_drop)

            # clustering approach
            kmeans = clustering.Clustering(self.log_file, self.logger)
            num_clusters = kmeans.elbow_plot(X)

            # divide data into clusters
            X = kmeans.create_clusters(X, num_clusters)

            X["label"] = y
            
            # getting the unique clusters from dataset
            list_of_clusters = X['ClusterNumber'].unique()

            # parsing all the clusters and looding for the best ML algorithm to fit on 
            # individual cluster
            for i in list_of_clusters:
                cluster_data = X[X["ClusterNumber"] == i]  # filter the data for one cluster

                if cluster_data.shape[0] > 15:
                    # prepare the feature and label columns
                    cluster_features = cluster_data.drop(["label", "ClusterNumber"], axis=1)
                    cluster_label = cluster_data["label"]

                    X_train, X_test, y_train, y_test = train_test_split(cluster_features,
                                                                        cluster_label,
                                                                        test_size=1/3,
                                                                        random_state=355)
                    model_finder = tuner.ModelFinder(self.log_file, self.logger)

                    # getting the best model 
                    best_model_name, best_model = model_finder.get_best_model(X_train,y_train,X_test,y_test)

                    # saving the best model
                    file_op = file_methods.FileOperations(self.log_file, self.logger)
                    saved_model = file_op.save_model(best_model, best_model_name + "_" + str(i))

            log_msg = f"""Training is successful!"""
            self.logger.log(self.log_file, log_msg)

            self.log_file.close()

        except Exception as e:
            error_log_msg = f"""Exception during training, exception message : {str(e)}"""
            self.logger.log(self.log_file, error_log_msg)
            self.log_file.close()
            raise Exception