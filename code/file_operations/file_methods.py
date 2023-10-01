import os
import shutil
import pickle


class FileOperations:
    """
    Class Name : FileOperations  
    Description : This class supports methods to save and load models  
    Method(s):  
    Version : 1.0  
    Revisions : None  
    """

    def __init__(self, file_obj, logger_obj) -> None:
        self.file_obj = file_obj
        self.logger_obj = logger_obj
        self.model_directory = "models/"  # directory to save the models

    def save_model(self, model, filename):
        """
        Method Name : save_model  
        Class Name : FileOperations  
        Description : Save the model to the 'models/' directory  
        Input(s):  
            model : the model to save  
            filename : file name to save  
        Output(s):  
            return 'success' if the file saved successfully  
        On Failure : Raises Exception  
        Version : 1.0  
        Revisions : None  
        """

        # logging
        log_msg = f"""Entered 'save_model' method of 'FileOperations' class."""
        self.logger_obj.log(self.file_obj, log_msg)

        try:
            path = os.path.join(self.model_directory, filename)  # create separate directory for each cluster
            if os.path.isdir(path):
                shutil.rmtree(self.model_directory)
                os.makedirs(path)
            else:
                os.makedirs(path)

            with open(path + '/' + str(filename) + '.sav', 'wb') as f:
                pickle.dump(model, f)  # saving the model
            # logging
            log_msg = f"""Model file {str(filename)} saved. 
            Exited the 'save_model' method of 'FileOperations' class."""
            self.logger_obj.log(self.file_obj, log_msg)

            return 'Success'
        
        except Exception as e:
            error_log_msg = f"""Exception occured in 'save_model' method of 'ModelFinder' class.
            Exception message : {str(e)}"""
            self.logger_obj.log(self.file_obj, error_log_msg)

            error_log_msg = f"""Model file {str(filename)} could not be saved. 
            Exited the 'save_model' method of 'FileOperations' class."""
            self.logger_obj.log(self.file_obj, error_log_msg)
            raise Exception()
        
    def load_model(self, filename):
        """
        Method Name : load_model  
        Class Name : Clustering  
        Description : The Model file loaded in memory  
        Input(s):  
            filename : file name in which the model is saved, which is to be loaded  
        Output(s):  
            returns the loaded model 
        On Failure : Raises Exception  
        Version : 1.0  
        Revisions : None  
        """

        # logging
        log_msg = f"""Entered the 'load_model' method of the 'FileOperation' class."""
        self.logger_obj.log(self.file_obj, log_msg)

        try:
            path = os.path.join(self.model_directory, filename, filename)
            complete_path = path + '.sav'
            with open(complete_path, "rb") as f:
                log_msg = f"""Model file {str(filename)} loaded successfully. 
                Exited 'load_model' method of 'FileOperations' class."""
                self.logger_obj.log(self.file_obj, log_msg)
                model = pickle.load(f)
                print(model)
                print(type(model))
                return model
            
        except Exception as e:
            error_log_msg = f"""Exception occured in 'load_model' method of 'FileOperations' class. 
            Exception message : {str(e)}"""
            self.logger_obj.log(self.file_obj, error_log_msg)

            error_log_msg = f"""Model file {str(filename)} could not be loaded. 
            Exited 'load_model' method of 'FileOperations' class."""
            self.logger_obj.log(self.file_obj, error_log_msg)

            raise Exception()
        
    def find_correct_model_file(self, cluster_number):
        """
        Method Name : find_correct_model_file  
        Class Name : FileOperations  
        Description : Select the correct model based on cluster number
        Input(s) :  
            cluster_num : cluster number to select the model
        Output(s) :  
            model_name : returns model name with the corresponding cluster  
        On Failure : Raises Exception  
        Version : 1.0  
        Revisions : None  
        """

        # logging
        log_msg = f"""Enetered the 'find_correct_model_file' method of the 'FileOperations' class."""
        self.logger_obj.log(self.file_obj, log_msg)

        try:
            self.cluster_number = cluster_number
            self.folder_name = self.model_directory
            self.list_of_model_files = []
            self.list_of_dirs = os.listdir(self.folder_name)

            for dir in self.list_of_dirs:
                try:
                    if str(dir[-1]) == str(cluster_number) :
                        self.model_name = dir
                        break
                    else:
                        continue
                except:
                    raise Exception
                # try:
                #     if self.file.index(str(self.cluster_number) != -1):
                #         self.model_name = self.file
                # except Exception as e:
                #     continue
                # print(dir.split('_')[1] == cluster_number)
            
            # print(self.model_name)
            # self.model_name = self.model_name.split('.')[0]
            
            # logging
            log_msg = f"""finding correct model file is successfult.
            Exited the 'find_correct_model_file' method of 'FileOperations' class."""
            self.logger_obj.log(self.file_obj, log_msg)

            return self.model_name
        
        except Exception as e:
            error_log_msg = f"""Exception occured in 'find_correct_model_file' method of 'FileOperations' class.
             Exception message : {str(e)} """
            self.logger_obj.log(self.file_obj, error_log_msg)

            error_log_msg = f"""Exited the 'find_correct_model_file' method of 'FileOperations' class."""
            self.logger_obj.log(self.file_obj, error_log_msg)

            raise Exception()



