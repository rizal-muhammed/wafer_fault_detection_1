import matplotlib.pyplot as plt

from sklearn.cluster import KMeans
from kneed import KneeLocator

from file_operations.file_methods import FileOperations



class Clustering:
    """
    Class Name : Clustering  
    Description : This class shall be used to group the data into different similar clusters
    before training the data  
    Method(s) :  
    Version : 1.0  
    Revisions : None  
    """

    def __init__(self, file_obj, logger_obj) -> None:
        self.file_obj = file_obj
        self.logger_obj = logger_obj

    def elbow_plot(self, df):
        """
        Method Name : elbow_plot  
        Class Name : Clustering  
        Description : This method shall be used to find the optimal number of clusters 
        given a dataset  
        Input(s):  
            df : input dataframe
        Output(s): An image of elbow plot for further reference  
            Optimal number of cluster.  
        Version : 1.0  
        Revisions : None  
        """

        # logging
        log_msg = f"""Entered elbo_plot method of Clustering class."""
        self.logger_obj.log(self.file_obj, log_msg)

        wcss = []  # initializing an empty list

        try:
            num_clusters = 10
            # we'll consider number of clusters in the range [1, num_clusters]
            for i in range(1, num_clusters+1):
                kmeans = KMeans(n_clusters=i,
                                init="k-means++", 
                                random_state=42)  # initializing the KMeans object
                kmeans.fit(df)
                wcss.append(kmeans.inertia_)
            
            # plotting the elbow plot
            plt.figure(figsize=(10, 5))
            plt.plot(range(1, num_clusters+1), wcss)  # plotting the graph between number of clusters Vs WCSS
            plt.title("Elbow Method")
            plt.xlabel("Number of clusters")
            plt.ylabel("WCSS")
            plt.grid()
            # plt.show()
            plt.savefig("preprocessing_data/KMeans_elbow_plot.png")  # saving the elbow plot for further reference

            # finding the optimal number of clusters
            self.knee_obj = KneeLocator(range(1, num_clusters+1),
                                  wcss, 
                                  curve='convex', 
                                  direction='decreasing')
            log_msg = f"""The optimal number of clusters is {str(self.knee_obj.knee)}.
            Exited elbow_plot method of Clustering class."""
            self.logger_obj.log(self.file_obj, log_msg)

            return self.knee_obj.knee
        
        except Exception as e:
            error_log_msg = f"""Exception occured in elobo_plot method of Clustering class. 
            The exception message : {str(e)}"""
            self.logger_obj.log(self.file_obj, error_log_msg)

            error_log_msg = f"""Finding the optimal number of clusters is failed.
            Exiting elbow_plot method of Clusters class."""
            self.logger_obj.log(self.file_obj, error_log_msg)

            raise Exception()
        
    def create_clusters(self, df, no_of_clusters):
        """
        Method Name : create_clusters
        Class Name : Clustering
        Input(s) :  
            df : input dataframe  
            no_of_clusters : number of clusters  
        Output(s):  
            A dataframe with cluster column  
        Version : 1.0  
        Revisions : None  
        """

        # logging
        log_msg = f"""Entered the create_clusters method of Clustering class."""
        self.logger_obj.log(self.file_obj, log_msg)

        self.df = df

        try:
            self.kmean = KMeans(n_clusters=no_of_clusters,
                                init="k-means++",
                                random_state=42)
            self.cluster_nums_array = self.kmean.fit_predict(self.df)  # divide data into clusters
            self.df["ClusterNumber"] = self.cluster_nums_array  # create a new column in dataframe which specify the cluster number a data instance belongs to

            # saving the kmean model
            self.file_op = FileOperations(self.file_obj, self.logger_obj)
            self.save_model = self.file_op.save_model(self.kmean, "KMeans")

            # logging
            log_msg = f"""Successfully created {str(no_of_clusters)} clusters. 
            Exiting 'create_clusters' method of 'Clustering' class."""
            self.logger_obj.log(self.file_obj, log_msg)

            return self.df
        except Exception as e:
            error_log_msg = f"""Exception occured in create_clusters method of 'Clustering' class. 
            The exception message : {str(e)}"""
            self.logger_obj.log(self.file_obj, error_log_msg)

            error_log_msg = f"""Fitting data to clusters in failed. Exited 'create_clusters' method 
            of 'Clustering' class."""
            self.logger_obj.log(self.file_obj, error_log_msg)
            raise Exception()