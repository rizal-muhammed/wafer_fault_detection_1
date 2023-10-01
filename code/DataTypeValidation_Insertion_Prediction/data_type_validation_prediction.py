import shutil
import sqlite3
from datetime import datetime
import os
import csv
import pandas as pd
from application_logging.logger import AppLogger


class DBOperations:
    """
    Class Name : DBOperations  
    Description : This class shall be used ofr handling all the SQL operations.  
    Version : 1.0  
    Revisions : None  
    """

    def __init__(self) -> None:
        self.path = 'Prediction_Database/'
        self.good_file_path = 'Prediction_Raw_Files_Validated/Good_Raw'
        self.bad_file_path = 'Prediction_Raw_Files_Validated/Bad_Raw'
        self.logger = AppLogger()

    def db_connection(self, db_name):
        """
        Method Name : db_connection  
        Class Name : DBOperations  
        Description : This method creates(if not exists) database with the given name and returns 
        a connection to it
        Input(s):  
            db_name : The name of the database  
        Output(s) : returns the connection to the corresponding database.
        Version : 1.0
        Revisions : None  
        """

        try:
            file = open("Prediction_Logs/DataBaseConnectionLog.txt", "a+")
            # logging
            log_msg = f"""Entered 'db_connection' method of 'DBOperations' class."""
            self.logger.log(file, log_msg)

            # connection establishment
            conn = sqlite3.connect(self.path + db_name + '.db')
            log_msg = f"""Opened {str(db_name)} database successfully."""
            self.logger.log(file, log_msg)
            file.close()

        except ConnectionError:
            file = open("Prediction_Logs/DataBaseConnectionLog.txt", "a+")
            error_log_msg = f"""Error while connecting to {str(db_name)} database : {str(ConnectionError)}"""
            self.logger.log(file, error_log_msg)
            file.close()
            raise ConnectionError
        
        return conn
    
    def create_table_db(self, db_name, column_names):
        """
        Method Name : create_table_db  
        Class Name : DBOperations  
        Descripton : This method shall be used to create a table in the corresponding  
        database  
        If 'Good_Raw_Data' table is present in the given database, then the columns are appended 
        to this table. If no such table is found in the given database, a table named 'Good_Raw_Data' 
        is created and columns are inserted.  
        Input(s) :   
            db_name : In which database the table should be created  
            column_names : (dictionary) column names along with their datatypes in the form of a   
            python dictonary  
        Output(s) :  upon successful completion of this method, a string message "Tables created successfully" 
        is returned
        Version : 1.0  
        Revisions : None  
        """

        try:
            # logging
            file_db_table_create_log = open('Prediction_Logs/DbTableCreateLog.txt', "a+")
            log_msg = f"""Entered 'create_table_db' method of 'DBOperations' class."""
            self.logger.log(file_db_table_create_log, log_msg)

            file_db_connection_log = open('Prediction_Logs/DataBaseConnectionLog.txt', "a+")
            log_msg = f"""Entered 'create_table_db' method of 'DBOperations' class."""
            self.logger.log(file_db_connection_log, log_msg)

            conn = self.db_connection(db_name)
            c = conn.cursor()
            c.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name='Good_Raw_Data'")
            if c.fetchone()[0] == 1:
                conn.close()
                log_msg = f"""Tables created successfully!"""
                self.logger.log(file_db_table_create_log, log_msg)
                file_db_table_create_log.close()

                log_msg = f"""Database {str(db_name)} closed successfully"""
                self.logger.log(file_db_connection_log, log_msg)
                file_db_connection_log.close()

                return "Tables created successfully"
            
            else:
                for key in column_names.keys():
                    dtype = column_names[key]

                    try:
                        # if the table already exists, then add the column
                        conn.execute('ALTER TABLE Good_Raw_Data ADD COLUMN "{column_name}" {data_type}'.format(column_name=key, data_type=dtype))

                    except:
                        # the table doesn't exists. So create table
                        create_table_sql = f"""
                        CREATE TABLE IF NOT EXISTS Good_Raw_Data (
                            "{key}" {dtype});
                            """
                        # conn.execute('CREATE TABLE Good_Raw_Data ({column_name} {data_type})'.format(column_name=key, data_type=dtype))
                        conn.execute(create_table_sql)

                conn.close()

                file_db_table_create_log = open('Prediction_Logs/DbTableCreateLog.txt', "a+")
                log_msg = f"""Tables created successfully!!"""
                self.logger.log(file_db_table_create_log, log_msg)
                file_db_table_create_log.close()

                file_db_connection_log = open('Prediction_Logs/DataBaseConnectionLog.txt', "a+")
                log_msg = f"""Closed {str(db_name)} database successfully"""
                self.logger.log(file_db_connection_log, log_msg)
                file_db_connection_log.close()

                return "Tables created successfully"

        except Exception as e:
            file_db_table_create_log = open('Prediction_Logs/DbTableCreateLog.txt', "a+")
            error_log_msg = f"""Error while creating table : {str(e)}"""
            self.logger.log(file_db_table_create_log, error_log_msg)
            file_db_table_create_log.close()

            conn.close()
            file_db_connection_log = open('Prediction_Logs/DataBaseConnectionLog.txt', "a+")
            error_log_msg = f"""Closed {str(db_name)} database successfully"""
            self.logger.log(file_db_connection_log, error_log_msg)
            file_db_connection_log.close()

            raise Exception()
        
    def insert_into_table_good_data(self, db_name):
        """
        Method Name : insert_into_table_good_data  
        Class Name : DBOperations  
        Descripton : This method inserts the good data files from the Good_Raw folder into 
        the Good_Raw_Data table
        Input(s) :  
            db_name : database name  
        Output(s) :  On successful completion of this method, a string message 'Insertion Successful' 
        is returned  
        On Failure : Raises Exception  
        Version : 1.0  
        Revisions : None  
        """

        conn = self.db_connection(db_name)
        c = conn.cursor()
        c.execute("PRAGMA table_info(Good_Raw_Data)")
        columns = [column[1] for column in c.fetchall()]
        good_file_path = self.good_file_path
        bad_file_path = self.bad_file_path
        onlyfiles = [file for file in os.listdir(good_file_path)]
        log_file = open("Prediction_Logs/DbInsertLog.txt", "a+")

        # logging
        log_msg = f"""Enetered 'insert_into_table_good_data' method in 'DBOperations' class."""
        self.logger.log(log_file, log_msg)

        for file in onlyfiles:
            try:
                complete_path = os.path.join(good_file_path, file)
                # with open(complete_path, "r") as f:
                #     next(f)  # excluding the header
                #     reader = csv.reader(f, delimiter="\n")
                #     for line in enumerate(reader):
                #         try:
                #             list_ = line[1][0].split(',')
                #             insert_query = f"INSERT INTO Good_Raw_Data ({', '.join(columns)}) VALUES ({', '.join(['?']*len(columns))})"
                #             conn.execute(insert_query, list_)
                #         except Exception as e:
                #             error_log_msg = f"""Could not insert data {str(file)}. 
                #             Exception message : {str(e)}"""
                # log_msg = f"""file {str(file)} loaded successfully"""
                # self.logger.log(log_file, log_msg)
                df = pd.read_csv(complete_path)
                df.rename({'Good/Bad': 'Output'}, axis=1, inplace=True)
                df.to_sql('Good_Raw_Data', conn, if_exists='append', index=False)

                log_msg = f"""file {str(file)} loaded successfully"""
                self.logger.log(log_file, log_msg)

                conn.commit()
                            
            except Exception as e:
                conn.rollback()
                error_log_msg = f"""Error while inserting into table, couldn't load {str(file)}, 
                Exception message : {str(e)}"""
                self.logger.log(log_file, error_log_msg)

                shutil.move(good_file_path + "/" + file, bad_file_path)
                error_log_msg = f"""couldn't load {str(file)}. Moved the file into bad directory. 
                Exiting 'insert_into_table_good_data' method of 'DBOperations' class."""
                self.logger.log(log_file, error_log_msg)
                log_file.close()
                conn.close()

        conn.close()
        log_file.close()

    def export_good_data_to_csv(self, db_name):
        """
        Method Name : export_good_data_to_csv  
        Class Name : DBOperations  
        Input(s):  
            db_name: database name.  
        Output(s): None
        On Failure : Raises Exception
        Version : 1.0
        Revisions : None
        """

        self.file_from_db = "Prediction_FileFromDB/"
        self.file_name = "InputFile.csv"
        log_file = open("Prediction_Logs/ExportToCsv.txt", "a+")

        try:
            conn = self.db_connection(db_name)
            sql_select = "SELECT * FROM Good_Raw_Data"
            cursor = conn.cursor()
            cursor.execute(sql_select)

            results = cursor.fetchall()

            # get the headers of the csv file
            headers = [i[0] for i in cursor.description]

            # make csv output directory
            if not os.path.isdir(self.file_from_db):
                os.makedirs(self.file_from_db)

            # open csv file for writing
            csv_file = csv.writer(open(self.file_from_db + self.file_name, 'w', newline=''),
                                  delimiter=',',
                                  lineterminator='\r\n',
                                  quoting=csv.QUOTE_ALL,
                                  escapechar='\\')
            
            # add the headers and data to the csv file
            csv_file.writerow(headers)
            csv_file.writerows(results)

            self.logger.log(log_file, "File exported to csv successfully!")
            return "success"
        
        except Exception as e:
            error_log_msg = f"""File export to csv failed. Exception message : {str(e)}"""
            self.logger.log(log_file, error_log_msg)
            log_file.close()
            return "failure"



