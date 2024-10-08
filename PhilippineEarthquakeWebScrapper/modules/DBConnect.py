"""
DBConnect

Version number: v0.1.1 (Beta)

Dependencies:
- pandas 1.5.1
- sqlalchemy 1.4.43
- geopandas 0.10.2

A module for reading data from source files, and connecting and extracting data from and loading to databases.

This module has four tools:

(1) Connector
  - this tool allows a user to connect to a database by specifying an environment which is configured through a
  user-configured 'db_config.json' file.

(2) FileReader
  - allows to read source files such as .csvs and .shp files and returns a pandas dataframe for analysis and/or 
  transformation

(3) DataDumper
  - allows data to be stored in a database table. You need to first pass an active connection (from the Connector tool) 
  in order to interact with a database.

(4) DatabaseExtractor 
  - allows extraction/querying of data from a database. You need to first pass an active connection (from the Connector tool)
  in order to start interacting with a database.


~~~ FOR FUTURE MAINTAINERS ~~~
The current version of this module is the general version of the extraction methods used in data refresh. However,
the main goal of this tool is to create a more generic ETL and Data Extraction module that will simplify and abstract the
nitty gritty of the common activities in interacting with company data and databases.

Feel free to extend the four (4) tools above. But remember to philisophy and vision of this tool: to simplify and abstract
intractions with data. Occam's Razor the way out of this!


Script Updates:
(1) 2023-10-18: (not yet logged)
    -> Class Connector
        -> def __init__()
            - used os.path.dirname(__file__) instead of os.getcwd()
            - reason: not able to correctly fetch the python file path
(2) 2023-10-24: (not yet logged)
    ->  Class FileReader
        -> added a function to read an excel file data

"""


import os
import json
import psycopg2
from psycopg2 import sql
from psycopg2 import extras
import pandas as pd
from sqlalchemy import create_engine, exc, text
from sqlalchemy.orm import sessionmaker
import os
from urllib.parse import quote
import geopandas as gpd


class DBConnect:
    '''
    Database connector class for data dumping.
    '''

    def __init__(self):
       self._version = 'v0.1.1'

    @property
    def version(self):
        '''
        Get Version Information
        '''
        return self._version
    
    ##########################################
    ## Connector Class
    ##########################################

    class Connector:
        '''
        Handles database connection initiation and disconnection for a specified environment.
        Needs db_config.json to get the database credentials of allowed environments.

        db_config.json format:
        {
          "environments": {
            "<env_name1>" : {
              "NAME": "sample_env_1",
              "HOST": "localhost",
              "PORT": "5432",
              "USER": "sample_user_1",
              "PASS": "pa$$@123"
            },
            "test": {
              "NAME": "sample_env_2",
              "HOST": "localhost",
              "PORT": "5432",
              "USER": "sample_user_2",
              "PASS": "pasW0rddz"
            }   
          }
        }
        '''
        def __init__(self, environment):
            try:
                # with open(os.path.join(os.getcwd(),'db_config.json')) as config_file:
                with open(os.path.join(os.path.dirname(__file__),'db_config.json')) as config_file: #--> used os.path.dirname(__file__) instead of os.getcwd()
                    self._environments = json.load(config_file)['environments']
            except Exception as e:
                print('Error')
            self.environment = environment
            self.engine = None
            self.conn = None
            self._status = "Not Connected"
            self.environment_creds = self._environments[self.environment]

        def getAvailableEnvironments(self):
            '''
            Returns the currently available environments you can connect to. 
            These values are configured in the environment variables of the db_config.json file
            '''
            return [{
                'env_name': env,
                'db_name': self._environments[env]['NAME'],
                'db_host': self._environments[env]['HOST'],
                'db_port': self._environments[env]['PORT']
            } for env in self._environments]

        def getStatus(self):
            """
            Returns the status of connection. Connected or disconnected.
            """
            return self._status

        def connect(self, echo=False):
            '''
            Connects to the database of the specified environment.
            '''
            try:
                creds = self._environments[self.environment]
                print(f"Connecting to {self.environment} database")
                self.engine = create_engine(f"postgresql://{creds['USER']}:{quote(creds['PASS'])}@{creds['HOST']}:{creds['PORT']}/{creds['NAME']}" , echo=echo)
                self.conn = self.engine.connect()
                print(f"[Connect] Successfully connected to {self.environment} database ({creds['NAME']})")
                self._status = f"Connected to {self.environment} ({creds['NAME']} on {creds['HOST']} port {creds['PORT']})"
            except exc.SQLAlchemyError as e:
                print('[Connection Attempt Error] Error connecting to local PostgreSQL database:', e)
            except KeyError as e:
                print('[Connection Attempt Error] Currently set env is unsupported.')

        def disconnect(self):
            '''
            Disconnects the current connect session, if it exists.
            '''
            if self.conn:
                self.engine.dispose()
                self.conn.close()
                self.engine = None
                self.conn = None
                print(f"[Disconnect] Successfully Disconnected from {self.environment} PostgreSQL database")
                self._status = "Not Connected"
            else:
                print('[Disconnection Attempt Error] No active connection to disconnect')

    ##########################################
    ## File Reader Classes
    ##########################################

    class FileReader:
        '''
        Generic file reader that collates all supported reader classes.
        Supported file types: .shp, .csv
        '''
        
        def read_file(self, parent_folder, file_name, file_sheetname=None):
            '''
            Read file based on the parent folder and file name values.
            '''

            file_extn = (list(os.path.splitext(file_name))[1]).lower()
            print(file_extn)
            print(file_sheetname)
            
            match file_extn:
                case '.shp':
                    self._data = DBConnect._ShapeFileReader(os.path.join(parent_folder, file_name))
                    return self._data.df

                case '.csv':
                    self._data = DBConnect._CsvFileReader(os.path.join(parent_folder, file_name))
                    return self._data.df
                case '.xlsx':
                    self._data = DBConnect._ExcelFileReader(os.path.join(parent_folder, file_name), file_sheetname)
                    return self._data.df
                case '.xls':
                    self._data = DBConnect._ExcelFileReader(os.path.join(parent_folder, file_name), file_sheetname)
                    return self._data.df
                case '.xlsb':
                    self._data = DBConnect._ExcelFileReader(os.path.join(parent_folder, file_name), file_sheetname)
                    return self._data.df
                case _:
                    print('Unsupported File Type')
                    return None

    class _ShapeFileReader:
        """
        Private class for reading shape and files with geo data. Basically a reskin of gpd.read. 
        """
        def __init__(self, file_path):
            self.df = gpd.read_file(file_path)

    class _CsvFileReader:
        """
          Private class for reading csv files. Basically a reskin of pd.read_csv. 
        """
        def __init__(self,file_path):
            self.df = pd.read_csv(file_path)
    
    class _ExcelFileReader:
        """
          Private class for reading excel files. Basically a reskin of pd.xlsx. 
        """
        def __init__(self,file_path, sheetname=None):
            if sheetname == 0:
                self.df = pd.read_excel(file_path)
            else:
                self.df = pd.read_excel(file_path, sheet_name=sheetname)

    ##############################################
    ## Database Dumping Class
    ##############################################

    class DataDumper:
        '''
        Load/import data to a database table, provided a database connection is passed to this class. To create 
        a connection, use DBConnect.Connector(), and pass the Connector.conn property to this class.
        '''
        def __init__(self, connection, engine):
            try:
                if connection and engine:
                    self.sql_conn = connection
                    self.sql_engine = engine
                    Session = sessionmaker(bind=self.sql_engine)
                    self.session = Session()
                else:
                    raise ValueError('Invalid Connection or Engine Values')
            except ValueError as ve:
                print(ve)
          
        def geo_data_import(self, df_data, output_table_name, schema, pre=None, callback=None, if_exists='replace'):
            """
            Import geo data. Use a geopandas dataframe as input data.

            Two "hooks" can be used to extend the functionality of this method: the pre() and the callback()

            - pre() can be used to perform some actions BEFORE sending the data to the database. Some uses would be for 
            data checks or transformations before database dumping.

            - callback() can be used to perform some set of actions AFTER the databse dumping has been successful. Some
            uses for this hook would be for logging, or other post dumping logic, as needed. 
            """
            try:
                if pre:
                    pre()

                gdf = gpd.GeoDataFrame(df_data.copy(), geometry='geometry')
                gdf.to_postgis(output_table_name, self.sql_engine, if_exists=if_exists, index=False, schema=schema, chunksize=10000)
                print('[Data Dumper] Loaded to SQL Table')
                
                if callback:
                    callback()
                    
            except Exception as e:
                print('[Data Dumper Error] Error in Importing to SQL Table.')
                print(e)

        def data_import(self, df_data, output_table_name, schema, pre=None ,sp_callback=None, if_exists='replace'):
            """
            Import data to a table. Use a pandas dataframe as input data.

            Two "hooks" can be used to extend the functionality of this method: the pre() and the callback()

            - pre() can be used to perform some actions BEFORE sending the data to the database. Some uses would be for 
            data checks or transformations before database dumping.

            - callback() can be used to perform some set of actions AFTER the databse dumping has been successful. Some
            uses for this hook would be for logging, or other post dumping logic, as needed. 
            """
            try:
                if pre:
                    pre()

                df = pd.DataFrame(df_data.copy())
                df.to_sql(output_table_name, self.sql_engine, if_exists=if_exists, index=False, schema=schema, chunksize=10000)
                print('[Data Dumper] Loaded to SQL Table')

                if sp_callback:
                    sp_callback()

            except Exception as e:
                print('[Data Dumper Error] Error in Importing to SQL Table.')
                print(e)


    ###########################################
    ## Database Extractor Class
    ###########################################

    class DatabaseExtractor:
        '''
        Get data based from a specified database, provided a database connection is passed to this class. To create 
        a connection, use DBConnect.Connector(), and pass the Connector.conn property to this class.
        
        Returns a pandas dataframe.
        '''
        def __init__(self, connection, engine):
            try:
                if connection and engine:
                    self.sql_conn = connection
                    self.sql_engine = engine
                    Session = sessionmaker(bind=self.sql_engine)
                    self.session = Session()
                else:
                    raise ValueError('Invalid Connection or Engine Values')
            except ValueError as ve:
                print(ve)

            self.data = None

        def get_data(self, table_name, schema, columns='*', row_limit = 0):
            '''
            Extracts the data from a database.
                columns is in dictionary type: Sample;  columns = {'circuit_id', 'customer_number'}
            '''

                        

            
            if row_limit > 0:
                query = f'SELECT {",".join(columns)} FROM {schema}.{table_name} LIMIT {row_limit};'
            else:
                query = f'SELECT {",".join(columns)} FROM {schema}.{table_name};'

            print(query)
            
            result = self.sql_conn.execution_options(autocommit=True).execute(text(query))

            return pd.DataFrame(result)
        
        
        def get_data_with_custom_query(self, sql_query):
            '''
            Extracts the data from a database using user defined sql query
            '''

            try:
                result = self.sql_conn.execution_options(autocommit=True).execute(text(sql_query))
                
                data_frames = []
                batch_size = 100000
                while True:
                    rows = result.fetchmany(batch_size)
                    if not rows:
                        break
                    
                    data_frames.append(pd.DataFrame(rows, columns=result.keys()))

                self.data = pd.concat(data_frames, ignore_index=True)

            except Exception as e:
                print(f"Error: {str(e)}")
                self.data = None

            return self.data


    ###########################################
    ## Database Stored Procedure Executor
    ###########################################
    class DatabaseStoredProcedureExecutor:
        '''
        This will execute the SP from a postgres database
        '''
        def __init__(self, environment_creds):
            try:
                self.dbname = environment_creds['NAME']
                self.user = environment_creds['USER']
                self.password = environment_creds['PASS']
                self.host = environment_creds['HOST']
                self.port = environment_creds['PORT']
                
            except ValueError as ve:
                print(ve)


        def execute_sp(self, sp_name):
             # Define the connection string
            conn = psycopg2.connect(
                dbname = self.dbname,
                user = self.user,
                password = self.password,
                host = self.host,
                port = self.port
            )

            # Create a cursor object
            cursor = conn.cursor()

            try:
                # Execute the stored procedure
                cursor.execute(sql.SQL(sp_name))

                # Commit the transaction if the procedure modifies data
                conn.commit()
            except psycopg2.Error as e:
                # Rollback the transaction in case of an error
                conn.rollback()
                print(f"Error: {e}")
            finally:
                # Close the cursor and connection
                cursor.close()
                conn.close()
            
