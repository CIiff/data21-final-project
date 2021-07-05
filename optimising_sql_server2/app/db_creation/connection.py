import sqlite3
import configparser
from pandasql import sqldf
from optimising_sql_server.app.db_creation.logger import logger
from sqlalchemy import create_engine,MetaData,Table
import pandas as pd
import pyodbc
import sqlalchemy
import urllib


# config = configparser.ConfigParser()
# config.read('db.ini')
# print(config['DEFAULT']['databaseName'])

# databaseName = config['DEFAULT']['databaseName']
# username = config['mysql']['username']
# password = config['mysql']['password']
# server = config['mysql']['server']
# driver = config['mysql']['driver']

username = 'SA'
password = 'Passw0rd2018'
server = 'localhost,1433'
driver= 'ODBC Driver 17 for SQL Server'


# CONNECTION_STRING = 'DRIVER='+driver+';SERVER='+server+';DATABASE='+databaseName+';UID='+username+';PWD='+ password



class CreateDB:

    databaseName = 'master'
    database = 'Data21Final'

    def __init__(self):
     
     self.connect_string = urllib.parse.quote_plus(f'DRIVER={driver};Server={server};Database={self.databaseName};UID=SA;PWD=Passw0rd2018')
     self.engine = sqlalchemy.create_engine(f'mssql+pyodbc:///?odbc_connect={self.connect_string}&autocommit=true') 
     self.db = self.engine.connect()
     self.check_for_db()
     self.engine.execute(f'USE {self.database}')
     self.engine.connect().execution_options(autocommit=False)
    
    #  for i in self.engine.execute('select DB_NAME() as [Current Database]'):
    #     logger.info(f'Connected to {i}\n')

    def check_for_db(self):
        dbs = [db[0] for db in self.engine.execute('SELECT name FROM sys.databases')]
        logger.info(dbs)
        # global databaseName
        if  self.database not in dbs:
            logger.info(f'Creating new database: {self.database}')
            self.engine.execute(f'CREATE DATABASE {self.database}')
        else: 
            self.databaseName = self.database



