import sqlite3
from pandasql import sqldf
from optimising_sql_server.app.db_creation.logger import logger
from sqlalchemy import create_engine,MetaData,Table
import pandas as pd
import pyodbc

databaseName = 'new_db'
username = 'SA'
password = 'Passw0rd2018'
server = 'localhost,1433'
driver= 'ODBC Driver 17 for SQL Server'
CONNECTION_STRING = 'DRIVER='+driver+';SERVER='+server+';DATABASE='+databaseName+';UID='+username+';PWD='+ password



class CreateDB:

    def __init__(self):
        # connect to SQLite
        # self.db = sqlite3.connect(f':{database}:')
        # create cursor
        # self.c = self.db.cursor()

        # Create/Open a Connection to Microsoft's SQL Server
        self.db = pyodbc.connect(CONNECTION_STRING)
        # create cursor
        self.c = self.db.cursor()
        logger.info(f'\nConnecting to {databaseName}\n')
        

