import sqlite3
from pandasql import sqldf
from optimising.app.db_creation.logger import logger

class CreateDB:

    def __init__(self,database):
        # self.db = sqlite3.connect(f':{database}:')
        self.db = sqlite3.connect(f'{database}_Test.db')
        self.c = self.db.cursor()
        logger.info(f'\nConnecting to {database}_Test.db\n')
        
