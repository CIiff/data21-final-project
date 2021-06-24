import sqlite3
from pandasql import sqldf

class CreateDB:

    def __init__(self,database):
        # self.db = sqlite3.connect(f':{database}:')
        self.db = sqlite3.connect(f'{database}_Test.db')
        self.c = self.db.cursor()
        
        
