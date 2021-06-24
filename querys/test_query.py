
from optimising.app.db_creation.connection import CreateDB
from optimising.app.db_creation.logger import logger
from pandasql import sqldf
from pprint import pprint



    # @staticmethod
    # def pysqldf(q):
    #     return sqldf(q,globals())


class getInformation(CreateDB):
    

    def __init__(self,database):
        super().__init__(database)        # SubClass initialization code
    
        self.data = self.c.execute("SELECT staff_name,department FROM staff ")

        pass
        


    
res = getInformation('memory').data.fetchall()
pprint(res)
  
  
# SELECT name FROM sqlite_master 
# WHERE type = 'table' 
# AND name NOT LIKE 'sqlite_%'
# ORDER BY 1;

    
      
      
