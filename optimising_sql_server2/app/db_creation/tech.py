from optimising_sql_server2.app.db_creation.candidate import *


class techTable(CreateDB):

    def __init__(self):
        super().__init__() # SubClass initialization code
       

    @staticmethod
    def pysqldf(q):
        return sqldf(q,globals())
    
    def create_table(self):
        with self.db:
            if 'tech' not in [table[0] for table in self.engine.execute("""SELECT *FROM SYSOBJECTS WHERE xtype = 'U';""")]:
                self.engine.execute("""
                    
                        CREATE TABLE tech
                                (
                                tech_id int IDENTITY(1,1) PRIMARY KEY,
                                tech VARCHAR(20)
                                )
                        
                        """)
                logger.info('CREATING TECH SQL TABLE')
      
    
    def data_entry(self):
        with self.engine.connect() as connection:
            self.pysqldf("""
                    SELECT
                    
                        tech_name AS tech
            
                    from tech_df
                    """).to_sql('tech',connection,index = False,if_exists= 'append')
            logger.info('\nLOADING TO TECH SQL TABLE\n')
        


    def sample_query(self):
        logger.info('TECH_TABLE \n')
        data = self.engine.execute("SELECT tech,tech_id FROM tech LIMIT 10")
        for row in data:
            logger.info(row)
        return data

    def create_tech_table(self):
        
        self.create_table()
        self.data_entry()
        # self.sample_query()


tech_df = json_df_dict['tech_df'].drop_duplicates(subset = ["tech_name"])
tech_df = tech_df["tech_name"]
tech_sql_tbl = techTable()



