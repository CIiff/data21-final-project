from optimising_sql_server2.app.db_creation.candidate import *
# from optimising_sql_server2.app.db_creation.candidate import CreateDB,json_df_dict,sqldf,logger


class weaknessesTable(CreateDB):

    def __init__(self):
        super().__init__() # SubClass initialization code
       

    @staticmethod
    def pysqldf(q):
        return sqldf(q,globals())
    
    def create_table(self):
        with self.db:
            if 'weaknesses' not in [table[0] for table in self.engine.execute("""SELECT *FROM SYSOBJECTS WHERE xtype = 'U';""")]:
                self.engine.execute("""
                    
                        CREATE TABLE weaknesses
                                (
                                weakness_id int IDENTITY(1,1) PRIMARY KEY,
                                weakness VARCHAR(30)
                                )
                        
                        """)
                logger.info('CREATING WEAKNESSES SQL TABLE')
      
    
    def data_entry(self):
        with self.engine.connect() as connection:
            self.pysqldf("""
                    SELECT
                    
                        weaknesses AS weakness
            
                    from weaknesses_df
            """).to_sql('weaknesses',connection,index = False,if_exists= 'append',chunksize= 500)
            logger.info('\nLOADING TO WEAKNESSES SQL TABLE\n')


    def sample_query(self):
        logger.info('WEAKNESSES_TABLE \n')
        data = self.engine.execute("SELECT weakness_id,weakness FROM weaknesses LIMIT 10")
        for row in data:
            logger.info(row)
        return data

    def create_weaknesses_table(self):
        
        self.create_table()
        if weaknesses_df.empty == False:
            self.data_entry()
        else:
            logger.info('No new Weaknesses data')
        # self.sample_query()

if json_df_dict != {}:
    weaknesses_df = json_df_dict['weakness_df'].drop_duplicates(subset = ["weaknesses"])
    if weaknesses_df.empty == False:
        weaknesses_df = weaknesses_df["weaknesses"]

weaknesses_sql_tbl = weaknessesTable()



