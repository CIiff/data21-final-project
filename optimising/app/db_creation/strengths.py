from optimising.app.db_creation.candidate import CreateDB,json_df_dict,sqldf,logger


class strengthsTable(CreateDB):

    def __init__(self,database):
        super().__init__(database) # SubClass initialization code
       

    @staticmethod
    def pysqldf(q):
        return sqldf(q,globals())
    
    def create_table(self):
        with self.db:
            self.c.executescript("""
                    DROP TABLE IF EXISTS strengths;
                    CREATE TABLE IF NOT EXISTS strengths
                            (
                            strength_id INTEGER NOT NULL PRIMARY KEY,
                            strength STRING
                            );
                    
                    """)
      
    
    def data_entry(self):
        
        self.pysqldf("""
                SELECT
                 
                    strengths AS strength
          
                from strengths_df
        """).to_sql('strengths',con=self.db,index=False,if_exists='append')


    def sample_query(self):
        logger.info('STRENGTHS TABLE \n')
        data = strengths_sql_tbl.c.execute("SELECT strength_id,strength FROM strengths LIMIT 10")
        for row in data:
            logger.info(row)
        return data

    def create_strengths_table(self):
        
        self.create_table()
        self.data_entry()
        self.db.commit()
        self.sample_query()


strengths_df = json_df_dict['strength_df'].drop_duplicates(subset = ["strengths"])
strengths_sql_tbl = strengthsTable('memory')



