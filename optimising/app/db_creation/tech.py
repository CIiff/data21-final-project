from optimising.app.db_creation.candidate import CreateDB,json_df_dict,sqldf,logger


class techTable(CreateDB):

    def __init__(self,database):
        super().__init__(database) # SubClass initialization code
       

    @staticmethod
    def pysqldf(q):
        return sqldf(q,globals())
    
    def create_table(self):
        with self.db:
            self.c.executescript("""
                    DROP TABLE IF EXISTS tech;
                    CREATE TABLE IF NOT EXISTS tech
                            (
                            tech_id INTEGER NOT NULL PRIMARY KEY,
                            tech TEXT
                            );
                    
                    """)
      
    
    def data_entry(self):
        
        self.pysqldf("""
                SELECT
                 
                    tech_name AS tech
          
                from tech_df
        """).to_sql('tech',con=self.db,index=False,if_exists='append')


    def sample_query(self):
        logger.info('TECH_TABLE \n')
        data = tech_sql_tbl.c.execute("SELECT tech,tech_id FROM tech LIMIT 10")
        for row in data:
            logger.info(row)
        return data

    def create_tech_table(self):
        
        self.create_table()
        self.data_entry()
        logger.info('\nLOADING TO TECH SQL TABLE\n')
        self.db.commit()
        # self.sample_query()


tech_df = json_df_dict['tech_df'].drop_duplicates(subset = ["tech_name"])
tech_sql_tbl = techTable('memory')



