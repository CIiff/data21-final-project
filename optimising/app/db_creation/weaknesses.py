from optimising.app.db_creation.candidate import CreateDB,json_df_dict,sqldf,logger


class weaknessesTable(CreateDB):

    def __init__(self,database):
        super().__init__(database) # SubClass initialization code
       

    @staticmethod
    def pysqldf(q):
        return sqldf(q,globals())
    
    def create_table(self):
        with self.db:
            self.c.executescript("""
                    DROP TABLE IF EXISTS weaknesses;
                    CREATE TABLE IF NOT EXISTS weaknesses
                            (
                            weakness_id INTEGER NOT NULL PRIMARY KEY,
                            weakness TEXT
                            );
                    
                    """)
      
    
    def data_entry(self):
        
        self.pysqldf("""
                SELECT
                 
                    weaknesses AS weakness
          
                from weaknesses_df
        """).to_sql('weaknesses',con=self.db,index=False,if_exists='append')


    def sample_query(self):
        logger.info('WEAKNESSES_TABLE \n')
        data = weaknesses_sql_tbl.c.execute("SELECT weakness_id,weakness FROM weaknesses LIMIT 10")
        for row in data:
            logger.info(row)
        return data

    def create_weaknesses_table(self):
        
        self.create_table()
        self.data_entry()
        logger.info('\nLOADING TO WEAKNESSES SQL TABLE\n')
        self.db.commit()
        # self.sample_query()


weaknesses_df = json_df_dict['weakness_df'].drop_duplicates(subset = ["weaknesses"])
weaknesses_sql_tbl = weaknessesTable('memory')



