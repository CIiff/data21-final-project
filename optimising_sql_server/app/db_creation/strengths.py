from optimising_sql_server.app.db_creation.candidate import CreateDB,json_df_dict,sqldf,logger


class strengthsTable(CreateDB):

    def __init__(self):
        super().__init__() # SubClass initialization code
       

    @staticmethod
    def pysqldf(q):
        return sqldf(q,globals())
    
    def create_table(self):
        with self.db:
            self.c.execute("""
                    DROP TABLE IF EXISTS strengths;
                    CREATE TABLE strengths
                            (
                            strength_id int IDENTITY(1,1) PRIMARY KEY,
                            strength VARCHAR(20)
                            );
                    
                    """)
      
    
    def data_entry(self):
        
        sql_insert = """
                INSERT INTO strengths(
                    strength
                )
                VALUES(
                    ?
                )"""
        self.c.executemany(sql_insert,strengths_df)
        # """).to_sql('strengths',con=self.db,index=False,if_exists='append')


    def sample_query(self):
        logger.info('STRENGTHS TABLE \n')
        data = self.c.execute("SELECT strength_id,strength FROM strengths LIMIT 10")
        for row in data:
            logger.info(row)
        return data

    def create_strengths_table(self):
        
        self.create_table()
        self.db.commit()
        self.data_entry()
        logger.info('\nLOADING TO STRENGTHS SQL TABLE\n')
        self.db.commit()
        # self.sample_query()


strengths_df = json_df_dict['strength_df'].drop_duplicates(subset = ["strengths"])
strengths_df = strengths_df["strengths"]
strengths_df = [[strength] for strength in strengths_df.values.tolist()]
strengths_sql_tbl = strengthsTable()



