from optimising_sql_server.app.db_creation.candidate import CreateDB,json_df_dict,sqldf,logger


class techTable(CreateDB):

    def __init__(self):
        super().__init__() # SubClass initialization code
       

    @staticmethod
    def pysqldf(q):
        return sqldf(q,globals())
    
    def create_table(self):
        # with self.db:
            self.c.execute("""
                    DROP TABLE IF EXISTS tech;
                    CREATE TABLE tech
                            (
                            tech_id int IDENTITY(1,1) PRIMARY KEY,
                            tech VARCHAR(20)
                            )
                    
                    """)
      
    
    def data_entry(self):
        
        sql_insert = """
                INSERT INTO tech(
                 
                    tech
                )
                VALUES
                (
                    ?
                )"""
        self.c.executemany(sql_insert,tech_df)
        # .to_sql('tech',con=self.db,index=False,if_exists='append')


    def sample_query(self):
        logger.info('TECH_TABLE \n')
        data = self.c.execute("SELECT tech,tech_id FROM tech LIMIT 10")
        for row in data:
            logger.info(row)
        return data

    def create_tech_table(self):
        
        self.create_table()
        self.db.commit()
        self.data_entry()
        logger.info('\nLOADING TO TECH SQL TABLE\n')
        self.db.commit()
        # self.sample_query()


tech_df = json_df_dict['tech_df'].drop_duplicates(subset = ["tech_name"])
tech_df = tech_df["tech_name"]
tech_df = [[tech] for tech in tech_df.values.tolist()]
tech_sql_tbl = techTable()



