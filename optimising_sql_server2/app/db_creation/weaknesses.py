from optimising_sql_server.app.db_creation.candidate import CreateDB,json_df_dict,sqldf,logger


class weaknessesTable(CreateDB):

    def __init__(self):
        super().__init__() # SubClass initialization code
       

    @staticmethod
    def pysqldf(q):
        return sqldf(q,globals())
    
    def create_table(self):
        with self.db:
            self.c.execute("""
                    DROP TABLE IF EXISTS weaknesses;
                    CREATE TABLE weaknesses
                            (
                            weakness_id int IDENTITY(1,1) PRIMARY KEY,
                            weakness VARCHAR(30)
                            );
                    
                    """)
      
    
    def data_entry(self):
        
        sql_insert = """
                INSERT INTO weaknesses(
                    weakness
                )
                VALUES(
                    ?
                )"""
                
        self.c.executemany(sql_insert,weaknesses_df)
        # """).to_sql('weaknesses',con=self.db,index=False,if_exists='append')


    def sample_query(self):
        logger.info('WEAKNESSES_TABLE \n')
        data = weaknesses_sql_tbl.c.execute("SELECT weakness_id,weakness FROM weaknesses LIMIT 10")
        for row in data:
            logger.info(row)
        return data

    def create_weaknesses_table(self):
        
        self.create_table()
        self.db.commit()
        self.data_entry()
        logger.info('\nLOADING TO WEAKNESSES SQL TABLE\n')
        self.db.commit()
        # self.sample_query()


weaknesses_df = json_df_dict['weakness_df'].drop_duplicates(subset = ["weaknesses"])
weaknesses_df = weaknesses_df["weaknesses"]
weaknesses_df = [[weakness] for weakness in weaknesses_df.values.tolist()]
weaknesses_sql_tbl = weaknessesTable()



