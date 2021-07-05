from optimising_sql_server.app.db_creation.candidate import CreateDB,json_df_dict,sqldf,logger
from optimising_sql_server.app.tranform_files.transform_sparta_day_txt import txt_sparta_day_df


class locationTable(CreateDB):

    def __init__(self):
        super().__init__() # SubClass initialization code
       

    @staticmethod
    def pysqldf(q):
        return sqldf(q,globals())
    
    def create_table(self):
        with self.db:
            self.c.execute("""
                    DROP TABLE IF EXISTS locations;
                    CREATE TABLE locations
                            (
                            location_id int IDENTITY(1,1) PRIMARY KEY,
                            location VARCHAR(30)
                            );
                    """)
      
    
    def data_entry(self):
        
        sql_insert = """
                INSERT INTO locations(
                 
                    location
                )
                VALUES(
                    ?
                )"""
        self.c.executemany(sql_insert,location_df)
        # .to_sql('locations',con=self.db,index=False,if_exists='append')


    def sample_query(self):
        logger.info('LOCATION_TABLE \n')
        data = self.c.execute("SELECT location_id,location FROM locations LIMIT 10")
        for row in data:
            logger.info(row)
        return data

    def create_location_table(self):
        
        self.create_table()
        self.db.commit()
        self.data_entry()
        logger.info('\nLOADING TO LOCATION SQL TABLE\n')
        self.db.commit()
        # self.sample_query()


location_df = txt_sparta_day_df.drop_duplicates(subset = ["location"])
location_df = location_df["location"]
location_df = [[location] for location in location_df.values.tolist()]
location_sql_tbl = locationTable()



