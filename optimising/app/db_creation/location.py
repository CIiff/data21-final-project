from optimising.app.db_creation.candidate import CreateDB,json_df_dict,sqldf,logger
from optimising.app.tranform_files.transform_sparta_day_txt import sparta_day_df


class locationTable(CreateDB):

    def __init__(self,database):
        super().__init__(database) # SubClass initialization code
       

    @staticmethod
    def pysqldf(q):
        return sqldf(q,globals())
    
    def create_table(self):
        with self.db:
            self.c.executescript("""
                    DROP TABLE IF EXISTS locations;
                    CREATE TABLE IF NOT EXISTS locations
                            (
                            location_id INTEGER NOT NULL PRIMARY KEY,
                            location STRING
                            );
                    
                    """)
      
    
    def data_entry(self):
        
        self.pysqldf("""
                SELECT
                 
                    location
          
                from location_df
        """).to_sql('locations',con=self.db,index=False,if_exists='append')


    def sample_query(self):
        logger.info('LOCATION_TABLE \n')
        data = location_sql_tbl.c.execute("SELECT location_id,location FROM locations LIMIT 10")
        for row in data:
            logger.info(row)
        return data

    def create_location_table(self):
        
        self.create_table()
        self.data_entry()
        self.db.commit()
        self.sample_query()


location_df = sparta_day_df.drop_duplicates(subset = ["location"])
location_sql_tbl = locationTable('memory')



