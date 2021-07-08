from optimising_sql_server2.app.db_creation.candidate import CreateDB, json_df_dict, sqldf, logger
from optimising_sql_server2.app.tranform_files.transform_sparta_day_txt import txt_sparta_day_df


class locationTable(CreateDB):

    def __init__(self):
        super().__init__()  # SubClass initialization code

    @staticmethod
    def pysqldf(q):
        return sqldf(q, globals())

    def create_table(self):
        with self.db:
            if 'locations' not in [table[0] for table in self.engine.execute("""SELECT *FROM SYSOBJECTS WHERE xtype = 'U';""")]:
                self.engine.execute("""
                        CREATE TABLE locations
                                (
                                location_id int IDENTITY(1,1) PRIMARY KEY,
                                location VARCHAR(30)
                                );
                        """)
                logger.info('CREATING LOCATIONS SQL TABLE')

    def data_entry(self):
        with self.engine.connect() as connection:
            self.pysqldf("""
                    SELECT
                    
                        location
            
                    from location_df
            """).to_sql('locations', connection, index=False, if_exists='append')
            logger.info('\nLOADING TO LOCATIONS SQL TABLE\n')

    def sample_query(self):
        logger.info('LOCATION_TABLE \n')
        data = self.engine.execute(
            "SELECT location_id,location FROM locations LIMIT 10")
        for row in data:
            logger.info(row)
        return data

    def create_location_table(self):

        self.create_table()
        if txt_sparta_day_df.empty == False:
            self.data_entry()
        else:
            logger.info('No new location to load')
        # self.sample_query()


if txt_sparta_day_df.empty == False:
    location_df = txt_sparta_day_df.drop_duplicates(subset=["location"])
    if location_df.empty == False:
        location_df = location_df["location"]

location_sql_tbl = locationTable()
