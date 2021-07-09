from optimising_sql_server2.app.db_creation.candidate import *


class strengthsTable(CreateDB):

    def __init__(self):
        super().__init__()  # SubClass initialization code

    @staticmethod
    def pysqldf(q):
        return sqldf(q, globals())

    def create_table(self):
        with self.db:
            if 'strengths' not in [table[0] for table in self.engine.execute("""SELECT *FROM SYSOBJECTS WHERE xtype = 'U';""")]:
                self.engine.execute("""
                    
                        CREATE TABLE strengths
                                (
                                strength_id int IDENTITY(1,1) PRIMARY KEY,
                                strength VARCHAR(20)
                                )
                        
                        """)
                logger.info('CREATING STRENGTHS SQL TABLE')

    def data_entry(self):
        with self.engine.connect() as connection:

            self.pysqldf("""
                SELECT
                 
                    strengths AS strength
          
                from strengths_df
                """).to_sql('strengths', connection, index=False, if_exists='append', chunksize=500)
        logger.info('\nLOADING TO STRENGTHS SQL TABLE\n')

    def sample_query(self):
        logger.info('STRENGTHS TABLE \n')
        data = self.engine.execute(
            "SELECT strength_id,strength FROM strengths LIMIT 10")
        for row in data:
            logger.info(row)
        return data

    def create_strengths_table(self):

        self.create_table()
        if strengths_df.empty == False:
            self.data_entry()
        else:
            logger.info('No new strengths data to load')
        # self.sample_query()


if json_df_dict != {}:
    strengths_df = json_df_dict['strength_df'].drop_duplicates(subset=[
                                                               "strengths"])
    if strengths_df.empty == False:
        strengths_df = strengths_df["strengths"]

strengths_sql_tbl = strengthsTable()
