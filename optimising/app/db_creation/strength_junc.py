from optimising.app.db_creation.strengths import *
from optimising.app.db_creation.candidate import candidate_sql_tbl





# creation of course staff junction table
class strengthsCandidateJunc(CreateDB):

    def __init__(self,database):
        super().__init__(database)        # SubClass initialization code
        
        strengths_sql_tbl.create_strengths_table()
        
    @staticmethod
    def pysqldf(q):
        return sqldf(q,globals())
 

    def create_table(self):
        with self.db:
            self.c.executescript("""
                    DROP TABLE IF EXISTS weaknesses_junction;
                    CREATE TABLE IF NOT EXISTS weaknesses_junction
                            (
                            strength_id INTEGER,
                            candidate_id INTEGER,
                            FOREIGN KEY(strength_id) REFERENCES strengths(strength_id)
                            ON DELETE SET NULL,
                            FOREIGN KEY(candidate_id) REFERENCES candidate(candidate_id)
                            ON DELETE SET NULL );
                            
                            """)

    def data_entry(self):
        
        self.pysqldf("""
                SELECT
                    candidate_name AS candidate_id,
                    strengths AS strength_id

                from strengths_junc_df
        """).to_sql('strengths_junction',con=self.db,index=False,if_exists='append')
    
    
    def update_strengths_df(self):

        df = json_df_dict['strength_df']
        logger.info(df.head(5))
        
        for row in strengths_sql_tbl.c.execute("SELECT strength,strength_id FROM strengths "):
            df['strengths'].replace({row[0]:row[1]},inplace=True)
        logger.info(df.head(5))
        for row in candidate_sql_tbl.c.execute("SELECT candidate_name,candidate_id FROM candidate ORDER BY candidate_name "):
            # logger.info(f'replacements {row}')
            df['candidate_name'].replace({(row[0]):str(row[1])},inplace=True)
        logger.info(df.head(5))
        return df


    def sample_query(self):
        logger.info('STREGNTH_JUNCTION_TABLE \n')
        data = strengths_junc_sql_tbl.c.execute("SELECT * FROM strengths_junction LIMIT 10")
        for row in data:
            logger.info(row)
        return data

    def create_strengths_junc_table(self):

        # self.update_weakness_df()
        self.create_table()
        self.data_entry()
        self.db.commit()
        self.sample_query()




strengths_junc_sql_tbl = strengthsCandidateJunc('memory')
strengths_junc_df = strengths_junc_sql_tbl.update_strengths_df()

