from optimising.app.db_creation.weaknesses import *
from optimising.app.db_creation.candidate import candidate_sql_tbl





# creation of course staff junction table
class weaknessesCandidateJunc(CreateDB):

    def __init__(self,database):
        super().__init__(database)        # SubClass initialization code
        
        weaknesses_sql_tbl.create_weaknesses_table()
        
    @staticmethod
    def pysqldf(q):
        return sqldf(q,globals())
 

    def create_table(self):
        with self.db:
            self.c.executescript("""
                    DROP TABLE IF EXISTS weaknesses_junction;
                    CREATE TABLE IF NOT EXISTS weaknesses_junction
                            (
                            weakness_id INTEGER,
                            candidate_id INTEGER,
                            FOREIGN KEY(weakness_id) REFERENCES weaknesses(weakness_id)
                            ON DELETE SET NULL,
                            FOREIGN KEY(candidate_id) REFERENCES candidate(candidate_id)
                            ON DELETE SET NULL );
                            
                            """)

    def data_entry(self):
        
        self.pysqldf("""
                SELECT
                    candidate_name AS candidate_id,
                    weaknesses AS weakness_id

                from weakness_junc_df
        """).to_sql('weaknesses_junction',con=self.db,index=False,if_exists='append')
    
    
    def update_weakness_df(self):

        df = json_df_dict['weakness_df']
        logger.info(df.head(5))
        
        for row in weaknesses_sql_tbl.c.execute("SELECT weakness,weakness_id FROM weaknesses "):
            df['weaknesses'].replace({row[0]:row[1]},inplace=True)
        logger.info(df.head(5))
        for row in candidate_sql_tbl.c.execute("SELECT candidate_name,candidate_id FROM candidate ORDER BY candidate_name "):
            # logger.info(f'replacements {row}')
            df['candidate_name'].replace({(row[0]):str(row[1])},inplace=True)
        logger.info(df.head(5))
        return df


    def sample_query(self):
        logger.info('WEAKNESSES_JUNC_TABLE \n')
        data = weaknesses_junc_sql_tbl.c.execute("SELECT * FROM weaknesses_junction LIMIT 10")
        for row in data:
            logger.info(row)
        return data

    def create_weaknessses_junc_table(self):

        # self.update_weakness_df()
        self.create_table()
        self.data_entry()
        self.db.commit()
        self.sample_query()




weaknesses_junc_sql_tbl = weaknessesCandidateJunc('memory')
weakness_junc_df = weaknesses_junc_sql_tbl.update_weakness_df()


