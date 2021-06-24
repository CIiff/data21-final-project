from optimising.app.db_creation.tech import *
from optimising.app.db_creation.candidate import candidate_sql_tbl





# creation of course staff junction table
class techCandidateJunc(CreateDB):

    def __init__(self,database):
        super().__init__(database)        # SubClass initialization code
        
        tech_sql_tbl.create_tech_table()
        
    @staticmethod
    def pysqldf(q):
        return sqldf(q,globals())
 

    def create_table(self):
        with self.db:
            self.c.executescript("""
                    DROP TABLE IF EXISTS tech_junction;
                    CREATE TABLE IF NOT EXISTS tech_junction
                            (
                            tech_id INTEGER,
                            candidate_id INTEGER,
                            score INTEGER,
                            FOREIGN KEY(tech_id) REFERENCES tech(tech_id)
                            ON DELETE SET NULL,
                            FOREIGN KEY(candidate_id) REFERENCES candidate(candidate_id)
                            ON DELETE SET NULL );
                            
                            """)

    def data_entry(self):
        
        self.pysqldf("""
                SELECT
                    candidate_name AS candidate_id,
                    tech_name AS tech_id,
                    tech_score AS score

                from tech_junc_df
        """).to_sql('tech_junction',con=self.db,index=False,if_exists='append')
    
    
    def update_tech_df(self):

        df = json_df_dict['tech_df']
        logger.info(df.head(5))
        
        for row in tech_sql_tbl.c.execute("SELECT tech,tech_id FROM tech "):
            df['tech_name'].replace({row[0]:row[1]},inplace=True)
        logger.info(df.head(5))
        for row in candidate_sql_tbl.c.execute("SELECT candidate_name,candidate_id FROM candidate ORDER BY candidate_name "):
            # logger.info(f'replacements {row}')
            df['candidate_name'].replace({(row[0]):str(row[1])},inplace=True)
        logger.info(df.head(5))
        return df


    def sample_query(self):
        logger.info('TECH_JUNCTION_TABLE \n')
        data = tech_junc_sql_tbl.c.execute("SELECT * FROM tech_junction LIMIT 10")
        for row in data:
            logger.info(row)
        return data

    def create_tech_junc_table(self):

        # self.update_weakness_df()
        self.create_table()
        self.data_entry()
        self.db.commit()
        self.sample_query()


tech_junc_sql_tbl = techCandidateJunc('memory')
tech_junc_df = tech_junc_sql_tbl.update_tech_df()



