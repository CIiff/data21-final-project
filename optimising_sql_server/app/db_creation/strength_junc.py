from optimising_sql_server.app.db_creation.strengths import *
from optimising_sql_server.app.db_creation.candidate import candidate_sql_tbl,tqdm





# creation of course staff junction table
class strengthsCandidateJunc(CreateDB):

    def __init__(self):
        super().__init__()        # SubClass initialization code
        
        strengths_sql_tbl.create_strengths_table()
        
    @staticmethod
    def pysqldf(q):
        return sqldf(q,globals())
 

    def create_table(self):
        with self.db:
            self.c.execute("""
                    DROP TABLE IF EXISTS strengths_junction;
                    CREATE TABLE strengths_junction
                            (
                            strength_id INT,
                            candidate_id INT,
                            FOREIGN KEY(strength_id) REFERENCES strengths(strength_id)
                            ON DELETE SET NULL,
                            FOREIGN KEY(candidate_id) REFERENCES candidate(candidate_id)
                            ON DELETE SET NULL );
                            
                            """)

    def data_entry(self):
        
        sql_insert = """
                INSERT INTO strengths_junction(
                    candidate_id,
                    strength_id
                )
                VALUES(
                    ?,?
                )"""
        self.c.executemany(sql_insert,strengths_junc_df.values.tolist())
            
        # .to_sql('strengths_junction',con=self.db,index=False,if_exists='append')
    
    
    def update_strengths_df(self):

        df = json_df_dict['strength_df']
        # logger.info(df.head(5))
        
        for row in self.c.execute("SELECT strength,strength_id FROM strengths "):
            df['strengths'].replace({row[0]:row[1]},inplace=True)
        # logger.info(df.head(5))
        for row in tqdm(self.c.execute("SELECT candidate_name,candidate_id FROM candidate ORDER BY candidate_name "),unit ='strengths',desc = 'Updating_Candidate_Strengths',position = 0):
            # logger.info(f'replacements {row}')
            df['candidate_name'].replace({(row[0]):str(row[1])},inplace=True)
        # logger.info(df.head(5))
        df = df.rename(columns=({'candidate_name':'candidate_id','strengths':'strength_id'}))
        return df


    def sample_query(self):
        logger.info('STREGNTH_JUNCTION_TABLE \n')
        data = self.c.execute("SELECT * FROM strengths_junction LIMIT 10")
        for row in data:
            logger.info(row)
        return data

    def create_strengths_junc_table(self):

        # self.update_weakness_df()
        self.create_table()
        self.db.commit()
        self.data_entry()
        logger.info('\nLOADING TO STRENGTHS_JUNC SQL TABLE\n')
        self.db.commit()
        # self.sample_query()




strengths_junc_sql_tbl = strengthsCandidateJunc()
strengths_junc_df = strengths_junc_sql_tbl.update_strengths_df()


