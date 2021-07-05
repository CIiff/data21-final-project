from optimising_sql_server2.app.db_creation.strengths import *
from optimising_sql_server2.app.db_creation.candidate import tqdm





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
            if 'strengths_junction' not in [table[0] for table in self.engine.execute("""SELECT *FROM SYSOBJECTS WHERE xtype = 'U';""")]:
                self.engine.execute("""
                        CREATE TABLE strengths_junction
                                (
                                strength_id INT,
                                candidate_id INT,
                                FOREIGN KEY(strength_id) REFERENCES strengths(strength_id)
                                ON DELETE SET NULL,
                                FOREIGN KEY(candidate_id) REFERENCES candidate(candidate_id)
                                ON DELETE SET NULL );
                                
                                """)
                logger.info('CREATING STRENGTHS_JUNCTION SQL TABLE')

    def data_entry(self):
        with self.engine.connect() as connection:
            self.pysqldf("""
                    SELECT
                        candidate_name AS candidate_id,
                        strengths AS strength_id

                    from strengths_junc_df
                    """).to_sql('strengths_junction',connection,index = False,if_exists= 'append')
            logger.info('\nLOADING TO STRENGTHS_JUNC SQL TABLE\n')
    
    

    def update_strengths_df(self):

        df = json_df_dict['strength_df']
        
        
        for row in self.engine.execute("SELECT strength,strength_id FROM strengths "):
            df['strengths'].replace({row[0]:str(row[1])},inplace=True)
        
        for row in tqdm(self.engine.execute("SELECT candidate_name,candidate_id FROM candidate ORDER BY candidate_name "),unit ='strengths',desc = 'Updating_Candidate_Strengths',position = 0):
            
            df['candidate_name'].replace({(row[0]):str(row[1])},inplace=True)
        
        return df


    def sample_query(self):
        logger.info('STREGNTH_JUNCTION_TABLE \n')
        data = self.engine.execute("SELECT * FROM strengths_junction LIMIT 10")
        for row in data:
            logger.info(row)
        return data

    def create_strengths_junc_table(self):

        
        self.create_table()
        self.data_entry()
        # self.sample_query()


strengths_junc_sql_tbl = strengthsCandidateJunc()
strengths_junc_df = strengths_junc_sql_tbl.update_strengths_df()


