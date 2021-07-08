from optimising_sql_server2.app.db_creation.weaknesses import *
from optimising_sql_server2.app.db_creation.connection import CreateDB
from tqdm import tqdm,trange





# creation of course staff junction table
class weaknessesCandidateJunc(CreateDB):

    def __init__(self):
        super().__init__()        # SubClass initialization code
        
        weaknesses_sql_tbl.create_weaknesses_table()
        
    @staticmethod
    def pysqldf(q):
        return sqldf(q,globals())
 

    def create_table(self):
        with self.db:
            if 'weaknesses_junction' not in [table[0] for table in self.engine.execute("""SELECT *FROM SYSOBJECTS WHERE xtype = 'U';""")]:
                self.engine.execute("""
                        
                        CREATE TABLE weaknesses_junction
                                (
                                weakness_id INT,
                                candidate_id INT,
                                FOREIGN KEY(weakness_id) REFERENCES weaknesses(weakness_id),
                                FOREIGN KEY(candidate_id) REFERENCES candidate(candidate_id)
                                )
                                """)
                logger.info('CREATING WEAKNESSES_JUNCTION SQL TABLE')

    def data_entry(self):
        with self.engine.connect() as connection:
            self.pysqldf("""
                SELECT
                    candidate_name AS candidate_id,
                    weaknesses AS weakness_id

                from weakness_junc_df
                """).to_sql('weaknesses_junction',connection,index = False,if_exists= 'append')
            logger.info('\nLOADING TO WEAKNESSES_JUNCTION SQL TABLE\n')
    
    def update_weakness_df(self):
        if json_df_dict != {}:
            df = json_df_dict['weakness_df']
            # logger.info(df.head(5))
            if df.empty == False:
                for row in self.engine.execute("SELECT weakness,weakness_id FROM weaknesses "):
                    df['weaknesses'].replace({row[0]:row[1]},inplace=True)
                # logger.info(df.head(5))
                for row in tqdm(self.engine.execute("SELECT candidate_name,candidate_id FROM candidate ORDER BY candidate_name "),unit ='weakness',desc = 'Updating_Candidate_Weaknesses',position = 0):
                    # logger.info(f'replacements {row}')
                    df['candidate_name'].replace({(row[0]):str(row[1])},inplace=True)
                # logger.info(df.head(5))
                # df = df.rename(columns=({'candidate_name':'candidate_id','weaknesses':'weakness_id'}))
                return df
        else:
            return pd.DataFrame()


    def sample_query(self):
        logger.info('WEAKNESSES_JUNC_TABLE \n')
        data = self.engine.execute("SELECT * FROM weaknesses_junction LIMIT 10")
        for row in data:
            logger.info(row)
        return data

    def create_weaknessses_junc_table(self):

        try:
            self.create_table()
            if weakness_junc_df.empty == False:
                self.data_entry()
            else:
                logger.info('No new data to load to Weaknesses_junction table')
        # self.sample_query()
        except AttributeError:
            pass





weaknesses_junc_sql_tbl = weaknessesCandidateJunc()
weakness_junc_df = weaknesses_junc_sql_tbl.update_weakness_df()
# weaknesses_junc_sql_tbl.create_weaknessses_junc_table()


