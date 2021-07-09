from optimising_sql_server2.app.db_creation.tech import *
from optimising_sql_server2.app.db_creation.candidate import candidate_sql_tbl


# creation of course staff junction table
class techCandidateJunc(CreateDB):

    def __init__(self):
        super().__init__()        # SubClass initialization code

        tech_sql_tbl.create_tech_table()

    @staticmethod
    def pysqldf(q):
        return sqldf(q, globals())

    def create_table(self):
        with self.db:
            if 'tech_junction' not in [table[0] for table in self.engine.execute("""SELECT *FROM SYSOBJECTS WHERE xtype = 'U';""")]:
                self.engine.execute("""
                    
                        CREATE TABLE tech_junction
                                (
                                tech_id INT,
                                candidate_id INT,
                                score INT,
                                FOREIGN KEY(tech_id) REFERENCES tech(tech_id),
                                FOREIGN KEY(candidate_id) REFERENCES candidate(candidate_id)
                                )
                                """)
                logger.info('CREATING TECH_JUNCTION SQL TABLE')

    def data_entry(self):
        with self.engine.connect() as connection:

            self.pysqldf("""
                    SELECT
                        candidate_name AS candidate_id,
                        tech_name AS tech_id,
                        tech_score AS score

                    from tech_junc_df
            """).to_sql('tech_junction', connection, index=False, if_exists='append')
            logger.info('\nLOADING TO TECH_JUNCTION SQL TABLE\n')

    def update_tech_df(self):
        if json_df_dict != {}:
            df = json_df_dict['tech_df']
            # logger.info(df.head(5))
            if df.empty == False:
                for row in self.engine.execute("SELECT tech,tech_id FROM tech "):
                    df['tech_name'].replace({row[0]: str(row[1])}, inplace=True)
                # logger.info(df.head(5))
                for row in self.engine.execute("SELECT candidate_name,candidate_id FROM candidate ORDER BY candidate_name "):
                    # logger.info(f'replacements {row}')
                    df['candidate_name'].replace(
                        {(row[0]): str(row[1])}, inplace=True)
                # logger.info(df.head(5))
                # df = df.rename(columns=({'candidate_name':'candidate_id','tech_name':'tech_id','tech_score':'score'}))
                return df
            else:
                return pd.DataFrame()

    def sample_query(self):
        logger.info('TECH_JUNCTION_TABLE \n')
        data = self.engine.execute("SELECT * FROM tech_junction LIMIT 10")
        for row in data:
            logger.info(row)
        return data

    def create_tech_junc_table(self):

        # self.update_weakness_df()
        self.create_table()
        if len(tech_junc_df.values) > 0:
            self.data_entry()
        else:
            logger.info('No new tech_junction Data')
        # self.sample_query()


tech_junc_sql_tbl = techCandidateJunc()
tech_junc_df = tech_junc_sql_tbl.update_tech_df()
