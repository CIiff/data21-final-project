from optimising.app.db_creation.location import *
from optimising.app.db_creation.weekly_performance import candidate_sql_tbl,json_df_dict,pd


class spartaDayTable(CreateDB):

    def __init__(self):
        super().__init__() # SubClass initialization code
       
        location_sql_tbl.create_location_table()
       

    @staticmethod
    def pysqldf(q):
        return sqldf(q,globals())
    
    def create_table(self):
        with self.db:
            self.c.executescript("""
                    DROP TABLE IF EXISTS sparta_day;
                    CREATE TABLE IF NOT EXISTS sparta_day
                            (
                            candidate_id INTEGER NOT NULL,
                            location_id  INTEGER,
                            date TEXT NOT NULL,
                            result TEXT,
                            self_development TEXT,
                            financial_support TEXT,
                            geo_flex TEXT,
                            course_interest TEXT,
                            presentation INTEGER,
                            presentation_max INTEGER,
                            psychometrics INTEGER,
                            psychometrics_max INTEGER,
                            FOREIGN KEY(candidate_id) REFERENCES candidate(candidate_id),
                            FOREIGN KEY(location_id) REFERENCES locations(location_id),
                            PRIMARY KEY(candidate_id,date)

                            );
                    
                    """)
      
    
    def data_entry(self):
        
        self.pysqldf("""
                SELECT
                    candidate_name AS candidate_id,
                    location AS location_id,
                    date,
                    result,
                    self_development,
                    financial_support,
                    geo_flex,
                    course_interest,
                    presentation,
                    presentation_max,
                    psychometrics,
                    psychometrics_max

                from combined_sparta_day_df
        """).to_sql('sparta_day',con=self.db,index=False,if_exists='append')



    def update_combined_sparta_day_df(self):
        df = combined_sparta_day_df
        for row in location_sql_tbl.c.execute("SELECT location,location_id FROM locations"):
            df['location'].replace({row[0]:row[1]},inplace=True)

        for row in candidate_sql_tbl.c.execute("SELECT candidate_name,candidate_id,staff_id FROM candidate"):
            df['candidate_name'].replace({row[0]:str(row[1])},inplace=True)

        return df


    def sample_query(self):
        logger.info('SPARTA_DAY_TABLE \n')
        data = sparta_day_sql_tbl.c.execute("SELECT * FROM sparta_day LIMIT 10")
        for row in data:
            logger.info(row)
        return data

    def create_sparta_day_table(self):
        
        self.create_table()
        self.data_entry()
        logger.info('\nLOADING TO SPARTA_DAY SQL TABLE\n')
        self.db.commit()
        # self.sample_query()




combined_sparta_day_df = (pd.merge(sparta_day_df,json_df_dict['sparta_day_df'])).drop_duplicates()



print(combined_sparta_day_df.head())



sparta_day_sql_tbl = spartaDayTable()
sparta_day_df = sparta_day_sql_tbl.update_combined_sparta_day_df()

















# # creation of sparta_day table model
# class SpartaDay(SqlAlchemyBase):
#     __tablename__ = 'sparta_day'

#     candidate_id 
#     location_id 
#     date = Column(Date, primary_key=True)
#     result 
#     self_development 
#     financial_support 
#     geo_flex 
#     course_interest 
#     presentation 
#     presentation_max 
#     psychometrics 
#     psychometrics_max 
