from optimising_sql_server.app.db_creation.location import *
from optimising_sql_server.app.db_creation.weekly_performance import candidate_sql_tbl,json_df_dict,pd
from tabulate import tabulate


class spartaDayTable(CreateDB):

    def __init__(self):
        super().__init__() # SubClass initialization code
       
        location_sql_tbl.create_location_table()
       

    @staticmethod
    def pysqldf(q):
        return sqldf(q,globals())
    
    def create_table(self):
        with self.db:
            self.c.execute("""
                    DROP TABLE IF EXISTS sparta_day;
                    CREATE TABLE sparta_day
                            (
                            candidate_id INT NOT NULL,
                            date DATE NOT NULL,
                            location_id  INT NULL,
                            result VARCHAR(10) NULL,
                            self_development VARCHAR(10) NULL,
                            financial_support VARCHAR(10) NULL,
                            geo_flex VARCHAR(10) NULL,
                            course_interest VARCHAR(15) NULL,
                            presentation INT NULL,
                            presentation_max INT NULL,
                            psychometrics INT NULL,
                            psychometrics_max INT NULL,
                            FOREIGN KEY(candidate_id) REFERENCES candidate(candidate_id),
                            FOREIGN KEY(location_id) REFERENCES locations(location_id),
                            PRIMARY KEY(candidate_id,date)

                            );
                    
                    """)
      
    
    def data_entry(self):
        
        sql_insert = """
                INSERT INTO sparta_day(
                    candidate_id,
                    date,
                    location_id,
                    result,
                    self_development,
                    financial_support,
                    geo_flex,
                    course_interest,
                    presentation,
                    presentation_max,
                    psychometrics,
                    psychometrics_max
                )
                VALUES(
                    ?,?,?,?,?,?,?,?,?,?,?,?
                )"""
        self.c.executemany(sql_insert,sparta_day_df.values.tolist())

        # .to_sql('sparta_day',con=self.db,index=False,if_exists='append')



    def update_combined_sparta_day_df(self):
        df = combined_sparta_day_df
        for row in self.c.execute("SELECT location,location_id FROM locations"):
            df['location'].replace({row[0]:row[1]},inplace=True)

        for row in self.c.execute("SELECT candidate_name,candidate_id,staff_id FROM candidate"):
            df['candidate_name'].replace({row[0]:str(row[1])},inplace=True)

        df = df.rename(columns=({'candidate_name':'candidate_id','location':'location_id'}))
        df = df[['candidate_id','date','location_id','result','self_development',
                'financial_support','geo_flex','course_interest',
                'presentation','presentation_max','psychometrics','psychometrics_max']]
      
        #with pd.option_context('display.max_rows', None, 'display.max_columns', None):
           # print(df)
            
        return df


    def sample_query(self):
        logger.info('SPARTA_DAY_TABLE \n')
        data = self.c.execute("SELECT * FROM sparta_day LIMIT 10")
        for row in data:
            logger.info(row)
        return data

    def create_sparta_day_table(self):
        
        self.create_table()
        self.db.commit()
        self.data_entry()
        logger.info('\nLOADING TO SPARTA_DAY SQL TABLE\n')
        self.db.commit()
        # self.sample_query()




combined_sparta_day_df = (pd.merge(txt_sparta_day_df,json_df_dict['sparta_day_df'])).drop_duplicates()


with pd.option_context('display.max_rows', None, 'display.max_columns', None):
    # print(tabulate(txt_sparta_day_df))
    print(tabulate(json_df_dict['sparta_day_df']))



sparta_day_sql_tbl = spartaDayTable()
sparta_day_df = sparta_day_sql_tbl.update_combined_sparta_day_df()
# sparta_day_sql_tbl.update_combined_sparta_day_df()


















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
