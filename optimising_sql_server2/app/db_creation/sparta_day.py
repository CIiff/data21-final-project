from optimising_sql_server2.app.db_creation.location import *
from optimising_sql_server2.app.db_creation.weekly_performance import json_df_dict,pd
from optimising_sql_server2.app.tranform_files.transform_sparta_day_txt import txt_sparta_day_df
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
            if 'sparta_day' not in [table[0] for table in self.engine.execute("""SELECT *FROM SYSOBJECTS WHERE xtype = 'U';""")]:
                self.engine.execute("""
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
                            )
                    
                    """)
      
    
    def data_entry(self):
        with self.engine.connect() as connection:
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

                    from sparta_day_df

            """).to_sql('sparta_day',connection,index = False,if_exists= 'append',chunksize= 500)
            logger.info('\nLOADING TO SPARTA_DAY SQL TABLE\n')



    def update_combined_sparta_day_df(self):
        if txt_sparta_day_df.empty == False:
            df = combined_sparta_day_df
            for row in self.engine.execute("SELECT location,location_id FROM locations"):
                df['location'].replace({row[0]:str(row[1])},inplace=True)

            for row in self.engine.execute("SELECT candidate_name,candidate_id,staff_id FROM candidate"):
                df['candidate_name'].replace({row[0]:str(row[1])},inplace=True)
            return df
            
        else:
            logger.info('No New data on Sparta Days')
            return pd.DataFrame()

    def sample_query(self):
        logger.info('SPARTA_DAY_TABLE \n')
        data = self.engine.execute("SELECT * FROM sparta_day LIMIT 10")
        for row in data:
            logger.info(row)
        return data

    def create_sparta_day_table(self):
        
        self.create_table()
        if sparta_day_df.empty == False:
            self.data_entry()
        # self.sample_query()
        pass


if txt_sparta_day_df.empty == False :
    combined_sparta_day_df = (pd.merge(txt_sparta_day_df,json_df_dict['sparta_day_df'])).drop_duplicates()


sparta_day_sql_tbl = spartaDayTable()
sparta_day_df = sparta_day_sql_tbl.update_combined_sparta_day_df()
# sparta_day_sql_tbl.update_combined_sparta_day_df()



# with pd.option_context('display.max_rows', None, 'display.max_columns', None):
#     print(tabulate(txt_sparta_day_df.head()))
#     print(tabulate(json_df_dict['sparta_day_df'].head()))















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
