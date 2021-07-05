from optimising_sql_server2.app.db_creation.course_staff_junc import *
# from optimising_sql_server2.app.db_creation.course import *
from optimising_sql_server2.app.db_creation.weaknesses_junc import *
# from optimising_sql_server2.app.db_creation.sparta_day import *
from optimising_sql_server2.app.db_creation.sparta_day import *
from optimising_sql_server2.app.db_creation.strength_junc import *
from optimising_sql_server2.app.db_creation.tech_junc import *
import numpy as np


class weeklyPerformanceTable(CreateDB):

    def __init__(self):
        super().__init__() # SubClass initialization code
       
        course_staff_junct.create_course_staff_junc_table()
        weaknesses_junc_sql_tbl.create_weaknessses_junc_table()
        strengths_junc_sql_tbl.create_strengths_junc_table()
        tech_junc_sql_tbl.create_tech_junc_table()
        sparta_day_sql_tbl.create_sparta_day_table()
        

    @staticmethod
    def pysqldf(q):
        return sqldf(q,globals())
    
    def create_table(self):
        if 'weekly_performance' not in [table[0] for table in self.engine.execute("""SELECT *FROM SYSOBJECTS WHERE xtype = 'U';""")]:
            with self.db:
                self.engine.execute("""
        
                        CREATE TABLE weekly_performance
                                (
                                candidate_id INT,
                                course_id INT,
                                week_no INT,
                                analytic INT ,
                                independent INT,
                                determined INT,
                                professional INT,
                                studious INT,
                                imaginative INT,
                                FOREIGN KEY(candidate_id) REFERENCES candidate(candidate_id),
                                FOREIGN KEY(course_id) REFERENCES course(course_id)

                                )
                        """)
                logger.info('CREATING WEEKLY_PERFORMANCE SQL TABLE')
      
    
    def data_entry(self):
        with self.engine.connect() as connection:
            self.pysqldf("""
                    SELECT
                        name AS candidate_id,
                        course_name AS course_id,
                        week_no,
                        analytic,
                        independent,
                        determined,
                        professional,
                        studious,
                        imaginative

                    from wk_performances_df
            """).to_sql('weekly_performance',connection,index = False,if_exists= 'append')
            logger.info('\nLOADING TO WEEEKLY_PERFORMANCES SQL TABLE\n')




    def update_performance_df(self):
        df = weekly_performances_df
        for row in self.engine.execute("SELECT course_name,course_id FROM course"):
            df['course_name'].replace({row[0]:str(row[1])},inplace=True)

        for row in self.engine.execute("SELECT candidate_name,candidate_id,staff_id FROM candidate"):
            df['name'].replace({row[0]:str(row[1])},inplace=True)
        
        df = df.dropna(thresh=6)
        df = df.replace({NaN:None})
        
        # df = df.rename(columns=({'name':'candidate_id','course_name':'course_id'}))
        # df = df[['candidate_id','course_id','week_no','analytic','independent',
        #         'determined','professional','studious','imaginative']]

        return df


    def sample_query(self):
        logger.info('WEEKLY_PERFOMANCE_TABLE \n')
        data = self.engine.execute("SELECT * FROM weekly_performance LIMIT 10")
        for row in data:
            logger.info(row)
        return data

    def create_weekly_performance_table(self):
        
        self.create_table()
        self.data_entry()
        # self.sample_query()


weekly_performance_sql_tbl = weeklyPerformanceTable()
wk_performances_df = weekly_performance_sql_tbl.update_performance_df()

def run_ETL_process():

    weekly_performance_sql_tbl.create_weekly_performance_table()
    # weekly_performance_sql_tbl.engine.close()
    weekly_performance_sql_tbl.db.close()

    pass

