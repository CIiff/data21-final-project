from optimising_sql_server2.app.db_creation.course_type import *
import pandas as pd
# from optimising_sql_server2.app.db_creation.connection import CreateDB




class courseTable(CreateDB):

    def __init__(self):
        super().__init__()        # SubClass initialization code
        course_type_sql_tbl.create_course_type_table()


    @staticmethod
    def pysqldf(q):
        return sqldf(q,globals())


    def create_table(self):
        with self.db:
            if 'course' not in [table[0] for table in self.engine.execute("""SELECT *FROM SYSOBJECTS WHERE xtype = 'U';""")]:
                self.engine.execute("""
                        CREATE TABLE course
                                (
                                course_id int IDENTITY(1,1)PRIMARY KEY,
                                course_type_id INT,
                                course_name VARCHAR(50),
                                duration INT,
                                start_date DATE,
                                FOREIGN KEY(course_type_id) REFERENCES course_type(course_type_id)

                                )
                        
                        """)
      
    def data_entry(self):
        with self.engine.connect() as connection:
            self.pysqldf("""
                    SELECT
                        course_name,
                        course_type_id,
                        start_date,
                        duration

                    from sql_courses_df
            """).to_sql('course',connection,index = False,if_exists= 'append')

            logger.info('\nLOADING TO COURSE SQL TABLE\n')

        
               
    def update_course_table(self):
        if courses_df.empty == False:
            df = courses_df
            for row in self.engine.execute("SELECT type,course_type_id FROM course_type "):
                df['course_type'].replace({row[0]:str(row[1])},inplace=True)

            df = df.rename(columns=({'course_type':'course_type_id'}))
            return df
        else:
            return pd.DataFrame()
     

    def sample_query(self):
        logger.info('COURSE TABLE \n')
        data = self.engine.execute("SELECT course_name,course_id FROM course LIMIT 10 ")
        for row in data:
            logger.debug(row)
        return data
        

    def create_course_table(self):
        try:
            self.create_table()
            if sql_courses_df.empty == False:
                self.data_entry()
            else:
                logger.info('No course data to load')
            # self.sample_query()
        except NameError:
            pass



course_sql_tbl = courseTable()
sql_courses_df = course_sql_tbl.update_course_table()
# course_sql_tbl.create_course_table()
# course_sql_tbl.db.close()


