from optimising.app.db_creation.course_type import *




class courseTable(CreateDB):

    def __init__(self,database):
        super().__init__(database)        # SubClass initialization code
        course_type_sql_tbl.create_course_type_table()


    @staticmethod
    def pysqldf(q):
        return sqldf(q,globals())


    def create_table(self):
        with self.db:
            self.c.executescript("""
                    DROP TABLE IF EXISTS course;
                    CREATE TABLE IF NOT EXISTS course
                            (
                            course_id INTEGER NOT NULL PRIMARY KEY,
                            course_type_id INT,
                            course_name TEXT,
                            start_date TEXT,
                            duration,
                            FOREIGN KEY(course_type_id) REFERENCES course_type(course_type_id)

                            );
                    
                    """)
      
    def data_entry(self):
        
        self.pysqldf("""
                SELECT
                    course_name,
                    course_type AS course_type_id,
                    start_date,
                    duration

                from sql_courses_df
        """).to_sql('course',con=self.db,index=False,if_exists='append')



    def update_course_table(self):
        df = courses_df
        for row in course_type_sql_tbl.c.execute("SELECT type,course_type_id FROM course_type "):
            df['course_type'].replace({row[0]:row[1]},inplace=True)

        return df
     

    def sample_query(self):
        logger.info('COURSE TABLE \n')
        data = course_sql_tbl.c.execute("SELECT course_name,course_id FROM course LIMIT 10 ")
        for row in data:
            logger.debug(row)
        return data
        

    def create_course_table(self):

        self.create_table()
        self.data_entry()
        self.db.commit()
        self.sample_query()



course_sql_tbl = courseTable('memory')
sql_courses_df = course_sql_tbl.update_course_table()
# course_sql_tbl.db.close()


