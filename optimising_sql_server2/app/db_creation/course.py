from optimising_sql_server.app.db_creation.course_type import *




class courseTable(CreateDB):

    def __init__(self):
        super().__init__()        # SubClass initialization code
        course_type_sql_tbl.create_course_type_table()


    @staticmethod
    def pysqldf(q):
        return sqldf(q,globals())


    def create_table(self):
        with self.db:
            self.c.execute("""
                    DROP TABLE IF EXISTS course;
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
        
        sql_insert = """
                INSERT INTO course(
                    course_name,
                    course_type_id,
                    duration,
                    start_date
                    )
                VALUES(
                    ?,?,?,?
                )"""

        self.c.executemany(sql_insert,sql_courses_df.values.tolist())
        
               
    def update_course_table(self):
        df = courses_df
        for row in self.c.execute("SELECT type,course_type_id FROM course_type "):
            df['course_type'].replace({row[0]:row[1]},inplace=True)

        df = df.rename(columns=({'course_type':'course_type_id'}))
        print(df.head())
        return df
     

    def sample_query(self):
        logger.info('COURSE TABLE \n')
        data = self.c.execute("SELECT course_name,course_id FROM course LIMIT 10 ")
        for row in data:
            logger.debug(row)
        return data
        

    def create_course_table(self):

        self.create_table()
        self.db.commit()
        self.data_entry()
        logger.info('\nLOADING TO COURSE SQL TABLE\n')
        self.db.commit()
        # self.sample_query()



course_sql_tbl = courseTable()
sql_courses_df = course_sql_tbl.update_course_table()
# course_sql_tbl.create_course_table()
# course_sql_tbl.db.close()


