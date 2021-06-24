from optimising.app.db_creation.candidate import *
from optimising.app.db_creation.course import *




# creation of course staff junction table
class CourseStaffJunc(CreateDB):

    def __init__(self,database):
        super().__init__(database)        # SubClass initialization code
        
        
        candidate_sql_tbl.create_candidate_table()
        course_sql_tbl.create_course_table()
        
    
    @staticmethod
    def pysqldf(q):
        return sqldf(q,globals())
 

    def create_table(self):
        with self.db:
            self.c.executescript("""
                    DROP TABLE IF EXISTS course_staff_junc;
                    CREATE TABLE IF NOT EXISTS course_staff_junc
                            (
                            course_id INTEGER,
                            staff_id INTEGER,
                            FOREIGN KEY(course_id) REFERENCES courses(course_id)
                            ON DELETE SET NULL,
                            FOREIGN KEY(staff_id) REFERENCES staff(staff_id)
                            ON DELETE SET NULL );
                            
                            """)

    def data_entry(self):
        
        self.pysqldf("""
                SELECT
                    course_name AS course_id,
                    trainer AS staff_id
                from course_trainer_junct_df
        """).to_sql('course_staff_junc',con=self.db,index=False,if_exists='append')
    
    
    def update_course_staff_df(self):

        course_trainer_junct_df = weekly_performances_df[['course_name','trainer']]
        course_trainer_junct_df = course_trainer_junct_df.drop_duplicates(subset = ["course_name"])

        for row in staff_sql_tbl.c.execute("SELECT staff_name,staff_id,department FROM staff"):
            course_trainer_junct_df['trainer'].replace({row[0]:str(row[1])},inplace=True)

        for row in course_sql_tbl.c.execute("SELECT course_name,course_id FROM course "):
            course_trainer_junct_df['course_name'].replace({row[0]:row[1]},inplace=True)

        return course_trainer_junct_df


    def sample_query(self):
        logger.info('COURSE _STAFF_JUNCTION TABLE \n')
        data = course_staff_junct.c.execute("SELECT * FROM course_staff_junc LIMIT 10")
        for row in data:
            logger.debug(row)
        return data

    def create_course_staff_junc_table(self):

        self.update_course_staff_df()
        self.create_table()
        self.data_entry()
        self.db.commit()
        self.sample_query()




course_staff_junct = CourseStaffJunc('memory')
course_trainer_junct_df = course_staff_junct.update_course_staff_df()


