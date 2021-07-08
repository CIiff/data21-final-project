from optimising_sql_server2.app.db_creation.candidate import *
from optimising_sql_server2.app.db_creation.course import *
# from optimising_sql_server2.app.db_creation.connection import CreateDB




# creation of course staff junction table
class CourseStaffJunc(CreateDB):

    def __init__(self):
        super().__init__()        # SubClass initialization code
        candidate_sql_tbl.create_candidate_table()
        course_sql_tbl.create_course_table()
        
    
    @staticmethod
    def pysqldf(q):
        return sqldf(q,globals())
 

    def create_table(self):
        with self.db:
            if 'course_staff_junc' not in [table[0] for table in self.engine.execute("""SELECT *FROM SYSOBJECTS WHERE xtype = 'U';""")]:
                self.engine.execute("""

                        CREATE TABLE course_staff_junc
                                (
                                course_id INT,
                                staff_id INT,
                                FOREIGN KEY(course_id) REFERENCES course(course_id),
                                FOREIGN KEY(staff_id) REFERENCES staff(staff_id)
                                )
                                
                                """)

    def data_entry(self):
        with self.engine.connect() as connection:

            self.pysqldf("""
                    SELECT
                        course_id,
                        staff_id
                    from course_trainer_junct_df
            """).to_sql('course_staff_junc',connection,index = False,if_exists= 'append')
            logger.info('\nLOADING TO COURSE_STAFF_JUNCTION SQL TABLE\n')

    
    
    def update_course_staff_df(self):
        if weekly_performances_df.empty == False:
            course_trainer_junct_df = weekly_performances_df[['course_name','trainer']]
            course_trainer_junct_df = course_trainer_junct_df.drop_duplicates(subset = ["course_name"])

            for row in tqdm(self.engine.execute("SELECT staff_name,staff_id,department FROM staff"),unit ='trainers',desc = 'Adding_Trainers_to_Staff',position = 0):
                course_trainer_junct_df['trainer'].replace({row[0]:str(row[1])},inplace=True)

            for row in tqdm(self.engine.execute("SELECT course_name,course_id FROM course "),unit ='course_id',desc = 'Updating_course_id',position = 0):
                course_trainer_junct_df['course_name'].replace({row[0]:str(row[1])},inplace=True)

            course_trainer_junct_df = course_trainer_junct_df.rename(columns={'course_name':'course_id','trainer':'staff_id'})
            
            return course_trainer_junct_df
        else:
            return pd.DataFrame()


    def sample_query(self):
        logger.info('COURSE _STAFF_JUNCTION TABLE \n')
        data = self.engine.execute("SELECT * FROM course_staff_junc LIMIT 10")
        for row in data:
            logger.debug(row)
        return data

    def create_course_staff_junc_table(self):
        
        # self.update_course_staff_df()
        self.create_table()
        if course_trainer_junct_df.empty == False:
            self.data_entry()
        else:
            logger.info('No data to load into course_staff_junc')
        # self.sample_query()




course_staff_junct = CourseStaffJunc()
course_trainer_junct_df = course_staff_junct.update_course_staff_df()

# course_staff_junct.create_course_staff_junc_table()


