from optimising_sql_server2.app.db_creation.logger import logger
from optimising_sql_server2.app.tranform_files.transform_academy_csv import courses_df
from optimising_sql_server2.app.db_creation.connection import CreateDB
from pandasql import sqldf



class courseTypeTable(CreateDB):
    
    def __init__(self):
        super().__init__()        # SubClass initialization code
        self.courses_df = courses_df


    @staticmethod
    def pysqldf(q):
        return sqldf(q,globals())

    def create_table(self):
        with self.db:
            if 'course_type' not in [table[0] for table in self.engine.execute("""SELECT *FROM SYSOBJECTS WHERE xtype = 'U';""")]:
                self.engine.execute("""
                        DROP TABLE IF EXISTS course_type;
                        CREATE TABLE course_type
                                (
                                course_type_id int IDENTITY(1,1) PRIMARY KEY,
                                type VARCHAR(20)
                                )
                        
                        """)
      
    def data_entry(self):
        with self.engine.connect() as connection:
            self.pysqldf("""
                    SELECT
                         type
                    from courses_type_df
            """).to_sql('course_type',connection,index = False,if_exists= 'append')

            logger.info('\nLOADING TO COURSE_TYPE SQL TABLE\n')




    def sample_query(self):
        logger.info('COURSE_TYPE_TABLE \n')
        data = self.engine.execute("SELECT type,course_type_id FROM course_type LIMIT 10")
        for row in data:
            logger.debug(row)
        return data

    def create_course_type_table(self):

        self.create_table()
        self.data_entry()
        # self.sample_query()




courses_type_df = courses_df.drop_duplicates(subset = ["course_type"])
courses_type_df = courses_type_df.rename(columns=({"course_type":"type"}))
courses_type_df = courses_type_df["type"]
# courses_type_df = [[course] for course in courses_type_df.values.tolist()]


course_type_sql_tbl = courseTypeTable()



