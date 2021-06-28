from optimising.app.db_creation.logger import logger
from optimising.app.tranform_files.transform_academy_csv import courses_df
from optimising.app.db_creation.connection import CreateDB
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
            self.c.executescript("""
                    DROP TABLE IF EXISTS course_type;
                    CREATE TABLE IF NOT EXISTS course_type
                            (
                            course_type_id INTEGER NOT NULL PRIMARY KEY,
                            type TEXT
                            );
                    
                    """)
      
    def data_entry(self):
        
        self.pysqldf("""
                SELECT
                    course_type as type
                from courses_type_df
        """).to_sql('course_type',con=self.db,index=False,if_exists='append')

    def sample_query(self):
        logger.info('COURSE_TYPE_TABLE \n')
        data = course_type_sql_tbl.c.execute("SELECT type,course_type_id FROM course_type LIMIT 10")
        for row in data:
            logger.debug(row)
        return data

    def create_course_type_table(self):

        self.create_table()
        self.data_entry()
        logger.info('\nLOADING TO COURSE_TYPE SQL TABLE\n')
        self.db.commit()
        # self.sample_query()




courses_type_df = courses_df.drop_duplicates(subset = ["course_type"])

course_type_sql_tbl = courseTypeTable()



