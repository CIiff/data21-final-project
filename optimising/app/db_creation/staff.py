from optimising.app.tranform_files.transform_academy_csv import weekly_performances_df
from optimising.app.tranform_files.transform_applicants_csv import candidate_df
from optimising.app.db_creation.connection import CreateDB
from optimising.app.db_creation.logger import logger
import pandas as pd
from pandasql import sqldf





class staffTable(CreateDB):
    

    def __init__(self,database):
        super().__init__(database)        # SubClass initialization code


    @staticmethod
    def pysqldf(q):
        return sqldf(q,globals())

    def create_table(self):
        with self.db:
            self.c.executescript("""
                    DROP TABLE IF EXISTS staff;
                    CREATE TABLE IF NOT EXISTS staff
                            (
                            staff_id INTEGER NOT NULL PRIMARY KEY,
                            staff_name TEXT,
                            department TEXT
                            );
                    
                    """)
   
    def recruiters_data_entry(self):
        
        self.pysqldf("""
                SELECT
                    staff_name,
                    department
                from recruiting_staff_df
        """).to_sql('staff',con=self.db,index=False,if_exists='append')
    
    def trainer_data_entry(self):

        self.pysqldf("""
                SELECT
                    trainer AS staff_name,
                    department
                from trainer_staff_df 
        """).to_sql('staff',con=self.db,index=False,if_exists='append')



    def sample_query(self):
        logger.info('STAFF TABLE \n')
        data = staff_sql_tbl.c.execute("SELECT staff_name,staff_id,department FROM staff LIMIT 10")
        for row in data:
            logger.debug(row)
        return data

    def create_staff_table(self):

        self.create_table()
        self.recruiters_data_entry()
        self.trainer_data_entry()
        self.db.commit()
        self.sample_query()




candidate_df = candidate_df.applymap(str)

# set up the talent talent staff members dataframe
staff_df = candidate_df.drop_duplicates(subset = ["staff_name"])
staff_df = staff_df[staff_df['staff_name'] != 'nan']
recruiting_staff_df = staff_df.assign(department= 'Talent')

#set up the trainers dataframe
trainer_df = weekly_performances_df.drop_duplicates(subset = ['trainer'])
trainer_df = trainer_df[trainer_df['trainer'] != 'nan']
trainer_staff_df = trainer_df.assign(department = 'Trainer')


# create a class instance - hence create a sql table and insert values
staff_sql_tbl = staffTable('memory')
# staff_sql_tbl.create_staff_table()




