from numpy import NaN
from optimising_sql_server.app.tranform_files.transform_academy_csv import weekly_performances_df
from optimising_sql_server.app.tranform_files.transform_applicants_csv import candidate_df
from optimising_sql_server.app.db_creation.connection import CreateDB
from optimising_sql_server.app.db_creation.logger import logger
import pandas as pd
from pandasql import sqldf





class staffTable(CreateDB):
    

    def __init__(self):
        super().__init__()        # SubClass initialization code


    @staticmethod
    def pysqldf(q):
        return sqldf(q,globals())

    def create_table(self):
        with self.db:
            self.c.execute("""
                    DROP TABLE IF EXISTS staff;
                    CREATE TABLE staff(
                        staff_id int IDENTITY(1,1) PRIMARY KEY,
                        staff_name VARCHAR(MAX),
                        department VARCHAR(130)
                        )
                    """)
   
    def recruiters_data_entry(self):
        
        sql_insert = """
                INSERT INTO staff(
                    staff_name,
                    department
                    )
                VALUES(
                    ?,?
                )"""
        self.c.executemany(sql_insert,recruiting_staff_df.values.tolist())
        # .to_sql('staff',con=self.db,index=False,if_exists='replace')
    
    def trainer_data_entry(self):

        sql_insert = """
                INSERT INTO staff(
                    staff_name,
                    department
                    )
                VALUES(
                    ?,?
                    )"""
        self.c.executemany(sql_insert,trainer_staff_df.values.tolist())
    #    .to_sql('staff',con=self.db,index=False,if_exists='append')



    def sample_query(self):
        logger.info('STAFF TABLE \n')
        data = self.c.execute("SELECT staff_name,staff_id,department FROM staff LIMIT 10")
        for row in data:
            logger.debug(row)
        return data

    def create_staff_table(self):

        self.create_table()
        self.db.commit()
        self.recruiters_data_entry()
        self.db.commit()
        self.trainer_data_entry()
        logger.info('\nLOADING TO STAFF SQL TABLE\n')
        self.db.commit()
        # self.sample_query()




candidate_df = candidate_df.applymap(str)

# set up the talent talent staff members dataframe
staff_df = candidate_df.drop_duplicates(subset = ["staff_name"])
staff_df = staff_df[staff_df['staff_name'] != 'nan']
staff_df = staff_df[staff_df['staff_name'] != 'None']
staff_df = staff_df[staff_df['staff_name'] != NaN]
recruiting_staff_df = staff_df.assign(department= 'Talent')
recruiting_staff_df = recruiting_staff_df[['staff_name','department']]
recruiting_staff_df = recruiting_staff_df.dropna()

print(recruiting_staff_df.values.tolist())
#set up the trainers dataframe
trainer_df = weekly_performances_df.drop_duplicates(subset = ['trainer'])
trainer_df = trainer_df[trainer_df['trainer'] != 'nan']
trainer_df = trainer_df[trainer_df['trainer'] != None]
trainer_staff_df = trainer_df.assign(department = 'Trainer')
trainer_staff_df = trainer_staff_df.rename(columns={'trainer':'staff_name'})
trainer_staff_df = trainer_staff_df[['staff_name','department']]




# create a class instance - hence create a sql table and insert values
staff_sql_tbl = staffTable()
# staff_sql_tbl.create_staff_table()




