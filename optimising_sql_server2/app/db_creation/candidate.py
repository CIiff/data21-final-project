from optimising_sql_server2.app.db_creation.staff import *
from optimising_sql_server2.app.tranform_files.transform_applicants_json import json_df_dict
from tqdm import  tqdm


class candidateTable(CreateDB):

  
    

    def __init__(self):
        super().__init__()        # SubClass initialization code
        staff_sql_tbl.create_staff_table()
        self.weekly_performances_df = weekly_performances_df
        self.candidate_df = candidate_df
        # print(candidate_df.head())

    def create_table(self):
        with self.db:
            if 'candidate' not in [table[0] for table in self.engine.execute("""SELECT *FROM SYSOBJECTS WHERE xtype = 'U';""")]:
                self.engine.execute("""
                        
                        CREATE TABLE candidate(
                                candidate_id int IDENTITY(1,1) PRIMARY KEY,
                                candidate_name VARCHAR(100),
                                gender VARCHAR(10),
                                dob DATE,
                                email VARCHAR(MAX),
                                city VARCHAR(MAX),
                                address VARCHAR(MAX),
                                postcode VARCHAR(10),
                                phone_number VARCHAR(15),
                                uni_name VARCHAR(MAX),
                                degree_result VARCHAR(10),
                                staff_id INT
                                )
                        
                        """)
                logger.info('CREATING CANDIDATE SQL TABLE')

            
    @staticmethod
    def pysqldf(q):
        return sqldf(q,globals())


    def data_entry(self):
        with self.engine.connect() as connection:
   
            self.pysqldf("""
                    SELECT
                        candidate_name,
                        gender, 
                        dob, 
                        email, 
                        city, 
                        address, 
                        postcode, 
                        phone_number, 
                        uni_name, 
                        degree_result, 
                        staff_id
                    from sql_candidate_df
            """).to_sql('candidate',connection,index = False,if_exists= 'append')
            logger.info('LOADING TO CANDIDATE SQL TABLE')

    

    def sample_query(self):
        logger.info('CANDIDATE TABLE \n')
        data = self.db.execute("SELECT candidate_name,candidate_id,staff_id FROM candidate LIMIT 10")
        for row in data:
            logger.debug(row)
        return data
        

    def add_new_names(self):
        candidate_list = set(self.candidate_df['candidate_name'].tolist())
        spartan_list = set(self.weekly_performances_df['name'].tolist())
        spartan_list2 =set(json_df_dict['sparta_day_df']['candidate_name'].tolist())

        a = spartan_list.difference(candidate_list)
        b = spartan_list2.difference(candidate_list)
        new_students = set(list(a)+list(b))

        logger.warning(f'New names from .json and academy_csv added to condidate_df\n{new_students}\n')
        for name in tqdm(new_students,unit ='name',desc = 'Adding_new_candidates',position = 0):
            self.candidate_df = self.candidate_df.append({'candidate_name':name},ignore_index=True)

        return self.candidate_df


    def update_candidate_df(self):

        df = self.add_new_names()

        df = df.applymap(str)
        df['staff_id'] = df['staff_name']
        for row in tqdm(self.db.execute("SELECT staff_name,staff_id,department FROM staff"),unit ='staff_id',desc = 'Updating_Staff_IDs',position = 0):
            df['staff_id'].replace({row[0]:str(row[1])},inplace=True)

        df = df.replace({'None':None})
        df = df.replace({'nan':None})
        df = df.drop(['staff_name', 'date'], axis = 1)

        print(f'\n{df.head(3)}')
        return df


    

    def create_candidate_table(self):

        self.create_table()
        self.data_entry()
        # self.sample_query()



candidate_sql_tbl = candidateTable()
sql_candidate_df = candidate_sql_tbl.update_candidate_df()
# candidate_sql_tbl.create_candidate_table()


