from optimising_sql_server2.app.load_files.get_files_from_s3 import getFiles,logger
from tqdm import tqdm,trange
from fuzzywuzzy import process
import pandas as pd



class transformAppCSV:


      def __init__(self):


         self.talent_csvs = getFiles('data21-final-project-preassignment','Talent','.csv')
         self.talent_csvs_list = self.talent_csvs.get_list_of_files()
         self.talent_csvs.download_csv_in_chucks()
         self.talent_csv_df_dict =  self.talent_csvs.csv_dict_keyed_by_course
         self.combined_applicants_df = pd.DataFrame()
         self.weekly_performance_df = pd.DataFrame()
         self.course_df = pd.DataFrame()
         
      def transform_dfs(self):
         if self.talent_csvs_list != []:
            for key in tqdm(self.talent_csv_df_dict.keys(),unit ='Applicants_csv_files',desc = 'Tranforming Applicants_csv_files',position = 0):
               
               # logger.debug(f'Transforming the {key} dataframe')

            
               # cleaning the phone_number
               self.talent_csv_df_dict[key]['phone_number'] = self.talent_csv_df_dict[key]['phone_number'].str.replace(r'[^+\w]','',regex=True)
               
               # logger.info on incorrectly formated phone numbers
               for row in self.talent_csv_df_dict[key].itertuples():
                  if type(row.phone_number) != float:
                     if row.phone_number[1:13].isdigit() == False or row.phone_number[0] != '+':
                        logger.debug(f"Wrongly formated phone_number column in {key}")
               #----------------------------------------------------------------------------------------------------------------------------------------------
               # cleaning the name
               self.talent_csv_df_dict[key][['name']] = self.talent_csv_df_dict[key]['name'].str.title()
               self.talent_csv_df_dict[key]['name'] = self.talent_csv_df_dict[key]['name'].str.replace(' - ','-',regex=True)
               self.talent_csv_df_dict[key]['name'] = self.talent_csv_df_dict[key]['name'].str.replace("' ",'',regex=True)
               
               
               #----------------------------------------------------------------------------------------------------------------------------------------------
               # cleaning the staff_names
               self.talent_csv_df_dict[key][['invited_by']] = self.talent_csv_df_dict[key]['invited_by'].str.title()
               self.talent_csv_df_dict[key]['invited_by'] = self.talent_csv_df_dict[key]['invited_by'].str.replace('Bruno Belbrook','Bruno Bellbrook',regex=True)
               self.talent_csv_df_dict[key]['invited_by'] = self.talent_csv_df_dict[key]['invited_by'].str.replace('Fifi Etton','Fifi Eton',regex=True)
               
               
               #----------------------------------------------------------------------------------------------------------------------------------------------
               # making gender title case to keep them constant 
               self.talent_csv_df_dict[key][['gender']] = self.talent_csv_df_dict[key]['gender'].str.title()
         
               # making citytitle case to keep them constant 
               self.talent_csv_df_dict[key][['city']] = self.talent_csv_df_dict[key]['city'].str.title()
               #----------------------------------------------------------------------------------------------------------------------------------------------
               # cleaning the university
               
               self.talent_csv_df_dict[key]['uni'].replace({'-':','}, inplace=True)
               self.talent_csv_df_dict[key]['uni'].replace({' ,':','},regex=True, inplace=True)
               self.talent_csv_df_dict[key]['uni'].replace({'´':"'"},regex=True, inplace=True)
               self.talent_csv_df_dict[key]['uni'] = self.talent_csv_df_dict[key]['uni'].str.lower()
               self.talent_csv_df_dict[key]['uni'] = self.talent_csv_df_dict[key]['uni'].str.title()
               self.talent_csv_df_dict[key]['uni'].replace({"'S":"'s"},regex=True, inplace=True)
               self.talent_csv_df_dict[key]['uni'].replace({"St\.":"St "},regex=True, inplace=True)
               self.talent_csv_df_dict[key]['uni'].replace({"Saint George's":"St George's"},regex=True, inplace=True)

               # cleaning the email 
               self.talent_csv_df_dict[key]['email'] = self.talent_csv_df_dict[key]['email'].str.replace(';','',regex=True)
               #----------------------------------------------------------------------------------------------------------------------------------------------
               # cleaning the invited_date 
               self.talent_csv_df_dict[key]['invited_date'] = self.talent_csv_df_dict[key]['invited_date'].astype(str).str.replace('.0','',regex=True)
               self.talent_csv_df_dict[key]['month_short'] = self.talent_csv_df_dict[key]['month'].str.slice(0, 3)
               self.talent_csv_df_dict[key]['invite_year'] = self.talent_csv_df_dict[key]['month'].str.extract(r'\b(\w+)$', expand=True)
                  # combining the invited date 
               self.talent_csv_df_dict[key]['invite_date'] = (self.talent_csv_df_dict[key]['invite_year']+' '+\
                                                         self.talent_csv_df_dict[key]['month_short']+' '+\
                                                         self.talent_csv_df_dict[key]['invited_date']).astype('datetime64')
               self.talent_csv_df_dict[key]['invite_date'] = self.talent_csv_df_dict[key]['invite_date'].astype('object')
               
               #----------------------------------------------------------------------------------------------------------------------------------------------
               # cleaning the date of birth column
               self.talent_csv_df_dict[key]['dob'] = pd.to_datetime(self.talent_csv_df_dict[key]['dob'], format='%d/%m/%Y').dt.date
            
               # dropping unwanted columns to keep only the data we want 
               self.talent_csv_df_dict[key].drop(columns=['id','invited_date','month_short','invite_year','month'],axis=1, inplace=True)


               #renaming columns to align with ERD
               self.talent_csv_df_dict[key].rename(columns={'name': 'candidate_name',
                           'uni': 'uni_name',
                           'degree': 'degree_result',
                           'invite_date': 'date',
                           'invited_by': 'staff_name'},inplace=True)


               # logging duplicates based on name, dob and address
               if self.talent_csv_df_dict[key][self.talent_csv_df_dict[key].duplicated(subset=['candidate_name','dob','address','date'])].empty == False:
                  logger.warning(f" Duplicated rows: {self.talent_csv_df_dict[key][self.talent_csv_df_dict[key].duplicated(subset=['candidate_name','dob','address','date'])]}")  

            

               # logging any dataframes with incorectly named columns 
               erd_cols = ['candidate_name', 'gender', 'dob', 'email', 'city', 'address', 'postcode', 'phone_number', 'uni_name', 'degree_result', 'staff_name', 'date']
               if erd_cols.sort() != (list(self.talent_csv_df_dict[key].columns)).sort():
                  logger.debug(f'{key} df has incorrect columns: {list(self.talent_csv_df_dict[key].columns)}')
               
               #loggin any rows with no candidate contact information 
               cols = ['candidate_name','phone_number','email']
               if self.talent_csv_df_dict[key][self.talent_csv_df_dict[key][cols].isna().all(1)].empty == False:
                  logger.warning(f'Rows with no condidate contact info {self.talent_csv_df_dict[key][self.talent_csv_df_dict[key][cols].isna().all(1)]}')
               

               # self.combined_applicants_df = self.combined_applicants_df.append(self.talent_csv_df_dict[key])
               self.combined_applicants_df =  pd.concat([self.combined_applicants_df, self.talent_csv_df_dict[key]], ignore_index=True)
               self.combined_applicants_df = self.combined_applicants_df.replace({pd.NaT: None})
               # logging any staff names that are similar to check for mispellings
               def get_matches(name, column,limit = 500):

                  results = process.extract(name,column, limit=limit)
                  for result in results:
                     if result[1] < 100  and result[1] > 90:
                        return result

               for name in self.talent_csv_df_dict[key]['staff_name'].unique():
                  match = get_matches(str(name),self.talent_csv_df_dict[key]['staff_name'])
                  if match != None:
                     logger.warning(f'{name} is {match[1]}% similar to {match[0]} ')

               
            logger.debug(f'Finished Dataframe Transformation')
            return self.combined_applicants_df

         else:
            logger.info('No new Applicant csv files found')
            return pd.DataFrame()


      

candidate_df =  transformAppCSV()
candidate_df = candidate_df.transform_dfs()


# getting #fails here

# needs to make the code do nothing if it finds no files