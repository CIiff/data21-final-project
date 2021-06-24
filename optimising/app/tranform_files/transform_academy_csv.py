from optimising.app.load_files.get_files_from_s3 import getFiles,logger
from optimising.app.db_creation.logger import logger
import pandas as pd
from fuzzywuzzy import process



class transformAcedamyCSV:


   

    def __init__(self):
        
        self.academy_csvs = getFiles('data21-final-project','Academy','.csv')
        self.academy_csvs_list = self.academy_csvs.get_list_of_files()
        self.academy_csv_dfs_dict = self.academy_csvs.create_dict_of_csv_dataframes()
        self.weekly_performance_df = pd.DataFrame()
        self.course_df = pd.DataFrame()
  
        
        pass


    def create_weekly_performance_df(self):
        for key in self.academy_csv_dfs_dict:
            split_var = key.split('_')
            df = self.academy_csv_dfs_dict[key]
            duration = (len(df.columns)-2)//6
            df = df.set_index('name')
    
            for name in list(df.index.unique()):
                # spartan_temp_df = pd.DataFrame()
                for wk in range(1,duration+1):
                    week_perfomance = {
                    'name':name,
                    'week_no': int(wk),
                    'analytic':df.loc[name,f'Analytic_W{wk}'],
                    'independent':df.loc[name,f'Independent_W{wk}'],
                    'determined':df.loc[name,f'Determined_W{wk}'],
                    'professional':df.loc[name,f'Professional_W{wk}'],
                    'studious':df.loc[name,f'Studious_W{wk}'],
                    'imaginative':df.loc[name,f'Imaginative_W{wk}'],
                    'course_name':f'{split_var[0]} {split_var[1]}',
                    'trainer':df.loc[name,'trainer']
                    }
                    self.weekly_performance_df = self.weekly_performance_df.append(week_perfomance,ignore_index=True)
        
        return self.weekly_performance_df

    def wk_performance_df(self):
        
        df = self.create_weekly_performance_df()
        df['trainer'] = df['trainer'].str.replace('Ely Kely','Elly Kelly',regex=True)
        df['name'] = df['name'].str.replace(' - ','-',regex=True)
        df['name'] = df['name'].str.replace("' ","'",regex=True)
        df['name'] = df['name'].str.replace("O' ","O'",regex=True)
        df['name'] = df['name'].str.replace("O''","O'",regex=True)
        df['name'] = df['name'].str.lower()
        df['name'] = df['name'].str.title()

        def get_matches(name, column,limit = 500):
            results = process.extract(name,column, limit=limit)
            for result in results:
                if result[1] < 100  and result[1] > 80:
                    return result
                    
        for trainer_name in list(df['trainer'].unique()):
                
                match = get_matches(trainer_name,df['trainer'])
                if match != None:
                    logger.warning(f'{trainer_name} is {match[1]}% similar to {match[0]}')

        return df


    def course_table_df(self):
        for key in self.academy_csv_dfs_dict:
            split_var = key.split('_')
            temp_course_df = self.academy_csv_dfs_dict[key]
            course_details = {
                'course_name':f'{split_var[0]} {split_var[1]}',
                'course_type':f'{split_var[0]}',
                'start_date': split_var[2],
                'duration': (len(temp_course_df.columns)-2)//6
            }
            self.course_df = self.course_df.append(course_details,ignore_index=True)
            
        
        return self.course_df.replace({pd.NaT: None})


    

spartan_performance_df = transformAcedamyCSV()
courses_df = spartan_performance_df.course_table_df()
weekly_performances_df = spartan_performance_df.wk_performance_df()



# PRINTING RESULTS
# with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # so that console doesn't truncate dataframe results -->> print all 0-n rows! 
#    pd.set_option("expand_frame_repr",True)

# pprint(weekly_performances_df.head())