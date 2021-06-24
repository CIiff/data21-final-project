from optimising.app.load_files.get_files_from_s3 import getFiles,logger
from pprint import pprint
import pandas as pd
import json


class transformJsonFiles():

    sparta_day_df = pd.DataFrame()



    def __init__(self):
        self.json_objects = getFiles('data21-final-project','Talent','.json')
        self.json_files = self.json_objects.get_list_of_files()
        self.json_files_dict = self.json_objects.get_dict_of_json_files()
        self.sparta_day_df2 = pd.DataFrame()
        self.weaknesses_junc_df =pd.DataFrame()
        self.strengths_junc_df =pd.DataFrame()
        self.tech_junc_df =pd.DataFrame()
        
        pass


    def make_dataframes(self):

      
        for json_id in self.json_files_dict:
            logger.info(f'Tranforming {json_id}.json file')
            
            applicant = json.loads(self.json_files_dict[json_id])
           
            applicant['name'] = applicant['name'].title()
            applicant['name'] = applicant['name'].replace(" - ","-")
            applicant['name'] = applicant['name'].replace("' ","'")
            # create sparta_day_df2 (candidate_name,date,result,self_development,financial_support,geo_flex,course_interest)
            df_inserts ={

                'candidate_name':applicant['name'],
                'date':applicant['date'],
                'result':applicant['result'],
                'self_development':applicant['self_development'],
                'financial_support':applicant['financial_support_self'],
                'geo_flex':applicant['geo_flex'],
                'course_interest':applicant['course_interest']
            }
            self.sparta_day_df2 =  self.sparta_day_df2.append(df_inserts,ignore_index=True)
            self.sparta_day_df2['date'] = self.sparta_day_df2['date'].astype('datetime64')
            self.sparta_day_df2['date'] = self.sparta_day_df2['date'].astype(str)
            # self.sparta_day_df2['date'] = pd.to_datetime(self.sparta_day_df2['date'],format='%d%m%Y')

            # create weakness_junc ( candidate_name,weakness) -->> (candidate_id,weekness_id)
            for weakness in applicant['weaknesses']:
                df_inserts ={

                    'candidate_name':applicant['name'],
                    'weaknesses':weakness
                }
                self.weaknesses_junc_df = self.weaknesses_junc_df.append(df_inserts,ignore_index=True)
    

            # create strengths_junc (candidate_name,strength) -->> (candidate_id,strength_id)
            for strength in applicant['strengths']:
                df_inserts ={

                    'candidate_name':applicant['name'],
                    'strengths':strength
                }
                self.strengths_junc_df = self.strengths_junc_df.append(df_inserts,ignore_index=True)



            # create tech_junction_df =   (cadidate_name,tech_name,tech_score) -->> candidate_id,tech_id,score)
            if 'tech_self_score' in applicant.keys():
                for tech in applicant['tech_self_score']:
                    df_inserts ={

                        'candidate_name':applicant['name'],
                        'tech_name':tech.title(),
                        'tech_score':int(applicant['tech_self_score'][tech])
                    }
                    self.tech_junc_df = self.tech_junc_df.append(df_inserts,ignore_index=True)
            else:
                    df_inserts ={

                        'candidate_name':applicant['name'],
                        'tech_name':None,
                        'tech_score':None
                    }
                    self.tech_junc_df = self.tech_junc_df.append(df_inserts,ignore_index=True)
                    
        json_dataframes_dict =  {
                'sparta_day_df':self.sparta_day_df2.replace({pd.NaT: None}),
                'weakness_df':self.weaknesses_junc_df.replace({pd.NaT: None}),
                'strength_df':self.strengths_junc_df.replace({pd.NaT: None}),
                'tech_df':self.tech_junc_df.replace({pd.NaT: None})}


        logger.info(f'Created all .json dataframes')
        return json_dataframes_dict





get_json = transformJsonFiles()
json_df_dict = get_json.make_dataframes()

# with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # so that console doesn't truncate dataframe results -->> print all 0-n rows! 
#    pd.set_option("expand_frame_repr",True)

    # pprint(json_df_dict)
