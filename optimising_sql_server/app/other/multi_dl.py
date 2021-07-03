from multiprocessing.pool import ThreadPool
from concurrent.futures import ProcessPoolExecutor
from optimising_sql_server.app.load_files.s3connection import connectToS3
from optimising_sql_server.app.db_creation.logger import logger
import pandas as pd
from tqdm import asyncio, tqdm
from tqdm.contrib.concurrent import thread_map,process_map
from pprint import pprint
import json
import asyncio


# sparta_days_txt = GetS3CSVinfo('data21-final-project', 'Academy/')

class getFiles(connectToS3):

    
    json_dict_keyed_by_file_id = {}

    def __init__(self, bucket_name, s3_sub_dir, file_ext):

        # Setting up connection to s3 resources from boto3.
        super().__init__()

    # user manually sets S3 bucket name, sub-directory within that bucket
        self.bucket_name = bucket_name
        self.s3_sub_dir = s3_sub_dir
        self.file_ext = file_ext
        self.bucket = self.s3_resource.Bucket(self.bucket_name)
        self.s3_file_keylist = []
        self.results = []
        self.j_list =None
        self.sparta_day_df2 = pd.DataFrame()
        self.weaknesses_junc_df = pd.DataFrame()
        self.strengths_junc_df = pd.DataFrame()
        self.tech_junc_df = pd.DataFrame()
        asyncio.run(self.main())
        print(self.sparta_day_df2)



    async def get_list_of_files(self):
        s3_file_key_list = []
        filekeys_in_bucket_subdir = self.bucket.objects.filter(
            Prefix=self.s3_sub_dir)
        # keep track of the number of CSVs in S3 sub-directory
        file_counter = 0
        for key_obj in filekeys_in_bucket_subdir:
            # only want CSVs in list!
            # if key_obj.key[-self.ext_len:] == self.file_ext:
            if key_obj.key.endswith(self.file_ext):
                file_counter += 1
                # print(f'Found CSV file: {key_obj.key}')
                s3_file_key_list.append(key_obj.key)
        logger.debug(
            f'Number of {self.file_ext} files found in s3://{self.bucket_name}/{self.s3_sub_dir} = {file_counter}')
        await asyncio.sleep(0.5)
        return s3_file_key_list

    def get_dict_of_json_files(self,json_file):
        
        # json_list = []
        # for s3_key in tqdm(self.s3_file_keylist,desc='Downloading json'):
        json_s3_object = self.s3_client.get_object(
            Bucket = self.bucket_name, 
            Key = json_file
        )
        # json_file_id = (json_file.split('/')[-1]).split('.')[0]

        self.results.append(json.loads(json_s3_object['Body'].read()))
            # logger.info(f'adding{s3_key} to json dict')
        # logger.debug(f'created json dict of {len(self.s3_file_keylist)}')
        return json_file


    async def download_in_chucks(self):

        
        # results = tqdm(ThreadPool(100).imap_unordered(self.get_dict_of_json_files,(tqdm(self.s3_file_keylist))),desc='Downloading Json_files',position=0)
            self.j_list = thread_map(self.get_dict_of_json_files,self.s3_file_keylist,max_workers = 100)
            print(self.j_list[:10])
            return self.j_list

    async def trial(self):

        sparta_day_df = pd.DataFrame()
        for spart in tqdm(self.results):
            # print(next(r))
            applicant = spart
        
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
            
            sparta_day_df = sparta_day_df.append(df_inserts,ignore_index=True)
            sparta_day_df['date'] = sparta_day_df['date'].astype('datetime64')
            await asyncio.sleep(0.5)
            return sparta_day_df

    async def main(self):
                task1 = asyncio.create_task(self.get_list_of_files())
                task3 = asyncio.create_task(self.download_in_chucks())
                task4 = asyncio.create_task(self.trial())

                self.s3_file_keylist = await task1
                pprint(self.s3_file_keylist)
                self.results = await task3
                print(self.results[:10])
                self.sparta_day_df2 = await task4
                print(self.sparta_day_df2.head())

    async def main(self):
            task1 = asyncio.create_task(self.get_list_of_files())
            task3 = asyncio.create_task(self.download_in_chucks())
            task4 = asyncio.create_task(self.trial())

            self.s3_file_keylist = await task1
            pprint(self.s3_file_keylist[:10])
            self.results = await task3
            print(self.results[:10])
            self.sparta_day_df2 = await task4
            print(self.sparta_day_df2.head())

json_objects = getFiles('data21-final-project','Talent','.json')
# json_objects = getFiles('data21-final-project','Talent','.json')


#///////////////////////////////////////////////////
            # # self.sparta_day_df2['date'] = self.sparta_day_df2['date'].astype(str)
            # # self.sparta_day_df2['date'] = pd.to_datetime(self.sparta_day_df2['date'],format='%d%m%Y')

            # # create weakness_junc ( candidate_name,weakness) -->> (candidate_id,weekness_id)
            # for weakness in applicant['weaknesses']:
            #     df_inserts ={

            #         'candidate_name':applicant['name'],
            #         'weaknesses':weakness
            #     }
            #     self.weaknesses_junc_df = self.weaknesses_junc_df.append(df_inserts,ignore_index=True)
    

            # # create strengths_junc (candidate_name,strength) -->> (candidate_id,strength_id)
            # for strength in applicant['strengths']:
            #     df_inserts ={

            #         'candidate_name':applicant['name'],
            #         'strengths':strength
            #     }
            #     self.strengths_junc_df = self.strengths_junc_df.append(df_inserts,ignore_index=True)



            # # create tech_junction_df =   (cadidate_name,tech_name,tech_score) -->> candidate_id,tech_id,score)
            # if 'tech_self_score' in applicant.keys():
            #     for tech in applicant['tech_self_score']:
            #         df_inserts ={

            #             'candidate_name':applicant['name'],
            #             'tech_name':tech.title(),
            #             'tech_score':int(applicant['tech_self_score'][tech])
            #         }
            #         self.tech_junc_df = self.tech_junc_df.append(df_inserts,ignore_index=True)
            # else:
            #         df_inserts ={

            #             'candidate_name':applicant['name'],
            #             'tech_name':None,
            #             'tech_score':None
            #         }
            #         self.tech_junc_df = self.tech_junc_df.append(df_inserts,ignore_index=True)
                    
            # json_dataframes_dict =  {
            #         'sparta_day_df':self.sparta_day_df2.replace({pd.NaT: None}),
            #         'weakness_df':self.weaknesses_junc_df.replace({pd.NaT: None}),
            #         'strength_df':self.strengths_junc_df.replace({pd.NaT: None}),
            #         'tech_df':self.tech_junc_df.replace({pd.NaT: None})}


            # logger.info(f'Created all .json dataframes')
            # return sparta_day_df

        # results2 = thread_map(trial,results)
        # return results2

#///////////////////////////////////////////////////////////////////////////////////////

#     async def main(self):
#             task1 = asyncio.create_task(self.get_list_of_files())
#             task3 = asyncio.create_task(self.download_in_chucks())
#             task4 = asyncio.create_task(self.trial())

#             self.s3_file_keylist = await task1
#             pprint(self.s3_file_keylist)
#             self.results = await task3
#             print(self.results[:10])
#             self.sparta_day_df2 = await task4
#             print(self.sparta_day_df2.head())

# json_objects = getFiles('data21-final-project','Talent','.json')
# json_files_dict = json_objects.download_in_chucks()
# print(json_files_dict)


# json_files = json_objects.get_list_of_files()
# pprint(len(json_objects.json_dict_keyed_by_file_id))








# # def download_url(url):
# #   print("downloading: ",url)
# #   # assumes that the last segment after the / represents the file name
# #   # if url is abc/xyz/file.txt, the file name will be file.txt
# #   file_name_start_pos = url.rfind("/") + 1
# #   file_name = url[file_name_start_pos:]
 
# #   r = requests.get(url, stream=True)
# #   if r.status_code == requests.codes.ok:
# #     with open(file_name, 'wb') as f:
# #       for data in r:
# #         f.write(data)
# #   return url
 
 
# # urls = ["https://jsonplaceholder.typicode.com/posts",
# #         "https://jsonplaceholder.typicode.com/comments",
# #         "https://jsonplaceholder.typicode.com/photos",
# #         "https://jsonplaceholder.typicode.com/todos",
# #         "https://jsonplaceholder.typicode.com/albums"
# #         ]
 
# # # Run 5 multiple threads. Each call will take the next element in urls list
# # results = ThreadPool(5).imap_unordered(download_url, urls)
# # for r in results:
# #     print(r)




# # url = "https://jsonplaceholder.typicode.com/posts"
# # file_name_start_pos = url.rfind("/") 

# # print(file_name_start_pos)