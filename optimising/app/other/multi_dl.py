from multiprocessing.pool import ThreadPool
from concurrent.futures import ProcessPoolExecutor
from optimising.app.load_files.s3connection import connectToS3
from optimising.app.db_creation.logger import logger
import pandas as pd
from tqdm import tqdm
from tqdm.contrib.concurrent import thread_map
from pprint import pprint


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
        self.s3_file_keylist = self.get_list_of_files()


    def get_list_of_files(self):
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
        return s3_file_key_list

    def get_dict_of_json_files(self,json_file):
        
        # for s3_key in tqdm(self.s3_file_keylist,desc='Downloading json'):
        json_s3_object = self.s3_client.get_object(
            Bucket = self.bucket_name, 
            Key = json_file
        )
        json_file_id = (json_file.split('/')[-1]).split('.')[0]

        self.json_dict_keyed_by_file_id[json_file_id] = (
                json_s3_object['Body'].read())
            # logger.info(f'adding{s3_key} to json dict')
        # logger.debug(f'created json dict of {len(self.s3_file_keylist)}')
        return json_file


    def download_in_chucks(self):

        
        # results = tqdm(ThreadPool(100).imap_unordered(self.get_dict_of_json_files,(tqdm(self.s3_file_keylist))),desc='Downloading Json_files',position=0)
        results = thread_map(self.get_dict_of_json_files,self.s3_file_keylist,max_workers = 500)
        # for r in results:
        #     # print(r)
        #     pass



json_objects = getFiles('data21-final-project','Talent','.json')
json_files_dict = json_objects.download_in_chucks()
json_files = json_objects.get_list_of_files()
pprint(len(json_objects.json_dict_keyed_by_file_id))








# def download_url(url):
#   print("downloading: ",url)
#   # assumes that the last segment after the / represents the file name
#   # if url is abc/xyz/file.txt, the file name will be file.txt
#   file_name_start_pos = url.rfind("/") + 1
#   file_name = url[file_name_start_pos:]
 
#   r = requests.get(url, stream=True)
#   if r.status_code == requests.codes.ok:
#     with open(file_name, 'wb') as f:
#       for data in r:
#         f.write(data)
#   return url
 
 
# urls = ["https://jsonplaceholder.typicode.com/posts",
#         "https://jsonplaceholder.typicode.com/comments",
#         "https://jsonplaceholder.typicode.com/photos",
#         "https://jsonplaceholder.typicode.com/todos",
#         "https://jsonplaceholder.typicode.com/albums"
#         ]
 
# # Run 5 multiple threads. Each call will take the next element in urls list
# results = ThreadPool(5).imap_unordered(download_url, urls)
# for r in results:
#     print(r)




# url = "https://jsonplaceholder.typicode.com/posts"
# file_name_start_pos = url.rfind("/") 

# print(file_name_start_pos)