from optimising_sql_server2.app.load_files.s3connection import connectToS3
from optimising_sql_server2.app.tranform_files.new_files_download import get_new_files_list
from optimising_sql_server2.app.db_creation.logger import logger
from tqdm.contrib.concurrent import thread_map, process_map
from multiprocessing.pool import ThreadPool
from colorama import Fore
from tqdm import tqdm
import pandas as pd
from pathlib import Path
import pandas as pd
import csv
import os


# sparta_days_txt = GetS3CSVinfo('data21-final-project', 'Academy/')

class getFiles(connectToS3):

    def __init__(self, bucket_name, s3_sub_dir, file_ext):

        # Setting up connection to s3 resources from boto3.
        super().__init__()

    # user manually sets S3 bucket name, sub-directory within that bucket
        self.bucket_name = bucket_name
        self.s3_sub_dir = s3_sub_dir
        self.file_ext = file_ext
        self.json_dict_keyed_by_file_id = {}
        self.csv_dict_keyed_by_course = {}
        self.bucket = self.s3_resource.Bucket(self.bucket_name)
        self.file_keylist = self.get_list_of_files()
        self.s3_file_keylist = get_new_files_list(self.file_keylist)

    # Method creates a list of csv file locations with the specified class S3 bucket and subdirectory

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

    def create_dict_of_csv_dataframes(self, csv_file):
        # csv_dict_keyed_by_course = {}
        # for s3_key in self.s3_file_keylist:
        csv_s3_object = self.s3_client.get_object(
            Bucket=self.bucket_name,  # bucket_name is a string
            Key=csv_file  # s3_key is a string
        )
        # string splitting to return course name & date from CSV filename, e.g. 'Business_29_2019-11-18',
        # 'Sept2019Applicants' as key for dictionary whose values are Pandas dataframes
        course_name_date = (csv_file.split('/')[-1]).split('.')[0]
        # read csv into pandas dataframe
        self.csv_dict_keyed_by_course[course_name_date] = pd.read_csv(
            csv_s3_object['Body'])
        return csv_file

    def download_csv_in_chucks(self):
        logger.info('\nDownloading csv_files')
        results = thread_map(self.create_dict_of_csv_dataframes,
                             self.s3_file_keylist[:6], max_workers=30)
        # results = tqdm(ThreadPool(30).imap_unordered(self.create_dict_of_csv_dataframes,(tqdm(self.s3_file_keylist[:50]))),desc='Downloading csv_files')
        # for i,r in enumerate(results,1):
        #     print(r,'\t',end='' if i% 2 else '\n')

    def create_dict_of_txt_files(self):
        txt_dict_keyed_by_course = {}
        for s3_key in self.s3_file_keylist:
            txt_s3_object = self.s3_client.get_object(
                Bucket=self.bucket_name,  # bucket_name is a string
                Key=s3_key  # s3_key is a string
            )
            #
            sparta_day_name_date = (s3_key.split('/')[-1]).split('.')[0]
            # read txt into a dict
            txt_dict_keyed_by_course[sparta_day_name_date] = (
                txt_s3_object['Body'].read())
        return txt_dict_keyed_by_course

    def get_dict_of_json_files(self, json_file):

        # for s3_key in tqdm(self.s3_file_keylist,desc='Downloading json'):
        json_s3_object = self.s3_client.get_object(
            Bucket=self.bucket_name,
            Key=json_file
        )
        json_file_id = (json_file.split('/')[-1]).split('.')[0]

        self.json_dict_keyed_by_file_id[json_file_id] = (
            json_s3_object['Body'].read())
        # logger.info(f'adding{s3_key} to json dict')
        # logger.debug(f'created json dict of {len(self.s3_file_keylist)}')
        return json_file

    def download_json_in_chucks(self):
        logger.info('\nDownloading json_files')
        results = thread_map(self.get_dict_of_json_files,
                             self.s3_file_keylist[:10], max_workers=500)

        # results = ThreadPool(600).imap_unordered(self.get_dict_of_json_files,self.s3_file_keylist)
        # for i,r in tqdm(enumerate(results,1),desc='Downloading Json_files',position=0):
        #     print(r,'\t',end='' if i% 4 else '\n')
