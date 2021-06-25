from optimising.app.load_files.s3connection import connectToS3
from optimising.app.db_creation.logger import logger
import pandas as pd
from tqdm import  tqdm,trange


# sparta_days_txt = GetS3CSVinfo('data21-final-project', 'Academy/')

class getFiles(connectToS3):
    def __init__(self, bucket_name, s3_sub_dir, file_ext):

        # Setting up connection to s3 resources from boto3.
        super().__init__()

    # user manually sets S3 bucket name, sub-directory within that bucket
        self.bucket_name = bucket_name
        self.s3_sub_dir = s3_sub_dir
        self.file_ext = file_ext
        self.bucket = self.s3_resource.Bucket(self.bucket_name)
        self.s3_file_keylist = self.get_list_of_files()

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

    def create_dict_of_csv_dataframes(self):
        csv_dict_keyed_by_course = {}
        for s3_key in self.s3_file_keylist:
            csv_s3_object = self.s3_client.get_object(
                Bucket=self.bucket_name,  # bucket_name is a string
                Key=s3_key  # s3_key is a string
            )
            # string splitting to return course name & date from CSV filename, e.g. 'Business_29_2019-11-18',
            # 'Sept2019Applicants' as key for dictionary whose values are Pandas dataframes
            course_name_date = (s3_key.split('/')[-1]).split('.')[0]
            # read csv into pandas dataframe
            csv_dict_keyed_by_course[course_name_date] = pd.read_csv(
                csv_s3_object['Body'])
        return csv_dict_keyed_by_course

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

    def get_dict_of_json_files(self):
        json_dict_keyed_by_file_id = {}
        for s3_key in tqdm(self.s3_file_keylist[:100],unit ='json_files',desc = 'Downloading Json',position = 0):
            json_s3_object = self.s3_client.get_object(
                Bucket = self.bucket_name, 
                Key = s3_key
            )
            json_file_id = (s3_key.split('/')[-1]).split('.')[0]

            json_dict_keyed_by_file_id[json_file_id] = (
                json_s3_object['Body'].read())
            # logger.info(f'adding{s3_key} to json dict')
        logger.debug(f'created json dict of {len(self.s3_file_keylist)}')
        return json_dict_keyed_by_file_id