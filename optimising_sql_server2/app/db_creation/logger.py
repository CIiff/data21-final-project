import logging
# removing logging pointer from root
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')

# loggind to file

file_handler = logging.FileHandler('applicants_CSVs_df_transform.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

# loggin to console
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
stream_handler.setLevel(logging.INFO)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)
