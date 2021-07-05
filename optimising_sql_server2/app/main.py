from optimising_sql_server.app.db_creation.weekly_performance import *
from datetime import datetime 
import time

start = time.perf_counter()

run_ETL_process()



time.sleep(3)

end = time.perf_counter()

print(f' Took {(end - start)/60}s')
logger.info(f'\nENDED ETL PROCESS AT: {datetime.now().strftime("%H:%M:%S")}\n')


# if __name__ == '__main__':
#     run_ETL_process()