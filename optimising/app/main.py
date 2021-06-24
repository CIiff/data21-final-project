from optimising.app.db_creation.weekly_performance import *
import time





starttime = time.time()
logger.info(f'Starting ETL PROCESS AT: {starttime}\n')

time.sleep(3)
run_ETL_process()

endtime = time.time()
runtime = endtime - starttime

logger.info(f'\nENDED ETL PROCESS AT: {starttime}\n')

time.sleep(3)
logger.info(f'START:{starttime}\n END:{endtime}\nTIME TAKEN TO RUN ETL = {runtime}')
