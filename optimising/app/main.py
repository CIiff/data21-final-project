from optimising.app.db_creation.weekly_performance import *
from datetime import datetime 
import time


run_ETL_process()



time.sleep(3)
logger.info(f'\nENDED ETL PROCESS AT: {datetime.now().strftime("%H:%M:%S")}\n')


