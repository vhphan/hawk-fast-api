import asyncio

import pandas as pd

from config import PROJECT_PATH
from utils.helpers import run_python_module


def main():
    asyncio.run(run_python_module('get_data.hourly_stats.find_abnormal_count',
                                  'python',
                                  '/data2/var/www/hawk-data/',
                                  ))
    df_anomalies = (pd.read_csv('/data2/var/www/hawk-data/data/hourly/anomalies/abnormal_count.csv')
                    .pipe(lambda x: x[x['count'] == 0]))
