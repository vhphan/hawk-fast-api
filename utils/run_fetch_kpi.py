import time

import schedule
from loguru import logger

from utils.helpers import run_python_module


def main():
    logger.info('Running fetch kpi')
    python_modules = [
        'tmp.manual_nr',
        'tmp.manual_lte',
    ]
    python_path = '/home/hawkuser/Desktop/tests/myenv/bin/python'
    working_dir = '/home/hawkuser/Desktop/tests'
    for python_module in python_modules:
        run_python_module(
            module_path=python_module,
            python_path=python_path,
            working_dir=working_dir
        )
    logger.info('Fetch kpi completed')
    return schedule.CancelJob


if __name__ == '__main__':
    schedule.every().day.at("02:59").do(main)
    while True:
        logger.info('Running any pending jobs')
        schedule.run_pending()
        logger.info('Sleeping for 5 minutes')
        time.sleep(300)
