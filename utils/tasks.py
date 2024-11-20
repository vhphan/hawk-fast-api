import asyncio
import functools
import os
import time

import aiohttp
import requests
from loguru import logger

from config import PROJECT_PATH


def clock(func):
    def log_execution_time(func_name, args, kwargs, elapsed, result):
        arg_lst = [repr(arg) for arg in args]
        arg_lst.extend(f'{k}={v!r}' for k, v in kwargs.items())
        arg_str = ', '.join(arg_lst)
        logger.info(f'[{elapsed:0.8f}s] {func_name}({arg_str}) -> {result!r}')

    @functools.wraps(func)
    async def async_clocked(*args, **kwargs):
        t0 = time.perf_counter()
        result = await func(*args, **kwargs)
        elapsed = time.perf_counter() - t0
        log_execution_time(func.__name__, args, kwargs, elapsed, result)
        return result

    @functools.wraps(func)
    def sync_clocked(*args, **kwargs):
        t0 = time.perf_counter()
        result = func(*args, **kwargs)
        elapsed = time.perf_counter() - t0
        log_execution_time(func.__name__, args, kwargs, elapsed, result)
        return result

    if asyncio.iscoroutinefunction(func):
        return async_clocked
    else:
        return sync_clocked


def retry_on_timeout(retries=1, delay=0):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except requests.exceptions.Timeout:
                logger.error('Request timed out on first attempt, retrying...')
                for attempt in range(retries):
                    try:
                        return func(*args, **kwargs)
                    except requests.exceptions.Timeout:
                        logger.error(f'Request timed out on retry attempt {attempt + 1}')
                        if attempt < retries - 1:
                            logger.info('Retrying...')
                            time.sleep(delay)
                logger.info('Attempting hard restart of Node.js Passenger')
                hard_restart_node_passenger()

        return wrapper

    return decorator


@retry_on_timeout(retries=1, delay=0)
@clock
async def check_end_point(timeout=25, region='ALL', tech='nr'):
    url = f'http://localhost:4000/node/kpi/v1/dailyStatsRegion?region={region}&tech={tech}'
    async with aiohttp.ClientSession() as session:
        logger.info(f'Checking end point: {url}')
        async with session.get(url) as response:
            if response.status == 200:
                logger.info('End point is up')


def hard_restart_node_passenger():
    logger.info("Hard Restarting Node.js Passenger")
    cur_dir = os.getcwd()
    os.chdir(f'{PROJECT_PATH}/../hawk-express-app')
    os.system('passenger stop')
    time.sleep(3)
    os.system('passenger start')
    os.chdir(cur_dir)


# def run_python_module(module_path, python_path, working_dir):
#     logger.info(f'Running python module: {module_path}')
#     cur_dir = os.getcwd()
#     os.chdir(working_dir)
#     os.system(f'{python_path} -m {module_path}')
#     os.chdir(cur_dir)
#     logger.info(f'Finished running python module: {module_path}')

