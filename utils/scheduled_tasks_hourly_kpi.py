import asyncio

import schedule
from dotenv import load_dotenv
from loguru import logger
from setproctitle import setproctitle

from config import PROJECT_PATH
from utils.helpers import run


async def run_insert_hourly_flex():
    module_paths = [
        'get_data.hourly_stats.optimize.nrcelldu_flex.insert',
        'get_data.hourly_stats.optimize.nrcellcu_flex.insert',
        'get_data.hourly_stats.optimize.eutrancellfdd_flex.insert',
    ]
    working_dir = f'{PROJECT_PATH}/../hawk-data'
    python_path = f'{working_dir}/venv/bin/python'
    for module_path in module_paths:
        cmd = f'{python_path} -m {module_path} --run-once'
        await run(cmd=cmd, working_dir=working_dir)
        await asyncio.sleep(60 * 3)


async def run_refresh_mv():
    try:
        module_paths = [
            'get_data.hourly_stats.refresh_mv',
        ]
        working_dir = f'{PROJECT_PATH}/../hawk-data'
        python_path = f'{working_dir}/venv/bin/python'
        for module_path in module_paths:
            cmd = f'{python_path} -m {module_path} --run-once'
            await run(cmd=cmd, working_dir=working_dir)
    except Exception as e:
        logger.error(e)


async def main():
    setproctitle('__scheduled_tasks_hourly_kpi__')
    # await run_refresh_mv()
    schedule.every(10).minutes.do(lambda: asyncio.create_task(run_insert_hourly_flex()))
    schedule.every(5).minutes.do(lambda: asyncio.create_task(run_refresh_mv()))
    while True:
        schedule.run_pending()
        logger.info('Sleeping for 60 seconds')
        await asyncio.sleep(60)

if __name__ == '__main__':
    load_dotenv()
    try:
        asyncio.run(main())
    except Exception as e:
        logger.error(e)
