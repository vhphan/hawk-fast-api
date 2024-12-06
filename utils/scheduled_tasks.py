import asyncio
import os
import time
from datetime import datetime

import schedule
from dotenv import load_dotenv
from loguru import logger

from utils.scheduler_config import TECHS, REGIONS

logger.add("logs/scheduled_tasks.log", rotation="1 day", retention="7 days")

from setproctitle import setproctitle

from config import PROJECT_PATH
from utils.tasks import check_end_point, hard_restart_node_passenger, clock


def job():
    logger.info("I'm working...")


def restart_node_passenger():
    tmp_file_path = f'{PROJECT_PATH}/../hawk-express-app/tmp/restart.txt'
    logger.info(f"Restarting Node.js Passenger: {tmp_file_path}")
    with open(tmp_file_path, 'a') as f:
        current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        f.write(f'restart {current_timestamp}')


def heartbeat():
    logger.info("Heartbeat")


@clock
async def run_timing_advance_etl():
    module_path = 'hawk_dop_app.scripts.timing_advance_etl'
    working_dir = f'{PROJECT_PATH}/../hawk-data'
    python_path = f'{working_dir}/venv/bin/python'
    await run_python_module(module_path, python_path, working_dir)


@clock
async def run_etl_network():
    module_path = 'hawk_dop_app.scripts.raster_etl_network'
    working_dir = f'{PROJECT_PATH}/../hawk-data'
    python_path = f'{working_dir}/venv/bin/python'
    await run_python_module(module_path, python_path, working_dir)


@clock
async def run_etl_plmn():
    module_path = 'hawk_dop_app.scripts.raster_etl'
    working_dir = f'{PROJECT_PATH}/../hawk-data'
    python_path = f'{working_dir}/venv/bin/python'
    await run_python_module(module_path, python_path, working_dir)


async def run_busy_hour_tasks():
    module_path = 'get_data.busy_hour.tasks.final_tasks'
    module_path_plmn = 'get_data.busy_hour.tasks.final_tasks_plmn'
    working_dir = f'{PROJECT_PATH}/../hawk-data'
    python_path = f'{working_dir}/venv/bin/python'
    await run_python_module(module_path, python_path, working_dir)
    await run_python_module(module_path_plmn, python_path, working_dir)


async def main():
    setproctitle('__scheduled_tasks__')
    schedule.every().day.at("02:30").do(hard_restart_node_passenger)
    schedule.every(15).minutes.do(heartbeat)

    for region in REGIONS:
        for tech in TECHS:
            schedule.every(10).minutes.do(
                lambda: asyncio.create_task(check_end_point(region=region, tech=tech, endpoint='dailyStatsRegion')))
            schedule.every(10).minutes.do(
                lambda: asyncio.create_task(check_end_point(region=region, tech=tech, endpoint='dailyStatsRegionFlex')))
            schedule.every(10).minutes.do(
                lambda: asyncio.create_task(check_end_point(region=region, tech=tech, endpoint='hourlyStatsRegion')))
            schedule.every(10).minutes.do(
                            lambda: asyncio.create_task(check_end_point(region=region, tech=tech, endpoint='hourlyStatsRegionFlex')))

    schedule.every(5).minutes.do(lambda: asyncio.create_task(check_end_point()))
    schedule.every().day.at("19:00").do(lambda: asyncio.create_task(run_timing_advance_etl()))
    schedule.every().sunday.at("22:00").do(lambda: asyncio.create_task(run_etl_network()))
    schedule.every().monday.at("02:00").do(lambda: asyncio.create_task(run_etl_plmn()))
    schedule.every().monday.at("12:00").do(lambda: asyncio.create_task(run_etl_plmn()))
    schedule.every().tuesday.at("02:00").do(lambda: asyncio.create_task(run_etl_plmn()))
    schedule.every().wednesday.at("02:00").do(lambda: asyncio.create_task(run_etl_plmn()))
    schedule.every().thursday.at("02:00").do(lambda: asyncio.create_task(run_etl_plmn()))
    schedule.every().monday.at("07:00").do(lambda: asyncio.create_task(run_busy_hour_tasks()))

    # schedule.every().hour.do(job)
    # schedule.every(5).seconds.do(job)
    # schedule.every().monday.do(job)
    # schedule.every().wednesday.at("13:15").do(job)
    # schedule.every().day.at("12:42", "Europe/Amsterdam").do(job)
    # schedule.every().minute.at(":17").do(job)

    start_time = time.time()
    total_duration_to_run = 60 * 60 * 24 * 365
    while True:
        schedule.run_pending()
        total_duration_ran = time.time() - start_time
        logger.info(f"Total duration ran: {total_duration_ran} seconds")

        if total_duration_ran > total_duration_to_run:
            break

        await asyncio.sleep(60)


async def run_python_module(module_path, python_path, working_dir, callback=None):
    logger.info(f'Running python module: {module_path}')
    cur_dir = os.getcwd()
    os.chdir(working_dir)
    process = await asyncio.create_subprocess_exec(python_path, '-m', module_path)
    await process.wait()
    os.chdir(cur_dir)
    logger.info(f'Finished running python module: {module_path}')
    if callback:
        callback()
    return process


if __name__ == '__main__':
    load_dotenv()
    try:
        asyncio.run(main())
    except Exception as e:
        logger.error(e)
    # nohup python -m utils.scheduled_tasks > output.log 2>&1 &
    # kill -9 $(pgrep -f __scheduled_tasks__)
