import asyncio
import time
from datetime import datetime

import schedule
from dotenv import load_dotenv
from loguru import logger

from utils.crons import insert_hourly_data, insert_hourly_data_v2
from utils.helpers import run_python_module
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
    working_dir = f'{PROJECT_PATH}/../hawk-data'
    python_path = f'{working_dir}/venv/bin/python'

    module_path = 'get_data.busy_hour.tasks.final_tasks'
    module_path_plmn = 'get_data.busy_hour.tasks.final_tasks_plmn'
    module_path_daily = 'get_data.busy_hour.tasks.generate_daily'
    module_path_plmn_daily = 'get_data.busy_hour.tasks.generate_daily_plmn'

    await run_python_module(module_path, python_path, working_dir)
    await run_python_module(module_path_plmn, python_path, working_dir)
    await asyncio.gather(
        run_python_module(module_path_daily, python_path, working_dir),
        run_python_module(module_path_plmn_daily, python_path, working_dir)
    )


async def main(debug=False):
    if debug:
        setproctitle('__scheduled_tasks_debug__')
    else:
        setproctitle('__scheduled_tasks__')

    logger.add("logs/scheduled_tasks.log", rotation="1 day", retention="7 days")
    schedule.every().day.at("02:30").do(hard_restart_node_passenger)
    schedule.every(15).minutes.do(heartbeat)

    endpoints = ['dailyStatsRegion', 'dailyStatsRegionFlex', 'hourlyStatsRegion', 'hourlyStatsRegionFlex']

    for i, region in enumerate(REGIONS):
        for j, tech in enumerate(TECHS):
            for k, endpoint in enumerate(endpoints):
                delay_duration = 2 * (i + j)
                schedule.every(10).minutes.do(
                    lambda region=region, tech=tech, endpoint=endpoint,
                           delay=delay_duration + k * 2: asyncio.create_task(
                        check_end_point(region=region, tech=tech, endpoint=endpoint, delay=delay_duration + k * 2))
                )

    if not debug:
        await schedule_raster_tasks()

    if debug:
        schedule.run_all()

    start_time = time.time()
    total_duration_to_run = 60 * 60 * 24 * 365
    while True:
        schedule.run_pending()
        total_duration_ran = time.time() - start_time
        logger.info(f"Total duration ran: {total_duration_ran} seconds")

        if total_duration_ran > total_duration_to_run:
            break

        await asyncio.sleep(60)


async def schedule_raster_tasks():
    schedule.every().day.at("19:00").do(lambda: asyncio.create_task(run_timing_advance_etl()))
    schedule.every().sunday.at("22:00").do(lambda: asyncio.create_task(run_etl_network()))
    schedule.every().monday.at("02:00").do(lambda: asyncio.create_task(run_etl_plmn()))
    schedule.every().monday.at("12:00").do(lambda: asyncio.create_task(run_etl_plmn()))
    schedule.every().tuesday.at("02:00").do(lambda: asyncio.create_task(run_etl_plmn()))
    schedule.every().wednesday.at("02:00").do(lambda: asyncio.create_task(run_etl_plmn()))
    schedule.every().thursday.at("02:00").do(lambda: asyncio.create_task(run_etl_plmn()))
    schedule.every().monday.at("09:00").do(lambda: asyncio.create_task(run_busy_hour_tasks()))


async def schedule_sa_kpi_hourly_tasks():
    schedule.every().hour().at(":15").do(lambda: asyncio.create_task(insert_hourly_data()))
    schedule.every().hour().at(":15").do(lambda: asyncio.create_task(insert_hourly_data_v2()))
    schedule.every().hour().at(":45").do(lambda: asyncio.create_task(insert_hourly_data()))
    schedule.every().hour().at(":45").do(lambda: asyncio.create_task(insert_hourly_data_v2()))


if __name__ == '__main__':
    load_dotenv()
    try:
        asyncio.run(main(debug=False))
    except Exception as e:
        logger.error(e)
    # nohup python -m utils.scheduled_tasks > output.log 2>&1 &
    # kill -9 $(pgrep -f __scheduled_tasks__)
