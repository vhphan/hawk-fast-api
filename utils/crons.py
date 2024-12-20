import asyncio
import json
from datetime import datetime, date
from functools import partial
from multiprocessing import Process

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv
from fastapi_utilities import repeat_every
from loguru import logger

from databases.apgdb import PgDB
from sa_kpi.insert_cell_data import insert_hourly_cell_data, insert_daily_cell_data
from utils.kpi import standard_kpi_transform, flex_kpi_transform
from utils.kpi_constants import GroupBy, DAILY_MAX_POINTS
from utils.sql_queries import daily_region_queries, hourly_region_queries

scheduler = AsyncIOScheduler(timezone='Asia/Singapore')


class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)


@repeat_every(seconds=60)
def heartbeat():
    logger.info(f"Heartbeat cron at {datetime.now()}")


async def generate_json_file(sql_query, transform_function, output_file):
    async with PgDB() as db:
        results = await db.query(sql_query, return_records=True)
        data = transform_function(DAILY_MAX_POINTS, 1, results)
        with open(output_file, 'w') as f:
            f.write(json.dumps(data, cls=DateTimeEncoder))


async def generate_daily_json_file_regions(kpi_type='standard'):
    if kpi_type == 'standard':
        daily_kpi_transform_region = partial(standard_kpi_transform, group_by=GroupBy.REGION)
        return await generate_json_file(daily_region_queries['standard'], daily_kpi_transform_region,
                                        'static/data/daily_standard_regions.json')

    if kpi_type == 'flex':
        daily_flex_kpi_transform_region = partial(flex_kpi_transform, group_by=GroupBy.REGION)
        return await generate_json_file(daily_region_queries['flex'],
                                        daily_flex_kpi_transform_region,
                                        'static/data/daily_flex_regions.json')
    raise ValueError(f"Invalid kpi_type: {kpi_type}")


async def generate_hourly_json_file_regions(kpi_type='standard'):
    if kpi_type == 'standard':
        hourly_kpi_transform_region = partial(standard_kpi_transform, group_by=GroupBy.REGION, time_unit='hourly')
        return await generate_json_file(hourly_region_queries['standard'], hourly_kpi_transform_region,
                                        'static/data/hourly_standard_regions.json')

    if kpi_type == 'flex':
        hourly_flex_kpi_transform_region = partial(flex_kpi_transform, group_by=GroupBy.REGION, time_unit='hourly')
        return await generate_json_file(hourly_region_queries['flex'],
                                        hourly_flex_kpi_transform_region,
                                        'static/data/hourly_flex_regions.json')
    raise ValueError(f"Invalid kpi_type: {kpi_type}")


async def insert_data_and_generate_json(time_unit='daily'):
    logger.info(f"Inserting {time_unit} data at {datetime.now()}")

    sql_file = {'daily': 'sql/sa_kpi/insert_daily.sql',
                'hourly': 'sql/sa_kpi/insert_hourly.sql'
                }.get(time_unit)

    await run_sql_file(sql_file)

    tasks = [
        generate_daily_json_file_regions('standard'),
        generate_daily_json_file_regions('flex')
    ] if time_unit == 'daily' else [
        generate_hourly_json_file_regions('standard'),
        generate_hourly_json_file_regions('flex')
    ]

    logger.info(f"Generating {time_unit} json files at {datetime.now()}")
    await asyncio.gather(*tasks)
    logger.info(f"Finished generating {time_unit} json files at {datetime.now()}")


@scheduler.scheduled_job('cron', hour=8, minute=30)
@scheduler.scheduled_job('cron', hour=9, minute=30)
@scheduler.scheduled_job('cron', hour=22, minute=20)
async def insert_daily_data():
    await insert_data_and_generate_json('daily')


@scheduler.scheduled_job('cron', minute=0)
@scheduler.scheduled_job('cron', minute=30)
async def insert_hourly_data():
    await insert_data_and_generate_json('hourly')


@scheduler.scheduled_job('cron', minute=7)
def run_insert_hourlycell_data_sa_kpi():
    process = Process(target=insert_hourly_cell_data)
    process.start()
    process.join()


@scheduler.scheduled_job('cron', hour=8, minute=55)
def run_insert_daily_cell_data_sa_kpi():
    process = Process(target=insert_daily_cell_data)
    process.start()
    process.join()


async def execute_query(sql):
    async with PgDB() as db:
        return await db.execute(sql)


async def run_sql_file(sql_file):
    with open(sql_file, 'r') as f:
        sql = f.read()
    sql_statements = sql.split(';')
    tasks = []
    for sql in sql_statements:
        sql = sql.strip()
        if not sql:
            continue
        tasks.append(execute_query(sql))
    await asyncio.gather(*tasks)


# async generate_cells_list():

if __name__ == '__main__':
    load_dotenv()
    asyncio.run(insert_hourly_data())
