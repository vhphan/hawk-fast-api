import asyncio
import json
from datetime import datetime
from typing import Optional

from async_lru import alru_cache
from fastapi import APIRouter, Depends, HTTPException
from fastapi import Response
from loguru import logger
from starlette.requests import Request

from databases.apgdb import get_db, PgDB
from utils.kpi import standard_kpi_transform, flex_kpi_transform
from utils.kpi_constants import GroupBy, TransformFunction, DAILY_MAX_POINTS, HOURLY_MAX_POINTS
from utils.sql_queries import daily_cluster_queries, hourly_cells_queries
from utils.tasks import clock

router = APIRouter()


@router.get("/daily/cluster/{kpi_type}")
async def daily_cluster_stats(cluster_id: str, kpi_type: str, db: PgDB = Depends(get_db)):
    match kpi_type:
        case 'standard':
            sql_query = daily_cluster_queries['standard']
        case 'flex':
            sql_query = daily_cluster_queries['flex']
        case _:
            return {'success': False, 'message': f"Invalid kpi_type: {kpi_type}"}

    async with db:
        results = await db.query(sql_query,
                                 return_records=True,
                                 params={'cluster_id': cluster_id}
                                 )

        transform_function: TransformFunction = standard_kpi_transform if kpi_type == 'standard' else flex_kpi_transform
        data = transform_function(DAILY_MAX_POINTS, 1, results, GroupBy.REGION)

        return {
            'success': True,
            'data': data,
            'meta': {
                'cluster_id': cluster_id,
                'start_days_ago': DAILY_MAX_POINTS,
                'end_days_ago': 1,
                'current_time': datetime.now()
            }
        }


@router.get("/region/{time_unit}/{kpi_type}")
def region_stats(kpi_type: str, time_unit: str, band: Optional[str] = None):

    if time_unit not in ['daily', 'hourly']:
        return HTTPException(status_code=400, detail=f"Invalid time_unit: {time_unit}")
    if band and band not in ['N3', 'N7']:
        return HTTPException(status_code=400, detail=f"Invalid band: {band}")
    if band:
        json_file = f'static/data/{time_unit}_{kpi_type}_regions_v2.json'
        with open(json_file, 'r') as f:
            data = json.load(f)
            return {
                'success': True,
                'data': data[band],
                'meta': {
                    'band': band,
                    'kpi_type': kpi_type,
                    'date_timestamp': datetime.now()
                }
            }

    json_file = f'static/data/{time_unit}_{kpi_type}_regions.json'
    with open(json_file, 'r') as f:
        data = json.load(f)
    return {
        'success': True,
        'data': data,
        'meta': {
            'kpi_type': kpi_type,
            'date_timestamp': datetime.now()
        }
    }


@router.post("/cells/{time_unit}/{kpi_type}")
@clock
async def cell_stats(time_unit: str, kpi_type: str, request: Request, db: PgDB = Depends(get_db)):
    match kpi_type, time_unit:
        # case 'standard', 'daily':
        #     sql_query = daily_cells_queries['standard_raw']
        # case 'flex', 'daily':
        #     sql_query = daily_cells_queries['flex_raw']

        case 'standard', 'hourly':
            sql_query = hourly_cells_queries['standard']
        case 'flex', 'hourly':
            sql_query = hourly_cells_queries['flex']
        case _:
            return {'success': False, 'message': f"Invalid or unsupported kpi_type: {kpi_type}"}

    body = await request.json()
    cells_array: list[str] = body['cells']

    logger.debug(f"{sql_query=}")

    async with db:
        tasks = []
        for cell in cells_array:
            tasks.append(db.query(sql_query, return_records=True, params={'cell': cell}))

        results = await asyncio.gather(*tasks)
        transform_func = standard_kpi_transform if kpi_type == 'standard' else flex_kpi_transform
        max_points = HOURLY_MAX_POINTS if time_unit == 'hourly' else DAILY_MAX_POINTS
        transformed = transform_func(max_points, 1, results[0], GroupBy.NO_GROUP, time_unit=time_unit)
        final_response = {
            'success': True,
            'data': transformed,
            'meta': {
                'cells': cells_array,
                'data_timestamp': datetime.now()
            }
        }
        # save response as json file with timestamp in filename
        # timestamp_str = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        # with open(f'static/data/{time_unit}_{kpi_type}_cells_{timestamp_str}.json', 'w') as f:
        #     f.write(json.dumps(final_response, cls=DateTimeEncoder))
        #     logger.info(f"Saving response to static/data/{time_unit}_{kpi_type}_cells_{timestamp_str}.json")
        return final_response


@alru_cache(maxsize=128)
async def get_cells_list(prefix: str, page: int, page_size: int, db: PgDB):
    async with db:
        sql = "select * from dnb.sa_kpi_results.get_cells_list(:prefix, :page, :page_size)"
        results = await db.query(sql, params={'prefix': prefix, 'page': int(page), 'page_size': page_size},
                                 return_records=True)
        return [
            {
                'label': result['cell_id'],
                'value': result['cell_id']
            }
            for result in results
        ]


@router.get("/cells-list/{prefix}/{page}")
async def cell_list(prefix: str, page: int, db: PgDB = Depends(get_db), page_size: int = 100):
    data = await get_cells_list(prefix, page, page_size, db)
    headers = {"Cache-Control": "public, max-age=900"}
    return Response(content=json.dumps({"success": True, "data": data}), media_type="application/json", headers=headers)
