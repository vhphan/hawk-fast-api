import asyncio
import json
from datetime import datetime

from async_lru import alru_cache
from fastapi import APIRouter, Depends
from fastapi import Response
from starlette.requests import Request

from databases.apgdb import get_db, PgDB
from utils.kpi import standard_kpi_transform, flex_kpi_transform
from utils.kpi_constants import GroupBy, TransformFunction, DAILY_MAX_POINTS
from utils.sql_queries import daily_cluster_queries, daily_cells_queries, hourly_cells_queries

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
def region_stats(kpi_type: str, time_unit: str):
    if time_unit not in ['daily', 'hourly']:
        return {'success': False, 'message': f"Invalid time_unit: {time_unit}"}
    json_file = f'static/data/{time_unit}_{kpi_type}_regions.json'
    with open(json_file, 'r') as f:
        data = json.load(f)
    return {
        'success': True,
        'data': data,
        'meta': {
            'kpi_type': kpi_type,
            'current_time': datetime.now()
        }
    }


@router.post("/cells/{time_unit}/{kpi_type}")
async def daily_cell_stats(time_unit: str, kpi_type: str, request: Request, db: PgDB = Depends(get_db)):
    match kpi_type, time_unit:
        case 'standard', 'daily':
            sql_query = daily_cells_queries['standard']
        case 'standard', 'hourly':
            sql_query = hourly_cells_queries['standard']
        case _:
            return {'success': False, 'message': f"Invalid kpi_type: {kpi_type}"}
    # extract list of cells from request body
    body = await request.json()
    cells_array: list[str] = body['cells']

    async with db:
        results = await db.query(sql_query, params={'cells_array': cells_array}, return_records=True)
        return {
            'success': True,
            'data': results
        }


@router.post("/cells-raw/daily/{kpi_type}")
async def daily_cell_stats_raw(kpi_type: str, request: Request, db: PgDB = Depends(get_db)):
    match kpi_type:
        case 'standard':
            sql_queries: dict[str, str] = daily_cells_queries['standard_raw']
        case _:
            return {'success': False, 'message': f"Invalid kpi_type: {kpi_type}"}
    # extract list of cells from request body
    body = await request.json()
    cells_array: list[str] = body['cells']

    async with db:
        tasks = []
        for sql in sql_queries.values():
            tasks.append(db.query(sql, params={'cells_array': cells_array}, return_records=True))

        results = await asyncio.gather(*tasks)

        return {
            'success': True,
            'data': dict(zip(sql_queries.keys(), results))
        }


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
    # data = await get_cells_list(prefix, page, page_size, db)
    # return {
    #     'success': True,
    #     'data': data
    # }, {
    #     'Cache-Control': 'public, max-age=300'
    # }
    data = await get_cells_list(prefix, page, page_size, db)
    headers = {"Cache-Control": "public, max-age=300"}
    return Response(content=json.dumps({"success": True, "data": data}), media_type="application/json", headers=headers)
