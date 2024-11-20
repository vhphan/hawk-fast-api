from datetime import datetime
from functools import lru_cache
from typing import Any

import pandas as pd

from utils.kpi_constants import GroupBy, MNOs


def standard_kpi_transform(start_days_ago: int,
                           end_days_ago: int,
                           query_results: list[dict[str, Any]],
                           group_by: GroupBy,
                           time_unit='daily'):
    min_date_id = min(result['date_id'] for result in query_results)
    datetime_range = get_dt_range(start_days_ago, end_days_ago, min_date_id, time_unit)
    kpis = [kpi for kpi in query_results[0] if kpi not in {'region', 'date_id', 'cluster_id'}]

    if group_by.value == 'region':
        regions = {result['region'] for result in query_results}
        final_results_template = {region: {kpi: [[day, None]
                                                 for day in datetime_range] for kpi in kpis}
                                  for region in
                                  regions}

        for region in regions:
            query_results_region = [record for record in query_results if record['region'] == region]
            date_ids = {result['date_id'] for result in query_results_region}

            if len(date_ids) != len(query_results_region):
                raise ValueError('There are duplicate date_ids in the query results')

            date_to_result = {date_id: next(record for record in query_results_region
                                            if record['date_id'] == date_id)
                              for date_id in date_ids}
            for kpi in kpis:
                for day in [day for day in datetime_range if day in date_to_result]:
                    final_results_template[region][kpi][datetime_range.index(day)][1] = date_to_result[day][kpi]

        return final_results_template

    final_results_template = {kpi: [[day, None] for day in datetime_range] for kpi in kpis}
    date_ids = {result['date_id'] for result in query_results}

    if len(date_ids) != len(query_results):
        raise ValueError('There are duplicate date_ids in the query results')

    date_to_result = {date_id: next(record for record in query_results if record['date_id'] == date_id)
                      for date_id in date_ids}
    for kpi in kpis:
        for day in [day for day in datetime_range if day in date_to_result]:
            final_results_template[kpi][datetime_range.index(day)][1] = date_to_result[day][kpi]

    return final_results_template


def flex_kpi_transform(start_days_ago: int,
                       end_days_ago: int,
                       query_results: list[dict[str, Any]],
                       group_by: GroupBy,
                       time_unit='daily'):
    min_date_id = min(result['date_id'] for result in query_results)
    datetime_range = get_dt_range(start_days_ago, end_days_ago, min_date_id, time_unit)
    kpis = [kpi for kpi in query_results[0] if kpi not in {'region', 'date_id', 'cluster_id', 'mno'}]
    if group_by.value == 'region':
        regions = {result['region'] for result in query_results}
        final_results_template = {
            region: {kpi: {mno: [[day, None]
                                 for day in datetime_range
                                 ]
                           for mno in MNOs}
                     for kpi in kpis}
            for region in regions}

        for region in regions:
            for mno in MNOs:
                query_results_region = [record for record in query_results if
                                        record['region'] == region and record['mno'] == mno]
                date_ids = {result['date_id'] for result in query_results_region}
                if len(date_ids) != len(query_results_region):
                    raise ValueError('There are duplicate date_ids in the query results')

                date_to_result = {
                    date_id: next(record for record in query_results_region if record['date_id'] == date_id)
                    for date_id in date_ids}

                for kpi in kpis:
                    for day in [day for day in datetime_range if day in date_to_result]:
                        final_results_template[region][kpi][mno][datetime_range.index(day)][1] = \
                            date_to_result[day][kpi]

        return final_results_template

    final_results_template = {kpi: {mno: [[day, None] for day in datetime_range] for mno in MNOs} for kpi in kpis}
    date_ids = {result['date_id'] for result in query_results}
    if len(date_ids) != len(query_results):
        raise ValueError('There are duplicate date_ids in the query results')
    date_to_result = {date_id: next(record for record in query_results if record['date_id'] == date_id)
                      for date_id in date_ids}
    for mno in MNOs:
        for kpi in kpis:
            for day in [day for day in datetime_range if day in date_to_result]:
                final_results_template[kpi][mno][datetime_range.index(day)][1] = date_to_result[day][kpi][mno]
    return final_results_template


def get_dt_daily_range(start_days_ago: int, end_days_ago: int, min_date_id):
    days_range = [datetime.date.today() - datetime.timedelta(days=x) for x in
                  range(start_days_ago, end_days_ago - 1, -1)]
    return [day for day in days_range if day >= min_date_id]


def get_dt_hourly_range(start_days_ago, min_date_id):
    hours_range = pd.date_range(datetime.now().date() - pd.Timedelta(days=start_days_ago),
                                datetime.now(),
                                freq='H')
    return [hour for hour in hours_range if hour >= min_date_id]


@lru_cache
def get_dt_range(start_days_ago, end_days_ago, min_date_id, time_unit='daily'):
    if time_unit == 'daily':
        return get_dt_daily_range(start_days_ago, end_days_ago, min_date_id)
    if time_unit == 'hourly':
        return get_dt_hourly_range(start_days_ago, min_date_id)


if __name__ == '__main__':
    print(get_dt_hourly_range(5, 0, datetime.datetime.now()))
