import pandas as pd
from dotenv import load_dotenv
from loguru import logger
from pendulum import today
from setproctitle import setproctitle

from databases.pgdb import PgDB


def get_datetime_range(time_unit='hourly'):
    if time_unit == 'hourly':
        start = (today().subtract(days=15)).date()
        today_end = today().end_of('day').strftime('%Y-%m-%d %H:00:00')
        return pd.date_range(start=start, end=today_end, freq='H').strftime('%Y-%m-%d %H:00:00')
    elif time_unit == 'daily':
        start = (today().subtract(days=180)).date()
        return pd.date_range(start=start, end=today().date(), freq='D').strftime('%Y-%m-%d')
    raise ValueError(f"Invalid time_unit: {time_unit}")


def create_partitions(time_unit='hourly'):
    logger.info(f'Creating partitions for {time_unit}')
    with PgDB() as db:
        if time_unit == 'hourly':
            db.query("SELECT sa_kpi_results_hourly.create_partitions_flex_15_days()")
            db.query("SELECT sa_kpi_results_hourly.create_partitions_std_15_days()")
        elif time_unit == 'daily':
            db.query("SELECT sa_kpi_results_daily.create_partitions_flex_180_days()")
            db.query("SELECT sa_kpi_results_daily.create_partitions_std_180_days()")
        else:
            raise ValueError(f"Invalid time_unit: {time_unit}")


def get_max_date_in_data(time_unit='hourly', kpi_type='standard'):
    with PgDB() as db:
        match (time_unit, kpi_type):
            case ('hourly', 'standard'):
                max_date_std = db.query("SELECT max(date_id) as max_date FROM dnb.sa_kpi_results_hourly.cell_standard;",
                                        return_json=True)[0]['max_date']
                return max_date_std
            case ('hourly', 'flex'):
                max_date_flex = db.query("SELECT max(date_id) as max_date FROM dnb.sa_kpi_results_hourly.cell_flex;",
                                         return_json=True)[0]['max_date']
                return max_date_flex
            case ('daily', 'standard'):
                max_date_std = db.query("SELECT max(date_id) as max_date FROM dnb.sa_kpi_results.cell_standard;",
                                        return_json=True)[0]['max_date']
                return max_date_std
            case ('daily', 'flex'):
                max_date_flex = db.query("SELECT max(date_id) as max_date FROM dnb.sa_kpi_results.cell_flex;",
                                         return_json=True)[0]['max_date']
                return max_date_flex
            case _:
                raise ValueError(f"Invalid time_unit: {time_unit}")


def insert_hourly_kpi_type(kpi_type='standard'):
    max_date = get_max_date_in_data('hourly', kpi_type)
    table_suffix = 'standard' if kpi_type == 'standard' else 'flex'
    with PgDB() as db:
        for date_id in get_datetime_range('hourly'):
            if pd.Timestamp(date_id) <= max_date:
                logger.info(f'Skipping {date_id}')
                continue
            sql_query = f"""
                insert into sa_kpi_results_hourly.cell_{table_suffix}_{date_id[0:10].replace('-', '_')}
                with dt as (select t1.date_id,
                           t1.nrcellcu,
                           {'t1.flex_filtername,' if kpi_type == 'flex' else ''}
                           {'t1.mno,' if kpi_type == 'flex' else ''}
                           ho_success_rate_5g_nom                 as ho_success_rate_5g_nom,
                           ho_success_rate_5g_den                 as ho_success_rate_5g_den,
                           inter_rat_ho_success_rate_nom          as inter_rat_ho_success_rate_nom,
                           inter_rat_ho_success_rate_den          as inter_rat_ho_success_rate_den,
                           eps_fallback_attempt                   as eps_fallback_attempt,
                           average_number_of_user_rrc_nom         as average_number_of_user_rrc_nom,
                           average_number_of_user_rrc_den         as average_number_of_user_rrc_den,
                           sa_max_rrc_connected_user_no           as sa_max_rrc_connected_user_no,
                           mean_user_dl_throughput_nr_nom         as mean_user_dl_throughput_nr_nom,
                           mean_user_dl_throughput_nr_den         as mean_user_dl_throughput_nr_den,
                           mean_user_ul_throughput_nr_nom         as mean_user_ul_throughput_nr_nom,
                           mean_user_ul_throughput_nr_den         as mean_user_ul_throughput_nr_den,
                           uplink_traffic_volume_nr_gb_nom        as uplink_traffic_volume_nr_gb_nom,
                           uplink_traffic_volume_nr_gb_den        as uplink_traffic_volume_nr_gb_den,
                           downlink_traffic_volume_nr_gb_nom      as downlink_traffic_volume_nr_gb_nom,
                           downlink_traffic_volume_nr_gb_den      as downlink_traffic_volume_nr_gb_den,
                           modulation_dl_den                      as modulation_dl_den,
                           dl_qpsk_nom                            as dl_qpsk_nom,
                           dl_16qam_nom                           as dl_16qam_nom,
                           dl_64qam_nom                           as dl_64qam_nom,
                           dl_256qam_nom                          as dl_256qam_nom,
                           rrc_resume_sr_nom                      as rrc_resume_sr_nom,
                           rrc_resume_sr_den                      as rrc_resume_sr_den,
                           total_traffic_volume_gb_nom            as total_traffic_volume_gb_nom,
                           total_traffic_volume_gb_den            as total_traffic_volume_gb_den,
                           vonr_call_setup_success_rate_nom       as vonr_call_setup_success_rate_nom,
                           vonr_call_setup_success_rate_den       as vonr_call_setup_success_rate_den,
                           vonr_drop_call_rate_nom                as vonr_drop_call_rate_nom,
                           vonr_drop_call_rate_den                as vonr_drop_call_rate_den,
                           sa_data_session_setup_success_rate_nom as sa_data_session_setup_success_rate_nom,
                           sa_data_session_setup_success_rate_den as sa_data_session_setup_success_rate_den,
                           sa_data_session_abnormal_release_nom   as sa_data_session_abnormal_release_nom,
                           sa_data_session_abnormal_release_den   as sa_data_session_abnormal_release_den
                           {', dl_prb_utilization_nom                 as dl_prb_utilization_nom,' if kpi_type == 'flex' else ''}
                           {'dl_prb_utilization_den                 as dl_prb_utilization_den,' if kpi_type == 'flex' else ''}
                           {'ul_prb_utilization_nom                 as ul_prb_utilization_nom,' if kpi_type == 'flex' else ''}
                           {'ul_prb_utilization_den                 as ul_prb_utilization_den' if kpi_type == 'flex' else ''}
                    from dnb.hourly_stats.dc_e_nr_events_nrcellcu_{table_suffix}_sa_raw t1
                             left join dnb.hourly_stats.dc_e_nr_events_nrcelldu_{table_suffix}_sa_raw t2
                                       on t1.date_id = t2.date_id
                                           and t1.nr_name = t2.nr_name
                                           and t1.nrcellcu = t2.nrcelldu
                                           {'and t1.flex_filtername = t2.flex_filtername' if kpi_type == 'flex' else ''}
                             left join dnb.hourly_stats.dc_e_nr_events_nrcellcu_v_{table_suffix}_sa_raw t3
                                       on t1.date_id = t3.date_id
                                           and t1.nr_name = t3.nr_name
                                           and t1.nrcellcu = t3.nrcellcu
                                           {'and t1.flex_filtername = t3.flex_filtername' if kpi_type == 'flex' else ''}
                             left join dnb.hourly_stats.dl_prb_sa_nsa_{table_suffix}_extended_raw t4
                                       on t1.date_id = t4.date_id
                                           and t1.nr_name = t4.nr_name
                                           and t1.nrcellcu = t4.nrcelldu
                                           {'and t1.mno = t4.mno' if kpi_type == 'flex' else ''}
                                where t1.date_id = '{date_id}'
                                and t1.date_id not in (
                                    select date_id
                                    from sa_kpi_results_hourly.cell_{table_suffix}
                                    group by date_id
                                        )
                                    )
                                select *
                                        from dt;
                """
            db.execute(sql_query)
            db.commit()
            logger.info(f'Inserted data {kpi_type} for {date_id}')


def insert_daily_kpi_type(kpi_type='standard'):
    max_date = get_max_date_in_data('daily', kpi_type)
    if max_date is None:
        max_date = pd.Timestamp('2021-01-01')
    table_suffix = 'standard' if kpi_type == 'standard' else 'flex'
    with PgDB() as db:
        for date_id in get_datetime_range('daily'):
            if pd.Timestamp(date_id) <= max_date:
                logger.info(f'Skipping {date_id}')
                continue
            sql_query = f"""
                insert into sa_kpi_results.cell_{table_suffix}
                with dt as (select t1.date_id,
                           t1.nrcellcu,
                           {'t1.flex_filtername,' if kpi_type == 'flex' else ''}
                           {'t1.mno,' if kpi_type == 'flex' else ''}
                           ho_success_rate_5g_nom                 as ho_success_rate_5g_nom,
                           ho_success_rate_5g_den                 as ho_success_rate_5g_den,
                           inter_rat_ho_success_rate_nom          as inter_rat_ho_success_rate_nom,
                           inter_rat_ho_success_rate_den          as inter_rat_ho_success_rate_den,
                           eps_fallback_attempt                   as eps_fallback_attempt,
                           average_number_of_user_rrc_nom         as average_number_of_user_rrc_nom,
                           average_number_of_user_rrc_den         as average_number_of_user_rrc_den,
                           sa_max_rrc_connected_user_no           as sa_max_rrc_connected_user_no,
                           mean_user_dl_throughput_nr_nom         as mean_user_dl_throughput_nr_nom,
                           mean_user_dl_throughput_nr_den         as mean_user_dl_throughput_nr_den,
                           mean_user_ul_throughput_nr_nom         as mean_user_ul_throughput_nr_nom,
                           mean_user_ul_throughput_nr_den         as mean_user_ul_throughput_nr_den,
                           uplink_traffic_volume_nr_gb_nom        as uplink_traffic_volume_nr_gb_nom,
                           uplink_traffic_volume_nr_gb_den        as uplink_traffic_volume_nr_gb_den,
                           downlink_traffic_volume_nr_gb_nom      as downlink_traffic_volume_nr_gb_nom,
                           downlink_traffic_volume_nr_gb_den      as downlink_traffic_volume_nr_gb_den,
                           modulation_dl_den                      as modulation_dl_den,
                           dl_qpsk_nom                            as dl_qpsk_nom,
                           dl_16qam_nom                           as dl_16qam_nom,
                           dl_64qam_nom                           as dl_64qam_nom,
                           dl_256qam_nom                          as dl_256qam_nom,
                           rrc_resume_sr_nom                      as rrc_resume_sr_nom,
                           rrc_resume_sr_den                      as rrc_resume_sr_den,
                           total_traffic_volume_gb_nom            as total_traffic_volume_gb_nom,
                           total_traffic_volume_gb_den            as total_traffic_volume_gb_den,
                           vonr_call_setup_success_rate_nom       as vonr_call_setup_success_rate_nom,
                           vonr_call_setup_success_rate_den       as vonr_call_setup_success_rate_den,
                           vonr_drop_call_rate_nom                as vonr_drop_call_rate_nom,
                           vonr_drop_call_rate_den                as vonr_drop_call_rate_den,
                           sa_data_session_setup_success_rate_nom as sa_data_session_setup_success_rate_nom,
                           sa_data_session_setup_success_rate_den as sa_data_session_setup_success_rate_den,
                           sa_data_session_abnormal_release_nom   as sa_data_session_abnormal_release_nom,
                           sa_data_session_abnormal_release_den   as sa_data_session_abnormal_release_den
                           {', dl_prb_utilization_nom                 as dl_prb_utilization_nom,' if kpi_type == 'flex' else ''}
                           {'dl_prb_utilization_den                 as dl_prb_utilization_den,' if kpi_type == 'flex' else ''}
                           {'ul_prb_utilization_nom                 as ul_prb_utilization_nom,' if kpi_type == 'flex' else ''}
                           {'ul_prb_utilization_den                 as ul_prb_utilization_den' if kpi_type == 'flex' else ''}
                    from dnb.daily_stats.dc_e_nr_events_nrcellcu_{table_suffix}_sa_day t1
                             left join dnb.daily_stats.dc_e_nr_events_nrcelldu_{table_suffix}_sa_day t2
                                       on t1.date_id = t2.date_id
                                           and t1.nr_name = t2.nr_name
                                           and t1.nrcellcu = t2.nrcelldu
                                           {'and t1.flex_filtername = t2.flex_filtername' if kpi_type == 'flex' else ''}
                             left join dnb.daily_stats.dc_e_nr_events_nrcellcu_v_{table_suffix}_sa_day t3
                                       on t1.date_id = t3.date_id
                                           and t1.nr_name = t3.nr_name
                                           and t1.nrcellcu = t3.nrcellcu
                                           {'and t1.flex_filtername = t3.flex_filtername' if kpi_type == 'flex' else ''}
                             left join dnb.daily_stats.dl_prb_sa_nsa_{table_suffix}_extended_day t4
                                       on t1.date_id = t4.date_id
                                           and t1.nr_name = t4.nr_name
                                           and t1.nrcellcu = t4.nrcelldu
                                           {'and t1.mno = t4.mno' if kpi_type == 'flex' else ''}
                                where t1.date_id = '{date_id}'
                                and t1.date_id not in (
                                    select date_id
                                    from sa_kpi_results.cell_{table_suffix}
                                    group by date_id
                                        )
                                    )
                                select *
                                        from dt;
                """
            db.execute(sql_query)
            db.commit()
            logger.info(f'Inserted data {kpi_type} for {date_id}')


def insert_hourly_cell_data():
    setproctitle('__insert_sa_cell_hourly__')
    load_dotenv()
    create_partitions('hourly')
    insert_hourly_kpi_type('standard')
    insert_hourly_kpi_type('flex')


def insert_daily_cell_data():
    setproctitle('__insert_sa_cell_daily__')
    load_dotenv()
    insert_daily_kpi_type('standard')
    insert_daily_kpi_type('flex')

if __name__ == '__main__':
    load_dotenv()
    insert_daily_kpi_type('flex')
