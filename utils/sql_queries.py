
daily_region_queries = {
    'standard': """
    select * from dnb.sa_kpi_results.dc_e_nr_events_nrcellcu_standard_sa_day t1
    left join dnb.sa_kpi_results.dc_e_nr_events_nrcellcu_v_standard_sa_day t2 using (region, cluster_id, date_id)
    left join dnb.sa_kpi_results.dc_e_nr_events_nrcelldu_standard_sa_day t3 using (region, cluster_id, date_id)
    where (t1.cluster_id='ALL' and t1.date_id >= current_date - 180) order by t1.date_id
    ;
    """,
    'flex': """
    select * from dnb.sa_kpi_results.dc_e_nr_events_nrcellcu_flex_sa_day t1
    left join dnb.sa_kpi_results.dc_e_nr_events_nrcellcu_v_flex_sa_day t2 using (region, cluster_id, mno, date_id)
    left join dnb.sa_kpi_results.dc_e_nr_events_nrcelldu_flex_sa_day t3 using (region, cluster_id, mno, date_id)
    left join dnb.sa_kpi_results.dl_prb_sa_nsa_flex_extended_day t4 using (region, cluster_id, mno, date_id)
    where (t1.cluster_id='ALL' and t1.date_id >= current_date - 180) order by t1.date_id
    ;
    """,
}

hourly_region_queries = {
    'standard': """
       select * from dnb.sa_kpi_results_hourly.dc_e_nr_events_nrcellcu_standard_sa_raw t1
       left join dnb.sa_kpi_results_hourly.dc_e_nr_events_nrcellcu_v_standard_sa_raw t2 using (region, cluster_id, date_id)
       left join dnb.sa_kpi_results_hourly.dc_e_nr_events_nrcelldu_standard_sa_raw t3 using (region, cluster_id, date_id)
       where (t1.cluster_id='ALL' and t1.date_id >= current_date - 14) order by t1.date_id
       ;
       """,
    'flex': """
       select * from dnb.sa_kpi_results_hourly.dc_e_nr_events_nrcellcu_flex_sa_raw t1
       left join dnb.sa_kpi_results_hourly.dc_e_nr_events_nrcellcu_v_flex_sa_raw t2 using (region, cluster_id, mno, date_id)
       left join dnb.sa_kpi_results_hourly.dc_e_nr_events_nrcelldu_flex_sa_raw t3 using (region, cluster_id, mno, date_id)
       left join dnb.sa_kpi_results_hourly.dl_prb_sa_nsa_flex_extended_raw t4 using (region, cluster_id, mno, date_id)
       where (t1.cluster_id='ALL' and t1.date_id >= current_date - 14) order by t1.date_id
       ;
       """,
}

daily_cluster_queries = {
    'standard': """
    select * from dnb.sa_kpi_results.dc_e_nr_events_nrcellcu_standard_sa_day t1
    left join dnb.sa_kpi_results.dc_e_nr_events_nrcellcu_v_standard_sa_day t2 using (region, cluster_id, date_id)
    left join dnb.sa_kpi_results.dc_e_nr_events_nrcelldu_standard_sa_day t3 using (region, cluster_id, date_id)
    where (t1.cluster_id = :cluster_id and t1.date_id >= current_date - 180) order by t1.date_id
    ;
    """,
    'flex': """
    select * from dnb.sa_kpi_results.dc_e_nr_events_nrcellcu_flex_sa_day t1
    left join dnb.sa_kpi_results.dc_e_nr_events_nrcellcu_v_flex_sa_day t2 using (region, cluster_id, mno, date_id)
    left join dnb.sa_kpi_results.dc_e_nr_events_nrcelldu_flex_sa_day t3 using (region, cluster_id, mno, date_id)
    left join dnb.sa_kpi_results.dl_prb_sa_nsa_flex_extended_day t4 using (region, cluster_id, mno, date_id)
    where (t1.cluster_id = :cluster_id and t1.date_id >= current_date - 180) order by t1.date_id
    ;
    """,
}

standard_tables = {
    "daily": [
        "dc_e_nr_events_nrcellcu_standard_sa_day",
        "dc_e_nr_events_nrcellcu_v_standard_sa_day",
        "dc_e_nr_events_nrcelldu_standard_sa_day",
    ]


}

daily_cells_queries = {
    'standard': """
        select * from dnb.sa_kpi_results.get_kpi_data_cells_standard(:cells_array)
    """,
    'standard_raw': {
        'dc_e_nr_events_nrcellcu_standard_sa_day': "select * from dnb.daily_stats.dc_e_nr_events_nrcellcu_standard_sa_day where nrcellcu = ANY(:cells_array) and date_id >= current_date - 90;",
        'dc_e_nr_events_nrcellcu_v_standard_sa_day': "select * from dnb.daily_stats.dc_e_nr_events_nrcellcu_v_standard_sa_day where nrcellcu = ANY(:cells_array) and date_id >= current_date - 90;",
        'dc_e_nr_events_nrcelldu_standard_sa_day': "select * from dnb.daily_stats.dc_e_nr_events_nrcelldu_standard_sa_day where nrcelldu = ANY(:cells_array) and date_id >= current_date - 90;",

    }
}

hourly_cells_queries = {
    'standard': """
        select * from dnb.sa_kpi_results_hourly.get_kpi_data_cells_standard_hourly(:cells_array)
    """,
    'standard_raw': {
        'dc_e_nr_events_nrcellcu_standard_sa_day': "select * from dnb.hourly_stats.dc_e_nr_events_nrcellcu_standard_sa_raw where nrcellcu = ANY(:cells_array) and date_id >= current_date - 14;",
        'dc_e_nr_events_nrcellcu_v_standard_sa_raw': "select * from dnb.hourly_stats.dc_e_nr_events_nrcellcu_v_standard_sa_raw where nrcellcu = ANY(:cells_array) and date_id >= current_date - 14;",
        'dc_e_nr_events_nrcelldu_standard_sa_raw': "select * from dnb.hourly_stats.dc_e_nr_events_nrcelldu_standard_sa_raw where nrcelldu = ANY(:cells_array) and date_id >= current_date - 14;",
    }
}
