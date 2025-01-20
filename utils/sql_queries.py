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
    'flex': """
        select * from dnb.sa_kpi_results.get_kpi_data_cells_flex(:cells_array)
    """,
    'standard_raw': {
        'dc_e_nr_events_nrcellcu_standard_sa_day': "select * from dnb.daily_stats.dc_e_nr_events_nrcellcu_standard_sa_day where nrcellcu = ANY(:cells_array) and date_id >= current_date - 90;",
        'dc_e_nr_events_nrcellcu_v_standard_sa_day': "select * from dnb.daily_stats.dc_e_nr_events_nrcellcu_v_standard_sa_day where nrcellcu = ANY(:cells_array) and date_id >= current_date - 90;",
        'dc_e_nr_events_nrcelldu_standard_sa_day': "select * from dnb.daily_stats.dc_e_nr_events_nrcelldu_standard_sa_day where nrcelldu = ANY(:cells_array) and date_id >= current_date - 90;",
    },
    'flex_raw': {
        'dc_e_nr_events_nrcellcu_flex_sa_day': "select * from dnb.daily_stats.dc_e_nr_events_nrcellcu_flex_sa_day where nrcellcu = ANY(:cells_array) and date_id >= current_date - 90;",
        'dc_e_nr_events_nrcellcu_v_flex_sa_day': "select * from dnb.daily_stats.dc_e_nr_events_nrcellcu_v_flex_sa_day where nrcellcu = ANY(:cells_array) and date_id >= current_date - 90;",
        'dc_e_nr_events_nrcelldu_flex_sa_day': "select * from dnb.daily_stats.dc_e_nr_events_nrcelldu_flex_sa_day where nrcelldu = ANY(:cells_array) and date_id >= current_date - 90;",
        'dl_prb_sa_nsa_flex_extended_day': "select * from dnb.daily_stats.dl_prb_sa_nsa_flex_extended_day where nrcelldu = ANY(:cells_array) and date_id >= current_date - 90;",
    }
}

hourly_cells_queries = {

    'standard': """
        select date_id,
        100 * ho_success_rate_5g_nom ||| ho_success_rate_5g_den               as ho_success_rate_5g,
        100 * inter_rat_ho_success_rate_nom ||| inter_rat_ho_success_rate_den as inter_rat_ho_success,
        eps_fallback_attempt,
        average_number_of_user_rrc_nom ||| average_number_of_user_rrc_den     as average_number_of_user_rrc,
        sa_max_rrc_connected_user_no,
        
        
        100 * vonr_call_setup_success_rate_nom ||| vonr_call_setup_success_rate_den as vonr_call_setup_success_rate,
       100 * vonr_drop_call_rate_nom ||| vonr_drop_call_rate_den                   as vonr_drop_call_rate,
       100 * sa_data_session_setup_success_rate_nom |||
       sa_data_session_setup_success_rate_den                                      as sa_data_session_setup_success_rate,
       100 * sa_data_session_abnormal_release_nom |||
       sa_data_session_abnormal_release_den                                        as sa_data_session_abnormal_release,
       
        mean_user_dl_throughput_nr_nom ||| mean_user_dl_throughput_nr_den as mean_user_dl_throughput_nr,
        mean_user_ul_throughput_nr_nom ||| mean_user_ul_throughput_nr_den as mean_user_ul_throughput_nr,
        dl_16qam_nom ||| modulation_dl_den                                as dl_16qam,
        dl_64qam_nom ||| modulation_dl_den                                as dl_64qam,
        dl_256qam_nom ||| modulation_dl_den                               as dl_256qam,
        dl_qpsk_nom ||| modulation_dl_den                                 as dl_qpsk,
        rrc_resume_sr_nom ||| rrc_resume_sr_den                           as rrc_resume_sr,
        uplink_traffic_volume_nr_gb_nom ||| (1024 * 1024 * 1024)          as uplink_traffic_volume_nr_gb,
        downlink_traffic_volume_nr_gb_nom ||| (1024 * 1024 * 1024)        as downlink_traffic_volume_nr_gb,
        total_traffic_volume_gb_nom ||| (1024 * 1024 * 1024)              as total_traffic_volume_gb
       
        from dnb.sa_kpi_results_hourly.cell_standard where nrcellcu = :cell
        and date_id >= current_date - 14
        ;
    """,

    'flex': """
        select 
        date_id,
        mno,
        100 * ho_success_rate_5g_nom ||| ho_success_rate_5g_den               as ho_success_rate_5g,
        100 * inter_rat_ho_success_rate_nom ||| inter_rat_ho_success_rate_den as inter_rat_ho_success,
        eps_fallback_attempt,
        average_number_of_user_rrc_nom ||| average_number_of_user_rrc_den     as average_number_of_user_rrc,
        sa_max_rrc_connected_user_no,
        
        100 * vonr_call_setup_success_rate_nom ||| vonr_call_setup_success_rate_den as vonr_call_setup_success_rate,
        100 * vonr_drop_call_rate_nom ||| vonr_drop_call_rate_den                   as vonr_drop_call_rate,
        100 * sa_data_session_setup_success_rate_nom |||
        sa_data_session_setup_success_rate_den                                      as sa_data_session_setup_success_rate,
        100 * sa_data_session_abnormal_release_nom |||
        sa_data_session_abnormal_release_den                                        as sa_data_session_abnormal_release,
        
        mean_user_dl_throughput_nr_nom ||| mean_user_dl_throughput_nr_den as mean_user_dl_throughput_nr,
        mean_user_ul_throughput_nr_nom ||| mean_user_ul_throughput_nr_den as mean_user_ul_throughput_nr,
        dl_16qam_nom ||| modulation_dl_den                                as dl_16qam,
        dl_64qam_nom ||| modulation_dl_den                                as dl_64qam,
        dl_256qam_nom ||| modulation_dl_den                               as dl_256qam,
        dl_qpsk_nom ||| modulation_dl_den                                 as dl_qpsk,
        rrc_resume_sr_nom ||| rrc_resume_sr_den                           as rrc_resume_sr,
        uplink_traffic_volume_nr_gb_nom ||| (1024 * 1024 * 1024)          as uplink_traffic_volume_nr_gb,
        downlink_traffic_volume_nr_gb_nom ||| (1024 * 1024 * 1024)        as downlink_traffic_volume_nr_gb,
        total_traffic_volume_gb_nom ||| (1024 * 1024 * 1024)              as total_traffic_volume_gb, 
        dl_prb_utilization_nom ||| dl_prb_utilization_den as dl_prb_utilization,
        ul_prb_utilization_nom ||| ul_prb_utilization_den as ul_prb_utilization
         from dnb.sa_kpi_results_hourly.cell_flex where nrcellcu = :cell
         and date_id >= current_date - 14
         ;
    """,

    'standard_raw': {
        'dc_e_nr_events_nrcellcu_standard_sa_day': "select * from dnb.hourly_stats.dc_e_nr_events_nrcellcu_standard_sa_raw where nrcellcu = ANY(:cells_array) and date_id >= current_date - 14;",
        'dc_e_nr_events_nrcellcu_v_standard_sa_raw': "select * from dnb.hourly_stats.dc_e_nr_events_nrcellcu_v_standard_sa_raw where nrcellcu = ANY(:cells_array) and date_id >= current_date - 14;",
        'dc_e_nr_events_nrcelldu_standard_sa_raw': "select * from dnb.hourly_stats.dc_e_nr_events_nrcelldu_standard_sa_raw where nrcelldu = ANY(:cells_array) and date_id >= current_date - 14;",
    },
    'flex_raw': {
        'dc_e_nr_events_nrcellcu_flex_sa_raw': "select * from dnb.hourly_stats.dc_e_nr_events_nrcellcu_flex_sa_raw where nrcellcu = ANY(:cells_array) and date_id >= current_date - 14;",
        'dc_e_nr_events_nrcellcu_v_flex_sa_raw': "select * from dnb.hourly_stats.dc_e_nr_events_nrcellcu_v_flex_sa_raw where nrcellcu = ANY(:cells_array) and date_id >= current_date - 14;",
        'dc_e_nr_events_nrcelldu_flex_sa_raw': "select * from dnb.hourly_stats.dc_e_nr_events_nrcelldu_flex_sa_raw where nrcelldu = ANY(:cells_array) and date_id >= current_date - 14;",
        'dl_prb_sa_nsa_flex_extended_raw': "select * from dnb.hourly_stats.dl_prb_sa_nsa_flex_extended_raw where nrcelldu = ANY(:cells_array) and date_id >= current_date - 14;",
    }

}
