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
