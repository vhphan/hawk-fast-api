insert into sa_kpi_results_hourly.dc_e_nr_events_nrcellcu_standard_sa_raw
with dt as (select t1.date_id,
                   nrcellcu,
                   "Region",
                   case when "Cluster_ID" is null then 'No Cluster' else "Cluster_ID" end as "Cluster_ID",
                   nr_name,
                   ho_success_rate_5g_nom,
                   ho_success_rate_5g_den,
                   inter_rat_ho_success_rate_nom,
                   inter_rat_ho_success_rate_den,
                   eps_fallback_attempt,
                   average_number_of_user_rrc_nom,
                   average_number_of_user_rrc_den,
                   sa_max_rrc_connected_user_no
            from hourly_stats.dc_e_nr_events_nrcellcu_standard_sa_raw t1
                     inner join dnb.rfdb.ob_cells_ref ob
                                on t1.nrcellcu = ob."Cellname"
                                    and t1.date_id >= ob.on_board_date::date
            where t1.date_id >= current_date - interval '14 days' and t1.date_id not in (select date_id
                                     from sa_kpi_results_hourly.dc_e_nr_events_nrcellcu_standard_sa_raw
                                     group by date_id)),
     dt2 as (select date_id,
                    "Region"                            as region,
                    "Cluster_ID"                        as cluster_id,
                    sum(ho_success_rate_5g_nom)         as ho_success_rate_5g_nom,
                    sum(ho_success_rate_5g_den)         as ho_success_rate_5g_den,
                    sum(inter_rat_ho_success_rate_nom)  as inter_rat_ho_success_rate_nom,
                    sum(inter_rat_ho_success_rate_den)  as inter_rat_ho_success_rate_den,
                    sum(eps_fallback_attempt)           as eps_fallback_attempt,
                    sum(average_number_of_user_rrc_nom) as average_number_of_user_rrc_nom,
                    sum(average_number_of_user_rrc_den) as average_number_of_user_rrc_den,
                    sum(sa_max_rrc_connected_user_no)   as sa_max_rrc_connected_user_no
             from dt

             group by date_id, rollup ("Region", "Cluster_ID"))
select date_id,
       case when region is null then 'ALL' else region end,
       case when cluster_id is null then 'ALL' else cluster_id end,
       100 * ho_success_rate_5g_nom ||| ho_success_rate_5g_den               as ho_success_rate_5g,
       100 * inter_rat_ho_success_rate_nom ||| inter_rat_ho_success_rate_den as inter_rat_ho_success,
       eps_fallback_attempt,
       average_number_of_user_rrc_nom ||| average_number_of_user_rrc_den     as average_number_of_user_rrc,
       sa_max_rrc_connected_user_no
from dt2;

insert into sa_kpi_results_hourly.dc_e_nr_events_nrcelldu_standard_sa_raw
with dt as (select t1.date_id,
                   nrcelldu,
                   "Region",
                   case when "Cluster_ID" is null then 'No Cluster' else "Cluster_ID" end as "Cluster_ID",
                   nr_name,
                   mean_user_dl_throughput_nr_nom,
                   mean_user_dl_throughput_nr_den,
                   mean_user_ul_throughput_nr_nom,
                   mean_user_ul_throughput_nr_den,
                   uplink_traffic_volume_nr_gb_nom,
                   uplink_traffic_volume_nr_gb_den,
                   downlink_traffic_volume_nr_gb_nom,
                   downlink_traffic_volume_nr_gb_den,
                   modulation_dl_den,
                   dl_qpsk_nom,
                   dl_16qam_nom,
                   dl_64qam_nom,
                   dl_256qam_nom,
                   rrc_resume_sr_nom,
                   rrc_resume_sr_den,
                   total_traffic_volume_gb_nom,
                   total_traffic_volume_gb_den
            from hourly_stats.dc_e_nr_events_nrcelldu_standard_sa_raw t1
                     inner join dnb.rfdb.ob_cells_ref ob
                                on t1.nrcelldu = ob."Cellname"
                                    and t1.date_id >= ob.on_board_date::date
            where t1.date_id >= current_date - interval '14 days' and t1.date_id not in (select date_id
                                     from sa_kpi_results_hourly.dc_e_nr_events_nrcelldu_standard_sa_raw
                                     group by date_id)),
     dt2 as (select date_id,
                    "Region"                               as region,
                    "Cluster_ID"                           as cluster_id,
                    sum(mean_user_dl_throughput_nr_nom)    as mean_user_dl_throughput_nr_nom,
                    sum(mean_user_dl_throughput_nr_den)    as mean_user_dl_throughput_nr_den,
                    sum(mean_user_ul_throughput_nr_nom)    as mean_user_ul_throughput_nr_nom,
                    sum(mean_user_ul_throughput_nr_den)    as mean_user_ul_throughput_nr_den,
                    sum(uplink_traffic_volume_nr_gb_nom)   as uplink_traffic_volume_nr_gb_nom,
                    sum(uplink_traffic_volume_nr_gb_den)   as uplink_traffic_volume_nr_gb_den,
                    sum(downlink_traffic_volume_nr_gb_nom) as downlink_traffic_volume_nr_gb_nom,
                    sum(downlink_traffic_volume_nr_gb_den) as downlink_traffic_volume_nr_gb_den,
                    sum(modulation_dl_den)                 as modulation_dl_den,
                    sum(dl_qpsk_nom)                       as dl_qpsk_nom,
                    sum(dl_16qam_nom)                      as dl_16qam_nom,
                    sum(dl_64qam_nom)                      as dl_64qam_nom,
                    sum(dl_256qam_nom)                     as dl_256qam_nom,
                    sum(rrc_resume_sr_nom)                 as rrc_resume_sr_nom,
                    sum(rrc_resume_sr_den)                 as rrc_resume_sr_den,
                    sum(total_traffic_volume_gb_nom)       as total_traffic_volume_gb_nom,
                    sum(total_traffic_volume_gb_den)       as total_traffic_volume_gb_den
             from dt
             group by date_id, rollup ("Region", "Cluster_ID"))
select date_id,
       case when region is null then 'ALL' else region end,
       case when cluster_id is null then 'ALL' else cluster_id end,
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
from dt2;

insert into sa_kpi_results_hourly.dc_e_nr_events_nrcellcu_v_standard_sa_raw
with dt as (select t1.date_id,
                   nrcellcu,
                   "Region",
                   case when "Cluster_ID" is null then 'No Cluster' else "Cluster_ID" end as "Cluster_ID",
                   nr_name,
                   vonr_call_setup_success_rate_nom,
                   vonr_call_setup_success_rate_den,
                   vonr_drop_call_rate_nom,
                   vonr_drop_call_rate_den,
                   sa_data_session_setup_success_rate_nom,
                   sa_data_session_setup_success_rate_den,
                   sa_data_session_abnormal_release_nom,
                   sa_data_session_abnormal_release_den
            from hourly_stats.dc_e_nr_events_nrcellcu_v_standard_sa_raw t1
                     inner join dnb.rfdb.ob_cells_ref ob
                                on t1.nrcellcu = ob."Cellname"
                                    and t1.date_id >= ob.on_board_date::date
            where t1.date_id >= current_date - interval '14 days' and t1.date_id not in (select date_id
                                     from sa_kpi_results_hourly.dc_e_nr_events_nrcellcu_v_standard_sa_raw
                                     group by date_id)),
     dt2 as (select date_id,
                    "Region"                                    as region,
                    "Cluster_ID"                                as cluster_id,
                    sum(vonr_call_setup_success_rate_nom)       as vonr_call_setup_success_rate_nom,
                    sum(vonr_call_setup_success_rate_den)       as vonr_call_setup_success_rate_den,
                    sum(vonr_drop_call_rate_nom)                as vonr_drop_call_rate_nom,
                    sum(vonr_drop_call_rate_den)                as vonr_drop_call_rate_den,
                    sum(sa_data_session_setup_success_rate_nom) as sa_data_session_setup_success_rate_nom,
                    sum(sa_data_session_setup_success_rate_den) as sa_data_session_setup_success_rate_den,
                    sum(sa_data_session_abnormal_release_nom)   as sa_data_session_abnormal_release_nom,
                    sum(sa_data_session_abnormal_release_den)   as sa_data_session_abnormal_release_den
             from dt
             group by date_id, rollup ("Region", "Cluster_ID"))
select date_id,
       case when region is null then 'ALL' else region end,
       case when cluster_id is null then 'ALL' else cluster_id end,
       100 * vonr_call_setup_success_rate_nom ||| vonr_call_setup_success_rate_den as vonr_call_setup_success_rate,
       100 * vonr_drop_call_rate_nom ||| vonr_drop_call_rate_den                   as vonr_drop_call_rate,
       100 * sa_data_session_setup_success_rate_nom |||
       sa_data_session_setup_success_rate_den                                      as sa_data_session_setup_success_rate,
       100 * sa_data_session_abnormal_release_nom |||
       sa_data_session_abnormal_release_den                                        as sa_data_session_abnormal_release
from dt2;

insert into sa_kpi_results_hourly.dc_e_nr_events_nrcelldu_flex_sa_raw
with dt as (select t1.date_id,
                   nrcelldu,
                   "Region",
                   case when "Cluster_ID" is null then 'No Cluster' else "Cluster_ID" end as "Cluster_ID",
                   nr_name,
                   split_part(flex_filtername, '_', 1) as flex_filtername,
                   mean_user_dl_throughput_nr_nom,
                   mean_user_dl_throughput_nr_den,
                   mean_user_ul_throughput_nr_nom,
                   mean_user_ul_throughput_nr_den,
                   uplink_traffic_volume_nr_gb_nom,
                   uplink_traffic_volume_nr_gb_den,
                   downlink_traffic_volume_nr_gb_nom,
                   downlink_traffic_volume_nr_gb_den,
                   modulation_dl_den,
                   dl_qpsk_nom,
                   dl_16qam_nom,
                   dl_64qam_nom,
                   dl_256qam_nom,
                   rrc_resume_sr_nom,
                   rrc_resume_sr_den,
                   total_traffic_volume_gb_nom,
                   total_traffic_volume_gb_den
            from hourly_stats.dc_e_nr_events_nrcelldu_flex_sa_raw t1
                     inner join dnb.rfdb.ob_cells_ref ob
                                on t1.nrcelldu = ob."Cellname"
                                    and t1.date_id >= ob.on_board_date::date
            where (t1.date_id >= current_date - interval '14 days') and t1.date_id not in (select date_id
                                     from sa_kpi_results_hourly.dc_e_nr_events_nrcelldu_flex_sa_raw
                                     group by date_id)),
     dt2 as (select date_id,
                    "Region"                               as region,
                    "Cluster_ID"                           as cluster_id,
                    flex_filtername,
                    sum(mean_user_dl_throughput_nr_nom)    as mean_user_dl_throughput_nr_nom,
                    sum(mean_user_dl_throughput_nr_den)    as mean_user_dl_throughput_nr_den,
                    sum(mean_user_ul_throughput_nr_nom)    as mean_user_ul_throughput_nr_nom,
                    sum(mean_user_ul_throughput_nr_den)    as mean_user_ul_throughput_nr_den,
                    sum(uplink_traffic_volume_nr_gb_nom)   as uplink_traffic_volume_nr_gb_nom,
                    sum(downlink_traffic_volume_nr_gb_nom) as downlink_traffic_volume_nr_gb_nom,
                    sum(modulation_dl_den)                 as modulation_dl_den,
                    sum(dl_qpsk_nom)                       as dl_qpsk_nom,
                    sum(dl_16qam_nom)                      as dl_16qam_nom,
                    sum(dl_64qam_nom)                      as dl_64qam_nom,
                    sum(dl_256qam_nom)                     as dl_256qam_nom,
                    sum(rrc_resume_sr_nom)                 as rrc_resume_sr_nom,
                    sum(rrc_resume_sr_den)                 as rrc_resume_sr_den,
                    sum(total_traffic_volume_gb_nom)       as total_traffic_volume_gb_nom
             from dt
             group by date_id, flex_filtername, rollup ("Region", "Cluster_ID"))
select date_id,
       case when region is null then 'ALL' else region end               as region,
       case when cluster_id is null then 'ALL' else cluster_id end       as cluster_id,
       rfdb.lookup_mno_using_mccmnc(flex_filtername)                     as mno,
       mean_user_dl_throughput_nr_nom ||| mean_user_dl_throughput_nr_den as mean_user_dl_throughput_nr,
       mean_user_ul_throughput_nr_nom ||| mean_user_ul_throughput_nr_den as mean_user_ul_throughput_nr,
       uplink_traffic_volume_nr_gb_nom ||| 1073741824                    as uplink_traffic_volume_nr_gb,
       downlink_traffic_volume_nr_gb_nom ||| 1073741824                  as downlink_traffic_volume_nr_gb,
       100 * dl_qpsk_nom ||| modulation_dl_den                           as dl_qpsk,
       100 * dl_16qam_nom ||| modulation_dl_den                          as dl_16qam,
       100 * dl_64qam_nom ||| modulation_dl_den                          as dl_64qam,
       100 * dl_256qam_nom ||| modulation_dl_den                         as dl_256qam,
       100 * rrc_resume_sr_nom ||| rrc_resume_sr_den                     as rrc_resume_sr,
       total_traffic_volume_gb_nom ||| 1073741824                        as total_traffic_volume_gb
from dt2;

insert into sa_kpi_results_hourly.dc_e_nr_events_nrcellcu_flex_sa_raw
with dt as (select t1.date_id,
                   nrcellcu,
                   "Region",
                   case when "Cluster_ID" is null then 'No Cluster' else "Cluster_ID" end as "Cluster_ID",
                   flex_filtername,
                   nr_name,
                   ho_success_rate_5g_nom,
                   ho_success_rate_5g_den,
                   inter_rat_ho_success_rate_nom,
                   inter_rat_ho_success_rate_den,
                   eps_fallback_attempt,
                   average_number_of_user_rrc_nom,
                   average_number_of_user_rrc_den,
                   sa_max_rrc_connected_user_no
            from hourly_stats.dc_e_nr_events_nrcellcu_flex_sa_raw t1
                     inner join dnb.rfdb.ob_cells_ref ob
                                on t1.nrcellcu = ob."Cellname"
                                    and t1.date_id >= ob.on_board_date::date
            where t1.date_id >= current_date - interval '14 days' and t1.date_id not in (select date_id
                                     from sa_kpi_results_hourly.dc_e_nr_events_nrcellcu_flex_sa_raw
                                     group by date_id)),
     dt2 as (select date_id,
                    "Region"                            as region,
                    "Cluster_ID"                        as cluster_id,
                    flex_filtername,
                    sum(ho_success_rate_5g_nom)         as ho_success_rate_5g_nom,
                    sum(ho_success_rate_5g_den)         as ho_success_rate_5g_den,
                    sum(inter_rat_ho_success_rate_nom)  as inter_rat_ho_success_rate_nom,
                    sum(inter_rat_ho_success_rate_den)  as inter_rat_ho_success_rate_den,
                    sum(eps_fallback_attempt)           as eps_fallback_attempt,
                    sum(average_number_of_user_rrc_nom) as average_number_of_user_rrc_nom,
                    sum(average_number_of_user_rrc_den) as average_number_of_user_rrc_den,
                    sum(sa_max_rrc_connected_user_no)   as sa_max_rrc_connected_user_no
             from dt
             group by date_id, flex_filtername, rollup ("Region", "Cluster_ID"))
select date_id,
       case when region is null then 'ALL' else region end                   as region,
       case when cluster_id is null then 'ALL' else cluster_id end           as cluster_id,
       rfdb.lookup_mno_using_mccmnc(flex_filtername)                         as mno,
       100 * ho_success_rate_5g_nom ||| ho_success_rate_5g_den               as ho_success_rate_5g,
       100 * inter_rat_ho_success_rate_nom ||| inter_rat_ho_success_rate_den as inter_rat_ho_success_rate,
       eps_fallback_attempt,
       average_number_of_user_rrc_nom ||| average_number_of_user_rrc_den     as average_number_of_user_rrc,
       sa_max_rrc_connected_user_no
from dt2;

insert into sa_kpi_results_hourly.dl_prb_sa_nsa_flex_extended_raw
with dt as (select t1.date_id,
                   nrcelldu,
                   case when mno = 'UMobile' then 'Umobile' when mno = 'Uni5G' then 'TM' else mno end as mno,
                   nr_name,
                   dl_prb_utilization_nom,
                   dl_prb_utilization_den,
                   ul_prb_utilization_nom,
                   ul_prb_utilization_den,
                   "Cellname",
                   dnb_index,
                   "Site_Name",
                   on_board_date,
                   "Region",
                   case when "Cluster_ID" is null then 'No Cluster' else "Cluster_ID" end as "Cluster_ID"
            from hourly_stats.dl_prb_sa_nsa_flex_extended_raw t1
                     inner join dnb.rfdb.ob_cells_ref ob
                                on t1.nrcelldu = ob."Cellname"
                                    and t1.date_id >= ob.on_board_date::date
            where t1.date_id >= current_date - interval '14 days' and t1.date_id not in (select date_id
                                     from sa_kpi_results_hourly.dl_prb_sa_nsa_flex_extended_raw
                                     group by date_id)),
     dt2 as (select date_id,
                    "Region"                          as region,
                    "Cluster_ID"                      as cluster_id,
                    mno,
                    100 * sum(dl_prb_utilization_nom) as dl_prb_utilization_nom,
                    sum(dl_prb_utilization_den) as dl_prb_utilization_den,
                    100 * sum(ul_prb_utilization_nom) as ul_prb_utilization_nom,
                    sum(ul_prb_utilization_den) as ul_prb_utilization_den
             from dt
             group by date_id, mno, rollup ("Region", "Cluster_ID"))
select date_id,
       case when region is null then 'ALL' else region end as region,
       case when cluster_id is null then 'ALL' else cluster_id end as cluster_id,
       mno,
       dl_prb_utilization_nom ||| dl_prb_utilization_den as dl_prb_utilization,
       ul_prb_utilization_nom ||| ul_prb_utilization_den as ul_prb_utilization
from dt2;

insert into sa_kpi_results_hourly.dc_e_nr_events_nrcellcu_v_flex_sa_raw
with dt as (select t1.date_id,
                   nrcellcu,
                   "Region",
                   case when "Cluster_ID" is null then 'No Cluster' else "Cluster_ID" end as "Cluster_ID",
                   nr_name,
                   flex_filtername,
                   vonr_call_setup_success_rate_nom,
                   vonr_call_setup_success_rate_den,
                   vonr_drop_call_rate_nom,
                   vonr_drop_call_rate_den,
                   sa_data_session_setup_success_rate_nom,
                   sa_data_session_setup_success_rate_den,
                   sa_data_session_abnormal_release_nom,
                   sa_data_session_abnormal_release_den
            from hourly_stats.dc_e_nr_events_nrcellcu_v_flex_sa_raw t1
                     inner join dnb.rfdb.ob_cells_ref ob
                                on t1.nrcellcu = ob."Cellname"
                                    and t1.date_id >= ob.on_board_date::date
            where t1.date_id >= current_date - interval '14 days' and t1.date_id not in (select date_id
                                     from sa_kpi_results_hourly.dc_e_nr_events_nrcellcu_v_flex_sa_raw
                                     group by date_id)),
     dt2 as (select date_id,
                    "Region"                                    as region,
                    "Cluster_ID"                                as cluster_id,
                    flex_filtername,
                    sum(vonr_call_setup_success_rate_nom)       as vonr_call_setup_success_rate_nom,
                    sum(vonr_call_setup_success_rate_den)       as vonr_call_setup_success_rate_den,
                    sum(vonr_drop_call_rate_nom)                as vonr_drop_call_rate_nom,
                    sum(vonr_drop_call_rate_den)                as vonr_drop_call_rate_den,
                    sum(sa_data_session_setup_success_rate_nom) as sa_data_session_setup_success_rate_nom,
                    sum(sa_data_session_setup_success_rate_den) as sa_data_session_setup_success_rate_den,
                    sum(sa_data_session_abnormal_release_nom)   as sa_data_session_abnormal_release_nom,
                    sum(sa_data_session_abnormal_release_den)   as sa_data_session_abnormal_release_den
             from dt
             group by date_id, flex_filtername, rollup ("Region", "Cluster_ID"))
select date_id,
       case when region is null then 'ALL' else region end,
       case when cluster_id is null then 'ALL' else cluster_id end,
       dnb.rfdb.lookup_mno_using_mccmnc(flex_filtername)                           as mno,
       100 * vonr_call_setup_success_rate_nom ||| vonr_call_setup_success_rate_den as vonr_call_setup_success_rate,
       100 * vonr_drop_call_rate_nom ||| vonr_drop_call_rate_den                   as vonr_drop_call_rate,
       100 * sa_data_session_setup_success_rate_nom |||
       sa_data_session_setup_success_rate_den                                      as sa_data_session_setup_success_rate,
       100 * sa_data_session_abnormal_release_nom |||
       sa_data_session_abnormal_release_den                                        as sa_data_session_abnormal_release
from dt2;
