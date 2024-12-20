create table sa_kpi_results.cell_standard
(
    date_id                                date,
    nrcellcu                               varchar(256),
    ho_success_rate_5g_nom                 double precision,
    ho_success_rate_5g_den                 double precision,
    inter_rat_ho_success_rate_nom          double precision,
    inter_rat_ho_success_rate_den          double precision,
    eps_fallback_attempt                   double precision,
    average_number_of_user_rrc_nom         double precision,
    average_number_of_user_rrc_den         double precision,
    sa_max_rrc_connected_user_no           double precision,
    vonr_call_setup_success_rate_nom       double precision,
    vonr_call_setup_success_rate_den       double precision,
    vonr_drop_call_rate_nom                double precision,
    vonr_drop_call_rate_den                double precision,
    sa_data_session_setup_success_rate_nom double precision,
    sa_data_session_setup_success_rate_den double precision,
    sa_data_session_abnormal_release_nom   double precision,
    sa_data_session_abnormal_release_den   double precision,
    mean_user_dl_throughput_nr_nom         double precision,
    mean_user_dl_throughput_nr_den         double precision,
    mean_user_ul_throughput_nr_nom         double precision,
    mean_user_ul_throughput_nr_den         double precision,
    uplink_traffic_volume_nr_gb_nom        double precision,
    uplink_traffic_volume_nr_gb_den        double precision,
    downlink_traffic_volume_nr_gb_nom      double precision,
    downlink_traffic_volume_nr_gb_den      double precision,
    modulation_dl_den                      double precision,
    dl_qpsk_nom                            double precision,
    dl_16qam_nom                           double precision,
    dl_64qam_nom                           double precision,
    dl_256qam_nom                          double precision,
    rrc_resume_sr_nom                      double precision,
    rrc_resume_sr_den                      double precision,
    total_traffic_volume_gb_nom            double precision,
    total_traffic_volume_gb_den            double precision,
    constraint cell_standard_pk
        unique (date_id, nrcellcu)
) PARTITION BY RANGE (date_id);

DO
$$
    DECLARE
        start_year  INT  := 2024;
        end_year    INT  := 2027;
        schema_name TEXT := 'sa_kpi_results';
    BEGIN
        FOR year IN start_year..end_year
            LOOP
                FOR month IN 1..12
                    LOOP
                        BEGIN
                            EXECUTE format('
                    CREATE TABLE %I.cell_flex_%s_%s PARTITION OF %I.cell_flex
                    FOR VALUES FROM (''%s-%s-01'') TO (''%s-%s-01'');
                ', schema_name, year, to_char(month, 'FM00'), schema_name, year, to_char(month, 'FM00'),
                                           CASE WHEN month = 12 THEN year + 1 ELSE year END,
                                           CASE WHEN month = 12 THEN '01' ELSE to_char(month + 1, 'FM00') END);
                            RAISE NOTICE 'Partition for %-% created.', year, to_char(month, 'FM00');
                        EXCEPTION
                            WHEN duplicate_table THEN
                                RAISE NOTICE 'Partition for %-% already exists.', year, to_char(month, 'FM00');
                        END;
                    END LOOP;
            END LOOP;
    END
$$;

create table sa_kpi_results.cell_flex
(
    date_id                                date,
    nrcellcu                               varchar(255),
    flex_filtername                        varchar(255),
    mno                                    varchar(255),
    ho_success_rate_5g_nom                 double precision,
    ho_success_rate_5g_den                 double precision,
    inter_rat_ho_success_rate_nom          double precision,
    inter_rat_ho_success_rate_den          double precision,
    eps_fallback_attempt                   double precision,
    average_number_of_user_rrc_nom         double precision,
    average_number_of_user_rrc_den         double precision,
    sa_max_rrc_connected_user_no           double precision,
    mean_user_dl_throughput_nr_nom         double precision,
    mean_user_dl_throughput_nr_den         double precision,
    mean_user_ul_throughput_nr_nom         double precision,
    mean_user_ul_throughput_nr_den         double precision,
    uplink_traffic_volume_nr_gb_nom        double precision,
    uplink_traffic_volume_nr_gb_den        double precision,
    downlink_traffic_volume_nr_gb_nom      double precision,
    downlink_traffic_volume_nr_gb_den      double precision,
    modulation_dl_den                      double precision,
    dl_qpsk_nom                            double precision,
    dl_16qam_nom                           double precision,
    dl_64qam_nom                           double precision,
    dl_256qam_nom                          double precision,
    rrc_resume_sr_nom                      double precision,
    rrc_resume_sr_den                      double precision,
    total_traffic_volume_gb_nom            double precision,
    total_traffic_volume_gb_den            double precision,
    vonr_call_setup_success_rate_nom       double precision,
    vonr_call_setup_success_rate_den       double precision,
    vonr_drop_call_rate_nom                double precision,
    vonr_drop_call_rate_den                double precision,
    sa_data_session_setup_success_rate_nom double precision,
    sa_data_session_setup_success_rate_den double precision,
    sa_data_session_abnormal_release_nom   double precision,
    sa_data_session_abnormal_release_den   double precision,
    dl_prb_utilization_nom                 double precision,
    dl_prb_utilization_den                 double precision,
    ul_prb_utilization_nom                 double precision,
    ul_prb_utilization_den                 double precision,
    constraint cell_flex_pk
        unique (nrcellcu, date_id, flex_filtername, mno)
)
    partition by RANGE (date_id);