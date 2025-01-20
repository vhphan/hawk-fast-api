tables_names = {
    'lte': [
        'dc_e_erbs_eutrancellfdd_raw',
        'dc_e_erbs_eutrancellfdd_flex_raw',
        'dc_e_erbs_eutrancellfdd_v_raw',
        'dc_e_erbs_eutrancellrelation_raw',
    ],
    'nr': [
        'dc_e_nr_nrcellcu_raw',
        'dc_e_nr_nrcelldu_raw',
        'dc_e_nr_events_nrcellcu_flex_raw',
        'dc_e_nr_events_nrcelldu_flex_raw',
        'dc_e_nr_nrcelldu_v_raw',
        'dc_e_erbsg2_mpprocessingresource_v_raw',
        'dc_e_vpp_rpuserplanelink_v_raw',
    ],
}

dependencies = {
    'dc_e_erbs_eutrancellfdd_raw': [
        'dc_e_erbs_eutrancellfdd_flex_raw',
        'dc_e_erbs_eutrancellfdd_v_raw'
    ]
}
