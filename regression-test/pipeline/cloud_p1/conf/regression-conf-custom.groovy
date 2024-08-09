testGroups = "p1"
//exclude groups and exclude suites is more prior than include groups and include suites.
excludeSuites = "000_the_start_sentinel_do_not_touch," + // keep this line as the first line
    "stress_test_insert_into," +
    "test_analyze_stats_p1," +
    "test_big_pad," +
    "test_bitmap_filter," +
    "test_broker_load," +
    "test_profile," +
    "test_refresh_mtmv," +
    "test_spark_load," +
    "zzz_the_end_sentinel_do_not_touch" // keep this line as the last line

excludeDirectories = "000_the_start_sentinel_do_not_touch," + // keep this line as the first line
    "backup_restore," +
    "fault_injection_p0," +
    "workload_manager_p1," +
    "ccr_syncer_p1," +
    "ccr_mow_syncer_p1," +
    "zzz_the_end_sentinel_do_not_touch" // keep this line as the last line

max_failure_num = 50

s3Source = "aliyun"
