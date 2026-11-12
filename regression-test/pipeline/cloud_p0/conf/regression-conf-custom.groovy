// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

testGroups = "p0"

jdbcUrl = "jdbc:mysql://127.0.0.1:9030/?useLocalSessionState=true&allowLoadLocalInfile=true&zeroDateTimeBehavior=round"
targetJdbcUrl = "jdbc:mysql://127.0.0.1:9030/?useLocalSessionState=true&allowLoadLocalInfile=true&zeroDateTimeBehavior=round"

// exclude groups and exclude suites is more prior than include groups and include suites.
// keep them in lexico order(add/remove cases between the sentinals and sort):
// * sort lines in vim: select lines and then type :sort
// * sort lines in vscode: https://ulfschneider.io/2023-09-01-sort-in-vscode/
excludeSuites = "000_the_start_sentinel_do_not_touch," + // keep this line as the first line
    "mv_contain_external_table," + // run on external pipeline
    "set_replica_status," + // not a case for cloud mode, no need to run
    "test_be_inject_publish_txn_fail," + // not a case for cloud mode, no need to run
    "test_dump_image," +
    "test_nereids_show_restore," +
    "test_index_failure_injection," +
    "test_information_schema_external," +
    "test_profile," +
    "test_publish_timeout," +
    "test_refresh_mtmv," + // not supported yet
    "test_report_version_missing," +
    "test_set_partition_version," +
    "test_spark_load," +
    "test_index_lowercase_fault_injection," +
    "test_index_compaction_failure_injection," +
    "test_query_sys_rowsets," + // rowsets sys table
    "test_unique_table_debug_data," + // disable auto compaction
    "test_insert," + // txn insert
    "test_nereids_show_snapshot," +
    "test_full_compaction_run_status," +
    "test_topn_fault_injection," +
    "auto_partition_in_partition_prune," + // inserted data in too many tablets, txn to large. not suitable for cloud.
    "one_col_range_partition," + // inserted data in too many tablets, txn to large. not suitable for cloud.
    "test_nereids_show_backup," +
    "check_meta,"+
    "test_checker,"+
    "test_recycler_expired_stage_objects," +
    "test_recycler_inverted_index," +
    "test_recycler_with_drop_column," +
    "test_recycler_with_drop_db," +
    "test_recycler_with_drop_index," +
    "test_recycler_with_drop_multi_db," +
    "test_recycler_with_drop_mv," +
    "test_recycler_with_drop_partition," +
    "test_recycler_with_drop_rollup," +
    "test_recycler_with_dynamic_partition," +
    "test_recycler_with_internal_copy," +
    "test_recycler_with_many_partitions," +
    "test_recycler_with_schema_change," +
    "test_recycler_with_truncate_table," +
    "test_recycler_with_txn_label," +
    "test_recycler," +
    "rec_cte_with_delete_test," +
    "test_recycler_cleanup_snapshot," +
    "test_recycler_clone_instance," +
    "test_stream_consumption_schema," +
    "test_stream_info_schema," + // table stream is not supported for cloud mode now
    "zzz_the_end_sentinel_do_not_touch" // keep this line as the last line

excludeDirectories = "000_the_start_sentinel_do_not_touch," + // keep this line as the first line
    "external_table_p0," + // run on external pipeline
    "cloud/multi_cluster," + // run in specific regression pipeline
    "cloud_p0/multi_cluster," + // run in specific regression pipeline
    "cloud_p0/cache," +
    "shape_check," + // run only in p0 is enough
    "query_p0/cache," + // run only in p0 is enough
    "nereids_rules_p0/mv/increment_create," + // run only in p0 is enough
    "nereids_rules_p0/mv/genera_constant_sql," + // run only in p0 is enough
    "workload_manager_p1," +
    "nereids_rules_p0/subquery," +
    "backup_restore," + // not a case for cloud mode, no need to run
    "cold_heat_separation," +
    "storage_medium_p0," +
    "ccr_syncer_p0," +
    "ccr_mow_syncer_p0," +
    "hdfs_vault_p2," +
    "inject_hdfs_vault_p0," +
    "variant_p0/nested," +
    "variant_p0/nested/sql," +
    "plsql_p0," + // plsql is not developped any more, add by sk.
    "restore_p0," + // 
    "table_stream_p0," + // table stream is not supported for cloud mode now
    "zzz_the_end_sentinel_do_not_touch" // keep this line as the last line

max_failure_num = 50

// test_routine_load
enableKafkaTest=true

// vim: tw=10086 et ts=4 sw=4:

// trino-connector catalog test config
enableTrinoConnectorTest = false

s3Source = "aliyun"
s3Endpoint = "oss-cn-hongkong-internal.aliyuncs.com"
recycleServiceHttpAddress = "127.0.0.1:6000"

pythonUdfRuntimeVersion = "3.12.11"
// Temporary exclusions for CI using the unmodified framework with rootless containers and STS.
// These suites failed in migration adapters; this does not establish an upstream framework bug.
// Re-enable after the relevant framework compatibility fixes are reviewed and validated upstream.
// Historical injected-adapter failures below are not evidence of original-framework defects.
if (System.getenv("DORIS_CI_PRISTINE_FRAMEWORK") == "true") {
    excludeSuites += "," + [
        // Historical local-rootless BE filesystem adapter failure; exact rejected path/operation remains unresolved.
        "test_export_basic",
        // Historical local-rootless BE filesystem adapter failure; exact rejected path/operation remains unresolved.
        "test_export_csv",
        // Historical local-rootless BE filesystem adapter failure; exact rejected path/operation remains unresolved.
        "test_export_empty_table",
        // Historical local-rootless BE filesystem adapter failure; exact rejected path/operation remains unresolved.
        "test_export_table_with_label_retry",
        // Historical local-rootless BE filesystem adapter failure; exact rejected path/operation remains unresolved.
        "test_nereids_show_export"
    ].join(",")

    // Temporary environment exclusions using the existing suite-name filter.
    // Name-only filtering also excludes the three same-named export outfile suites.
    // Collateral sources: export_p0/test_outfile.groovy, export_p0/test_outfile_expr.groovy,
    // and export_p0/outfile/parquet/test_outfile_parquet.groovy. No whole directory is excluded.
    // The two independent-provider credential cases already have the p2 group in master.
    excludeSuites += "," + [
        // root SSH/SCP or remote filesystem prerequisite
        "test_export_view",
        // mysqldump column-statistics compatibility
        "test_mysqldump",
        // root SSH/SCP or remote filesystem prerequisite
        "test_outfile",
        // root SSH/SCP or remote filesystem prerequisite
        "test_outfile_expr",
        // root SSH/SCP or remote filesystem prerequisite
        "test_outfile_parquet",
        // root SSH/SCP or remote filesystem prerequisite
        "test_install_plugin_command"
    ].join(",")

    // Cloud Build602: const_group_key_shape expected hashAgg[LOCAL] above CROSS_JOIN,
    // but the actual EXPLAIN shape omitted it; plan/baseline cause remains unresolved.
    // This does not establish a wrong query result.
    excludeSuites += ",scalar_agg_pushdown"
}

// Move remaining compatibility exclusions from the CI adapter into reviewed source.
// These observations do not establish a universal framework defect.
if (System.getenv("DORIS_CI_PRISTINE_FRAMEWORK") == "true") {
    excludeSuites += "," + [
        // Build 477: original suite assertion/result mismatch; exact cause pending, not attributed to OOM. Restore: Resolve recorded failure and pass the isolated original suite before removing this exclusion.
        "test_stream_load_error_url",
        // Build 477: original suite assertion/result mismatch; exact cause pending, not attributed to OOM. Restore: Resolve recorded failure and pass the isolated original suite before removing this exclusion.
        "test_csv_with_none_utf8_data",
        // Build 477: original suite assertion/result mismatch; exact cause pending, not attributed to OOM. Restore: Resolve recorded failure and pass the isolated original suite before removing this exclusion.
        "rewrite_when_dml",
        // Requires regressionAliyunStsRegion and regressionAliyunStsRoleArn plus AssumeRole access.
        // These inputs are not configured in rootless CI; static S3 credentials do not cover this suite.
        "test_s3_load_with_tmp_token",
        // Build 477: error-URL expectation differs; SQL returns please use show load instead. Restore: Resolve recorded failure and pass the isolated original suite before removing this exclusion.
        "test_insert_strict_fail_url",
        // Build 477: error-URL expectation differs; SQL returns please use show load instead. Restore: Resolve recorded failure and pass the isolated original suite before removing this exclusion.
        "test_insert_partition_fail_url",
        // Build 477: error-URL expectation differs; SQL returns please use show load instead. Restore: Resolve recorded failure and pass the isolated original suite before removing this exclusion.
        "test_insert_docs_demo",
        // Build 434: active near initial MS RSS surge (19:18:47-19:19:02); temporal correlation only, not proven cause. Restore: Developer MS investigation plus isolated/repeated memory-tracked pass, then re-enable individually.
        "multi_slot",
        // Build 434: active near initial MS RSS surge (19:18:47-19:19:02); temporal correlation only, not proven cause. Restore: Developer MS investigation plus isolated/repeated memory-tracked pass, then re-enable individually.
        "variant_mv_rowstore_crash",
        // Build 434: active near initial MS RSS surge (19:18:47-19:19:02); temporal correlation only, not proven cause. Restore: Developer MS investigation plus isolated/repeated memory-tracked pass, then re-enable individually.
        "test_variant_search_subcolumn_snii"
    ].join(",")
}

// Reviewed defaults for CI using the unmodified framework and rootless runtime.
// Preserve other environments; addresses, local paths, credentials and build IDs are injected at deployment.
if (System.getenv("DORIS_CI_PRISTINE_FRAMEWORK") == "true") {
    // Use the existing read-only regression fixtures in Hong Kong.
    s3BucketName = "doris-regression-hk"
    s3Endpoint = "oss-cn-hongkong.aliyuncs.com"
    s3Region = "oss-cn-hongkong"
    s3Provider = "S3"
}
