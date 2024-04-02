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
// exclude groups and exclude suites is more prior than include groups and include suites.
// keep them in lexico order(add/remove cases between the sentinals and sort):
// * sort lines in vim: select lines and then type :sort
// * sort lines in vscode: https://ulfschneider.io/2023-09-01-sort-in-vscode/
excludeSuites = "000_the_start_sentinel_do_not_touch," + // keep this line as the first line
    "set_and_unset_variable," +
    "set_replica_status," + // not a case for cloud mode, no need to run
    "test_be_inject_publish_txn_fail," + // not a case for cloud mode, no need to run
    "test_bitmap_filter," +
    "test_compaction_uniq_cluster_keys_with_delete," +
    "test_compaction_uniq_keys_cluster_key," +
    "test_disable_move_memtable," +
    "test_dump_image," +
    "test_index_failure_injection," +
    "test_information_schema_external," +
    "test_insert_move_memtable," +
    "test_materialized_view_move_memtable," +
    "test_pk_uk_case_cluster," +
    "test_point_query_cluster_key," +
    "test_profile," +
    "test_publish_timeout," +
    "test_refresh_mtmv," + // not supported yet
    "test_report_version_missing," +
    "test_set_partition_version," +
    "test_show_transaction," + // not supported yet
    "test_spark_load," +
    "test_stream_load_move_memtable," +
    "test_stream_load_new_move_memtable," +
    "test_array_index1," +
    "test_array_index2," +
    "test_index_lowercase_fault_injection," +
    "zzz_the_end_sentinel_do_not_touch" // keep this line as the last line


excludeDirectories = "000_the_start_sentinel_do_not_touch," + // keep this line as the first line
    "cloud/multi_cluster," + // run in specific regression pipeline
    "workload_manager_p1," +
    "nereids_rules_p0/subquery," +
    "unique_with_mow_p0/cluster_key," +
    "unique_with_mow_p0/ssb_unique_sql_zstd_cluster," +
    "unique_with_mow_p0/ssb_unique_load_zstd_c," +
    "backup_restore," + // not a case for cloud mode, no need to run
    "cold_heat_separation," +
    "storage_medium_p0," +
    "zzz_the_end_sentinel_do_not_touch" // keep this line as the last line

max_failure_num = 100

// vim: tw=10086 et ts=4 sw=4:
