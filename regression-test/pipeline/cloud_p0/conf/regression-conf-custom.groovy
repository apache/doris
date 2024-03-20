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
//exclude groups and exclude suites is more prior than include groups and include suites.
excludeSuites = "test_index_failure_injection,test_dump_image,test_profile,test_spark_load,test_refresh_mtmv,test_bitmap_filter,test_information_schema_external,test_stream_load_new_move_memtable,test_stream_load_move_memtable,test_materialized_view_move_memtable,test_disable_move_memtable,test_insert_move_memtable,set_and_unset_variable,test_pk_uk_case_cluster,test_point_query_cluster_key,test_compaction_uniq_cluster_keys_with_delete,test_compaction_uniq_keys_cluster_key,test_set_partition_version,test_show_transaction"

excludeDirectories = """
    cloud/multi_cluster,
    workload_manager_p1,
    nereids_rules_p0/subquery,
    unique_with_mow_p0/cluster_key,
    unique_with_mow_p0/ssb_unique_sql_zstd_cluster,
    unique_with_mow_p0/ssb_unique_load_zstd_c,
    nereids_rules_p0/mv,
    backup_restore,
    cold_heat_separation,
    storage_medium_p0
"""

max_failure_num = 200
