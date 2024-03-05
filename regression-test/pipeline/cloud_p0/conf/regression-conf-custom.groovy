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
testDirectories = "ddl_p0,database_p0,load,load_p0,query_p0,table_p0,account_p0,autobucket,bitmap_functions,bloom_filter_p0,cast_decimal_to_boolean,cast_double_to_decimal,compression_p0,connector_p0,correctness,correctness_p0,csv_header_p0,data_model_p0,database_p0,datatype_p0,delete_p0,demo_p0,empty_relation,export_p0,external_table_p0,fault_injection_p0,flink_connector_p0,insert_overwrite_p0,insert_p0,internal_schema_p0,javaudf_p0,job_p0,json_p0,jsonb_p0,meta_action_p0,metrics_p0,mtmv_p0,mysql_fulltext,mysql_ssl_p0,mysql_tupleconvert_p0,mysqldump_p0,nereids_arith_p0,nereids_function_p0,nereids_p0,nereids_rules_p0,nereids_syntax_p0,nereids_tpcds_p0,nereids_tpch_p0,partition_p0,performance_p0,postgres,query_profile,row_store,show_p0,source_p0,sql_block_rule_p0,ssb_unique_load_zstd_p0,ssb_unique_sql_zstd_p0,statistics,table_p0,tpch_unique_sql_zstd_p0,trino_p0,types,types_p0,update,version_p0,view_p0,with_clause_p0,workload_manager_p0,schema_change_p0,variant_p0,variant_github_events_p0_new,variant_github_events_p0,unique_with_mow_p0"
//exclude groups and exclude suites is more prior than include groups and include suites.
excludeSuites = "test_index_failure_injection,test_dump_image,test_profile,test_spark_load,test_refresh_mtmv,test_bitmap_filter,test_information_schema_external,test_primary_key_partial_update_parallel"
excludeDirectories = "workload_manager_p1,nereids_rules_p0/subquery,unique_with_mow_p0/cluster_key,unique_with_mow_p0/ssb_unique_sql_zstd_cluster,point_query_p0,nereids_rules_p0/mv"
