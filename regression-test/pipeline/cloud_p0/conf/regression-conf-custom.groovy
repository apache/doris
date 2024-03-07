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
excludeSuites = "test_index_failure_injection,test_dump_image,test_profile,test_spark_load,test_refresh_mtmv,test_bitmap_filter,test_information_schema_external,test_primary_key_partial_update_parallel"
excludeDirectories = "workload_manager_p1,nereids_rules_p0/subquery,unique_with_mow_p0/cluster_key,unique_with_mow_p0/ssb_unique_sql_zstd_cluster,point_query_p0,nereids_rules_p0/mv"
max_failure_num = 200
