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

suite("test_tablet_prune") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql "drop table if exists t_customers_wide_index"
    sql """
    CREATE TABLE `t_customers_wide_index` (   `CUSTOMER_ID` int NULL,   `ADDRESS` varchar(1500) NULL) ENGINE=OLAP UNIQUE KEY(`CUSTOMER_ID`) DISTRIBUTED BY HASH(`CUSTOMER_ID`) BUCKETS 32 PROPERTIES ( "file_cache_ttl_seconds" = "0", "is_being_synced" = "false", "storage_medium" = "hdd", "storage_format" = "V2", "inverted_index_storage_format" = "V2", "enable_unique_key_merge_on_write" = "true", "light_schema_change" = "true", "store_row_column" = "true", "row_store_page_size" = "16384", "disable_auto_compaction" = "false", "enable_single_replica_compaction" = "false", "group_commit_interval_ms" = "10000", "group_commit_data_bytes" = "134217728", "enable_mow_light_delete" = "false" );    """
    sql """
    insert into t_customers_wide_index values (1, "111");
    """
    explain {
        sql("SELECT * from t_customers_wide_index WHERE customer_id = 1817422;")
        contains "tablets=1/32"
    }
}