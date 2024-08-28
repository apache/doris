/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

suite("test_no_use_index") {
    // create database and tables
    sql 'DROP DATABASE IF EXISTS test_no_use_index'
    sql 'CREATE DATABASE IF NOT EXISTS test_no_use_index'
    sql 'use test_no_use_index'

    // setting planner to nereids
    sql 'set exec_mem_limit=21G'
    sql 'set be_number_for_test=1'
    sql 'set parallel_pipeline_task_num=1'
    sql "set disable_nereids_rules=PRUNE_EMPTY_PARTITION"
    sql 'set enable_nereids_planner=true'
    sql 'set enable_nereids_distribute_planner=false'
    sql "set ignore_shape_nodes='PhysicalProject'"
    sql 'set enable_fallback_to_original_planner=false'
    sql 'set runtime_filter_mode=OFF'
    sql 'set enable_sync_mv_cost_based_rewrite = false'

    // create tables
    sql """
        CREATE TABLE `t1` (
                          `k1` int(11) NULL,
                          `k2` int(11) NULL,
                          `v1` int(11) SUM NULL
                        ) ENGINE=OLAP
                        AGGREGATE KEY(`k1`, `k2`)
                        COMMENT 'OLAP'
                        DISTRIBUTED BY HASH(`k1`) BUCKETS 3
                        PROPERTIES (
                            "replication_allocation" = "tag.location.default: 1",
                            "in_memory" = "false",
                            "storage_format" = "V2",
                            "disable_auto_compaction" = "false"
                        );
    """
    sql """ alter table t1 add rollup r1(k2, k1); """
    Thread.sleep(1000)
    explain {
        sql """select k1 from t1;"""
        contains("mv_k1")
    }
    explain {
        sql """select /*+ no_use_index */ k1 from t1;"""
        notContains("mv_k1")
    }
    explain {
        sql """select /*+ no_use_index(t1) */ k1 from t1;"""
        contains("Use index hint should be zero or more than one parameter")
    }
    explain {
        sql """select /*+ no_use_index(t1 r1) */ k1 from t1;"""
        notContains("mv_k1")
    }
    explain {
        sql """select /*+ no_use_index(t1 r2) */ k1 from t1;"""
        contains("mv_k1")
    }
    sql """drop table if exists t1;"""
}
