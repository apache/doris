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

suite("test_use_mv") {
    // create database and tables
    sql 'DROP DATABASE IF EXISTS test_use_mv'
    sql 'CREATE DATABASE IF NOT EXISTS test_use_mv'
    sql 'use test_use_mv'

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

    sql """drop table if exists t1;"""
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
    waitForRollUpJob("t1", 5000, 1)
    sql """ alter table t1 add rollup r2(k2); """
    waitForRollUpJob("t1", 5000, 1)
    createMV("create materialized view k1_k2_sumk3 as select k1, k2, sum(v1) from t1 group by k1, k2;")
    explain {
        sql """select /*+ no_use_mv */ k1 from t1;"""
        notContains("t1(r1)")
    }
    explain {
        sql """select /*+ no_use_mv(t1) */ k1 from t1;"""
        contains("parameter of no_use_mv hint must be in pairs")
    }
    explain {
        sql """select /*+ no_use_mv(t1.`*`) */ k1 from t1;"""
        contains("t1(t1)")
    }
    explain {
        sql """select /*+ use_mv(t1.`*`) */ k1 from t1;"""
        contains("use_mv hint should only have one mv in one table")
    }
    explain {
        sql """select /*+ use_mv(t1.r1,t1.r2) */ k1 from t1;"""
        contains("use_mv hint should only have one mv in one table")
    }
    explain {
        sql """select /*+ use_mv(t1.r1) use_mv(t1.r2) */ k1 from t1;"""
        contains("one use_mv hint is allowed")
    }
    explain {
        sql """select /*+ no_use_mv(t1.r1) no_use_mv(t1.r2) */ k1 from t1;"""
        contains("only one no_use_mv hint is allowed")
    }
    explain {
        sql """select /*+ no_use_mv(t1.r3) */ k1 from t1;"""
        contains("do not have mv: r3 in table: t1")
    }
    explain {
        sql """select /*+ use_mv(t1.r1) no_use_mv(t1.r1) */ k1 from t1;"""
        contains("conflict mv exist in use_mv and no_use_mv in the same time")
    }
    explain {
        sql """select /*+ use_mv(t1.k1_k2_sumk3) */ k1, k2, sum(v1) from t1 group by k1, k2;"""
        contains("t1(k1_k2_sumk3)")
    }
    explain {
        sql """select /*+ use_mv(t1.k1_k2_sumk3) */ k1, k2, min(v1) from t1 group by k1, k2;"""
        notContains("t1(k1_k2_sumk3)")
    }

}
