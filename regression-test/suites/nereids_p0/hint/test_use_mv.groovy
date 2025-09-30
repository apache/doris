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
                          `k3` int(11) NULL,
                          `v1` int(11) SUM NULL
                        ) ENGINE=OLAP
                        AGGREGATE KEY(`k1`, `k2`, `k3`)
                        COMMENT 'OLAP'
                        DISTRIBUTED BY HASH(`k1`) BUCKETS 3
                        PROPERTIES (
                            "replication_allocation" = "tag.location.default: 1",
                            "in_memory" = "false",
                            "storage_format" = "V2",
                            "disable_auto_compaction" = "false"
                        );
    """
    sql """ insert into t1 values (101, 101, 101, 102);"""
    sql """ alter table t1 add rollup r1(k2, k1); """
    waitingMVTaskFinishedByMvName("test_use_mv", "t1","r1")
    sql """ alter table t1 add rollup r2(k2); """
    waitingMVTaskFinishedByMvName("test_use_mv", "t1","r2")
    createMV("create materialized view k1_k2_sumk3 as select k1 as a1, k2 as a2, sum(v1) from t1 group by k1, k2;")

    // there is a bug in use_mv or no_use_mv hint when use *, so we comment the related case temporarily
//    explain {
//        sql """select /*+ no_use_mv(t1.*) */ k1 from t1 group by k1;"""
//        notContains("t1(r1)")
//    }
//    explain {
//        sql """select /*+ no_use_mv(t1.`*`) */ k1 from t1;"""
//        contains("t1(t1)")
//    }
    explain {
        sql """select /*+ use_mv(t1.r1) use_mv(t1.r2) */ k1 from t1;"""
        contains("only one USE_MV hint is allowed")
    }
    explain {
        sql """select /*+ no_use_mv(t1.r1) no_use_mv(t1.r2) */ k1 from t1;"""
        contains("only one NO_USE_MV hint is allowed")
    }
    explain {
        sql """select /*+ no_use_mv(t1.r3) */ k1 from t1;"""
        contains("UnUsed: no_use_mv([t1, r3])")
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

    // create database and tables
    def db = "test_cbo_use_mv"
    sql "DROP DATABASE IF EXISTS ${db}"
    sql "CREATE DATABASE IF NOT EXISTS ${db}"
    sql "use ${db}"

    sql """drop table if exists t1;"""
    sql """drop table if exists t2;"""
    sql """drop table if exists t3;"""

    sql """create table t1 (c1 int, c11 int) distributed by hash(c1) buckets 3 properties('replication_num' = '1');"""
    sql """create table t2 (c2 int, c22 int) distributed by hash(c2) buckets 3 properties('replication_num' = '1');"""
    sql """create table t3 (c3 int, c33 int) distributed by hash(c3) buckets 3 properties('replication_num' = '1');"""

    sql """insert into t1 values (101, 101);"""
    sql """insert into t2 values (102, 102);"""
    sql """insert into t3 values (103, 103);"""

    def query1 = """select * from t1"""
    def query2 = """select * from t2"""
    def query3 = """select c1 from t1"""
    def query4 = """select c3 from t3"""

    async_create_mv(db, query1, "mv1")
    async_create_mv(db, query2, "mv2")
    async_create_mv(db, query3, "mv3")
    async_create_mv(db, query4, "mv4")

    sql """use ${db};"""
    explain {
        sql """memo plan select /*+ use_mv(mv1)*/ * from t1;"""
        contains("internal.test_cbo_use_mv.mv1 chose")
    }
    explain {
        sql """memo plan select /*+ no_use_mv(mv1)*/ * from t1;"""
        contains("Used: no_use_mv([mv1])")
        notContains("internal.test_cbo_use_mv.mv1 chose")
    }
    sql """use test_use_mv"""
    explain {
        sql """memo plan select /*+ use_mv(mv1)*/ * from ${db}.t1;"""
        contains("UnUsed: use_mv([mv1])")
    }
    explain {
        sql """memo plan select /*+ no_use_mv(mv1)*/ * from ${db}.t1;"""
        contains("UnUsed: no_use_mv([mv1])")
    }
    sql """use ${db};"""
    explain {
        sql """memo plan select /*+ use_mv(internal.${db}.mv1)*/ * from t1;"""
        contains("Used: use_mv([internal, test_cbo_use_mv, mv1])")
	contains("internal.test_cbo_use_mv.mv1 chose")
    }
    explain {
        sql """memo plan select /*+ no_use_mv(internal.${db}.mv1)*/ * from t1;"""
        contains("Used: no_use_mv([internal, test_cbo_use_mv, mv1])")
    }
    sql """use ${db};"""
    explain {
        sql """memo plan select /*+ use_mv(mv1) */ * from t1"""
        contains("internal.test_cbo_use_mv.mv1 chose")
	contains("Used: use_mv([mv1])")
    }
    explain {
        sql """memo plan select /*+ no_use_mv(mv1) */ * from t1"""
        contains("Used: no_use_mv([mv1])")
	notContains("internal.test_cbo_use_mv.mv1 chose")
    }
    explain {
        sql """memo plan select /*+ use_mv(mv4) */ * from t3"""
        contains("UnUsed: use_mv([mv4])")
    }
    explain {
        sql """memo plan select /*+ no_use_mv(mv4) */ * from t3"""
        contains("Used: no_use_mv([mv4])")
    }
    explain {
        sql """memo plan select /*+ use_mv(mv1, mv2) */ * from t1 union all select * from t2"""
        contains("Used: use_mv([mv1].[mv2] )")
        contains("internal.test_cbo_use_mv.mv2 chose")
        contains("internal.test_cbo_use_mv.mv1 chose")
    }
    explain {
        sql """memo plan select /*+ no_use_mv(mv1, mv2) */ * from t1 union all select * from t2"""
        contains("Used: no_use_mv([mv1].[mv2] )")
        notContains("internal.test_cbo_use_mv.mv2 chose")
        notContains("internal.test_cbo_use_mv.mv1 chose")
    }
    explain {
        sql """memo plan select /*+ use_mv(mv1) no_use_mv(mv2) */ * from t1 union all select * from t2"""
        contains("Used: use_mv([mv1]) no_use_mv([mv2])")
        notContains("internal.test_cbo_use_mv.mv2 chose")
        contains("internal.test_cbo_use_mv.mv1 chose")
    }
    explain {
        sql """memo plan select /*+ use_mv(mv2) no_use_mv(mv1) */ * from t1 union all select * from t2"""
        contains("Used: use_mv([mv2]) no_use_mv([mv1])")
        notContains("internal.test_cbo_use_mv.mv1 chose")
        contains("internal.test_cbo_use_mv.mv2 chose")
    }
    explain {
        sql """memo plan select /*+ use_mv(mv1, mv3) */ c1 from t1"""
        contains("Used: use_mv([mv1].[mv3] )")
        contains("internal.test_cbo_use_mv.mv1 chose")
        contains("internal.test_cbo_use_mv.mv3 not chose")
    }
    explain {
        sql """memo plan select /*+ use_mv(mv3, mv1) */ c1 from t1"""
        contains("Used: use_mv([mv3].[mv1] )")
        contains("internal.test_cbo_use_mv.mv1 chose")
        contains("internal.test_cbo_use_mv.mv3 not chose")
    }
    explain {
        sql """memo plan select /*+ use_mv(mv1) */ c1 from t1"""
        contains("Used: use_mv([mv1])")
        contains("internal.test_cbo_use_mv.mv1 chose")
    }
    explain {
        sql """memo plan select /*+ use_mv(mv3) */ c1 from t1"""
        contains("Used: use_mv([mv3])")
        contains("internal.test_cbo_use_mv.mv3 chose")
    }
    explain {
        sql """memo plan select /*+ no_use_mv(mv1, mv3) */ c1 from t1"""
        contains("Used: no_use_mv([mv1].[mv3] )")
        notContains("internal.test_cbo_use_mv.mv3")
        notContains("internal.test_cbo_use_mv.mv1")
    }
    explain {
        sql """memo plan select /*+ no_use_mv(mv1) */ c1 from t1"""
        contains("Used: no_use_mv([mv1])")
        contains("internal.test_cbo_use_mv.mv3 chose")
        notContains("internal.test_cbo_use_mv.mv1")
    }
    explain {
        sql """memo plan select /*+ no_use_mv(mv3) */ c1 from t1"""
        contains("Used: no_use_mv([mv3])")
        contains("internal.test_cbo_use_mv.mv1 chose")
        notContains("internal.test_cbo_use_mv.mv3")
    }

}
