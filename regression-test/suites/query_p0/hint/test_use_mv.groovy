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
    sql 'set enable_nereids_distribute_planner=false'
    sql "set ignore_shape_nodes='PhysicalProject'"
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

    // a sync index and an async materialized view can have the same name because the sync index name
    // is table local, the USE_MV hint must still be able to reference the async materialized view
    def syncAsyncDb = "test_use_mv_sync_async"
    sql "DROP DATABASE IF EXISTS ${syncAsyncDb}"
    sql "CREATE DATABASE IF NOT EXISTS ${syncAsyncDb}"
    sql "use ${syncAsyncDb}"
    sql """drop table if exists t_sync_async;"""
    sql """create table t_sync_async (k int, v int, w int) duplicate key(k)
        distributed by hash(k) buckets 3 properties('replication_num' = '1');"""
    sql """insert into t_sync_async values (1, 1, 1), (2, 2, 2);"""
    // the sync index is named m as the async materialized view below, it can not answer v
    sql """alter table t_sync_async add rollup m(w, k);"""
    waitingMVTaskFinishedByMvName(syncAsyncDb, "t_sync_async", "m")
    // the async materialized view is named m as well, it answers k and v
    async_create_mv(syncAsyncDb, "select k, v from t_sync_async", "m")
    sql """use ${syncAsyncDb};"""
    explain {
        sql """select /*+ use_mv(internal.${syncAsyncDb}.m) */ k, v from t_sync_async;"""
        contains("internal.${syncAsyncDb}.m chose")
    }
    order_qt_sync_async_same_name """select /*+ use_mv(internal.${syncAsyncDb}.m) */ k, v from t_sync_async order by k;"""
    // the sync index selected directly by the INDEX clause must be reported as used as well
    explain {
        sql """select /*+ use_mv(t_sync_async.m) */ k from t_sync_async index m;"""
        contains("Used: use_mv([t_sync_async, m])")
        contains("t_sync_async(m)")
    }

    // USE_MV forces a materialization whose bucket layout differs from the table it replaces, the
    // colocate join of the replaced table must not be kept, an exchange is required for the
    // materialization. The filter cost of the base table is lower because dt is its prefix key,
    // so only the USE_MV hint forces the materialization into the plan.
    def bucketDb = "test_use_mv_bucket_layout"
    sql "DROP DATABASE IF EXISTS ${bucketDb}"
    sql "CREATE DATABASE IF NOT EXISTS ${bucketDb}"
    sql "use ${bucketDb}"
    sql """drop table if exists colocate_a;"""
    sql """drop table if exists colocate_b;"""
    sql """drop materialized view if exists mv_bucket_layout;"""
    sql """create table colocate_a (dt int, k int, v int) duplicate key(dt, k)
        partition by range(dt) (partition p1 values less than (2), partition p2 values less than (3))
        distributed by hash(k) buckets 4
        properties('replication_num' = '1', 'colocate_with' = 'grp_use_mv');"""
    sql """create table colocate_b (k int, w int) duplicate key(k)
        distributed by hash(k) buckets 4
        properties('replication_num' = '1', 'colocate_with' = 'grp_use_mv');"""
    sql """insert into colocate_a values (1, 1, 1), (1, 2, 2), (2, 2, 3);"""
    sql """insert into colocate_b values (1, 10), (2, 20), (3, 30);"""
    sql """create materialized view mv_bucket_layout
        build immediate refresh complete on manual
        distributed by hash(k) buckets 7
        properties('replication_num' = '1')
        as select k, dt, v from colocate_a where dt >= 0;"""
    waitingMTMVTaskFinishedByMvName("mv_bucket_layout", bucketDb)
    sql """sync;"""
    sql """use ${bucketDb};"""
    sql """set pre_materialized_view_rewrite_strategy = 'NOT_IN_RBO';"""
    sql """set filter_cost_factor = 10000;"""
    explain {
        sql """select /*+ use_mv(internal.${bucketDb}.mv_bucket_layout) */ colocate_a.k, colocate_a.v, colocate_b.k
            from colocate_a join colocate_b on colocate_a.k = colocate_b.k where colocate_a.dt = 1;"""
        contains("Used: use_mv([internal, ${bucketDb}, mv_bucket_layout])")
        notContains("HAS_COLO_PLAN_NODE: true")
    }
    order_qt_use_mv_bucket_layout """select /*+ use_mv(internal.${bucketDb}.mv_bucket_layout) */ colocate_a.k, colocate_a.v, colocate_b.k
        from colocate_a join colocate_b on colocate_a.k = colocate_b.k where colocate_a.dt = 1 order by colocate_a.k;"""
    sql """set filter_cost_factor = 0.0001;"""
    // the pre rewrite (RBO) path finalizes the USE_MV hint from the pre rewritten plan as well
    sql """set pre_materialized_view_rewrite_strategy = 'FORCE_IN_RBO';"""
    explain {
        sql """select /*+ use_mv(internal.${bucketDb}.mv_bucket_layout) */ k, v from colocate_a where dt = 1;"""
        contains("Used: use_mv([internal, ${bucketDb}, mv_bucket_layout])")
        contains("RBO.internal.${bucketDb}.mv_bucket_layout chose")
    }
    sql """set pre_materialized_view_rewrite_strategy = 'TRY_IN_RBO';"""

    // several hinted materializations are alternatives of each other: at least one of them must be
    // enforced, even when the base plan is cheaper than every materialization plan
    def hardDb = "test_use_mv_hard_constraint"
    sql "DROP DATABASE IF EXISTS ${hardDb}"
    sql "CREATE DATABASE IF NOT EXISTS ${hardDb}"
    sql "use ${hardDb}"
    sql """drop table if exists A;"""
    sql """create table A (dt int, k int, v int) duplicate key(dt, k)
        distributed by hash(k) buckets 4 properties('replication_num' = '1');"""
    sql """insert into A values (1, 1, 1), (1, 2, 2), (2, 2, 3);"""
    sql """drop materialized view if exists m1;"""
    sql """create materialized view m1
        build immediate refresh complete on manual
        distributed by hash(k) buckets 7
        properties('replication_num' = '1')
        as select k, dt, v from A where dt >= 0;"""
    waitingMTMVTaskFinishedByMvName("m1", hardDb)
    sql """drop materialized view if exists m2;"""
    sql """create materialized view m2
        build immediate refresh complete on manual
        distributed by hash(k) buckets 9
        properties('replication_num' = '1')
        as select k, dt, v from A where dt >= 0;"""
    waitingMTMVTaskFinishedByMvName("m2", hardDb)
    sql """drop materialized view if exists m_hash_v;"""
    sql """create materialized view m_hash_v
        build immediate refresh complete on manual
        distributed by hash(v) buckets 5
        properties('replication_num' = '1')
        as select k, dt, v from A where dt >= 0;"""
    waitingMTMVTaskFinishedByMvName("m_hash_v", hardDb)
    sql """drop materialized view if exists m_cost;"""
    sql """create materialized view m_cost
        build immediate refresh complete on manual
        distributed by hash(k) buckets 5
        properties('replication_num' = '1')
        as select k from A;"""
    waitingMTMVTaskFinishedByMvName("m_cost", hardDb)
    sql """sync;"""
    sql """use ${hardDb};"""
    sql """set pre_materialized_view_rewrite_strategy = 'NOT_IN_RBO';"""
    // the base plan is cheaper than the materialization plans because only A matches the dt prefix
    // of the filter, so the hint is the only reason to choose a materialization
    sql """set filter_cost_factor = 10000;"""
    explain {
        sql """select /*+ use_mv(internal.${hardDb}.m1, internal.${hardDb}.m2) */ k, dt, v from A where dt = 1;"""
        contains("Used: use_mv([internal, ${hardDb}, m1].[internal, ${hardDb}, m2] )")
        notContains("TABLE: ${hardDb}.A(A)")
    }
    order_qt_use_mv_multi_hint """select /*+ use_mv(internal.${hardDb}.m1, internal.${hardDb}.m2) */ k, dt, v from A where dt = 1 order by k;"""

    // the gather enforcer of the result sink must be rebuilt on the distribution which the
    // materialization provides, instead of requiring the storage hash of the replaced table
    sql """set enable_parallel_result_sink = false;"""
    explain {
        sql """select /*+ use_mv(internal.${hardDb}.m_hash_v) */ k, dt, v from A where dt = 1;"""
        contains("Used: use_mv([internal, ${hardDb}, m_hash_v])")
        notContains("TABLE: ${hardDb}.A(A)")
        contains("PARTITION: UNPARTITIONED")
    }
    order_qt_use_mv_gather_hint """select /*+ use_mv(internal.${hardDb}.m_hash_v) */ k, dt, v from A where dt = 1 order by k;"""
    sql """set enable_parallel_result_sink = true;"""

    // the producer contract of the materialized CTE must be respected: the order of the producer is
    // not dropped by a realignment, and the restored order is a real local sort
    sql """set enable_cte_materialize = true;"""
    sql """set inline_cte_referenced_threshold = 1;"""
    sql """set cte_inline_mode = -1;"""
    explain {
        sql """with cte as (select k, v, row_number() over (partition by k order by v) as rn
                from A where dt = 1)
            select /*+ use_mv(internal.${hardDb}.m1) */ c1.k, c1.rn, c2.rn
            from cte c1 join cte c2 on c1.k = c2.k;"""
        contains("Used: use_mv([internal, ${hardDb}, m1])")
        notContains("TABLE: ${hardDb}.A(A)")
    }
    order_qt_use_mv_cte_hint """with cte as (select k, v, row_number() over (partition by k order by v) as rn
            from A where dt = 1)
        select /*+ use_mv(internal.${hardDb}.m1) */ c1.k, c1.rn, c2.rn
        from cte c1 join cte c2 on c1.k = c2.k order by c1.k;"""
    sql """set filter_cost_factor = 0.0001;"""

    // the cost of the selected materialization plan is kept for the EXPLAIN output
    explain {
        sql """physical plan select /*+ use_mv(internal.${hardDb}.m_cost) */ k + 1 from A;"""
        contains("Used: use_mv([internal, ${hardDb}, m_cost])")
        notContains("TABLE: ${hardDb}.A(A)")
        notContains("cost = 0.0")
    }

}
