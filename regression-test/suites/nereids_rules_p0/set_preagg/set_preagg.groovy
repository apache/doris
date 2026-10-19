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

suite("set_preagg") {
    // preagg_t4 gets two loads of the same full key (1,1,1,1,1,1) to create
    // duplicate full keys across rowsets: storage SUM merges v7 to 1+1=2 while
    // pre-agg ON would expose the raw {1,1} rows (used by the DISTINCT SUM test).
    multi_sql """
        set disable_nereids_rules='PRUNE_EMPTY_PARTITION';
        set forbid_unknown_col_stats=false;
        set enable_stats=false;
        drop table if exists preagg_t1;
        drop table if exists preagg_t2;
        drop table if exists preagg_t3;
        drop table if exists preagg_t4;
        drop table if exists preagg_t5;
        drop table if exists preagg_f_l;
        drop table if exists preagg_f_r;
        drop table if exists preagg_asof_l;
        drop table if exists preagg_asof_r;
        drop table if exists preagg_g;
        drop table if exists preagg_own_l;
        drop table if exists preagg_own_r;

        create table preagg_t1(
            k1 int null,
            k2 int null,
            k3 int null,
            k4 int null,
            k5 int null,
            k6 int null,
            v7 bigint SUM,
            v8 bigint SUM,
            v9 bigint MAX
        )
        aggregate key (k1,k2,k3,k4,k5,k6)
        distributed BY hash(k1) buckets 3
        properties("replication_num" = "1");

        create table preagg_t2(
            k1 int null,
            k2 int null,
            k3 int null,
            k4 int null,
            k5 int null,
            k6 int null,
            v7 bigint SUM,
            v8 bigint SUM,
            v9 bigint MAX
        )
        aggregate key (k1,k2,k3,k4,k5,k6)
        distributed BY hash(k1) buckets 3
        properties("replication_num" = "1");
        create table preagg_t3(
            k1 int null,
            k2 int null,
            k3 int null,
            k4 int null,
            k5 int null,
            k6 int null,
            v7 bigint SUM,
            v8 bigint SUM,
            v9 bigint MAX
        )
        aggregate key (k1,k2,k3,k4,k5,k6)
        distributed BY hash(k1) buckets 3
        properties("replication_num" = "1");
        create table preagg_t4(
            k1 int null,
            k2 int null,
            k3 int null,
            k4 int null,
            k5 int null,
            k6 int null,
            v7 bigint SUM,
            v9 bigint MAX
        )
        aggregate key (k1,k2,k3,k4,k5,k6)
        distributed BY hash(k1) buckets 3
        properties("replication_num" = "1");
        create table preagg_t5(
            k1 int null,
            v double MAX
        )
        aggregate key (k1)
        distributed BY hash(k1) buckets 1
        properties("replication_num" = "1");
        create table preagg_f_l(
            k1 int null,
            v9 bigint MAX,
            v9m bigint MIN
        )
        aggregate key (k1)
        distributed BY hash(k1) buckets 1
        properties("replication_num" = "1");
        create table preagg_f_r(
            k1 int null,
            v9 bigint MAX,
            v9m bigint MIN
        )
        aggregate key (k1)
        distributed BY hash(k1) buckets 1
        properties("replication_num" = "1");
        create table preagg_asof_l(
            grp int null,
            ts datetime null
        )
        aggregate key (grp, ts)
        distributed BY hash(grp) buckets 1
        properties("replication_num" = "1");
        create table preagg_asof_r(
            grp int null,
            ts datetime null,
            v7 bigint SUM
        )
        aggregate key (grp, ts)
        distributed BY hash(grp) buckets 1
        properties("replication_num" = "1");
        create table preagg_g(
            k1 int null,
            v7 bigint SUM,
            v9 bigint MAX
        )
        aggregate key (k1)
        distributed BY hash(k1) buckets 1
        properties("replication_num" = "1");
        create table preagg_own_l(
            k1 int null,
            v7 bigint SUM
        )
        aggregate key (k1)
        distributed BY hash(k1) buckets 1
        properties("replication_num" = "1");
        create table preagg_own_r(
            k1 int null,
            v7 bigint SUM
        )
        aggregate key (k1)
        distributed BY hash(k1) buckets 1
        properties("replication_num" = "1");

        insert into preagg_t1 values
            (1,1,1,1,1,1, 10, 100, 1000),
            (1,1,1,1,1,2, 20, 200, 900),
            (-1,0,0,0,0,0, 30, 300, 800),
            (2,0,0,0,0,0, 40, 400, 700);
        insert into preagg_t2 values
            (1,1,1,1,1,1, 50, 500, 5000),
            (1,1,1,1,1,2, 60, 600, 4000),
            (2,0,0,0,0,0, 70, 700, 3000);
        insert into preagg_t3 values
            (1,1,1,1,1,1, 80, 800, 8000),
            (2,0,0,0,0,0, 90, 900, 7000);
        insert into preagg_t4 values (1,1,1,1,1,1, 1, 100);
        insert into preagg_t4 values (1,1,1,1,1,1, 1, 200);
        insert into preagg_t4 values (2,0,0,0,0,0, 5, 300);
        insert into preagg_t5 values (1, -1e-300);
        insert into preagg_t5 values (1, 0.0);
        insert into preagg_f_l values (1, 100, 100);
        insert into preagg_f_l values (-1, 50, 50);
        insert into preagg_f_r values (1, 1000, 1000);
        insert into preagg_f_r values (1, 2000, 2000);
        insert into preagg_f_r values (-1, 500, 500);
        insert into preagg_f_r values (-1, 600, 600);
        insert into preagg_asof_l values (1,'2020-01-01 00:15:00'),(2,'2020-01-01 00:00:00');
        insert into preagg_asof_r values (1,'2020-01-01 00:00:00',100);
        insert into preagg_asof_r values (1,'2020-01-01 00:00:00',200);
        insert into preagg_asof_r values (2,'2020-01-01 00:00:00',300);
        insert into preagg_g values (1, -2, 10);
        insert into preagg_g values (1, 3, 20);
        insert into preagg_own_l values (1, 10);
        insert into preagg_own_l values (1, 20);
        insert into preagg_own_l values (0, 5);
        insert into preagg_own_l values (0, 7);
        insert into preagg_own_r values (1, 100);
        insert into preagg_own_r values (1, 200);
        insert into preagg_own_r values (0, 50);
        insert into preagg_own_r values (0, 80);
    """

    // preagg_own_l/preagg_own_r: full keys k1=1 and k1=0 are each loaded twice
    // in separate rowsets, so PREAGG ON would expose duplicate full-key partial
    // rows. preagg_own_l's k1=0 row makes t.a = abs(0) = 0, exercising the ELSE
    // branch (r.v7) in test_b; preagg_own_r's repeated keys make its own ON/OFF
    // status observable through join fan-out as well.

    explain {
        sql("""
            select preagg_t3.k2, t12.k2, sum(t12.v1), max(preagg_t3.v9)
            from 
            (
                select ta1.k1 k1, ta1.k2 k2, ta2.k1 k3, ta2.k2 k4, sum(ta1.t1_sum_v7) v1, sum(ta2.t2_sum_v7) v2
                from 
                    (select k1, k2, k3, k4, k5, sum(v7) t1_sum_v7 from preagg_t1 group by k1, k2, k3, k4, k5) as ta1
                inner join 
                    (select k1, k2, k3, k4, k5, sum(v7) t2_sum_v7 from preagg_t2 group by k1, k2, k3, k4, k5) as ta2
                on ta1.k3 = ta2.k3
                group by k1, k2, k3, k4
            ) t12 inner join preagg_t3 on t12.k1 = preagg_t3.k1
            group by preagg_t3.k2, t12.k2
            order by 1, 2;
        """)
        contains "(preagg_t1), PREAGGREGATION: ON"
        contains "(preagg_t2), PREAGGREGATION: ON"
        contains "(preagg_t3), PREAGGREGATION: OFF. Reason: can't turn preAgg on because aggregate function sum"
    }
    order_qt_q01 """
        select preagg_t3.k2, t12.k2, sum(t12.v1), max(preagg_t3.v9)
        from 
        (
            select ta1.k1 k1, ta1.k2 k2, ta2.k1 k3, ta2.k2 k4, sum(ta1.t1_sum_v7) v1, sum(ta2.t2_sum_v7) v2
            from 
                (select k1, k2, k3, k4, k5, sum(v7) t1_sum_v7 from preagg_t1 group by k1, k2, k3, k4, k5) as ta1
            inner join 
                (select k1, k2, k3, k4, k5, sum(v7) t2_sum_v7 from preagg_t2 group by k1, k2, k3, k4, k5) as ta2
            on ta1.k3 = ta2.k3
            group by k1, k2, k3, k4
        ) t12 inner join preagg_t3 on t12.k1 = preagg_t3.k1
        group by preagg_t3.k2, t12.k2
        order by 1, 2;
    """

    explain {
        sql("""
            select preagg_t3.k2, t12.k2, max(preagg_t3.v9)
            from 
            (
                select ta1.k1 k1, ta1.k2 k2, ta2.k1 k3, ta2.k2 k4, max(ta1.t1_sum_v7) v1, sum(ta2.t2_sum_v7) v2
                from 
                    (select k1, k2, k3, k4, k5, sum(v7) t1_sum_v7 from preagg_t1 group by k1, k2, k3, k4, k5) as ta1
                inner join 
                    (select k1, k2, k3, k4, k5, sum(v7) t2_sum_v7 from preagg_t2 group by k1, k2, k3, k4, k5) as ta2
                on ta1.k3 = ta2.k3
                group by k1, k2, k3, k4
            ) t12 inner join preagg_t3 on t12.k1 = preagg_t3.k1
            group by preagg_t3.k2, t12.k2
            order by 1, 2;
        """)
        notContains "PREAGGREGATION: OFF"
    }
    order_qt_q02 """
        select preagg_t3.k2, t12.k2, max(preagg_t3.v9)
        from 
        (
            select ta1.k1 k1, ta1.k2 k2, ta2.k1 k3, ta2.k2 k4, max(ta1.t1_sum_v7) v1, sum(ta2.t2_sum_v7) v2
            from 
                (select k1, k2, k3, k4, k5, sum(v7) t1_sum_v7 from preagg_t1 group by k1, k2, k3, k4, k5) as ta1
            inner join 
                (select k1, k2, k3, k4, k5, sum(v7) t2_sum_v7 from preagg_t2 group by k1, k2, k3, k4, k5) as ta2
            on ta1.k3 = ta2.k3
            group by k1, k2, k3, k4
        ) t12 inner join preagg_t3 on t12.k1 = preagg_t3.k1
        group by preagg_t3.k2, t12.k2
        order by 1, 2;
    """

    explain {
        sql("""
            select preagg_t3.k2, t12.k2, max(t12.v2), max(preagg_t3.v9), sum(t12.v3)
            from 
            (
                select ta1.k1 k1, ta1.k2 k2, ta2.k1 k3, ta2.k2 k4, max(ta1.t1_sum_v7) v1, max(ta2.k4) v2, count(distinct ta2.k5) v3
                from 
                    (select k1, k2, k3, k4, k5, sum(v7) t1_sum_v7 from preagg_t1 group by k1, k2, k3, k4, k5) as ta1
                inner join 
                    (select k1, k2, k3, k4, k5, v7 from preagg_t2) as ta2
                on ta1.k3 = ta2.k3
                group by k1, k2, k3, k4
            ) t12 inner join preagg_t3 on t12.k1 = preagg_t3.k1
            group by preagg_t3.k2, t12.k2
            order by 1, 2;
        """)
        contains "(preagg_t1), PREAGGREGATION: ON"
        contains "(preagg_t2), PREAGGREGATION: ON"
        contains "(preagg_t3), PREAGGREGATION: OFF. Reason: can't turn preAgg on because aggregate function sum"
    }
    order_qt_q03 """
        select preagg_t3.k2, t12.k2, max(t12.v2), max(preagg_t3.v9), sum(t12.v3)
        from 
        (
            select ta1.k1 k1, ta1.k2 k2, ta2.k1 k3, ta2.k2 k4, max(ta1.t1_sum_v7) v1, max(ta2.k4) v2, count(distinct ta2.k5) v3
            from 
                (select k1, k2, k3, k4, k5, sum(v7) t1_sum_v7 from preagg_t1 group by k1, k2, k3, k4, k5) as ta1
            inner join 
                (select k1, k2, k3, k4, k5, v7 from preagg_t2) as ta2
            on ta1.k3 = ta2.k3
            group by k1, k2, k3, k4
        ) t12 inner join preagg_t3 on t12.k1 = preagg_t3.k1
        group by preagg_t3.k2, t12.k2
        order by 1, 2;
    """

    explain {
        sql("""
            select preagg_t3.k2, t12.k2, sum(t12.v2), max(preagg_t3.v9)
            from 
            (
                select ta1.k1 k1, ta1.k2 k2, ta2.k1 k3, ta2.k2 k4, max(ta1.t1_sum_v7) v1, max(ta2.v7) v2
                from 
                    (select k1, k2, k3, k4, k5, sum(v7) t1_sum_v7 from preagg_t1 group by k1, k2, k3, k4, k5) as ta1
                inner join 
                    (select k1, k2, k3, k4, k5, v7 from preagg_t2) as ta2
                on ta1.k3 = ta2.k3
                group by k1, k2, k3, k4
            ) t12 inner join preagg_t3 on t12.k1 = preagg_t3.k1
            group by preagg_t3.k2, t12.k2
            order by 1, 2;
        """)
        contains "(preagg_t1), PREAGGREGATION: ON"
        contains "(preagg_t2), PREAGGREGATION: OFF. Reason: max(v7) is not match agg mode SUM"
        contains "(preagg_t3), PREAGGREGATION: OFF. Reason: can't turn preAgg on because aggregate function sum"
    }
    order_qt_q04 """
        select preagg_t3.k2, t12.k2, sum(t12.v2), max(preagg_t3.v9)
        from 
        (
            select ta1.k1 k1, ta1.k2 k2, ta2.k1 k3, ta2.k2 k4, max(ta1.t1_sum_v7) v1, max(ta2.v7) v2
            from 
                (select k1, k2, k3, k4, k5, sum(v7) t1_sum_v7 from preagg_t1 group by k1, k2, k3, k4, k5) as ta1
            inner join 
                (select k1, k2, k3, k4, k5, v7 from preagg_t2) as ta2
            on ta1.k3 = ta2.k3
            group by k1, k2, k3, k4
        ) t12 inner join preagg_t3 on t12.k1 = preagg_t3.k1
        group by preagg_t3.k2, t12.k2
        order by 1, 2;
    """

    explain {
        sql("""
            select preagg_t3.k2, t12.k2, max(t12.v2), max(preagg_t3.v9), sum(t12.v3)
            from 
            (
                select ta1.k1 k1, ta1.k2 k2, ta2.k1 k3, ta2.k2 k4, max(case when ta2.k1 > 0 then ta2.v9 when ta2.k1 = 0 then null when ta2.k1 < 0 then ta2.v9 else null end) v2, count(distinct ta2.k5) v3
                from 
                    (select k1, k2, k3, k4, k5, sum(v7) t1_sum_v7 from preagg_t1 group by k1, k2, k3, k4, k5) as ta1
                inner join 
                    (select k1, k2, k3, k4, k5, v7, v8, v9 from preagg_t2) as ta2
                on ta1.k3 = ta2.k3
                group by k1, k2, k3, k4
            ) t12 inner join preagg_t3 on t12.k1 = preagg_t3.k1
            group by preagg_t3.k2, t12.k2
            order by 1, 2;
        """)
        contains "(preagg_t1), PREAGGREGATION: ON"
        contains "(preagg_t2), PREAGGREGATION: ON"
        contains "(preagg_t3), PREAGGREGATION: OFF. Reason: can't turn preAgg on because aggregate function sum"
    }

    explain {
        sql("""
            select preagg_t3.k2, t12.k2, max(t12.v2), max(preagg_t3.v9), sum(t12.v3)
            from 
            (
                select ta1.k1 k1, ta1.k2 k2, ta2.k1 k3, ta2.k2 k4, sum(case when ta2.k1 > 0 then ta2.v7 when ta2.k1 = 0 then 0 when ta2.k1 < 0 then ta2.v8 else 0 end) v2, count(distinct ta2.k5) v3
                from 
                    (select k1, k2, k3, k4, k5, sum(v7) t1_sum_v7 from preagg_t1 group by k1, k2, k3, k4, k5) as ta1
                inner join 
                    (select k1, k2, k3, k4, k5, v7, v8 from preagg_t2) as ta2
                on ta1.k3 = ta2.k3
                group by k1, k2, k3, k4
            ) t12 inner join preagg_t3 on t12.k1 = preagg_t3.k1
            group by preagg_t3.k2, t12.k2
            order by 1, 2;
        """)
        contains "(preagg_t1), PREAGGREGATION: ON"
        contains "(preagg_t2), PREAGGREGATION: ON"
        contains "(preagg_t3), PREAGGREGATION: OFF. Reason: can't turn preAgg on because aggregate function sum"
    }
    order_qt_q05 """
        select preagg_t3.k2, t12.k2, max(t12.v2), max(preagg_t3.v9), sum(t12.v3)
        from 
        (
            select ta1.k1 k1, ta1.k2 k2, ta2.k1 k3, ta2.k2 k4, max(case when ta2.k1 > 0 then ta2.v9 when ta2.k1 = 0 then null when ta2.k1 < 0 then ta2.v9 else null end) v2, count(distinct ta2.k5) v3
            from 
                (select k1, k2, k3, k4, k5, sum(v7) t1_sum_v7 from preagg_t1 group by k1, k2, k3, k4, k5) as ta1
            inner join 
                (select k1, k2, k3, k4, k5, v7, v8, v9 from preagg_t2) as ta2
            on ta1.k3 = ta2.k3
            group by k1, k2, k3, k4
        ) t12 inner join preagg_t3 on t12.k1 = preagg_t3.k1
        group by preagg_t3.k2, t12.k2
        order by 1, 2;
    """

    explain {
        sql("""
            select preagg_t3.k2, t12.k2, sum(t12.v2), max(preagg_t3.v9)
            from 
            (
                select ta1.k1 k1, ta1.k2 k2, ta2.k1 k3, ta2.k2 k4, max(ta1.t1_sum_v7) v1, sum(ta2.v7) v2
                from 
                    (select k1, k2, k3, k4, k5, sum(v7) t1_sum_v7 from preagg_t1 group by k1, k2, k3, k4, k5) as ta1
                inner join 
                    (select k1, k2, k3, k4, k5, v7 from preagg_t2) as ta2
                on ta1.k3 = ta2.k3
                group by k1, k2, k3, k4
            ) t12 inner join preagg_t3 on t12.k1 = preagg_t3.k1
            group by preagg_t3.k2, t12.k2
            order by 1, 2;
        """)
        contains "(preagg_t1), PREAGGREGATION: ON"
        contains "(preagg_t2), PREAGGREGATION: ON"
        contains "(preagg_t3), PREAGGREGATION: OFF. Reason: can't turn preAgg on because aggregate function sum"
    }
    order_qt_q06 """
        select preagg_t3.k2, t12.k2, max(t12.v2), max(preagg_t3.v9), sum(t12.v3)
        from 
        (
            select ta1.k1 k1, ta1.k2 k2, ta2.k1 k3, ta2.k2 k4, sum(case when ta2.k1 > 0 then ta2.v7 when ta2.k1 = 0 then 0 when ta2.k1 < 0 then ta2.v8 else 0 end) v2, count(distinct ta2.k5) v3
            from 
                (select k1, k2, k3, k4, k5, sum(v7) t1_sum_v7 from preagg_t1 group by k1, k2, k3, k4, k5) as ta1
            inner join 
                (select k1, k2, k3, k4, k5, v7, v8 from preagg_t2) as ta2
            on ta1.k3 = ta2.k3
            group by k1, k2, k3, k4
        ) t12 inner join preagg_t3 on t12.k1 = preagg_t3.k1
        group by preagg_t3.k2, t12.k2
        order by 1, 2;
    """

    explain {
        sql("""
            select preagg_t3.k2, t12.k2, max(t12.v2), max(preagg_t3.v9), min(t12.v3)
            from 
            (
                select ta1.k1 k1, ta1.k2 k2, ta2.k1 k3, ta2.k2 k4, max(ta1.t1_sum_v7) v1, count(distinct ta2.k4) v2, count(distinct ta2.k5) v3
                from 
                    (select k1, k2, k3, k4, k5, sum(v7) t1_sum_v7 from preagg_t1 group by k1, k2, k3, k4, k5) as ta1
                left join 
                    (select k1, k2, k3, k4, k5, v7, v8 from preagg_t2) as ta2
                on ta1.k3 = ta2.k3
                group by k1, k2, k3, k4
            ) t12 inner join preagg_t3 on t12.k1 = preagg_t3.k1
            group by preagg_t3.k2, t12.k2
            order by 1, 2;
        """)
        notContains "PREAGGREGATION: OFF"
    }
    order_qt_q07 """
        select preagg_t3.k2, t12.k2, sum(t12.v2), max(preagg_t3.v9)
        from 
        (
            select ta1.k1 k1, ta1.k2 k2, ta2.k1 k3, ta2.k2 k4, max(ta1.t1_sum_v7) v1, sum(ta2.v7) v2
            from 
                (select k1, k2, k3, k4, k5, sum(v7) t1_sum_v7 from preagg_t1 group by k1, k2, k3, k4, k5) as ta1
            inner join 
                (select k1, k2, k3, k4, k5, v7 from preagg_t2) as ta2
            on ta1.k3 = ta2.k3
            group by k1, k2, k3, k4
        ) t12 inner join preagg_t3 on t12.k1 = preagg_t3.k1
        group by preagg_t3.k2, t12.k2
        order by 1, 2;
    """
    explain {
        sql("""
            select preagg_t3.k2, t12.k2, max(t12.v2), max(preagg_t3.v9), sum(t12.v3)
            from 
            (
                select ta1.k1 k1, ta1.k2 k2, ta2.k1 k3, ta2.k2 k4, max(ta1.t1_sum_v7) v1, count(case when ta2.k1 > 0 then ta2.v7 when ta2.k1 = 0 then 0 when ta1.k1 < 0 then ta2.v8 else 0 end) v2, sum(ta2.v7) v3
                from 
                    (select k1, k2, k3, k4, k5, sum(v7) t1_sum_v7 from preagg_t1 group by k1, k2, k3, k4, k5) as ta1
                left join 
                    (select k1, k2, k3, k4, k5, v7, v8 from preagg_t2) as ta2
                on ta1.k3 = ta2.k3
                group by k1, k2, k3, k4
            ) t12 inner join preagg_t3 on t12.k1 = preagg_t3.k1
            group by preagg_t3.k2, t12.k2
            order by 1, 2;
        """)
        contains "(preagg_t1), PREAGGREGATION: ON"
        contains "(preagg_t2), PREAGGREGATION: OFF. Reason: count"
        contains "(preagg_t3), PREAGGREGATION: OFF. Reason: can't turn preAgg on because aggregate function sum"
    }
    order_qt_q08 """
        select preagg_t3.k2, t12.k2, max(t12.v2), max(preagg_t3.v9), sum(t12.v3)
        from 
        (
            select ta1.k1 k1, ta1.k2 k2, ta2.k1 k3, ta2.k2 k4, max(ta1.t1_sum_v7) v1, count(case when ta2.k1 > 0 then ta2.v7 when ta2.k1 = 0 then 0 when ta1.k1 < 0 then ta2.v8 else 0 end) v2, sum(ta2.v7) v3
            from 
                (select k1, k2, k3, k4, k5, sum(v7) t1_sum_v7 from preagg_t1 group by k1, k2, k3, k4, k5) as ta1
            left join 
                (select k1, k2, k3, k4, k5, v7, v8 from preagg_t2) as ta2
            on ta1.k3 = ta2.k3
            group by k1, k2, k3, k4
        ) t12 inner join preagg_t3 on t12.k1 = preagg_t3.k1
        group by preagg_t3.k2, t12.k2
        order by 1, 2;
    """

    explain {
        sql("""
            select preagg_t3.k2, t12.k2, max(t12.v2), max(preagg_t3.v9), count(distinct t12.v3), count(distinct t12.k4) v3
            from 
            (
                select ta1.k1 k1, ta1.k2 k2, ta2.k1 k3, ta2.k2 k4, ta1.t1_sum_v7 v1, ta2.v9 v2, ta2.k5 v3
                from 
                    (select k1, k2, k3, k4, k5, sum(v7) t1_sum_v7 from preagg_t1 group by k1, k2, k3, k4, k5) as ta1
                inner join 
                    (select k1, k2, k3, k4, k5, v9 from preagg_t2) as ta2
                on ta1.k3 = ta2.k3
            ) t12 right join preagg_t3 on t12.k1 = preagg_t3.k1
            group by preagg_t3.k2, t12.k2
            order by 1, 2;
        """)
        contains "(preagg_t1), PREAGGREGATION: ON"
        contains "(preagg_t2), PREAGGREGATION: ON"
        contains "(preagg_t3), PREAGGREGATION: ON"
    }
    order_qt_q09 """
        select preagg_t3.k2, t12.k2, max(t12.v2), max(preagg_t3.v9), count(distinct t12.v3), count(distinct t12.k4) v3
        from 
        (
            select ta1.k1 k1, ta1.k2 k2, ta2.k1 k3, ta2.k2 k4, ta1.t1_sum_v7 v1, ta2.v9 v2, ta2.k5 v3
            from 
                (select k1, k2, k3, k4, k5, sum(v7) t1_sum_v7 from preagg_t1 group by k1, k2, k3, k4, k5) as ta1
            inner join 
                (select k1, k2, k3, k4, k5, v9 from preagg_t2) as ta2
            on ta1.k3 = ta2.k3
        ) t12 right join preagg_t3 on t12.k1 = preagg_t3.k1
        group by preagg_t3.k2, t12.k2
        order by 1, 2;
    """

    explain {
        sql("""
            select preagg_t3.k2, t12.k2, max(preagg_t3.v9), count(distinct t12.v3), count(distinct t12.k4) v3
            from 
            (
                select ta1.k1 k1, ta1.k2 k2, ta2.k1 k3, ta2.k2 k4, ta1.t1_sum_v7 v1, ta1.k5 v3
                from 
                    (select k1, k2, k3, k4, k5, sum(v7) t1_sum_v7 from preagg_t1 group by k1, k2, k3, k4, k5) as ta1
                inner join 
                    (select k1, k2, k3, k4, k5, v9 from preagg_t2) as ta2
                on ta1.k3 = ta2.k3
            ) t12 right join preagg_t3 on t12.k1 = preagg_t3.k1
            group by preagg_t3.k2, t12.k2
            order by 1, 2;
        """)
        contains "(preagg_t1), PREAGGREGATION: ON"
        contains "(preagg_t2), PREAGGREGATION: ON"
        contains "(preagg_t3), PREAGGREGATION: ON"
    }
    order_qt_q10 """
        select preagg_t3.k2, t12.k2, max(preagg_t3.v9), count(distinct t12.v3), count(distinct t12.k4) v3
        from 
        (
            select ta1.k1 k1, ta1.k2 k2, ta2.k1 k3, ta2.k2 k4, ta1.t1_sum_v7 v1, ta1.k5 v3
            from 
                (select k1, k2, k3, k4, k5, sum(v7) t1_sum_v7 from preagg_t1 group by k1, k2, k3, k4, k5) as ta1
            inner join 
                (select k1, k2, k3, k4, k5, v9 from preagg_t2) as ta2
            on ta1.k3 = ta2.k3
        ) t12 right join preagg_t3 on t12.k1 = preagg_t3.k1
        group by preagg_t3.k2, t12.k2
        order by 1, 2;
    """

    explain {
        sql("""
            select preagg_t3.k2, t12.k2, sum(t12.v1), max(preagg_t3.v9), count(distinct t12.v3), count(distinct t12.k4) v3
            from 
            (
                select ta1.k1 k1, ta1.k2 k2, ta2.k1 k3, ta2.k2 k4, ta1.t1_sum_v7 v1, ta1.k5 v3
                from 
                    (select k1, k2, k3, k4, k5, sum(v7) t1_sum_v7 from preagg_t1 group by k1, k2, k3, k4, k5) as ta1
                inner join 
                    (select k1, k2, k3, k4, k5, v9 from preagg_t2) as ta2
                on ta1.k3 = ta2.k3
            ) t12 right join preagg_t3 on t12.k1 = preagg_t3.k1
            group by preagg_t3.k2, t12.k2
            order by 1, 2;
        """)
        contains "(preagg_t1), PREAGGREGATION: ON"
        contains "(preagg_t2), PREAGGREGATION: OFF. Reason: can't turn preAgg on because aggregate function sum"
        contains "(preagg_t3), PREAGGREGATION: OFF"
    }
    order_qt_q11 """
        select preagg_t3.k2, t12.k2, sum(t12.v1), max(preagg_t3.v9), count(distinct t12.v3), count(distinct t12.k4) v3
        from 
        (
            select ta1.k1 k1, ta1.k2 k2, ta2.k1 k3, ta2.k2 k4, ta1.t1_sum_v7 v1, ta1.k5 v3
            from 
                (select k1, k2, k3, k4, k5, sum(v7) t1_sum_v7 from preagg_t1 group by k1, k2, k3, k4, k5) as ta1
            inner join 
                (select k1, k2, k3, k4, k5, v9 from preagg_t2) as ta2
            on ta1.k3 = ta2.k3
        ) t12 right join preagg_t3 on t12.k1 = preagg_t3.k1
        group by preagg_t3.k2, t12.k2
        order by 1, 2;
    """

    explain {
        sql("""
            select cw.k1, cw.k2, cw.v7, cw.v9
            from preagg_t1 cw
            inner join (
                select k1, k2, max(v9) as v9
                from preagg_t1
                where k1 in (1, 2)
                group by k1, k2
            ) mw on cw.k1 = mw.k1 and cw.v9 = mw.v9;
        """)
        contains "(preagg_t1), PREAGGREGATION: OFF. Reason: No valid aggregate on scan."
        contains "(preagg_t1), PREAGGREGATION: ON"
    }
    order_qt_q12 """
        select cw.k1, cw.k2, cw.v7, cw.v9
        from preagg_t1 cw
        inner join (
            select k1, k2, max(v9) as v9
            from preagg_t1
            where k1 in (1, 2)
            group by k1, k2
        ) mw on cw.k1 = mw.k1 and cw.v9 = mw.v9
        order by 1, 2, 3, 4;
    """

    // Aggregate over limited subquery: Limit between aggregate and scan goes
    // through generic visitor path, which should block preagg collection
    // without clearing the aggregate's own frame.
    explain {
        sql("""
            select k1, sum(v7)
            from (select k1, v7 from preagg_t1 limit 10) s
            group by k1;
        """)
        notContains "(preagg_t1), PREAGGREGATION: ON"
    }

    // Aggregate over non-Olap relation (numbers TVF).
    // The TVF scan goes through the generic visitor path, which should not
    // clear the aggregate's frame and cause EmptyStackException.
    explain {
        sql("""
            select count(*) from (select * from numbers("number"="10")) t;
        """)
    }
    order_qt_q13 """
        select count(*) from (select * from numbers("number"="10")) t;
    """

    explain {
        sql("""select count(distinct k6, v7) from preagg_t1;""")
        notContains "(preagg_t1), PREAGGREGATION: ON"
    }
    order_qt_q14 """
        select count(distinct k6, v7) from preagg_t1;
    """

    explain {
        sql("""select count(distinct k6, k5) from preagg_t1;""")
        contains "(preagg_t1), PREAGGREGATION: ON"
    }
    order_qt_q15 """
        select count(distinct k6, k5) from preagg_t1;
    """

    // Negative: count(DISTINCT IF(...), value_col) — checkAggWithKeyAndValueSlots
    // only inspects child(0) (the IF). Without a multi-arg guard, it would
    // miss v7 in child(1) and incorrectly return ON.
    explain {
        sql("""
            select count(distinct if(k6 > 0, k5, 0), v7) from preagg_t1;
        """)
        notContains "(preagg_t1), PREAGGREGATION: ON"
    }
    order_qt_q16 """
        select count(distinct if(k6 > 0, k5, 0), v7) from preagg_t1;
    """

    explain {
        sql("""
            select count(distinct case when k6 > 0 then k5 else 0 end, v7) from preagg_t1;
        """)
        notContains "(preagg_t1), PREAGGREGATION: ON"
    }
    order_qt_q17 """
        select count(distinct case when k6 > 0 then k5 else 0 end, v7) from preagg_t1;
    """

    // Negative: count(DISTINCT key + random()) — volatile in the expression
    // argument. With pre-agg ON, random() would be evaluated per partial row
    // instead of per merged logical row, changing the distinct count.
    explain {
        sql("""select count(distinct k6 + random()) from preagg_t1;""")
        notContains "(preagg_t1), PREAGGREGATION: ON"
    }

    // max/min(key + random()) have the same volatile concern.
    explain {
        sql("""select max(k6 + random()) from preagg_t1;""")
        notContains "(preagg_t1), PREAGGREGATION: ON"
    }

    explain {
        sql("""select min(k6 + random()) from preagg_t1;""")
        notContains "(preagg_t1), PREAGGREGATION: ON"
    }

    // Volatile in an IF condition inside a mixed-key-value aggregate.
    // The condition k6 + random() > 0 uses only key input slots but is
    // volatile, so pre-agg must be OFF.
    explain {
        sql("""
            select sum(if(k6 + random() > 0, v7, 0)) from preagg_t1;
        """)
        notContains "(preagg_t1), PREAGGREGATION: ON"
    }

    // Positive: two-project join where both aliases resolve to key-only
    // expressions. With merge+resolve, x = k5 + 1 resolves fully to base
    // key columns, so pre-agg can be ON for both scans.
    explain {
        sql("""
            select count(distinct a)
            from (
                select l.k1 + l.x as a
                from (
                    select t1.k1, t1.k5 + 1 as x from preagg_t1 t1
                ) l
                inner join (
                    select abs(t2.k1) as rk from preagg_t2 t2
                ) r on l.k1 = r.rk
            ) t;
        """)
        contains "(preagg_t1), PREAGGREGATION: ON"
        contains "(preagg_t2), PREAGGREGATION: ON"
    }
    order_qt_q18 """
        select count(distinct a)
        from (
            select l.k1 + l.x as a
            from (
                select t1.k1, t1.k5 + 1 as x from preagg_t1 t1
            ) l
            inner join (
                select abs(t2.k1) as rk from preagg_t2 t2
            ) r on l.k1 = r.rk
        ) t;
    """

    // Negative: two-project join; x carries v7 + 1 (a value column). Even
    // with merge+resolve, the fully resolved expression contains v7 so
    // pre-agg is correctly OFF.
    explain {
        sql("""
            select count(distinct a)
            from (
                select l.k1 + l.x as a
                from (
                    select t1.k1, t1.v7 + 1 as x from preagg_t1 t1
                ) l
                inner join (
                    select abs(t2.k1) as rk from preagg_t2 t2
                ) r on l.k1 = r.rk
            ) t;
        """)
        notContains "(preagg_t1), PREAGGREGATION: ON"
    }
    order_qt_q19 """
        select count(distinct a)
        from (
            select l.k1 + l.x as a
            from (
                select t1.k1, t1.v7 + 1 as x from preagg_t1 t1
            ) l
            inner join (
                select abs(t2.k1) as rk from preagg_t2 t2
            ) r on l.k1 = r.rk
        ) t;
    """

    // Bypass 1: volatile in an other-table aggregate function. max(r.k1 + random())
    // is whitelisted as a duplicate-insensitive MAX for scan l; the candidate set
    // for l is empty, so without a central volatile check it returns ON before
    // reaching the per-function guard. Both scans must be OFF.
    explain {
        sql("""
            select max(r.k1 + random())
            from preagg_t1 l
            inner join preagg_t2 r on l.k1 = r.k1;
        """)
        notContains "(preagg_t1), PREAGGREGATION: ON"
        notContains "(preagg_t2), PREAGGREGATION: ON"
    }

    // Bypass 2: volatile filter with no input slots. random() < 0.5 has an
    // empty input-slot set, so the slot-based value-column check bypasses it.
    // The central volatile guard must reject pre-agg on this scan.
    explain {
        sql("""
            select sum(v7) from preagg_t1 where random() < 0.5;
        """)
        notContains "(preagg_t1), PREAGGREGATION: ON"
    }

    // Foreign value column in mixed aggregate: sum(if(abs(l.k1) > 0, r.v7, 0))
    // references l.k1 (local key) and r.v7 (foreign value). The mixed helper
    // must not use r.v7's SUM type to justify pre-agg on l — r.v7 is not
    // a column of l. l must be OFF; r can be ON.
    explain {
        sql("""
            select sum(if(t.a > 0, r.v7, 0))
            from (select abs(k1) as a from preagg_t1) t
            inner join preagg_t2 r on t.a = r.k1;
        """)
        notContains "(preagg_t1), PREAGGREGATION: ON"
        // r's local slots are only {r.v7} with IF; the value-only IF/CaseWhen
        // handler validates conditions (foreign key t.a → safe) and returns ON.
        contains "(preagg_t2), PREAGGREGATION: ON"
    }

    // CASE WHEN symmetry of the foreign-key-condition / local-value-return
    // pattern on r: conditions reference only foreign keys, return references
    // r.v7, so r should be ON.
    explain {
        sql("""
            select sum(case when t.a > 0 then r.v7 else 0 end)
            from (select abs(k1) as a from preagg_t1) t
            inner join preagg_t2 r on t.a = r.k1;
        """)
        notContains "(preagg_t1), PREAGGREGATION: ON"
        contains "(preagg_t2), PREAGGREGATION: ON"
    }
    order_qt_q20 """
        select sum(case when t.a > 0 then r.v7 else 0 end)
        from (select abs(k1) as a from preagg_t1) t
        inner join preagg_t2 r on t.a = r.k1;
    """

    // Negative: max(cast(v9 as double)) — no cast is peeled for MAX/MIN.
    // DOUBLE/DECIMAL→FLOAT can underflow to -0.0 and change the observable
    // tie representative (signed zero) under MAX/MIN, so even nondecreasing
    // casts are not MAX/MIN homomorphisms. The checker sees a Cast and
    // returns OFF.
    explain {
        sql("""select max(cast(v9 as double)) from preagg_t1;""")
        notContains "(preagg_t1), PREAGGREGATION: ON"
    }
    order_qt_q21 """select max(cast(v9 as double)) from preagg_t1;"""

    // Negative: sum(cast(v7 as double)) — sum(cast(x)) and cast(sum(x)) are
    // not interchangeable due to precision/overflow, so cast must NOT be
    // unwrapped. OneValueSlotAggChecker sees a Cast, not a SlotReference,
    // and correctly returns OFF.
    explain {
        sql("""select sum(cast(v7 as double)) from preagg_t1;""")
        notContains "(preagg_t1), PREAGGREGATION: ON"
    }
    order_qt_q22 """select sum(cast(v7 as double)) from preagg_t1;"""

    // Negative: max(cast(v9 as string)) — non-numeric cast is never safe
    // for MAX/MIN because string comparison differs from numeric comparison.
    explain {
        sql("""select max(cast(v9 as string)) from preagg_t1;""")
        notContains "(preagg_t1), PREAGGREGATION: ON"
    }
    order_qt_q23 """select max(cast(v9 as string)) from preagg_t1;"""

    // Negative mixed-path IF with cast in return: max(if(k6 > 0, cast(v9 as double), 0))
    // — no cast is peeled for MAX/MIN, so the IF return stays wrapped in Cast
    // and the checker returns OFF.
    explain {
        sql("""
            select max(if(k6 > 0, cast(v9 as double), 0)) from preagg_t1;
        """)
        notContains "(preagg_t1), PREAGGREGATION: ON"
    }
    order_qt_q24 """
        select max(if(k6 > 0, cast(v9 as double), 0)) from preagg_t1;
    """

    // Negative mixed-path IF with cast in return:
    // sum(if(k6 > 0, cast(v7 as double), 0)) — sum(cast(x)) is not
    // interchangeable with cast(sum(x)), so the guard keeps the Cast
    // wrapper and the checker returns OFF.
    explain {
        sql("""
            select sum(if(k6 > 0, cast(v7 as double), 0)) from preagg_t1;
        """)
        notContains "(preagg_t1), PREAGGREGATION: ON"
    }
    order_qt_q25 """
        select sum(if(k6 > 0, cast(v7 as double), 0)) from preagg_t1;
    """

    // --- Cast regression tests ---
    // No cast is peeled for MAX/MIN: DOUBLE/DECIMAL→FLOAT can underflow to
    // -0.0 and change the observable tie representative (signed zero) under
    // MAX/MIN, so even nondecreasing casts are not MAX/MIN homomorphisms.
    // Any cast-wrapped aggregate is therefore conservatively OFF.

    // Negative: BIGINT→DECIMAL(20,0) widening cast → OFF (no peeling).
    explain {
        sql("""select max(cast(v9 as decimal(20,0))) from preagg_t1;""")
        notContains "(preagg_t1), PREAGGREGATION: ON"
    }
    order_qt_q26 """select max(cast(v9 as decimal(20,0))) from preagg_t1;"""

    // Negative: BIGINT→LARGEINT widening cast → OFF (no peeling).
    explain {
        sql("""select max(cast(v9 as largeint)) from preagg_t1;""")
        notContains "(preagg_t1), PREAGGREGATION: ON"
    }
    order_qt_q27 """select max(cast(v9 as largeint)) from preagg_t1;"""

    // Negative: BIGINT→INT is narrowing (not injective, not float) → OFF.
    explain {
        sql("""select max(cast(v9 as int)) from preagg_t1;""")
        notContains "(preagg_t1), PREAGGREGATION: ON"
    }
    order_qt_q28 """select max(cast(v9 as int)) from preagg_t1;"""

    // Negative: BIGINT→TINYINT is narrowing (not injective, not float) → OFF.
    explain {
        sql("""select max(cast(v9 as tinyint)) from preagg_t1;""")
        notContains "(preagg_t1), PREAGGREGATION: ON"
    }
    order_qt_q29 """select max(cast(v9 as tinyint)) from preagg_t1;"""

    // Negative mixed-path IF with widening cast: no peeling → OFF.
    explain {
        sql("""
            select max(if(k6 > 0, cast(v9 as decimal(20,0)), 0)) from preagg_t1;
        """)
        notContains "(preagg_t1), PREAGGREGATION: ON"
    }
    order_qt_q30 """
        select max(if(k6 > 0, cast(v9 as decimal(20,0)), 0)) from preagg_t1;
    """

    // Mixed-path IF with unsafe cast: max(if(..., cast(v9 as tinyint), 0))
    // return cast is narrowing non-injective → not peeled → checker OFF.
    explain {
        sql("""
            select max(if(k6 > 0, cast(v9 as tinyint), 0)) from preagg_t1;
        """)
        notContains "(preagg_t1), PREAGGREGATION: ON"
    }
    order_qt_q31 """
        select max(if(k6 > 0, cast(v9 as tinyint), 0)) from preagg_t1;
    """

    // Negative: CASE WHEN with widening cast → OFF (no peeling).
    explain {
        sql("""
            select max(case when k6 > 0 then cast(v9 as decimal(20,0)) else 0 end) from preagg_t1;
        """)
        notContains "(preagg_t1), PREAGGREGATION: ON"
    }
    order_qt_q32 """
        select max(case when k6 > 0 then cast(v9 as decimal(20,0)) else 0 end) from preagg_t1;
    """

    // CASE WHEN with unsafe narrowing cast → OFF.
    explain {
        sql("""
            select max(case when k6 > 0 then cast(v9 as tinyint) else 0 end) from preagg_t1;
        """)
        notContains "(preagg_t1), PREAGGREGATION: ON"
    }
    order_qt_q33 """
        select max(case when k6 > 0 then cast(v9 as tinyint) else 0 end) from preagg_t1;
    """

    // -------------------------------------------------------------------------
    // Two-table derived-key ownership tests (with repeated aggregate-key data)
    //
    // The mixed helper must validate returns relative to the current scan: a
    // foreign value column must never justify exposing this scan's partial
    // (unmerged) rows. Under join fan-out, a return that references a foreign
    // value column would be evaluated once per partial row and double-counted.
    // -------------------------------------------------------------------------

    // Test A: derived-key fan-out with repeated full aggregate keys. preagg_own_l
    // loads k1=1 twice (v7=10,20) and k1=0 twice (v7=5,7) in separate rowsets, so
    // if preagg_own_l were wrongly ON, its duplicate full-key partial rows would
    // fan out under the join and double-count the foreign r.v7 in the IF return.
    // Correct (l OFF, merged): t has a=1 (k1=1 merged v7=30) and a=0 (k1=0 merged
    // v7=12). r is legitimately ON, exposing its own partials k1=1 {100,200} and
    // k1=0 {50,80}: a=1 joins both → 100+200=300; a=0 joins both but if(0>0,...)=0
    // → total 300. If l were wrongly ON, a=1 would appear twice and join both r
    // partials → 300*2=600, breaking the oracle.
    explain {
        sql("""
            select sum(if(t.a > 0, r.v7, 0))
            from (select abs(k1) as a from preagg_own_l) t
            inner join preagg_own_r r on t.a = r.k1;
        """)
        notContains "(preagg_own_l), PREAGGREGATION: ON"
        contains "(preagg_own_r), PREAGGREGATION: ON"
    }
    order_qt_test_a """
        select sum(if(t.a > 0, r.v7, 0)) as res
        from (select abs(k1) as a from preagg_own_l) t
        inner join preagg_own_r r on t.a = r.k1;
    """

    // Test B: foreign value in the IF return on BOTH sides — ownership check must
    // turn both scans OFF:
    //   sum(if(t.a > 0, t.v7, r.v7))
    //   - for scan l: return r.v7 is foreign → l OFF
    //   - for scan r: return t.v7 is foreign → r OFF
    // preagg_own_l's k1=0 row gives t.a = 0, so the ELSE branch (r.v7) is actually
    // evaluated, and both tables carry repeated full keys so a wrongly-ON scan
    // exposes partial rows and changes the oracle. Correct (both OFF, merged):
    // l = {a=1(v7=30), a=0(v7=12)}, r = {k1=1→300, k1=0→130};
    // a=1 → if(1>0,30,300)=30, a=0 → if(0>0,12,130)=130 → total 160.
    // If l were wrongly ON: a=1 and a=0 each appear twice → 10+20+130+130=290.
    // If r were wrongly ON: r partials fan out → 30+30+50+80=190.
    explain {
        sql("""
            select sum(if(t.a > 0, t.v7, r.v7))
            from (select abs(k1) as a, v7 from preagg_own_l) t
            inner join preagg_own_r r on t.a = r.k1;
        """)
        notContains "(preagg_own_l), PREAGGREGATION: ON"
        notContains "(preagg_own_r), PREAGGREGATION: ON"
    }
    order_qt_test_b """
        select sum(if(t.a > 0, t.v7, r.v7)) as res
        from (select abs(k1) as a, v7 from preagg_own_l) t
        inner join preagg_own_r r on t.a = r.k1;
    """

    // Positive MAX join case: MAX is idempotent (max(x, x) = x), so a foreign
    // value branch in the IF return cannot change the result even under join
    // fan-out. The ownership fence is skipped for MAX/MIN (it stays for
    // SUM/COUNT), so both scans may be ON. With the one-time dataset:
    // l.k1=1 (v9 1000,900) and l.k1=2 (v9 700) all satisfy l.k1 > 0, so
    // max(if(l.k1 > 0, l.v9, r.v9)) = 1000.
    explain {
        sql("""
            select max(if(l.k1 > 0, l.v9, r.v9))
            from preagg_t1 l
            inner join preagg_t2 r on l.k1 = r.k1;
        """)
        contains "(preagg_t1), PREAGGREGATION: ON"
        contains "(preagg_t2), PREAGGREGATION: ON"
    }
    order_qt_q34 """
        select max(if(l.k1 > 0, l.v9, r.v9))
        from preagg_t1 l
        inner join preagg_t2 r on l.k1 = r.k1;
    """

    // Negative: sum(DISTINCT ...) with a nested-aggregate condition slot.
    //   sum(distinct if(t.c > 0, r.v7, 0))
    // t.c = sum(t.v7) has no OriginalColumn, so splitKeyValueSlots drops it from
    // the condition check (it is unclassified). Previously this let the route
    // reach visitSum, which did NOT reject DISTINCT, so preagg_t4 was wrongly
    // turned ON. Storage SUM would then merge the duplicate full key (v7=1+1=2)
    // and break DISTINCT semantics: ON sees {1,1} → 1 while OFF sees merged 2.
    // KeyAndValueSlotsAggChecker.visitSum must reject sum.isDistinct() exactly
    // like OneValueSlotAggChecker.visitSum does.
    explain {
        sql("""
            select sum(distinct if(t.c > 0, r.v7, 0))
            from (select k1, sum(v7) as c from preagg_t1 group by k1) t
            inner join preagg_t4 r on t.k1 = r.k1;
        """)
        contains "(preagg_t1), PREAGGREGATION: ON"
        notContains "(preagg_t4), PREAGGREGATION: ON"
    }
    order_qt_q35 """
        select sum(distinct if(t.c > 0, r.v7, 0))
        from (select k1, sum(v7) as c from preagg_t1 group by k1) t
        inner join preagg_t4 r on t.k1 = r.k1;
    """

    // Negative signed-zero: signbit(max(if(k1 > 0, cast(v as float), cast(0 as float))))
    // with v a DOUBLE MAX column. No cast is peeled for MAX/MIN: DOUBLE→FLOAT can
    // underflow (-1e-300 → FLOAT -0.0) and change the observable tie representative
    // under signbit, so preagg_t5 must stay OFF. preagg_t5 holds the same full key
    // k1=1 in two loads (v = -1e-300 and +0.0); storage MAX merges to +0.0, so the
    // merged result is signbit(+0.0) = 0.
    explain {
        sql("""
            select signbit(max(if(k1 > 0, cast(v as float), cast(0 as float)))) from preagg_t5;
        """)
        notContains "(preagg_t5), PREAGGREGATION: ON"
    }
    order_qt_q36 """
        select signbit(max(if(k1 > 0, cast(v as float), cast(0 as float)))) from preagg_t5;
    """

    // Negative ASOF join selected-side: r.v7 is a direct correctly typed SUM
    // column and ts is an aggregate key, so the match condition references only
    // key columns — the pre-existing join-value check cannot keep r OFF. Only
    // the ASOF-specific fence (asofSelectedSideRelationIds) forces the selected
    // side OFF. preagg_asof_r loads the SAME full key (grp=1, ts=00:00) twice
    // with v7=100 and v7=200 in separate rowsets; storage SUM merges to 300.
    // With r OFF the probe l.ts=00:15 matches the merged row and returns 300.
    // If r were wrongly ON, ASOF sees the two identical partials (ts=00:00
    // both) and may pick either 100 or 200 — never 300 — so the oracle
    // distinguishes a faulty ON from the correct merged result, and the
    // ASOF-specific OFF reason pins the mechanism under test.
    explain {
        sql("""
            select sum(if(l.grp > 0, r.v7, 0))
            from preagg_asof_l l asof left join preagg_asof_r r
                MATCH_CONDITION(l.ts >= r.ts) on l.grp = r.grp
            where l.grp = 1;
        """)
        notContains "(preagg_asof_l), PREAGGREGATION: ON"
        contains "(preagg_asof_r), PREAGGREGATION: OFF. Reason: can't turn preAgg on because the scan is the selected side of an ASOF join"
    }
    order_qt_q37 """
        select sum(if(l.grp > 0, r.v7, 0))
        from preagg_asof_l l asof left join preagg_asof_r r
            MATCH_CONDITION(l.ts >= r.ts) on l.grp = r.grp
        where l.grp = 1;
    """

    // Positive MAX foreign-branch fanout: preagg_f_l has a non-positive key
    // (k1=-1) so if(l.k1 > 0, l.v9, r.v9) actually evaluates the foreign branch
    // r.v9, and preagg_f_r repeats BOTH full keys across separate loads
    // (k1=1: v9=1000,2000; k1=-1: v9=500,600), so PREAGG ON would expose
    // duplicate full-key partial rows. MAX is idempotent, so the foreign value
    // fanout cannot change the result: merged r is k1=1→2000, k1=-1→600;
    // max(if(1>0, 100, ...)=100, if(-1>0, ..., 600)=600) = 600, and ON over
    // partials gives max(100, 100, 500, 600) = 600 too. Both scans may be ON.
    explain {
        sql("""
            select max(if(l.k1 > 0, l.v9, r.v9))
            from preagg_f_l l inner join preagg_f_r r on l.k1 = r.k1;
        """)
        contains "(preagg_f_l), PREAGGREGATION: ON"
        contains "(preagg_f_r), PREAGGREGATION: ON"
    }
    order_qt_q38 """
        select max(if(l.k1 > 0, l.v9, r.v9))
        from preagg_f_l l inner join preagg_f_r r on l.k1 = r.k1;
    """

    // MIN symmetry of the foreign-branch fanout, on the MIN column v9m:
    // min(if(l.k1 > 0, l.v9m, r.v9m)). Merged r is k1=1→min(1000,2000)=1000,
    // k1=-1→min(500,600)=500; min(100, 500) = 100, and ON over partials gives
    // min(100, 100, 500, 600) = 100 too. Both scans ON.
    explain {
        sql("""
            select min(if(l.k1 > 0, l.v9m, r.v9m))
            from preagg_f_l l inner join preagg_f_r r on l.k1 = r.k1;
        """)
        contains "(preagg_f_l), PREAGGREGATION: ON"
        contains "(preagg_f_r), PREAGGREGATION: ON"
    }
    order_qt_q39 """
        select min(if(l.k1 > 0, l.v9m, r.v9m))
        from preagg_f_l l inner join preagg_f_r r on l.k1 = r.k1;
    """

    // Retained non-movable project expressions (e.g. assert_true) must run on
    // storage-merged rows. pruneOutputs deliberately keeps the unused
    // assert_true(v7 > 0, 'bad') output; pre-agg ON would evaluate it on the
    // raw partial row v7=-2 (preagg_g loads (1,-2,10) and (1,3,20) in separate
    // rowsets) and throw InvalidArgument, while pre-agg OFF merges v7 to
    // -2+3=1 and the assert passes, returning max(if(1>0, 20, 0)) = 20.
    explain {
        sql("""
            select max(if(k1 > 0, v9, 0))
            from (select k1, v9, assert_true(v7 > 0, 'bad') as checked from preagg_g) t
        """)
        notContains "(preagg_g), PREAGGREGATION: ON"
    }
    order_qt_test_c """
        select max(if(k1 > 0, v9, 0))
        from (select k1, v9, assert_true(v7 > 0, 'bad') as checked from preagg_g) t
    """

    // Slotless volatile retained non-movable outputs (e.g.
    // assert_true(random() >= 0, 'bad')) have no input slots, so the value-slot
    // fence misses them, and the volatility checks only cover agg/filter/join/
    // grouping expressions. Pre-agg ON would evaluate random() once per raw
    // partial row (preagg_g loads (1,-2,10) and (1,3,20) in separate rowsets)
    // instead of once per merged row — a different evaluation cardinality — so
    // preagg_g must stay OFF. random() >= 0 always holds, so the assert never
    // fires and the query still returns 20.
    explain {
        sql("""
            select max(if(k1 > 0, v9, 0))
            from (select k1, v9, assert_true(random() >= 0, 'bad') as checked from preagg_g) t
        """)
        notContains "(preagg_g), PREAGGREGATION: ON"
    }
    order_qt_test_f """
        select max(if(k1 > 0, v9, 0))
        from (select k1, v9, assert_true(random() >= 0, 'bad') as checked from preagg_g) t
    """

    // Exercise the literal acceptance in KeyAndValueSlotsAggChecker.visitMax:
    // max(if(k6 > 0, v9, 0)) has a cast-free non-NULL literal (0) in the else
    // branch. It reaches the checker with pre-agg ON — k6 is a key column, v9 a
    // MAX column, the literal 0 is accepted. If literal acceptance regressed to
    // NULL-only, this scan would flip OFF and the suite would miss it.
    explain {
        sql("""
            select max(if(k6 > 0, v9, 0)) from preagg_t1;
        """)
        contains "(preagg_t1), PREAGGREGATION: ON"
    }
    order_qt_test_d """
        select max(if(k6 > 0, v9, 0)) from preagg_t1;
    """

    // MIN/CASE symmetry for the parallel changed branch (visitMin): the CASE
    // else branch holds a cast-free non-NULL literal 0, and preagg_f_l's
    // non-positive key k1=-1 makes the else branch actually evaluate. pre-agg
    // stays ON via the same literal acceptance.
    explain {
        sql("""
            select min(case when k1 > 0 then v9m else 0 end) from preagg_f_l;
        """)
        contains "(preagg_f_l), PREAGGREGATION: ON"
    }
    order_qt_test_e """
        select min(case when k1 > 0 then v9m else 0 end) from preagg_f_l;
    """

}
