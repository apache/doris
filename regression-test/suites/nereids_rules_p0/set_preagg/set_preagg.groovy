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
    multi_sql """
        set disable_nereids_rules='PRUNE_EMPTY_PARTITION';
        set forbid_unknown_col_stats=false;
        set enable_stats=false;
        drop table if exists preagg_t1;
        drop table if exists preagg_t2;
        drop table if exists preagg_t3;

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
    """

    // One-time data setup. All suites below rely on this single dataset so the
    // whole file is self-contained and re-runnable. The data deliberately
    // includes repeated aggregate-key material:
    //   - preagg_t1 has two rows with k1=1 / k1=-1 (abs(k1) maps both to the
    //     same derived key 1) to exercise derived-key fan-out.
    //   - k6 takes 1/2/0 so IF(k6 > 0, ...) conditions have both true/false rows.
    //   - v7/v8 are SUM columns, v9 is a MAX column.
    sql """
        truncate table preagg_t1;
        truncate table preagg_t2;
        truncate table preagg_t3;

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
    """

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

    // max(cast(v9 as double)): a widening numeric cast wraps a value column.
    // The cast-unwrapping loop peels it so OneValueSlotAggChecker sees v9
    // with MAX aggregation type and returns ON — storage MAX + cast is safe.
    explain {
        sql("""select max(cast(v9 as double)) from preagg_t1;""")
        contains "(preagg_t1), PREAGGREGATION: ON"
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

    // Mixed-path IF with cast in return: max(if(k6 > 0, cast(v9 as double), 0))
    // enters checkAggWithKeyAndValueSlots. The return cast is safe for MAX
    // (max(cast(x)) = cast(max(x))), so the strip+match yields ON.
    explain {
        sql("""
            select max(if(k6 > 0, cast(v9 as double), 0)) from preagg_t1;
        """)
        contains "(preagg_t1), PREAGGREGATION: ON"
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

    // --- SAFE vs UNSAFE cast regression tests ---
    // peelCastForMaxMin peels casts that are order-preserving for MAX/MIN:
    //   - Injective numeric→numeric casts (widening integral/decimal)
    //   - Numeric→float casts (nondecreasing, e.g. BIGINT→DOUBLE)
    // It rejects:
    //   - Non-injective narrowing casts (e.g. BIGINT→TINYINT, overflow)
    //   - Injective but order-changing casts (e.g. BIGINT→STRING)

    // Positive: BIGINT→DECIMAL(20,0) is injective (wider range), numeric → ON.
    explain {
        sql("""select max(cast(v9 as decimal(20,0))) from preagg_t1;""")
        contains "(preagg_t1), PREAGGREGATION: ON"
    }
    order_qt_q26 """select max(cast(v9 as decimal(20,0))) from preagg_t1;"""

    // Positive: BIGINT→LARGEINT is a widening integral cast (injective) → ON.
    explain {
        sql("""select max(cast(v9 as largeint)) from preagg_t1;""")
        contains "(preagg_t1), PREAGGREGATION: ON"
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

    // Mixed-path IF with safe cast: max(if(..., cast(v9 as decimal(20,0)), 0))
    // return cast is injective numeric → peeled → matches MAX → ON.
    explain {
        sql("""
            select max(if(k6 > 0, cast(v9 as decimal(20,0)), 0)) from preagg_t1;
        """)
        contains "(preagg_t1), PREAGGREGATION: ON"
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

    // CASE WHEN with safe widening cast → ON.
    explain {
        sql("""
            select max(case when k6 > 0 then cast(v9 as decimal(20,0)) else 0 end) from preagg_t1;
        """)
        contains "(preagg_t1), PREAGGREGATION: ON"
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

    // Test A: derived-key fan-out. t has two different keys k1=1 and k1=-1 that
    // both map to the same derived key a=abs(k1)=1; r.v7 is a foreign value
    // used in the IF return. t must stay OFF (aggSlots intersection keeps only
    // local key k1 → key-only path → sum is not distinct), so each logical row
    // of t joins every r row with r.k1 = a and counts r.v7 once. With the
    // one-time dataset: a=1 (3 t-rows) × r.k1=1 (2 r-rows, v7=50,60) plus
    // a=2 (1 t-row) × r.k1=2 (1 r-row, v7=70) → 3*(50+60)+70 = 400. If t were
    // wrongly ON, its rows would fan out under join.
    // r has local v7 and foreign key condition → ON is safe.
    explain {
        sql("""
            select sum(if(t.a > 0, r.v7, 0))
            from (select abs(k1) as a from preagg_t1) t
            inner join preagg_t2 r on t.a = r.k1;
        """)
        notContains "(preagg_t1), PREAGGREGATION: ON"
        contains "(preagg_t2), PREAGGREGATION: ON"
    }
    order_qt_test_a """
        select sum(if(t.a > 0, r.v7, 0)) as res
        from (select abs(k1) as a from preagg_t1) t
        inner join preagg_t2 r on t.a = r.k1;
    """

    // Test B: foreign value in IF return on BOTH sides — ownership check must
    // turn both scans OFF:
    //   sum(if(t.a > 0, t.v7, r.v7))
    //   - for scan t: return r.v7 is foreign → t OFF
    //   - for scan r: return t.v7 is foreign → r OFF
    // With both OFF, storage merges by key and the logical result is exact.
    // With the one-time dataset: a=1 (3 t-rows, v7=10,20,30) × r.k1=1 (2 rows)
    // → 2*(10+20+30)=120, plus a=2 (t.v7=40) × r.k1=2 → 40, total 160.
    explain {
        sql("""
            select sum(if(t.a > 0, t.v7, r.v7))
            from (select abs(k1) as a, v7 from preagg_t1) t
            inner join preagg_t2 r on t.a = r.k1;
        """)
        notContains "(preagg_t1), PREAGGREGATION: ON"
        notContains "(preagg_t2), PREAGGREGATION: ON"
    }
    order_qt_test_b """
        select sum(if(t.a > 0, t.v7, r.v7)) as res
        from (select abs(k1) as a, v7 from preagg_t1) t
        inner join preagg_t2 r on t.a = r.k1;
    """

}
