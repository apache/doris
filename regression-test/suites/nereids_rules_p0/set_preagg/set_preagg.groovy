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
        contains "(preagg_t2), PREAGGREGATION: OFF. Reason: count("
        contains "(preagg_t3), PREAGGREGATION: OFF. Reason: can't turn preAgg on because aggregate function sum"
    }

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
        notContains "PREAGGREGATION: OFF"
    }

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
        notContains "PREAGGREGATION: OFF"
    }

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
        contains "(preagg_t3), PREAGGREGATION: OFF. Reason: can't turn preAgg on because aggregate function sum"
    }
}
