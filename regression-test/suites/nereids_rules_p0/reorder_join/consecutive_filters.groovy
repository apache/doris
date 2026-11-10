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

suite("consecutive_filters") {
    sql "drop table if exists reorder_filter_a"
    sql "drop table if exists reorder_filter_b"
    sql "drop table if exists reorder_filter_c"
    sql """create table reorder_filter_a (k int null, v int)
        duplicate key(k) distributed by hash(k) buckets 1 properties('replication_num'='1')"""
    sql """create table reorder_filter_b (k int null, v int)
        duplicate key(k) distributed by hash(k) buckets 1 properties('replication_num'='1')"""
    sql """create table reorder_filter_c (k int null, v int)
        duplicate key(k) distributed by hash(k) buckets 1 properties('replication_num'='1')"""
    sql "insert into reorder_filter_a values (null,10),(1,11),(2,12),(2,13)"
    sql "insert into reorder_filter_b values (null,20),(1,21),(1,22),(3,23)"
    sql "insert into reorder_filter_c values (null,30),(1,31),(2,32),(4,33)"

    // Leave filter pushdown to ReorderJoin so its retry sees consecutive filters.
    // RAND() >= 0 keeps a volatile conjunct while producing a deterministic result.
    order_qt_volatile_retry """
        select /*+ SET_VAR(disable_nereids_rules='PUSH_DOWN_FILTER_THROUGH_JOIN') */ a.k, b.k
        from reorder_filter_a a left join reorder_filter_b b on a.k = b.k
        where a.k is null and b.k is null and rand() >= 0
    """

    order_qt_left """
        with ab as (
            select a.k as ak, a.v as av, b.k as bk, b.v as bv
            from reorder_filter_a a left join reorder_filter_b b on a.k = b.k
            where b.k is null
        )
        select ab.ak, ab.av, ab.bk, ab.bv, c.k, c.v
        from ab left join reorder_filter_c c on ab.ak = c.k
        where (ab.ak is null or ab.ak = 2) and (c.k is null or c.k = 2)
    """

    order_qt_right """
        with ab as (
            select a.k as ak, a.v as av, b.k as bk, b.v as bv
            from reorder_filter_a a right join reorder_filter_b b on a.k = b.k
            where a.k is null
        )
        select ab.ak, ab.av, ab.bk, ab.bv, c.k, c.v
        from reorder_filter_c c right join ab on c.k = ab.bk
        where (ab.bk is null or ab.bk = 3) and (c.k is null or c.k = 3)
    """

    order_qt_full """
        with ab as (
            select a.k as ak, a.v as av, b.k as bk, b.v as bv
            from reorder_filter_a a full join reorder_filter_b b on a.k = b.k
            where a.k is null or b.k is null
        )
        select ab.ak, ab.av, ab.bk, ab.bv, c.k, c.v
        from ab full join reorder_filter_c c on coalesce(ab.ak, ab.bk) = c.k
        where (ab.ak is null or ab.bk is null) and (c.k is null or c.k in (2,3,4))
    """

    order_qt_shared_cte """
        with ab as (
            select a.k as ak, a.v as av, b.k as bk, b.v as bv
            from reorder_filter_a a left join reorder_filter_b b on a.k = b.k
            where b.k is null
        )
        select ab.ak, ab.av, c.k, c.v
        from ab left join reorder_filter_c c on ab.ak = c.k
        where ab.ak is null and c.k is null
        union all
        select ab.ak, ab.av, c.k, c.v
        from ab left join reorder_filter_c c on ab.ak = c.k
        where (ab.ak is null or ab.ak = 2) and (c.k is null or c.k = 2)
    """
}
