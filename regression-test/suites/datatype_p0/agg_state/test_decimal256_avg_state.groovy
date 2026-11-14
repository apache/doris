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

suite("test_decimal256_avg_state") {
    sql "set enable_agg_state=true"
    sql "drop table if exists decimal256_avg_state_input"
    sql """create table decimal256_avg_state_input (k int, v decimal(18,2))
        duplicate key(k) distributed by hash(k) buckets 3
        properties("replication_num"="1")"""
    sql """insert into decimal256_avg_state_input values
        (1,10),(1,20),(2,100),(3,null),(4,40)"""
    sql "set enable_decimal256=true"
    order_qt_direct "select avg(v),avg_merge(avg_state(v)) from decimal256_avg_state_input"
    order_qt_rollup_original """select a,b,c,avg(v) from
        (select k a,k%2 b,k%3 c,v from decimal256_avg_state_input) t group by rollup(a,b,c)"""
    // Exercise the equivalent ROLLUP state plan independently of optimizer rewrites.
    order_qt_rollup_state_path """with preagg as (
        select k a,k%2 b,k%3 c,avg_combine(v) s from decimal256_avg_state_input
        group by k,k%2,k%3)
        select a,b,c,avg_finalize(s) from preagg
        union all
        select a,b,cast(null as int) c,avg_merge(s) from preagg
        group by grouping sets ((a,b),(a),())"""
    order_qt_grouped """select k,avg(v),avg_merge(avg_state(v))
        from decimal256_avg_state_input group by k"""
    order_qt_finalize """select k,avg_finalize(s) from
        (select k,avg_combine(v) s from decimal256_avg_state_input group by k) t"""
    order_qt_merge """select avg_merge(s) from
        (select k,avg_combine(v) s from decimal256_avg_state_input group by k) t"""
    order_qt_union """select avg_finalize(s) from
        (select avg_union(avg_state(v)) s from decimal256_avg_state_input) t"""
    order_qt_empty """select avg(v),avg_merge(avg_state(v))
        from decimal256_avg_state_input where k<0"""
    order_qt_all_null """select avg(v),avg_merge(avg_state(v))
        from decimal256_avg_state_input where k=3"""

    order_qt_decimal32_scale6 """select avg(cast(v as decimal(9,6))),
        avg_merge(avg_state(cast(v as decimal(9,6)))) from decimal256_avg_state_input"""
    order_qt_decimal128_scale6 """select avg(cast(v as decimal(38,6))),
        avg_merge(avg_state(cast(v as decimal(38,6)))) from decimal256_avg_state_input"""
    order_qt_decimal256_input """select avg(cast(v as decimal(76,6))),
        avg_merge(avg_state(cast(v as decimal(76,6)))) from decimal256_avg_state_input"""
    // The sum exceeds Decimal128's range, so widening only the final output is insufficient.
    order_qt_wide_accumulator """select avg(v),avg_merge(avg_state(v)) from
        (select cast('99999999999999999999999999999999999999' as decimal(38,0)) v
         from decimal256_avg_state_input) t"""
    order_qt_constant """select avg(cast(12.34 as decimal(18,2))),
        avg_merge(avg_state(cast(12.34 as decimal(18,2))))"""

    sql "drop table if exists decimal256_avg_state_explicit"
    sql """create table decimal256_avg_state_explicit (
        k int,s agg_state<AVG(decimal(18,2))> generic)
        aggregate key(k) distributed by hash(k) buckets 3 properties("replication_num"="1")"""
    sql """insert into decimal256_avg_state_explicit
        select k,avg_state(v) from decimal256_avg_state_input"""
    order_qt_explicit "select k,avg_finalize(s) from decimal256_avg_state_explicit"

    // Stored combine states must retain their signature through FE catalog and BE storage metadata.
    sql "drop table if exists decimal256_avg_state_stored"
    sql """create table decimal256_avg_state_stored (
        k int,s agg_state<AVG(decimal(18,2))> generic)
        aggregate key(k) distributed by hash(k) buckets 3 properties("replication_num"="1")"""
    sql """insert into decimal256_avg_state_stored
        select k,avg_combine(v) s from decimal256_avg_state_input group by k"""
    sql "set enable_decimal256=false"
    order_qt_stored "select k,avg_finalize(s) from decimal256_avg_state_stored"
    order_qt_stored_merge "select avg_merge(s) from decimal256_avg_state_stored"
    order_qt_stored_union """select avg_finalize(s) from
        (select avg_union(s) s from decimal256_avg_state_stored) t"""
    order_qt_decimal128 "select avg(v),avg_merge(avg_state(v)) from decimal256_avg_state_input"

    sql "drop table if exists decimal128_avg_state_stored"
    sql """create table decimal128_avg_state_stored (
        k int,s agg_state<avg(decimal(18,2))> generic)
        aggregate key(k) distributed by hash(k) buckets 3 properties("replication_num"="1")"""
    sql """insert into decimal128_avg_state_stored
        select k,avg_state(v) from decimal256_avg_state_input"""
    sql "set enable_decimal256=true"
    order_qt_legacy "select k,avg_finalize(s) from decimal128_avg_state_stored"
    order_qt_legacy_merge "select avg_merge(s) from decimal128_avg_state_stored"
    // SQL declarations infer their result type from the session; recreate with the producer setting.
    sql "drop table if exists decimal128_avg_state_recreated"
    def savedDdl = sql "show create table decimal128_avg_state_stored"
    sql "set enable_decimal256=false"
    sql savedDdl[0][1].replace("decimal128_avg_state_stored", "decimal128_avg_state_recreated")
    sql "insert into decimal128_avg_state_recreated select * from decimal128_avg_state_stored"
    order_qt_recreated_legacy "select avg_merge(s) from decimal128_avg_state_recreated"
    sql "set enable_decimal256=true"
    test {
        sql """insert into decimal128_avg_state_stored
            select k,avg_state(v) from decimal256_avg_state_input"""
        exception "exact AggState type match"
    }
}
