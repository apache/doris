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

suite("outer_join_runtime_filter") {
    sql "drop table if exists outer_rf_contact"
    sql "drop table if exists outer_rf_address"
    sql "drop table if exists outer_rf_phone"
    sql "drop table if exists outer_rf_event"

    sql """create table outer_rf_contact (org int, k int, v int)
        duplicate key(org, k) distributed by hash(org) buckets 2
        properties("replication_num" = "1")"""
    sql """create table outer_rf_address (org int, k int, v int)
        duplicate key(org, k) distributed by hash(org) buckets 2
        properties("replication_num" = "1")"""
    sql """create table outer_rf_phone (org int, k int, v int)
        duplicate key(org, k) distributed by hash(org) buckets 2
        properties("replication_num" = "1")"""
    sql """create table outer_rf_event (org int, k int)
        duplicate key(org, k) distributed by hash(org) buckets 2
        properties("replication_num" = "1")"""

    sql """insert into outer_rf_contact values
        (1, 1, 11), (1, 1, 111), (1, 2, 12), (2, 1, 21), (2, 2, 22),
        (1, null, 10), (null, 1, 1), (3, 3, 33)"""
    sql """insert into outer_rf_address values
        (1, 1, 100), (1, 1, 101), (2, 2, 202),
        (1, null, 110), (null, 1, 10), (4, 4, 404)"""
    sql "insert into outer_rf_phone values (1, 1, 1000), (2, 2, 2000), (3, 3, 3000)"
    sql "insert into outer_rf_event values (1, 1), (2, 2), (3, 3), (1, null), (null, 1), (1, 0)"
    sql "sync"

    sql "set disable_join_reorder = true"
    sql "set enable_runtime_filter_prune = false"
    sql "set runtime_filter_mode = 'GLOBAL'"
    sql "set runtime_filter_type = 8"
    // Waiting for the new consumers also exercises the build-side RF dependency.
    sql "set runtime_filter_wait_infinitely = true"

    def queries = [
        left: """select a.org, a.k, a.v, b.v from outer_rf_contact a
            left join outer_rf_address b on a.org = b.org and a.k = b.k
            inner join outer_rf_event e on a.org = e.org and a.k = e.k""",
        right: """select a.org, a.k, a.v, b.v from outer_rf_address b
            right join outer_rf_contact a on b.org = a.org and b.k = a.k
            inner join outer_rf_event e on a.org = e.org and a.k = e.k""",
        nested: """select a.org, a.k, a.v, b.v, p.v from outer_rf_contact a
            left join outer_rf_address b on b.org = a.org and b.k = a.k
            left join outer_rf_phone p on a.org = p.org and a.k = p.k
            inner join outer_rf_event e on a.org = e.org and a.k = e.k""",
        alias: """select a.org, a.k, a.v, b.v from outer_rf_contact a
            left join (select org as o, k as id, v from outer_rf_address) b
                on a.org = b.o and a.k = b.id
            inner join outer_rf_event e on a.org = e.org and a.k = e.k""",
        shared_cte: """with addresses as (select * from outer_rf_address)
            select a.org, a.k, a.v, b.v, p.v from outer_rf_contact a
            left join addresses b on a.org = b.org and a.k = b.k
            left join addresses p on a.org = p.org and a.k = p.k
            inner join outer_rf_event e on a.org = e.org and a.k = e.k""",
        null_safe_outer: """select a.org, a.k, a.v, b.v from outer_rf_contact a
            left join outer_rf_address b on a.org <=> b.org and a.k <=> b.k
            inner join outer_rf_event e on a.org = e.org and a.k = e.k""",
        null_safe_parent: """select a.org, a.k, a.v, b.v from outer_rf_contact a
            left join outer_rf_address b on a.org = b.org and a.k = b.k
            inner join outer_rf_event e on a.org <=> e.org and a.k <=> e.k""",
        coalesce: """select a.org, a.k, a.v, b.v from outer_rf_contact a
            left join outer_rf_address b on a.org = b.org and a.k = b.k
            inner join outer_rf_event e on a.org = e.org and coalesce(b.k, 0) = e.k"""
    ]

    [false, true].each { expand ->
        sql "set expand_runtime_filter_by_inner_join = ${expand}"
        queries.each { name, query ->
            "order_qt_${name}_${expand}"(query)
        }
    }

    qt_left_plan "explain shape plan ${queries.left}"
    qt_nested_plan "explain shape plan ${queries.nested}"

    // Check IN, BLOOM, MIN_MAX and IN_OR_BLOOM through the same multi-target path.
    [1, 2, 4, 8].each { type ->
        sql "set runtime_filter_type = ${type}"
        "order_qt_filter_type_${type}"(queries.left)
    }

    sql "truncate table outer_rf_address"
    order_qt_empty_non_preserved queries.left
    sql "truncate table outer_rf_event"
    order_qt_empty_producer queries.left
}
