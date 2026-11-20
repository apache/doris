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

suite("outer_join_runtime_filter_row_count") {
    sql "drop table if exists outer_rf_row_count_left"
    sql "drop table if exists outer_rf_row_count_payload"
    sql "drop table if exists outer_rf_row_count_right"
    sql "drop table if exists outer_rf_row_count_build"

    sql """create table outer_rf_row_count_left (k int)
        duplicate key(k) distributed by hash(k) buckets 1
        properties("replication_num" = "1")"""
    sql """create table outer_rf_row_count_payload (v int)
        duplicate key(v) distributed by hash(v) buckets 1
        properties("replication_num" = "1")"""
    sql """create table outer_rf_row_count_right (k int, v int)
        duplicate key(k) distributed by hash(k) buckets 1
        properties("replication_num" = "1")"""
    sql """create table outer_rf_row_count_build (k int)
        duplicate key(k) distributed by hash(k) buckets 1
        properties("replication_num" = "1")"""

    sql "insert into outer_rf_row_count_left values (2)"
    sql "insert into outer_rf_row_count_payload values (20)"
    sql "insert into outer_rf_row_count_right values (1, 10), (2, 20)"
    sql "insert into outer_rf_row_count_build values (2)"

    sql "set disable_join_reorder = true"
    sql "set enable_runtime_filter_prune = false"
    sql "set expand_runtime_filter_by_inner_join = true"
    sql "set runtime_filter_mode = 'GLOBAL'"
    sql "set runtime_filter_type = 1"
    sql "set runtime_filter_wait_infinitely = true"

    def scalarQuery = """select l.k, d.v from outer_rf_row_count_left l
        left join outer_rf_row_count_payload d
            on l.k = (select r.k from outer_rf_row_count_right r)
        inner join outer_rf_row_count_build e on l.k = e.k"""
    qt_scalar_plan "explain shape plan ${scalarQuery}"

    // Keep the Sort and Limit separate to exercise the PhysicalLimit visitor.
    sql "set disable_nereids_rules = 'LIMIT_SORT_TO_TOP_N,ELIMINATE_SORT'"
    def limitQuery = """select l.k, r.v from outer_rf_row_count_left l
        left join (select k, v from outer_rf_row_count_right order by k limit 1) r on l.k = r.k
        inner join outer_rf_row_count_build e on l.k = e.k"""
    qt_limit_plan "explain shape plan ${limitQuery}"
    order_qt_limit_result limitQuery
    sql "set disable_nereids_rules = default"

    test {
        sql scalarQuery
        exception "Expected EQ 1 to be returned by expression"
    }

    // The build-side projection must evaluate the row with k = 1 before the ancestor join filters it.
    sql "insert into outer_rf_row_count_left values (1)"
    def projectQuery = """select l.k, r.checked from outer_rf_row_count_left l
        left join (
            select k, assert_true(v > 10, 'bad') as checked from outer_rf_row_count_right
        ) r on l.k = r.k
        inner join outer_rf_row_count_build e on l.k = e.k"""
    for (def expand : [false, true]) {
        sql "set expand_runtime_filter_by_inner_join = ${expand}"
        test {
            sql projectQuery
            exception "bad"
        }
    }
}
