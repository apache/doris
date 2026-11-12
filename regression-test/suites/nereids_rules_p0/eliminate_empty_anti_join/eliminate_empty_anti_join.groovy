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

suite("eliminate_empty_anti_join") {
    sql "drop table if exists empty_anti_join_t"
    sql """create table empty_anti_join_t (k int, v int)
        duplicate key(k) distributed by hash(k) buckets 1
        properties ("replication_num" = "1")"""
    sql "insert into empty_anti_join_t values (1, 10), (1, 10), (2, 20), (null, 30)"

    def emptyLeft = """select l.k, l.v from
        (select k, v from empty_anti_join_t where false) l
        left anti join empty_anti_join_t r on l.k = r.k"""
    def emptyRight = """select r.k, r.v from empty_anti_join_t l
        right anti join (select k, v from empty_anti_join_t where false) r on l.k = r.k"""
    def otherRightEmpty = """select l.k, l.v from empty_anti_join_t l
        left anti join (select k, v from empty_anti_join_t where false) r on l.k = r.k"""
    def otherLeftEmpty = """select r.k, r.v from
        (select k, v from empty_anti_join_t where false) l
        right anti join empty_anti_join_t r on l.k = r.k"""

    qt_left_plan "explain shape plan ${emptyLeft}"
    qt_right_plan "explain shape plan ${emptyRight}"
    order_qt_left_result emptyLeft
    order_qt_right_result emptyRight
    order_qt_other_right_empty otherRightEmpty
    order_qt_other_left_empty otherLeftEmpty
    order_qt_left_both_empty """select l.k, l.v from
        (select k, v from empty_anti_join_t where false) l
        left anti join (select k, v from empty_anti_join_t where false) r on l.k = r.k"""
    order_qt_right_both_empty """select r.k, r.v from
        (select k, v from empty_anti_join_t where false) l
        right anti join (select k, v from empty_anti_join_t where false) r on l.k = r.k"""

    def nested = """select v, k from (${emptyLeft} union all ${emptyRight}) e
        union all select v, k from (${emptyLeft}) e"""
    qt_nested_plan "explain shape plan ${nested}"
    order_qt_nested_result nested
    order_qt_union_survivor "${nested} union all select v, k from empty_anti_join_t"
    order_qt_union_constant "${nested} union all select 99, 9"
    order_qt_union_distinct "${emptyLeft} union select k, v from empty_anti_join_t"
    order_qt_global_aggregate "select count(*) from (${emptyLeft}) e"
    order_qt_mark_result """select k, not exists
        (select 1 from empty_anti_join_t r where false and r.k = l.k)
        from empty_anti_join_t l"""
}
