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

suite("infer_predicate_qualify") {
    sql "drop table if exists infer_predicate_qualify_input"
    sql """
        create table infer_predicate_qualify_input (k bigint not null, a bigint not null, b bigint not null)
        duplicate key(k) distributed by hash(k) buckets 1
        properties("replication_num"="1")
    """
    sql "insert into infer_predicate_qualify_input values (1,2,1),(2,2,1)"

    order_qt_strict """
        select t.k, t.a, t.b, row_number() over (order by t.k) as rn
        from infer_predicate_qualify_input t
        qualify t.a > t.b and rn > t.b and t.a > rn
    """
    order_qt_non_strict """
        select t.k, t.a, t.b, row_number() over (order by t.k) as rn
        from infer_predicate_qualify_input t
        qualify t.a >= t.b and rn >= t.b and t.a >= rn
    """
    order_qt_mixed """
        select t.k, t.a, t.b, row_number() over (order by t.k) as rn
        from infer_predicate_qualify_input t
        qualify t.a >= t.b and rn >= t.b and t.a > rn
    """
    order_qt_reordered """
        select t.k, t.a, t.b, row_number() over (order by t.k) as rn
        from infer_predicate_qualify_input t
        qualify t.a > rn and rn > t.b and t.a > t.b
    """
    // Include a row satisfying all strict comparisons, so preserving every row is also detected.
    sql "insert into infer_predicate_qualify_input values (3,4,1)"
    order_qt_matching_row """
        select t.k, t.a, t.b, row_number() over (order by t.k) as rn
        from infer_predicate_qualify_input t
        qualify t.a > t.b and rn > t.b and t.a > rn
    """
}
