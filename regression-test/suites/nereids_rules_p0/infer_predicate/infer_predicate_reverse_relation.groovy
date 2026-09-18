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

suite("infer_predicate_reverse_relation") {
    sql "drop table if exists infer_predicate_reverse_relation_input"
    sql """
        create table infer_predicate_reverse_relation_input (
            k int not null, a int null, b int null, c int null
        ) duplicate key(k) distributed by hash(k) buckets 1
        properties("replication_num"="1")
    """
    sql """
        insert into infer_predicate_reverse_relation_input values
        (1,1,1,1),(2,2,2,2),(3,3,2,1),(4,1,2,3),
        (5,null,1,1),(6,1,null,1),(7,1,1,null),(8,null,null,null)
    """

    // Both strict comparisons contradict the equalities. Inference must not admit a = b = c.
    order_qt_strict """
        select * from infer_predicate_reverse_relation_input
        where a = c and c > a and c >= b and b > c and b = c
    """
    order_qt_reordered """
        select * from infer_predicate_reverse_relation_input
        where b = c and b > c and c >= b and c > a and a = c
    """
    order_qt_commuted """
        select * from infer_predicate_reverse_relation_input
        where c = a and a < c and b <= c and c < b and c = b
    """
    // Equal non-null rows do satisfy the non-strict variant.
    order_qt_non_strict """
        select * from infer_predicate_reverse_relation_input
        where a = c and c >= a and c >= b and b >= c and b = c
    """
}
