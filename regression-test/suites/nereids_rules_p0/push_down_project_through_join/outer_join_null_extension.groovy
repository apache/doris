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

suite("outer_join_null_extension") {
    sql "set enable_prune_nested_column=true"

    sql "drop table if exists outer_join_null_extension_left"
    sql "drop table if exists outer_join_null_extension_right"

    sql """
        create table outer_join_null_extension_left (
            id int,
            arr array<int>
        ) duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        create table outer_join_null_extension_right (
            id int,
            arr array<int>
        ) duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql "insert into outer_join_null_extension_left values (1, [1]), (2, [2])"
    sql "insert into outer_join_null_extension_right values (1, [1]), (3, [3])"

    // The expression is non-null for a null-extended right row, so it must stay above the join.
    order_qt_left_project """
        select l.id, element_at(coalesce(r.arr, [99]), 1)
        from outer_join_null_extension_left l
        left join outer_join_null_extension_right r on l.id = r.id
        order by l.id
    """
    order_qt_left_filter """
        select l.id
        from outer_join_null_extension_left l
        left join outer_join_null_extension_right r on l.id = r.id
        where element_at(coalesce(r.arr, [99]), 1) = 99
        order by l.id
    """

    // Mirror the same checks for the null-generating left side of a right outer join.
    order_qt_right_project """
        select r.id, element_at(coalesce(l.arr, [98]), 1)
        from outer_join_null_extension_left l
        right join outer_join_null_extension_right r on l.id = r.id
        order by r.id
    """
    order_qt_right_filter """
        select r.id
        from outer_join_null_extension_left l
        right join outer_join_null_extension_right r on l.id = r.id
        where element_at(coalesce(l.arr, [98]), 1) = 98
        order by r.id
    """

    // Both inputs are null-generating for a full outer join.
    order_qt_full_project """
        select coalesce(l.id, r.id),
               element_at(coalesce(l.arr, [98]), 1),
               element_at(coalesce(r.arr, [99]), 1)
        from outer_join_null_extension_left l
        full outer join outer_join_null_extension_right r on l.id = r.id
        order by coalesce(l.id, r.id)
    """

    // Control cases: preserved-side expressions remain pushable and null-propagating expressions are safe.
    order_qt_preserved_side """
        select l.id, element_at(coalesce(l.arr, [98]), 1)
        from outer_join_null_extension_left l
        left join outer_join_null_extension_right r on l.id = r.id
        order by l.id
    """
    order_qt_null_propagating """
        select l.id, element_at(r.arr, 1)
        from outer_join_null_extension_left l
        left join outer_join_null_extension_right r on l.id = r.id
        order by l.id
    """

    // Disabling the rule must produce the same result as the guarded rewrite.
    sql "set disable_nereids_rules='PUSH_DOWN_PROJECT_THROUGH_JOIN'"
    order_qt_left_project_rule_disabled """
        select l.id, element_at(coalesce(r.arr, [99]), 1)
        from outer_join_null_extension_left l
        left join outer_join_null_extension_right r on l.id = r.id
        order by l.id
    """
    sql "set disable_nereids_rules=''"
}
