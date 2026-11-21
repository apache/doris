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

suite("nested_mtmv_struct_info") {
    String db = context.config.getDbNameByFile(context.file)
    sql "set enable_materialized_view_rewrite = true"
    sql "set enable_materialized_view_nest_rewrite = true"
    sql "set enable_sql_cache = false"

    sql "drop materialized view if exists struct_info_nested_mv"
    sql "drop materialized view if exists struct_info_left_mv"
    sql "drop materialized view if exists struct_info_right_mv"
    sql "drop table if exists struct_info_table1"
    sql "drop table if exists struct_info_table2"
    sql """
        create table struct_info_table1 (id int, value int)
        partition by range(id) (
            partition p1 values [('1'), ('2')),
            partition p2 values [('2'), ('3')),
            partition p3 values [('3'), ('4'))
        )
        distributed by hash(id) buckets 2
        properties ('replication_num' = '1')
    """
    sql "create table struct_info_table2 like struct_info_table1"
    sql "insert into struct_info_table1 values (1, 1), (2, 2), (3, 3)"
    sql "insert into struct_info_table2 values (1, 1), (2, 2), (3, 3)"

    create_async_partition_mv(db, "struct_info_left_mv", """
        select t1.id, t2.value
        from struct_info_table1 t1 left join struct_info_table2 t2 on t1.id = t2.id
    """, "(id)")
    create_async_partition_mv(db, "struct_info_right_mv", """
        select t2.id, t2.value
        from struct_info_table1 t1 right join struct_info_table2 t2 on t1.id = t2.id
    """, "(id)")
    String query = """
        select t1.id, t2.value
        from struct_info_left_mv t1 left join struct_info_right_mv t2 on t1.id = t2.id
    """
    create_async_partition_mv(db, "struct_info_nested_mv", query, "(id)")
    String baseQuery = """
        select t1.id, t2.value
        from struct_info_table1 t1 left join struct_info_table2 t2 on t1.id = t2.id
    """

    mv_rewrite_success_without_check_chosen(query, "struct_info_nested_mv")
    order_qt_initial query
    sql "set enable_materialized_view_rewrite = false"
    order_qt_initial_without_rewrite query
    sql "set enable_materialized_view_rewrite = true"

    // The reported INSERT of id=4 needs a partition: the original p3 upper bound is exclusive.
    sql "alter table struct_info_table1 add partition p4 values [('4'), ('5'))"
    sql "insert into struct_info_table1 values (4, 4)"
    order_qt_stale_first_level baseQuery
    sql "set enable_materialized_view_rewrite = false"
    order_qt_stale_first_level_without_rewrite baseQuery
    sql "set enable_materialized_view_rewrite = true"
    sql "refresh materialized view struct_info_left_mv auto"
    waitingMTMVTaskFinished(getJobName(db, "struct_info_left_mv"))
    sql "sync"

    // Refresh only the first-level MV. Exploring rewrite with the stale nested MV
    // must retain the correct child-to-bitmap correspondence.
    order_qt_stale_nested query
    sql "set enable_materialized_view_rewrite = false"
    order_qt_stale_nested_without_rewrite query
    sql "set enable_materialized_view_rewrite = true"
    sql "set enable_sql_cache = true"
    order_qt_stale_nested_cache_first query
    order_qt_stale_nested_cache_repeat query
    sql "set enable_sql_cache = false"

    sql "refresh materialized view struct_info_nested_mv auto"
    waitingMTMVTaskFinished(getJobName(db, "struct_info_nested_mv"))
    sql "sync"
    mv_rewrite_success_without_check_chosen(query, "struct_info_nested_mv")
    order_qt_refreshed_nested query

    // A materialized CTE consumer has no table-ID candidate. Keep it on the leading
    // side of a join during CBO MV exploration to exercise the missing-child path.
    sql "set inline_cte_referenced_threshold = 0"
    sql "set pre_materialized_view_rewrite_strategy = 'NOT_IN_RBO'"
    order_qt_cte_child """
        with c as (select * from struct_info_left_mv)
        select c.id, t.value from c left join struct_info_right_mv t on c.id = t.id
        union all
        select c.id, t.value from c left join struct_info_right_mv t on c.id = t.id
    """
}
