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

suite("topn_lazy_order_by_alias") {
    // TopN lazy materialization only skips the rewrite when a plan validation fails, and that
    // validation only runs with fe_debug on. Pin it off so an invalid plan surfaces as an error.
    sql """ set fe_debug = false; """

    sql """
        drop table if exists topn_lazy_order_by_alias_tbl;
        create table topn_lazy_order_by_alias_tbl (
            sort_col int,
            lazy_col int,
            other_col int
        ) duplicate key(sort_col)
        distributed by hash(sort_col) buckets 1
        properties('replication_num' = '1');
    """
    sql """
        insert into topn_lazy_order_by_alias_tbl values (3,30,300),(1,10,100),(2,20,200);
    """

    // Sorting by an alias of lazy_col: lazy_col feeds the sort key, so it must stay materialized
    // instead of being pruned from the scan while `lazy_col AS x` below the TopN still reads it.
    order_qt_repeated_alias_of_sort_key """
        select lazy_col as x, lazy_col as y
        from topn_lazy_order_by_alias_tbl order by x limit 1;
    """

    // Same shape ordered by the other alias.
    order_qt_repeated_alias_reversed """
        select lazy_col as x, lazy_col as y
        from topn_lazy_order_by_alias_tbl order by y limit 1;
    """

    // A bare column plus an alias of it, sorted by the bare column.
    order_qt_bare_column_and_alias """
        select lazy_col, lazy_col as y
        from topn_lazy_order_by_alias_tbl order by lazy_col limit 1;
    """

    // Only the column the order key reads is forced materialized; other_col is still lazily fetched.
    order_qt_other_column_still_lazy """
        select lazy_col as x, other_col as y
        from topn_lazy_order_by_alias_tbl order by x limit 1;
    """

    // Control: sorting by a column that is not aliased keeps both columns lazily fetched.
    order_qt_order_by_plain_column """
        select lazy_col as x, other_col as y
        from topn_lazy_order_by_alias_tbl order by sort_col limit 1;
    """

    // The same shapes through the inverted-index filter path.
    sql """ set topn_lazy_materialization_using_index = true; """

    order_qt_using_index_repeated_alias """
        select lazy_col as x, lazy_col as y
        from topn_lazy_order_by_alias_tbl where sort_col > 0 order by x limit 1;
    """

    order_qt_using_index_bare_column_and_alias """
        select lazy_col, lazy_col as y
        from topn_lazy_order_by_alias_tbl where sort_col > 0 order by lazy_col limit 1;
    """

    sql """ set topn_lazy_materialization_using_index = false; """

    // Only the outer TopN is rewritten. Probing its output `y` resolves to lazy_col, which the inner TopN
    // still reads through `lazy_col AS x`, so lazy_col has to stay materialized. Plain execution hides the
    // invalid plan behind StmtExecutor.queryRetry, so the plan shape is asserted as well.
    sql """ set detail_shape_nodes = 'PhysicalProject'; """

    qt_nested_topn_plan """
        explain shape plan
        select y from (
            select lazy_col as x, lazy_col as y, other_col as z
            from topn_lazy_order_by_alias_tbl
            order by x limit 2) s
        order by z limit 1;
    """

    order_qt_nested_topn_result """
        select y from (
            select lazy_col as x, lazy_col as y, other_col as z
            from topn_lazy_order_by_alias_tbl
            order by x limit 2) s
        order by z limit 1;
    """

    // Column `w` is read by nothing below the TopN, so it can still be fetched lazily through both TopNs.
    qt_nested_topn_still_lazy_plan """
        explain shape plan
        select w from (
            select lazy_col as x, other_col as w, lazy_col as y, sort_col
            from topn_lazy_order_by_alias_tbl
            order by x limit 2) s
        order by sort_col limit 1;
    """
}
