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

suite("test_point_query_read_time_hidden_columns") {
    sql "set show_hidden_columns = false"

    sql "drop table if exists point_query_hidden_full_row_store"
    sql """
        create table point_query_hidden_full_row_store (
            k int,
            v int
        ) unique key(k)
        distributed by hash(k) buckets 1
        properties (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "store_row_column" = "true"
        )
    """

    sql "drop table if exists point_query_hidden_partial_row_store"
    sql """
        create table point_query_hidden_partial_row_store (
            k int,
            v int,
            v2 int
        ) unique key(k)
        distributed by hash(k) buckets 1
        properties (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "row_store_columns" = "k,v"
        )
    """

    sql "insert into point_query_hidden_full_row_store values (1, 10)"
    sql "insert into point_query_hidden_partial_row_store values (1, 10, 20)"
    sql "set show_hidden_columns = true"

    qt_full_row_store_short_circuit """
        select __DORIS_VERSION_COL__ > 0
        from point_query_hidden_full_row_store
        where k = 1
    """
    qt_full_row_store_normal """
        select /*+ SET_VAR(enable_short_circuit_query=false) */ __DORIS_VERSION_COL__ > 0
        from point_query_hidden_full_row_store
        where k = 1
    """
    qt_partial_row_store_short_circuit """
        select __DORIS_VERSION_COL__ > 0
        from point_query_hidden_partial_row_store
        where k = 1
    """
    qt_partial_row_store_normal """
        select /*+ SET_VAR(enable_short_circuit_query=false) */ __DORIS_VERSION_COL__ > 0
        from point_query_hidden_partial_row_store
        where k = 1
    """

    qt_version_minmax_pushdown """
        select min(__DORIS_VERSION_COL__) > 0, max(__DORIS_VERSION_COL__) > 0
        from point_query_hidden_full_row_store
    """
    qt_version_minmax_normal """
        select /*+ SET_VAR(enable_pushdown_minmax_on_unique=false) */
               min(__DORIS_VERSION_COL__) > 0, max(__DORIS_VERSION_COL__) > 0
        from point_query_hidden_full_row_store
    """

    sql "set topn_lazy_materialization_threshold = 1024"

    sql "drop table if exists hidden_version_topn_column_store"
    sql """
        create table hidden_version_topn_column_store (
            k int,
            sort_key int,
            payload varchar(32)
        ) unique key(k)
        distributed by hash(k) buckets 1
        properties (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "true"
        )
    """
    // Each insert creates a singleton rowset at versions 2 and 3. The sort keys interleave
    // the rowsets in the lazy-fetch request, exercising per-batch replacement and scattering.
    sql "insert into hidden_version_topn_column_store values (1, 1, 'first-1'), (3, 3, 'first-3')"
    sql "insert into hidden_version_topn_column_store values (2, 2, 'second-2'), (4, 4, 'second-4')"

    explain {
        sql """
            shape plan
            select k, payload, __DORIS_VERSION_COL__
            from hidden_version_topn_column_store
            order by sort_key
            limit 4
        """
        contains("PhysicalLazyMaterialize")
    }
    qt_topn_column_store_versions """
        select k, payload, __DORIS_VERSION_COL__
        from hidden_version_topn_column_store
        order by sort_key
        limit 4
    """

    sql "drop table if exists hidden_version_topn_row_store"
    sql """
        create table hidden_version_topn_row_store (
            k int,
            sort_key int,
            payload varchar(32)
        ) unique key(k)
        distributed by hash(k) buckets 1
        properties (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "store_row_column" = "true",
            "disable_auto_compaction" = "true"
        )
    """
    // Repeat the interleaved rowset fetch through the full row-store materialization path.
    sql "insert into hidden_version_topn_row_store values (1, 1, 'first-1'), (3, 3, 'first-3')"
    sql "insert into hidden_version_topn_row_store values (2, 2, 'second-2'), (4, 4, 'second-4')"

    explain {
        sql """
            shape plan
            select k, payload, __DORIS_VERSION_COL__
            from hidden_version_topn_row_store
            order by sort_key
            limit 4
        """
        contains("PhysicalLazyMaterialize")
    }
    qt_topn_row_store_versions """
        select k, payload, __DORIS_VERSION_COL__
        from hidden_version_topn_row_store
        order by sort_key
        limit 4
    """

    sql "set show_hidden_columns = false"
}
