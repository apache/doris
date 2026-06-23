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

// Regression tests for the IS NULL / IS NOT NULL column pruning optimization.
//
// When IS NULL (or IS NOT NULL) is the *only* use of a nullable column, the FE
// should emit a DATA access path with a "NULL" component so that the BE can
// satisfy the query by reading only the null flag instead of the full column data.
// The EXPLAIN plan should show:
//   nested columns:  <col>: all access paths: [<col>.NULL]
//
// When the same column is also accessed for data (e.g., projected or used in
// element_at), the NULL-only path must be stripped from allAccessPaths and
// predicateAccessPaths unless the same path is still present in allAccessPaths.

suite("null_column_pruning") {
    sql """ DROP TABLE IF EXISTS ncp_tbl """
    sql """
        CREATE TABLE ncp_tbl (
            id          INT,
            str_col     STRING NULL,
            struct_col  STRUCT<city: STRING, zip: INT> NULL,
            arr_col     ARRAY<INT> NULL,
            map_col     MAP<STRING, INT> NULL,
            int_col     INT NULL
        ) ENGINE = OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """

    sql """
        INSERT INTO ncp_tbl VALUES
            (1, 'hello', named_struct('city', null, 'zip', 10001), [1, 2, 3], {'a': 1, 'b': 2 }, 1)
    """
    // ─── Struct IS NULL only ────────────────────────────────────────────────────
    // Only null check on struct_col → emit [struct_col, NULL] access path,
    // type stays full struct (no pruning needed).
    explain {
        sql "select 1 from ncp_tbl where struct_col is null"
        contains "nested columns"
        contains "struct_col.NULL"
    }

    order_qt_1 "select 1 from ncp_tbl where struct_col is null";

    // ─── String IS NULL only ────────────────────────────────────────────────────
    // Only null check on str_col → emit [str_col, NULL] access path,
    // type stays string (no pruning needed).
    explain {
        sql "select 1 from ncp_tbl where str_col is null"
        contains "nested columns"
        contains "str_col.NULL"
    }

    order_qt_2 "select 1 from ncp_tbl where str_col is null";

    // Direct full access to the same field covers its null flag for any data type.
    // The exact [str_col.NULL] metadata path must be removed.
    explain {
        sql "select id, str_col from ncp_tbl where str_col is null"
        notContains "str_col.NULL"
        notContains "predicate access paths:"
    }

    order_qt_string_full_access_strips_null """
        select id, str_col from ncp_tbl where str_col is null
        order by id
    """

    // ─── String IS NOT NULL only ────────────────────────────────────────────────
    explain {
        sql "select 1 from ncp_tbl where str_col is not null"
        contains "nested columns"
        contains "str_col.NULL"
    }

    order_qt_3 "select 1 from ncp_tbl where str_col is not null";

    // ─── Struct IS NOT NULL only ────────────────────────────────────────────────
    // IS NOT NULL is the same optimization (only null flag needed).
    explain {
        sql "select 1 from ncp_tbl where struct_col is not null"
        contains "nested columns"
        contains "struct_col.NULL"
    }

    order_qt_4 "select 1 from ncp_tbl where struct_col is not null";

    // ─── Struct IS NULL in aggregate ────────────────────────────────────────────
    explain {
        sql "select count(*) from ncp_tbl where struct_col is null"
        contains "nested columns"
        contains "struct_col.NULL"
    }

    order_qt_5 "select count(*) from ncp_tbl where struct_col is null";

    // ─── Array IS NULL only ─────────────────────────────────────────────────────
    explain {
        sql "select 1 from ncp_tbl where arr_col is null"
        contains "nested columns"
        contains "arr_col.NULL"
    }

    order_qt_6 "select 1 from ncp_tbl where arr_col is null";

    explain {
        sql "select id, arr_col from ncp_tbl where arr_col is null"
        contains "nested columns"
        contains "all access paths: [arr_col]"
        notContains "arr_col.NULL"
        notContains "predicate access paths:"
    }

    order_qt_array_full_access_strips_null """
        select id, arr_col from ncp_tbl where arr_col is null
        order by id
    """

    // ─── Map IS NULL only ───────────────────────────────────────────────────────
    explain {
        sql "select 1 from ncp_tbl where map_col is null"
        contains "nested columns"
        contains "map_col.NULL"
    }

    order_qt_7 "select 1 from ncp_tbl where map_col is null";

    explain {
        sql "select id, map_col from ncp_tbl where map_col is null"
        contains "nested columns"
        contains "all access paths: [map_col]"
        notContains "map_col.NULL"
        notContains "predicate access paths:"
    }

    order_qt_map_full_access_strips_null """
        select id, map_col from ncp_tbl where map_col is null
        order by id
    """

    // ─── Int IS NULL only ───────────────────────────────────────────────────────
    // Nullable primitive type (INT) accessed only via IS NULL → emit [int_col, NULL]
    // access path so BE only reads the null flag.
    explain {
        sql "select 1 from ncp_tbl where int_col is null"
        contains "nested columns"
        contains "int_col.NULL"
    }

    order_qt_8 "select 1 from ncp_tbl where int_col is null";

    // ─── Int IS NOT NULL only ───────────────────────────────────────────────────
    explain {
        sql "select 1 from ncp_tbl where int_col is not null"
        contains "nested columns"
        contains "int_col.NULL"
    }

    order_qt_9 "select 1 from ncp_tbl where int_col is not null";

    // ─── Mixed: int IS NULL + projected ────────────────────────────────────────
    // int_col IS NULL in WHERE + int_col in SELECT → data is also needed.
    // [int_col, NULL] stripped from allAccessPaths because [int_col] (full data)
    // covers the same prefix and inherently includes the null flag.
    explain {
        sql "select int_col from ncp_tbl where int_col is null"
        notContains "nested columns"
    }

    order_qt_10 "select int_col from ncp_tbl where int_col is null";

    // ─── Mixed: struct IS NULL + partial field access ───────────────────────────
    // struct_col IS NULL in WHERE + element_at in SELECT → child data is also needed.
    // The parent struct_col.NULL path must NOT stay in allAccessPaths with child paths.
    // BE StructFileColumnIterator treats a leading NULL sub-path as NULL_MAP_ONLY; if
    // allAccessPaths were [struct_col.NULL, struct_col.city], BE would skip the city
    // child iterator and default-fill the projected value. The normal nullable struct
    // read materializes the parent null map together with child data, and
    // predicateAccessPaths is filtered so it remains a subset of allAccessPaths.
    explain {
        sql "select element_at(struct_col, 'city') from ncp_tbl where struct_col is null"
        contains "nested columns"
        contains "all access paths: [struct_col.city]"
        notContains "predicate access paths:"
    }

    order_qt_11 "select element_at(struct_col, 'city') from ncp_tbl where struct_col is null";

    // This query verifies the real correctness risk: one branch needs the parent null
    // map, another branch needs a child null map, and the projection needs another
    // child data path. Keeping struct_col.NULL in allAccessPaths would put BE in
    // NULL_MAP_ONLY mode for the whole struct and return the default zip value instead
    // of reading the zip child column.
    explain {
        sql "select element_at(struct_col, 'zip') from ncp_tbl where struct_col is null or element_at(struct_col, 'city') is null"
        contains "nested columns"
        contains "all access paths: [struct_col.city.NULL, struct_col.zip]"
        contains "predicate access paths: [struct_col.city.NULL]"
    }

    order_qt_parent_null_with_child_data "select element_at(struct_col, 'zip') from ncp_tbl where struct_col is null or element_at(struct_col, 'city') is null";

    // ─── Non-optimizable: struct IS NULL + full struct projected ────────────────
    // Full struct access covers its own null flag, so [struct_col.NULL] is stripped
    // from allAccessPaths but kept in predicateAccessPaths.
    explain {
        sql "select struct_col from ncp_tbl where struct_col is null"
        contains "nested columns"
        contains "all access paths: [struct_col]"
        notContains "predicate access paths:"
    }

    order_qt_12 "select struct_col from ncp_tbl where struct_col is null";

    // ─── Nested struct field IS NULL ────────────────────────────────────────────
    // element_at(struct_col, 'city') IS NULL should produce a null-flag-only
    // predicate path [struct_col.city.NULL] while the projection reads city data.
    // [struct_col.city.NULL] is stripped from allAccessPaths because [struct_col.city]
    // covers the same prefix (full city data includes its null flag).
    explain {
        sql "select element_at(struct_col, 'city') from ncp_tbl where element_at(struct_col, 'city') is null"
        contains "nested columns"
        contains "all access paths: [struct_col.city]"
        notContains "predicate access paths:"
    }

    order_qt_13 "select element_at(struct_col, 'city') from ncp_tbl where element_at(struct_col, 'city') is null";

    // =========================================================================
    // IS NULL on nested-type extraction functions (map_keys, map_values,
    // element_at, and nested combinations)
    // =========================================================================

    // ─── map_keys(map_col) IS NULL ─────────────────────────────────────────────
    // map_keys(nullable_map) returns a NULL array only when the parent map itself
    // is NULL, so the null-only path must be the parent map null map. Emitting
    // map_col.KEYS.NULL would ask BE to inspect the key child null map instead.
    explain {
        sql "select count(1) from ncp_tbl where map_keys(map_col) is null"
        contains "nested columns"
        contains "map_col.NULL"
        notContains "map_col.KEYS.NULL"
    }

    order_qt_14 "select count(1) from ncp_tbl where map_keys(map_col) is null";

    // ─── map_keys(map_col) IS NOT NULL ──────────────────────────────────────────
    explain {
        sql "select count(1) from ncp_tbl where map_keys(map_col) is not null"
        contains "nested columns"
        contains "map_col.NULL"
        notContains "map_col.KEYS.NULL"
    }

    order_qt_15 "select count(1) from ncp_tbl where map_keys(map_col) is not null";

    // ─── map_values(map_col) IS NULL ────────────────────────────────────────────
    // A non-NULL map containing a NULL value, e.g. {'b': NULL}, still produces a
    // non-NULL values array [NULL]. Therefore map_values(map_col) IS NULL is a
    // parent-map null check, not a VALUES-child null check.
    explain {
        sql "select count(1) from ncp_tbl where map_values(map_col) is null"
        contains "nested columns"
        contains "map_col.NULL"
        notContains "map_col.VALUES.NULL"
    }

    order_qt_16 "select count(1) from ncp_tbl where map_values(map_col) is null";

    // ─── map_values(map_col) IS NOT NULL ────────────────────────────────────────
    explain {
        sql "select count(1) from ncp_tbl where map_values(map_col) is not null"
        contains "nested columns"
        contains "map_col.NULL"
        notContains "map_col.VALUES.NULL"
    }

    order_qt_17 "select count(1) from ncp_tbl where map_values(map_col) is not null";

    sql """ DROP TABLE IF EXISTS ncp_map_null_semantics_tbl """
    sql """
        CREATE TABLE ncp_map_null_semantics_tbl (
            id      INT,
            map_col MAP<STRING, INT> NULL
        ) ENGINE = OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    sql """
        INSERT INTO ncp_map_null_semantics_tbl VALUES
            (1, NULL),
            (2, {'a': 1}),
            (3, {'b': NULL})
    """

    explain {
        sql "select id from ncp_map_null_semantics_tbl where map_values(map_col) is null order by id"
        contains "nested columns"
        contains "map_col.NULL"
        notContains "map_col.VALUES.NULL"
    }

    // Only the NULL map row should match. Row 3 has a NULL value element, but
    // map_values({'b': NULL}) is the non-NULL array [NULL].
    order_qt_map_values_parent_null_semantics """
        select id from ncp_map_null_semantics_tbl where map_values(map_col) is null order by id
    """

    order_qt_map_values_parent_not_null_semantics """
        select id from ncp_map_null_semantics_tbl where map_values(map_col) is not null order by id
    """

    // ─── element_at(arr_col, 1) IS NULL ─────────────────────────────────────────
    explain {
        sql "select count(1) from ncp_tbl where arr_col[1] is null"
        contains "nested columns"
        contains "arr_col.*.NULL"
    }

    order_qt_18 "select count(1) from ncp_tbl where arr_col[1] is null";

    // ─── element_at(arr_col, 1) IS NOT NULL ─────────────────────────────────────
    explain {
        sql "select count(1) from ncp_tbl where arr_col[1] is not null"
        contains "nested columns"
        contains "arr_col.*.NULL"
    }

    order_qt_19 "select count(1) from ncp_tbl where arr_col[1] is not null";

    // ─── element_at(map_col, 'a') IS NULL ───────────────────────────────────────
    explain {
        sql "select count(1) from ncp_tbl where map_col['a'] is null"
        contains "nested columns"
        contains "map_col.KEYS"
        contains "map_col.VALUES.NULL"
    // expectedPlan
    //    nested columns:
    //    map_col:
    //      origin type: map<text,int>
    //      all access paths: [map_col.KEYS, map_col.VALUES.NULL]
    //      predicate access paths: [map_col.KEYS, map_col.VALUES.NULL]

    }

    order_qt_20 "select count(1) from ncp_tbl where map_col['a'] is null";

    // ─── element_at(map_col, 'a') IS NOT NULL ───────────────────────────────────
    explain {
        sql "select count(1) from ncp_tbl where map_col['a'] is not null"
        contains "nested columns"
        contains "map_col.KEYS"
        contains "map_col.VALUES.NULL"
    }

    order_qt_21 "select count(1) from ncp_tbl where map_col['a'] is not null";

    // ─── element_at(struct_col, 'city') IS NULL only (no projection) ────────
    explain {
        sql "select count(1) from ncp_tbl where element_at(struct_col, 'city') is null"
        contains "nested columns"
        contains "struct_col.city.NULL"
    }

    order_qt_22 "select count(1) from ncp_tbl where element_at(struct_col, 'city') is null";

    // ─── element_at(struct_col, 'zip') IS NULL ──────────────────────────────
    explain {
        sql "select count(1) from ncp_tbl where element_at(struct_col, 'zip') is null"
        contains "nested columns"
        contains "struct_col.zip.NULL"
    }

    order_qt_23 "select count(1) from ncp_tbl where element_at(struct_col, 'zip') is null";

    // ─── element_at IS NOT NULL ─────────────────────────────────────────────
    explain {
        sql "select count(1) from ncp_tbl where element_at(struct_col, 'city') is not null"
        contains "nested columns"
        contains "struct_col.city.NULL"
    }

    order_qt_24 "select count(1) from ncp_tbl where element_at(struct_col, 'city') is not null";

    // ─── Mixed: map_keys IS NULL + map_keys projected ──────────────────────────
    // Projection needs key data, while the predicate checks whether the parent map
    // is NULL. The parent NULL path must not stay in either access path list, so BE
    // does not switch the whole map iterator to NULL_MAP_ONLY and skip the keys child.
    explain {
        sql "select map_keys(map_col) from ncp_tbl where map_keys(map_col) is null"
        contains "nested columns"
        contains "all access paths: [map_col.KEYS]"
        notContains "predicate access paths:"
    }

    order_qt_25 "select map_keys(map_col) from ncp_tbl where map_keys(map_col) is null";

    // ─── Mixed: map_values IS NULL + map_values projected ──────────────────────
    // Projection needs value data, while the predicate checks whether the parent
    // map is NULL. A NULL value element does not make map_values(map_col) NULL.
    explain {
        sql "select map_values(map_col) from ncp_tbl where map_values(map_col) is null"
        contains "nested columns"
        contains "all access paths: [map_col.VALUES]"
        notContains "predicate access paths:"
    }

    order_qt_26 "select map_values(map_col) from ncp_tbl where map_values(map_col) is null";

    // ─── Nested types: struct containing map and array ─────────────────────────
    sql """ DROP TABLE IF EXISTS ncp_nested_tbl """
    sql """
        CREATE TABLE ncp_nested_tbl (
            id              INT,
            nested_struct   STRUCT<inner_map: MAP<STRING, INT>, inner_arr: ARRAY<STRING>> NULL,
            arr_of_structs  ARRAY<STRUCT<name: STRING, age: INT>> NULL,
            map_of_arrs     MAP<STRING, ARRAY<INT>> NULL
        ) ENGINE = OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    sql """
        INSERT INTO ncp_nested_tbl SELECT
            1,
            named_struct('inner_map', map('x', 10), 'inner_arr', array('a', 'b')),
            array(named_struct('name', 'Alice', 'age', 30)),
            map('k', array(1, 2))
    """

    // ─── element_at → map field IS NULL ─────────────────────────────────────
    explain {
        sql "select count(1) from ncp_nested_tbl where element_at(nested_struct, 'inner_map') is null"
        contains "nested columns"
        contains "nested_struct.inner_map.NULL"
    }

    order_qt_27 "select count(1) from ncp_nested_tbl where element_at(nested_struct, 'inner_map') is null";

    // ─── element_at → array field IS NULL ───────────────────────────────────
    explain {
        sql "select count(1) from ncp_nested_tbl where element_at(nested_struct, 'inner_arr') is null"
        contains "nested columns"
        contains "nested_struct.inner_arr.NULL"
    }

    order_qt_28 "select count(1) from ncp_nested_tbl where element_at(nested_struct, 'inner_arr') is null";

    // ─── map_keys through element_at IS NULL ────────────────────────────────
    explain {
        sql "select count(1) from ncp_nested_tbl where map_keys(element_at(nested_struct, 'inner_map')) is null"
        contains "nested columns"
        contains "nested_struct.inner_map.NULL"
        notContains "nested_struct.inner_map.KEYS.NULL"
    }

    order_qt_29 "select count(1) from ncp_nested_tbl where map_keys(element_at(nested_struct, 'inner_map')) is null";

    // ─── map_values through element_at IS NULL ──────────────────────────────
    explain {
        sql "select count(1) from ncp_nested_tbl where map_values(element_at(nested_struct, 'inner_map')) is null"
        contains "nested columns"
        contains "nested_struct.inner_map.NULL"
        notContains "nested_struct.inner_map.VALUES.NULL"
    }

    order_qt_30 "select count(1) from ncp_nested_tbl where map_values(element_at(nested_struct, 'inner_map')) is null";

    // ─── map_values(map_of_arrs) IS NULL ────────────────────────────────────────
    explain {
        sql "select count(1) from ncp_nested_tbl where map_values(map_of_arrs) is null"
        contains "nested columns"
        contains "map_of_arrs.NULL"
        notContains "map_of_arrs.VALUES.NULL"
    }

    order_qt_31 "select count(1) from ncp_nested_tbl where map_values(map_of_arrs) is null";

    // ─── map_keys(map_of_arrs) IS NULL ──────────────────────────────────────────
    explain {
        sql "select count(1) from ncp_nested_tbl where map_keys(map_of_arrs) is null"
        contains "nested columns"
        contains "map_of_arrs.NULL"
        notContains "map_of_arrs.KEYS.NULL"
    }

    order_qt_32 "select count(1) from ncp_nested_tbl where map_keys(map_of_arrs) is null";

    // ─── Non-nullable column IS NULL → no nested column pruning ─────────────────
    // A NOT NULL column has no null flags; IS NULL is always false and the optimizer
    // must NOT generate a .NULL access path for it.
    sql """ DROP TABLE IF EXISTS ncp_tbl_nn """
    sql """
        CREATE TABLE ncp_tbl_nn (
            id      INT NOT NULL,
            str_col STRING NULL
        ) ENGINE = OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    sql """ INSERT INTO ncp_tbl_nn VALUES (1, 'hello') """

    explain {
        sql "select 1 from ncp_tbl_nn where id is null"
        notContains "nested columns"
    }

    order_qt_33 "select 1 from ncp_tbl_nn where id is null";

    // ─── length(str_col) = 0 OR str_col IS NULL ────────────────────────────────
    // length(str_col) already uses the OFFSET path, and BE can derive null-ness
    // from that layout, so the extra NULL-only path is redundant.
    explain {
        sql "select 1 from ncp_tbl where length(str_col) = 0 or str_col is null"
        contains "nested columns"
        contains "str_col.OFFSET"
        notContains "str_col.NULL"
    }

    order_qt_34 "select 1 from ncp_tbl where length(str_col) = 0 or str_col is null";
}
