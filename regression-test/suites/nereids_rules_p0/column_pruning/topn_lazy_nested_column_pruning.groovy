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

suite("topn_lazy_nested_column_pruning") {
    sql """ set topn_lazy_materialization_threshold=1024; """
    sql """ DROP TABLE IF EXISTS tlncp_tbl """
    sql """
        CREATE TABLE tlncp_tbl (
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
        INSERT INTO tlncp_tbl VALUES
            (1, 'hello', named_struct('city', null, 'zip', 10001), [1, 2, 3], {'a': 1, 'b': 2 }, 1)
    """

        sql """
    drop table if exists vt;
    CREATE TABLE IF NOT EXISTS vt (
        id BIGINT NOT NULL,
        s varchar(100) null,
        payload VARIANT<
            'name' : STRING,
            'age' : INT
        > NULL
    )
    ENGINE = OLAP
    DUPLICATE KEY(`id`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES (
        "replication_num" = "1",
        "storage_format" = "V3"
    );


    INSERT INTO vt VALUES
        (1, 'aaa', '{"name": "张三", "age": 25}'),
        (2, 'bbb', '{"name": "李四", "age": 30}'),
        (3, 'ccc', '{"name": "王五", "age": 28, "city": "北京"}');
    """

    // =============================================
    // Test 1: STRUCT type - lazy mat + nested column pruning
    // =============================================
    explain {
        sql """
            select id, substring(element_at(struct_col, 'city'), 1) as city
            from tlncp_tbl
            order by id
            limit 3
        """
        contains("VMaterializeNode")
        // struct_col lazy, scan only outputs id + rowId
        contains("final projections: id[#0], __DORIS_GLOBAL_ROWID_COL__tlncp_tbl[#6]")
        // nested column pruning: struct_col pruned to city only
        contains("nested columns:")
        contains("pruned type: struct<city:text>")
        contains("row_ids: [__DORIS_GLOBAL_ROWID_COL__tlncp_tbl]")
    }

    // =============================================
    // Test 2: STRUCT with select * - struct_col explicit in output
    // =============================================
    explain {
        sql """
            select *, substring(element_at(struct_col, 'city'), 1) as city
            from tlncp_tbl
            order by id
            limit 3
        """
        contains("VMaterializeNode")
        contains("row_ids: [__DORIS_GLOBAL_ROWID_COL__tlncp_tbl]")
    }

    // =============================================
    // Test 3: VARIANT type - lazy mat + sub path pruning
    // =============================================
    explain {
        sql """
            select id, s, substring(element_at(payload, 'name'), 1) as name
            from vt
            order by id
            limit 3
        """
        contains("VMaterializeNode")
        // payload lazy, scan only outputs id + rowId
        contains("final projections: id[#0], __DORIS_GLOBAL_ROWID_COL__vt[#4]")
        // sub path pruning for variant
        contains("nested columns:")
        contains("sub path: [name]")
        contains("row_ids: [__DORIS_GLOBAL_ROWID_COL__vt]")
    }

    // =============================================
    // Test 4: STRUCT - verify actual query results
    // =============================================
    qt_struct_result """
        select id, substring(element_at(struct_col, 'city'), 1) as city
        from tlncp_tbl
        order by id
        limit 3
    """

    // =============================================
    // Test 5: VARIANT - verify actual query results
    // =============================================
    qt_variant_result """
        select id, s, substring(element_at(payload, 'name'), 1) as name
        from vt
        order by id
        limit 3
    """

    // =============================================
    // Test 6: MAP subscript - lazy mat + nested column pruning
    // =============================================
    explain {
        sql """
            select id, element_at(map_col, 'a') as val
            from tlncp_tbl
            order by id
            limit 3
        """
        contains("VMaterializeNode")
        // map_col lazy, scan only outputs id + rowId
        contains("final projections: id[#0], __DORIS_GLOBAL_ROWID_COL__tlncp_tbl[#6]")
        contains("row_ids: [__DORIS_GLOBAL_ROWID_COL__tlncp_tbl]")
    }

    // =============================================
    // Test 7: MAP subscript - verify actual query results
    // =============================================
    qt_map_result """
        select id, element_at(map_col, 'a') as val
        from tlncp_tbl
        order by id
        limit 3
    """

    // =============================================
    // Test 8: ARRAY subscript - lazy mat + nested column pruning
    // =============================================
    explain {
        sql """
            select id, element_at(arr_col, 1) as val
            from tlncp_tbl
            order by id
            limit 3
        """
        contains("VMaterializeNode")
        // arr_col lazy, scan only outputs id + rowId
        contains("final projections: id[#0], __DORIS_GLOBAL_ROWID_COL__tlncp_tbl[#6]")
        contains("row_ids: [__DORIS_GLOBAL_ROWID_COL__tlncp_tbl]")
    }

    // =============================================
    // Test 9: ARRAY subscript - verify actual query results
    // =============================================
    qt_array_result """
        select id, element_at(arr_col, 1) as val
        from tlncp_tbl
        order by id
        limit 3
    """

    // =============================================
    // Test 10: Multi-level VARIANT nested - insert nested data
    // =============================================
    sql """
        INSERT INTO vt VALUES
            (4, 'ddd', '{"address": {"city": "上海", "zip": "200000"}}'),
            (5, 'eee', '{"address": {"city": "北京", "zip": "100000"}}')
    """

    // =============================================
    // Test 11: Multi-level VARIANT nested - explain
    //   Access payload['address']['city'] via two levels:
    //   inner payload['address'] → variant with subColPath [address]
    //   outer element_at(..., 'city') → final value
    // =============================================
    explain {
        sql """
            select id, element_at(payload['address'], 'city') as city
            from vt
            where id >= 4
            order by id
            limit 3
        """
        contains("VMaterializeNode")
        // payload lazy, scan only outputs id + rowId
        contains("final projections: id[#0], __DORIS_GLOBAL_ROWID_COL__vt[#4]")
        // sub path pruning for variant: only read address sub-path during materialization
        contains("nested columns:")
        contains("sub path: [address.city]")
        contains("row_ids: [__DORIS_GLOBAL_ROWID_COL__vt]")
    }

    // =============================================
    // Test 12: Multi-level VARIANT nested - verify query results
    // =============================================
    qt_variant_nested_result """
        select id, element_at(payload['address'], 'city') as city
        from vt
        where id >= 4
        order by id
        limit 3
    """

    // =============================================
    // Test 13: using_index=true with MAP subscript
    //   Verify that map/array lazy mat still works when
    //   topn_lazy_materialization_using_index is enabled.
    //   Regression test for the risk that MaterializeProbeVisitor
    //   .visitPhysicalProject skips alias→child slot tracing
    //   when using_index=true, which could prevent base columns
    //   from being probed as lazy candidates.
    // =============================================
    sql """ set topn_lazy_materialization_using_index = true; """
    explain {
        sql """
            select id, element_at(map_col, 'a') as val
            from tlncp_tbl
            order by id
            limit 3
        """
        contains("VMaterializeNode")
        contains("row_ids: [__DORIS_GLOBAL_ROWID_COL__tlncp_tbl]")
    }
    qt_map_using_index_result """
        select id, element_at(map_col, 'a') as val
        from tlncp_tbl
        order by id
        limit 3
    """
    sql """ set topn_lazy_materialization_using_index = false; """

    // =============================================
    // Test 14: using_index=true with ARRAY subscript
    // =============================================
    sql """ set topn_lazy_materialization_using_index = true; """
    explain {
        sql """
            select id, element_at(arr_col, 1) as val
            from tlncp_tbl
            order by id
            limit 3
        """
        contains("VMaterializeNode")
        contains("row_ids: [__DORIS_GLOBAL_ROWID_COL__tlncp_tbl]")
    }
    qt_array_using_index_result """
        select id, element_at(arr_col, 1) as val
        from tlncp_tbl
        order by id
        limit 3
    """
    sql """ set topn_lazy_materialization_using_index = false; """

    // =============================================
    // Test 15: STRUCT nested expr BEFORE id — verify column order preserved
    //   SELECT city_expr, id should produce [city, id] not [id, city]
    // =============================================
    explain {
        sql """
            select substring(element_at(struct_col, 'city'), 1) as city, id
            from tlncp_tbl
            order by id
            limit 3
        """
        contains("VMaterializeNode")
        contains("row_ids: [__DORIS_GLOBAL_ROWID_COL__tlncp_tbl]")
    }
    qt_struct_col_order_result """
        select substring(element_at(struct_col, 'city'), 1) as city, id
        from tlncp_tbl
        order by id
        limit 3
    """

    // =============================================
    // Test 16: VARIANT nested expr BEFORE id — verify column order preserved
    // =============================================
    explain {
        sql """
            select substring(element_at(payload, 'name'), 1) as name, id, s
            from vt
            order by id
            limit 3
        """
        contains("VMaterializeNode")
        contains("row_ids: [__DORIS_GLOBAL_ROWID_COL__vt]")
    }
    qt_variant_col_order_result """
        select substring(element_at(payload, 'name'), 1) as name, id, s
        from vt
        order by id
        limit 3
    """

    // =============================================
    // Test 17: MAP nested expr BEFORE id — verify column order preserved
    // =============================================
    explain {
        sql """
            select element_at(map_col, 'a') as val, id
            from tlncp_tbl
            order by id
            limit 3
        """
        contains("VMaterializeNode")
        contains("row_ids: [__DORIS_GLOBAL_ROWID_COL__tlncp_tbl]")
    }
    qt_map_col_order_result """
        select element_at(map_col, 'a') as val, id
        from tlncp_tbl
        order by id
        limit 3
    """

    // =============================================
    // Test 18: Project expression kept below TopN still needs its input slot
    // =============================================
    sql """ set enable_topn_expr_pullup = false; """
    explain {
        sql """
            select *, substring(element_at(struct_col, 'city'), 1) as city
            from tlncp_tbl
            order by id
            limit 3
        """
        contains("VMaterializeNode")
        contains("final projections: id[#0], struct_col[#2], substring(element_at(struct_col[#2]")
        contains("row_ids: [__DORIS_GLOBAL_ROWID_COL__tlncp_tbl]")
    }
    qt_project_under_topn_consumed_slot """
        select *, substring(element_at(struct_col, 'city'), 1) as city
        from tlncp_tbl
        order by id
        limit 3
    """
    sql """ set enable_topn_expr_pullup = true; """

    // =============================================
    // Test 19: TopN lazy rowid fetch should honor nested access paths
    //   Sparse multi-path STRUCT/MAP/ARRAY pruning uses a pruned slot type.
    //   Rowid fetch must pass the slot access paths to storage iterators so
    //   the iterator child layout matches the pruned result column layout.
    // =============================================
    sql """ set enable_decimal256 = true; """
    sql """ set enable_prune_nested_column = true; """
    sql """ DROP TABLE IF EXISTS tlncp_sparse_nested_tbl """
    sql """
        CREATE TABLE tlncp_sparse_nested_tbl (
            pk INT,
            deep STRUCT<
                nested_str: VARCHAR(64),
                inner_s: STRUCT<deep_str: VARCHAR(64), flag: BOOLEAN, deep_char: CHAR(8)>,
                deep_map: MAP<VARCHAR(32), STRUCT<leaf: VARCHAR(64), n: INT, char_leaf: CHAR(8)>>
            > NULL,
            typed STRUCT<
                string_leaf: STRING,
                decimal_leaf: DECIMAL(76,56),
                typed_arr: ARRAY<STRUCT<string_leaf: STRING, decimal_leaf: DECIMAL(76,56)>>,
                typed_map: MAP<VARCHAR(32), STRUCT<string_leaf: STRING, decimal_leaf: DECIMAL(76,56)>>
            > NULL
        ) ENGINE = OLAP
        UNIQUE KEY(pk)
        DISTRIBUTED BY HASH(pk) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "enable_unique_key_merge_on_write" = "true",
            "store_row_column" = "true"
        )
    """

    sql """
        INSERT INTO tlncp_sparse_nested_tbl VALUES
            (1,
             named_struct(
                 'nested_str', 'unused-one',
                 'inner_s', named_struct('deep_str', 'DeepOne', 'flag', true, 'deep_char', 'dc1'),
                 'deep_map', map('b', named_struct('leaf', 'leaf-one', 'n', 11, 'char_leaf', 'cb1'))),
             named_struct(
                 'string_leaf', 'root-one',
                 'decimal_leaf', cast('10.00000000000000000000000000000000000000000000000000000000' as DECIMAL(76,56)),
                 'typed_arr', array(named_struct('string_leaf', 'arr-one', 'decimal_leaf',
                     cast('1.00000000000000000000000000000000000000000000000000000000' as DECIMAL(76,56)))),
                 'typed_map', map('b', named_struct('string_leaf', 'map-one', 'decimal_leaf',
                     cast('2.00000000000000000000000000000000000000000000000000000000' as DECIMAL(76,56)))))),
            (2,
             named_struct(
                 'nested_str', 'unused-two',
                 'inner_s', named_struct('deep_str', 'DeepTwo', 'flag', false, 'deep_char', 'dc2'),
                 'deep_map', map('b', named_struct('leaf', 'leaf-two', 'n', 22, 'char_leaf', 'cb2'))),
             named_struct(
                 'string_leaf', 'root-two',
                 'decimal_leaf', cast('20.00000000000000000000000000000000000000000000000000000000' as DECIMAL(76,56)),
                 'typed_arr', array(named_struct('string_leaf', 'arr-two', 'decimal_leaf',
                     cast('3.00000000000000000000000000000000000000000000000000000000' as DECIMAL(76,56)))),
                 'typed_map', map('b', named_struct('string_leaf', 'map-two', 'decimal_leaf',
                     cast('4.00000000000000000000000000000000000000000000000000000000' as DECIMAL(76,56)))))),
            (3,
             named_struct(
                 'nested_str', 'unused-three',
                 'inner_s', named_struct('deep_str', 'DeepThree', 'flag', true, 'deep_char', 'dc3'),
                 'deep_map', map('b', named_struct('leaf', 'leaf-three', 'n', 33, 'char_leaf', 'cb3'))),
             named_struct(
                 'string_leaf', 'root-three',
                 'decimal_leaf', cast('30.00000000000000000000000000000000000000000000000000000000' as DECIMAL(76,56)),
                 'typed_arr', array(named_struct('string_leaf', 'arr-three', 'decimal_leaf',
                     cast('5.00000000000000000000000000000000000000000000000000000000' as DECIMAL(76,56)))),
                 'typed_map', map('b', named_struct('string_leaf', 'map-three', 'decimal_leaf',
                     cast('6.00000000000000000000000000000000000000000000000000000000' as DECIMAL(76,56))))))
    """

    explain {
        sql """
            SELECT
                pk,
                CHAR_LENGTH(element_at(element_at(element_at(typed, 'typed_map'), 'b'), 'string_leaf')) AS char_len,
                LENGTH(LOWER(element_at(element_at(deep, 'inner_s'), 'deep_str'))) AS lower_len,
                LENGTH(element_at(element_at(element_at(deep, 'deep_map'), 'b'), 'char_leaf')) AS char_storage_len,
                ((element_at(element_at(element_at(typed, 'typed_arr'), 1), 'decimal_leaf') + 1) IS NULL) AS expr_is_null
            FROM tlncp_sparse_nested_tbl
            WHERE pk <= 3
            ORDER BY ABS(pk % 3), pk
            LIMIT 2
        """
        contains("VMaterializeNode")
        contains("row_ids: [__DORIS_GLOBAL_ROWID_COL__tlncp_sparse_nested_tbl]")
    }

    qt_sparse_struct_map_array_result """
        SELECT
            pk,
            CHAR_LENGTH(element_at(element_at(element_at(typed, 'typed_map'), 'b'), 'string_leaf')) AS char_len,
            LENGTH(LOWER(element_at(element_at(deep, 'inner_s'), 'deep_str'))) AS lower_len,
            LENGTH(element_at(element_at(element_at(deep, 'deep_map'), 'b'), 'char_leaf')) AS char_storage_len,
            ((element_at(element_at(element_at(typed, 'typed_arr'), 1), 'decimal_leaf') + 1) IS NULL) AS expr_is_null
        FROM tlncp_sparse_nested_tbl
        WHERE pk <= 3
        ORDER BY ABS(pk % 3), pk
        LIMIT 2
    """
}
