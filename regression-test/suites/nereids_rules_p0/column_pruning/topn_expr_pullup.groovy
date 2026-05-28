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

suite("topn_expr_pullup") {
    sql """ set topn_lazy_materialization_threshold=1024; """
    sql """ DROP TABLE IF EXISTS tep_tbl """
    sql """
        CREATE TABLE tep_tbl (
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
    sql """ INSERT INTO tep_tbl VALUES
        (1, 'hello', named_struct('city', 'NYC', 'zip', 10001), [1, 2, 3], {'a': 1, 'b': 2}, 10),
        (2, 'world', named_struct('city', 'LA', 'zip', 90001), [4, 5], {'x': 3}, 20),
        (3, 'doris', named_struct('city', 'SF', 'zip', 94101), [6], {'y': 5}, 30)
    """

    // =============================================
    // Test 1: STRUCT PPD — expression pulled above TopN + lazy mat
    // =============================================
    explain {
        sql """ select id, struct_element(struct_col, 'city') as city
            from tep_tbl order by id limit 3 """
        contains("VMaterializeNode")
        contains("row_ids: [__DORIS_GLOBAL_ROWID_COL__tep_tbl]")
    }
    qt_struct_ppd """ select id, struct_element(struct_col, 'city') as city
        from tep_tbl order by id limit 3 """

    // =============================================
    // Test 2: STRUCT PPD — column order preserved (expr before id)
    // =============================================
    qt_struct_col_order """ select struct_element(struct_col, 'city') as city, id
        from tep_tbl order by id limit 3 """

    // =============================================
    // Test 3: Non-PPD — upper(str_col) pulled up
    // =============================================
    explain {
        sql """ select id, upper(str_col) as name
            from tep_tbl order by id limit 3 """
        contains("VMaterializeNode")
    }
    qt_upper_str """ select id, upper(str_col) as name
        from tep_tbl order by id limit 3 """

    // =============================================
    // Test 4: Non-PPD — math expr pulled up
    // =============================================
    qt_math_expr """ select id, int_col + 1 as next_val
        from tep_tbl order by id limit 3 """

    // =============================================
    // Test 5: Non-PPD — concat pulled up
    // =============================================
    qt_concat_expr """ select id, concat(str_col, '-suffix') as label
        from tep_tbl order by id limit 3 """

    // =============================================
    // Test 6: MAP subscript pulled up
    // =============================================
    explain {
        sql """ select id, element_at(map_col, 'a') as val
            from tep_tbl order by id limit 3 """
        contains("VMaterializeNode")
    }
    qt_map_subscript """ select id, element_at(map_col, 'a') as val
        from tep_tbl order by id limit 3 """

    // =============================================
    // Test 7: ARRAY subscript pulled up
    // =============================================
    qt_array_subscript """ select id, element_at(arr_col, 1) as val
        from tep_tbl order by id limit 3 """

    // =============================================
    // Test 8: Simple alias NOT pulled up (child is Slot)
    // =============================================
    qt_simple_alias """ select id as a, struct_element(struct_col, 'city') as city
        from tep_tbl order by a limit 3 """

    // =============================================
    // Test 9: Negative — order by depends on expression, stays below TopN
    // =============================================
    qt_order_by_expr """ select id, struct_element(struct_col, 'city') as city
        from tep_tbl order by city limit 3 """

    // =============================================
    // Test 10: select * with expression pulled up
    // =============================================
    explain {
        sql """ select *, struct_element(struct_col, 'city') as city
            from tep_tbl order by id limit 3 """
        contains("VMaterializeNode")
    }
    qt_select_star """ select *, struct_element(struct_col, 'city') as city
        from tep_tbl order by id limit 3 """

    // =============================================
    // Test 11: Switch disabled — same results
    // =============================================
    sql """ set enable_topn_expr_pullup = false; """
    qt_switch_off """ select id, struct_element(struct_col, 'city') as city
        from tep_tbl order by id limit 3 """
    sql """ set enable_topn_expr_pullup = true; """

    // =============================================
    // Test 12: Join — PPD on left table
    // =============================================
    qt_join_ppd """ select t1.id, struct_element(t1.struct_col, 'city') as city, t2.int_col
        from tep_tbl t1 join tep_tbl t2 on t1.id = t2.id
        order by t1.id limit 3 """

    // =============================================
    // Test 13: Join — non-PPD on right table
    // =============================================
    qt_join_nonppd """ select t1.id, upper(t2.str_col) as name
        from tep_tbl t1 join tep_tbl t2 on t1.id = t2.id
        order by t1.id limit 3 """

    // =============================================
    // Test 14: Join — both sides have pull-up expressions
    // =============================================
    qt_join_both """ select t1.id, struct_element(t1.struct_col, 'city') as c1, upper(t2.str_col) as c2
        from tep_tbl t1 join tep_tbl t2 on t1.id = t2.id
        order by t1.id limit 3 """

    // =============================================
    // Test 15: Join — condition references baseSlot (should still pull up)
    // =============================================
    qt_join_base_slot """ select t1.id, struct_element(t1.struct_col, 'city') as c1
        from tep_tbl t1 join tep_tbl t2 on t1.id = t2.id and t1.str_col = t2.str_col
        order by t1.id limit 3 """

    // =============================================
    // Cleanup
    // =============================================
    sql """ DROP TABLE IF EXISTS tep_tbl """
}
