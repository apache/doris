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

suite("unnest_select_list_test", "unnest") {

    String prefix_str = "unnest_select_list_"
    def tb_name1 = prefix_str + "table1"

    sql """drop table if exists ${tb_name1}"""
    sql """
        CREATE TABLE IF NOT EXISTS ${tb_name1} (
            id INT,
            user_name VARCHAR(50),
            scores ARRAY<INT>,
            user_ids BITMAP,
            properties MAP<STRING, STRING>
        ) 
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1");"""

    sql """
        INSERT INTO ${tb_name1} VALUES
        (1, 'Alice', [90, 85, 95], to_bitmap(101), {'age': '25', 'city': 'Beijing'}),
        (2, 'Bob', [70, 80], bitmap_from_string('201,202'), {'level': 'VIP', 'rank': 'A'}),
        (3, 'EmptyCase', [], bitmap_empty(), {}),
        (4, 'NullCase', NULL, bitmap_empty(), NULL);"""

    // Test using UNNEST on an array in the SELECT list.
    qt_select_unnest_array """SELECT id, user_name, UNNEST(scores) as score FROM ${tb_name1} ORDER BY id, user_name, score;"""

    // array
    test {
        // Test that using WITH ORDINALITY for UNNEST in the SELECT list is not supported.
        sql """
            SELECT 
                id, 
                user_name, 
                UNNEST(scores) WITH ORDINALITY AS (score_pos, score_val)
            FROM ${tb_name1};"""
        exception "mismatched input"
    }

    // Test UNNEST on a constant array in the SELECT list.
    qt_select_unnest_constant_array """SELECT UNNEST(['a', 'b', 'c']) as col1 ORDER BY col1;"""

    test {
        // Test that WITH ORDINALITY is not supported for UNNEST on a constant array in the SELECT list.
        sql """SELECT UNNEST(['a', 'b', 'c']) WITH ORDINALITY AS (val, pos);"""
        exception "mismatched input"
    }

    // Test UNNEST on a modified array (result of a function) in the SELECT list.
    qt_select_unnest_on_modified_array """
        SELECT 
            id, 
            UNNEST(ARRAY_SORT(scores)) AS sorted_score
        FROM ${tb_name1} WHERE id = 1
        ORDER BY id, sorted_score;"""

    test {
        // Test that nested UNNEST calls in the SELECT list are not supported.
        sql """SELECT UNNEST(t.v) FROM (SELECT UNNEST(ARRAY(ARRAY(1,2))) AS v) t;"""
        exception "Could not find function unnest"
    }


    // bitmap
    // Test using UNNEST on a bitmap in the SELECT list.
    qt_select_unnest_bitmap """
        SELECT 
            id, 
            user_name, 
            UNNEST(user_ids) AS exploded_id
        FROM ${tb_name1}
        ORDER BY id, user_name, exploded_id;"""

    // Test UNNEST on a constant bitmap in the SELECT list.
    qt_select_unnest_constant_bitmap """SELECT UNNEST(bitmap_from_string('10,20,30')) AS uid ORDER BY uid;"""

    // Test aggregating the results of an UNNEST in a subquery's SELECT list.
    qt_aggregate_on_select_unnest_in_subquery """
        SELECT SUM(uid) FROM (
            SELECT UNNEST(user_ids) AS uid FROM ${tb_name1}
        ) t;"""

    // Test UNNEST on the result of a bitmap function in the SELECT list.
    qt_select_unnest_on_bitmap_function_result """SELECT UNNEST(BITMAP_AND(user_ids, to_bitmap(201))) AS intersection FROM ${tb_name1} ORDER BY intersection;"""
    // Test using UNNEST directly inside a COUNT aggregate function.
    qt_count_of_unnest """SELECT COUNT(UNNEST(scores)) FROM ${tb_name1};"""

    // map
    // Test using UNNEST on a map in the SELECT list.
    qt_select_unnest_map """
        SELECT 
            id, 
            UNNEST(properties) AS map_entry
        FROM ${tb_name1}
        ORDER BY id, map_entry;"""

    // Test UNNEST on a constant map in the SELECT list.
    qt_select_unnest_constant_map """SELECT UNNEST(MAP('k1', 'v1', 'k2', 'v2')) AS entry ORDER BY entry;"""

    // Test filtering the results of an UNNEST in a subquery's SELECT list.
    qt_filter_on_select_unnest_in_subquery """
        SELECT * FROM (
            SELECT id, UNNEST(properties) AS entry FROM ${tb_name1}
        ) t 
        WHERE entry LIKE '%VIP%'
        ORDER BY t.id, t.entry;"""

    // null
    // Test UNNEST on a NULL array column in the SELECT list.
    qt_select_unnest_null_array """SELECT UNNEST(scores) as score FROM ${tb_name1} WHERE id = 4 ORDER BY score NULLS FIRST;"""

    // cross join
    test {
        // Test that multiple UNNEST functions with different types in the SELECT list are invalid.
        sql """SELECT UNNEST(scores), UNNEST(user_ids) FROM ${tb_name1};"""
        exception "multiple UNNEST functions in same place must have ARRAY argument type"
    }

    // nested
    test {
        // Test that nested UNNEST on a constant array in the SELECT list is invalid.
        sql """SELECT UNNEST(UNNEST(ARRAY(ARRAY(1,2), ARRAY(3,4))));"""
        exception "Could not find function unnest"
    }

    // agg
    qt_sql_count_unnest """SELECT COUNT(UNNEST(scores)) FROM ${tb_name1};"""

    // cross
    test {
        // Test that multiple UNNEST calls on the same bitmap column in the SELECT list are invalid.
        sql """SELECT 
                UNNEST(user_ids) AS s_val,
                UNNEST(user_ids) AS u_id
            FROM ${tb_name1} WHERE id = 2;"""
        exception "multiple UNNEST functions in same place must have ARRAY argument type"
    }

    // alias name
    // fail
    test {
        // Test that an alias from an UNNEST in the SELECT list cannot be referenced in the same list.
        sql """SELECT 
                    UNNEST(scores) AS s_val,
                    s_val + 10 AS s_val_added
                FROM ${tb_name1};"""
        exception "Unknown column"
    }

    // expression compute
    test {
        // Test that an expression using an alias from an UNNEST in the same SELECT list is invalid.
        sql """SELECT UNNEST(scores) as s_val, (s_val * 2) as doubled_score FROM ${tb_name1};"""
        exception "Unknown column"
    }

    // DISTINCT
    // Test DISTINCT on an unnested constant array.
    qt_distinct_on_unnested_constant_array """SELECT DISTINCT UNNEST([1, 1, 2, 2]) AS v ORDER BY v;"""

    // Test DISTINCT on a combination of a regular column and an unnested column.
    qt_distinct_on_column_and_unnested_column """SELECT DISTINCT id, UNNEST(scores) as score FROM ${tb_name1} ORDER BY id, score;"""

    // Test DISTINCT on an unnested bitmap.
    qt_distinct_on_unnested_bitmap """SELECT DISTINCT UNNEST(bitmap_from_string('1,1,2')) AS v ORDER BY v;"""

    // subquery
    // Test filtering and grouping on a table unnested in the FROM clause.
    qt_from_unnest_with_filter_and_group_by """
        SELECT 
            view_table.score_val, 
            COUNT(*) 
        FROM ${tb_name1}
        CROSS JOIN LATERAL UNNEST(scores) WITH ORDINALITY AS view_table(pos, score_val)
        WHERE view_table.pos > 1 
        GROUP BY view_table.score_val
        ORDER BY view_table.score_val;"""

    // Test a scalar subquery in the SELECT list that contains an UNNEST operation.
    qt_scalar_subquery_with_unnest """
        SELECT 
            id, 
            (SELECT SUM(v) FROM (SELECT UNNEST(scores) AS v from ${tb_name1}) t) as total_score
        FROM ${tb_name1}
        ORDER BY id;"""


}
