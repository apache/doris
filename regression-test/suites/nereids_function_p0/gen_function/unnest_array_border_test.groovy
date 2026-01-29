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

suite("unnest_array_border_test", "unnest") {

    String prefix_str = "test_unnest_array_"
    def tb_name1 = prefix_str + "table1"
    def tb_name2 = prefix_str + "table2"

    // normal array
    sql """drop table if exists ${tb_name1}"""
    sql """CREATE TABLE ${tb_name1} (
            id INT,
            name VARCHAR(20),
            scores ARRAY<INT>
        ) 
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );"""

    sql """INSERT INTO ${tb_name1} VALUES 
            (1, 'Alice', [90, 85, 88]),
            (2, 'Bob', [70, 80]),
            (3, 'Charlie', []),
            (3, 'ddd', null),
            (3, 'ddd', [null]);
            -- (3, 'ddd', [[null], [1, 2, 3]]),
            -- (3, 'ddd', ["aa", 11]);"""

    // Test unnesting a normal array of integers.
    qt_unnest_normal_array """SELECT id, name, score, score_index FROM ${tb_name1}, UNNEST(scores) WITH ORDINALITY AS t(score, score_index) ORDER BY id, name, score, score_index;"""


    // array[json]
    sql """drop table if exists ${tb_name2}"""
    sql """
        CREATE TABLE ${tb_name2} (
            id INT,
            jsons_str ARRAY<STRING>
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");"""

    sql """
        INSERT INTO ${tb_name2} VALUES 
        (1, ['{"name": "A", "val": 1}', '{"name": "B", "val": 2}']);"""

    // Test unnesting an array of JSON objects.
    qt_unnest_json_array """
        SELECT 
            id,
            obj,
            idx
        FROM (
            SELECT id, CAST(jsons_str AS ARRAY<JSONB>) as jsons_jsonb
            FROM ${tb_name2}
        ) t,
        UNNEST(jsons_jsonb) WITH ORDINALITY AS v(obj, idx)
        ORDER BY id, idx;"""

    // array[bitmap]
    // Test unnesting an array of BITMAP values.
    qt_unnest_bitmap_array """SELECT 
            row_id,
            bin_map,
            bitmap_count(bin_map) AS cnt,
            idx
        FROM (
            SELECT 
                1 AS row_id,
                ARRAY(
                    to_bitmap(1), 
                    to_bitmap('2,3,4'), 
                    bitmap_empty()
                ) AS bitmap_arr
        ) t,
        UNNEST(bitmap_arr) WITH ORDINALITY AS v(idx, bin_map)
        ORDER BY row_id, idx, cnt;"""

    // array[map]
    // Test unnesting an array of MAP objects.
    qt_unnest_map_array """
        SELECT 
            user_id,
            current_map,
            idx,
            current_map['category'] AS category_val
        FROM (
            SELECT 101 AS user_id, 
                   ARRAY(
                        MAP('category', 'electronics', 'rank', 'A'),
                       MAP('category', 'books', 'rank', 'B')
                   ) AS map_arr
            UNION ALL
            SELECT 102 AS user_id, 
                   ARRAY(
                       MAP('category', 'home', 'status', 'active')
                   ) AS map_arr
        ) t,
        UNNEST(map_arr) WITH ORDINALITY AS v(idx, current_map)
        ORDER BY user_id, idx, category_val;
        """

    // array(array)
    // Test unnesting a nested array (array of arrays).
    qt_unnest_nested_array """
        SELECT 
            id,
            sub_array,
            idx
        FROM (
            SELECT 1 AS id, 
                   ARRAY(
                       ARRAY('a', 'b'), 
                       ARRAY('c', 'd', 'e'), 
                       ARRAY('f')
                   ) AS multi_dim_arr
        ) t,
        UNNEST(multi_dim_arr) WITH ORDINALITY AS v(sub_array, idx)
        ORDER BY id, idx;
        """

    // array(int, char)
    // Test unnesting an array with mixed data types (char and int).
    qt_unnest_mixed_type_array """SELECT 
            id,
            sub_array,
            idx
        FROM (
            SELECT 1 AS id, 
                   ARRAY(
                       'a', 1
                   ) AS multi_dim_arr
        ) t,
        UNNEST(multi_dim_arr) WITH ORDINALITY AS v(sub_array, idx)
        ORDER BY id, idx;"""


    // unnest(char)
    test {
        // Test that unnesting a non-array type (char) fails.
        sql """select unnest('a')"""
        exception "Can not find the compatibility function signature"
    }

    // unnest(array(bitmap))
    // Test converting a BITMAP to an array and then unnesting it.
    qt_unnest_bitmap_to_array """SELECT 
            user_group,
            individual_id,
            pos
        FROM (
            SELECT 
                'VIP_Users' AS user_group, 
                bitmap_from_string('101,505,202,303') AS bm
        ) t,
        UNNEST(bitmap_to_array(bm)) WITH ORDINALITY AS v(individual_id, pos)
        ORDER BY user_group, pos, individual_id;"""

    // unnest(array(int), array(decimal), array(json))
    // Test unnesting multiple arrays of different types in parallel.
    qt_unnest_multiple_arrays """SELECT 
            id,
            val_int,
            val_decimal,
            val_json,
            idx
        FROM (
            SELECT 
                1 AS id,
                ARRAY(1, 2, 3) AS arr_int,
                ARRAY(CAST(10.5 AS DECIMAL(10,2)), CAST(20.0 AS DECIMAL(10,2))) AS arr_decimal,
                (select CAST(jsons_str AS ARRAY<JSONB>) as jsons_jsonb FROM ${tb_name2}) as arr_json
        ) t,
        UNNEST(arr_int, arr_decimal, arr_json) WITH ORDINALITY AS v(val_int, val_decimal, val_json, idx)
        ORDER BY id, idx, val_int, val_decimal;"""

}
