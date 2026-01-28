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

suite("unnest_map_border_test", "unnest") {

    // normal array
    // Test the basic unnesting of a map into key-value pairs.
    qt_unnest_simple_map """
        SELECT 
            user_id,
            k,
            v
        FROM (
            SELECT 
                101 AS user_id,
                MAP('category', 'electronics', 'rank', 'A', 'region', 'East') AS attr_map
        ) t,
        UNNEST(attr_map) AS m(k, v)
        ORDER BY user_id, k, v;"""

    // map(null: null)
    // Test unnesting a map that contains a NULL value.
    qt_unnest_map_with_null_value """SELECT 
            item_id,
            attr_name,
            attr_val
        FROM (
            SELECT 
                501 AS item_id,
                MAP('color', 'red', 'size', NULL, 'brand', 'Doris') AS item_map
        ) t,
        UNNEST(item_map) AS v(attr_name, attr_val)
        ORDER BY item_id, attr_name, attr_val NULLS LAST;"""

    // map(null)
    // Test unnesting an empty map.
    qt_unnest_empty_map """SELECT 
            item_id,
            attr_name,
            attr_val
        FROM (
            SELECT 
                501 AS item_id,
                MAP() AS item_map
        ) t,
        UNNEST(item_map) AS v(attr_name, attr_val)
        ORDER BY item_id, attr_name, attr_val;"""

    // map(array)
    // Test unnesting a map where the values themselves are arrays.
    qt_unnest_map_with_array_values """SELECT 
            user_id,
            attr_key,
            attr_values
        FROM (
            SELECT 
                1001 AS user_id,
                -- 构造 Map，Key 是字符串，Value 是数组
                MAP(
                    'tags', ARRAY('gaming', 'coding', 'music'),
                    'roles', ARRAY('admin', 'editor'),
                    'empty_list', ARRAY()
                ) AS complex_map
        ) t, 
        UNNEST(complex_map) AS v(attr_key, attr_values)
        ORDER BY user_id, attr_key;"""

    // Test a chained UNNEST, first on a map of arrays, and then on the resulting array values.
    qt_chained_unnest_map_and_then_array """
        SELECT 
            user_id,
            attr_key,
            tag_element
        FROM (
            SELECT 
                1001 AS user_id,
                MAP(
                    'tags', ARRAY('gaming', 'coding'),
                    'roles', ARRAY('admin')
                ) AS complex_map
        ) t, 
        UNNEST(complex_map) AS v1(attr_key, attr_values),
        UNNEST(attr_values) AS v2(tag_element)
        ORDER BY user_id, attr_key, tag_element;"""

}
