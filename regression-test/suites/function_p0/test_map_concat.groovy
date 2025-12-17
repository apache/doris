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

suite("test_map_concat") {
    sql """ set enable_nereids_planner=true; """
    sql """ set enable_fallback_to_original_planner=false; """

    def testTable = "test_map_concat_table"

    sql """
        drop table if exists ${testTable};
    """

    sql """
    CREATE TABLE ${testTable} (
        id INT,
        map1 MAP<VARCHAR, VARCHAR>,
        map2 MAP<VARCHAR, INT>,
        map3 MAP<INT, VARCHAR>
    ) ENGINE=OLAP
    DUPLICATE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
    );
    """

    sql """
        insert into ${testTable} values
        (1, {'a': 'apple', 'b': 'banana'}, {'x': 10, 'y': 20}, {1: 'one', 2: 'two'}),
        (2, {'c': 'cherry'}, {'z': 30}, {3: 'three'}),
        (3, {}, {}, {}),
        (4, NULL, NULL, NULL);
    """

    qt_sql """
        select map_concat() as empty_map;
    """

    qt_sql """
        select map_concat(map('single', 'argument')) as single_argument;
    """

    qt_sql """
        select map_concat({'a': 'apple'}, {'b': 'banana'}, {'c': 'cherry'}) as literal_maps_merged;
    """

    qt_sql """
        select id, map_concat(map1, map2, map3) as all_maps_merged from ${testTable} order by id;
    """

    qt_sql """
        select 
            map_concat(map1, {}) as merged_with_empty,
            map_concat(map1, NULL) as map_with_null,
            map_concat(NULL, NULL) as null_with_null
        from ${testTable} where id = 1;
    """

    qt_sql """
        select map_concat({'a': 'apple', 'b': 'banana'}, {'b': 'blueberry', 'c': 'cherry'}) as conflict_resolution;
    """

    qt_sql """
        select 
            map_size(map_concat(map1, map2)) as two_maps_size,
            map_keys(map_concat({'x': 10}, {'y': 20})) as keys_result
        from ${testTable} where id = 1;
    """

    qt_sql """
        select 
            map_concat({1: 'one', 2: 'two'}, {3: 'three', 4: 'four'}) as int_key_maps,
            map_concat(
                CAST({'bigint1': 10000000000, 'bigint2': 20000000000} AS MAP<VARCHAR, BIGINT>),
                CAST({'bigint3': 30000000000} AS MAP<VARCHAR, BIGINT>)
            ) as bigint_values,
            map_concat(
                CAST({'double1': 1.2345678901, 'double2': 2.3456789012} AS MAP<VARCHAR, DOUBLE>),
                CAST({'double3': 3.4567890123} AS MAP<VARCHAR, DOUBLE>)
            ) as double_values,
            map_concat({'flag1': true}, {'flag2': false}) as boolean_values,
            map_concat({'arr1': [1, 2, 3]}, {'arr2': [4, 5, 6]}) as array_values,
            map_concat(
                CAST({'decimal1': 123.456, 'decimal2': 789.012} AS MAP<VARCHAR, DECIMAL(10,3)>),
                CAST({'decimal3': 345.678} AS MAP<VARCHAR, DECIMAL(10,3)>)
            ) as decimal_values,
            map_concat({'date1': DATE '2023-01-01', 'date2': DATE '2023-12-31'}, 
                      {'timestamp1': TIMESTAMP '2023-01-01 12:00:00'}) as timestamp_values,
            -- 类型混合测试：兼容类型间的混合
            map_concat(
                CAST({'int_val': 100} AS MAP<VARCHAR, INT>),
                CAST({'bigint_val': 200} AS MAP<VARCHAR, BIGINT>)
            ) as int_bigint_mixed,
            map_concat(
                CAST({'decimal_val': 123.456} AS MAP<VARCHAR, DECIMAL(10,3)>),
                CAST({'double_val': 789.012} AS MAP<VARCHAR, DOUBLE>)
            ) as decimal_double_mixed,
            map_concat({'中文键': '中文值', 'key with emoji 🔥': 'value with emoji 🚀'}, 
                      {'key with accents café': 'value with accents naïve'}) as utf8_charset_test;
    """

    qt_sql """
        select 
            map_concat(
                map_concat({'a': 1}, {'b': 2}),
                map_concat({'c': 3}, {'d': 4})
            ) as nested_concat,
            map_concat(
                CAST({'a': 1} AS MAP<VARCHAR, INT>),
                CAST({'b': '2'} AS MAP<VARCHAR, VARCHAR>)
            ) as type_mismatch;
    """

    qt_sql """
        select id, map_concat(map1, map2) as merged
        from ${testTable}
        where map_size(map_concat(map1, map2)) > 0
        order by id;
    """
}
