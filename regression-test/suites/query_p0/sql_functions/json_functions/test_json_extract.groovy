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

suite("test_json_extract") {
    qt_sql_string1 """ SELECT JSON_EXTRACT_STRING('{"k1":"v31","k2":300}', '\$.k1'); """
    qt_sql_string2 """ SELECT JSON_EXTRACT_STRING(null, '\$.k1'); """
    qt_sql_string3 """ SELECT JSON_EXTRACT_STRING('{"k1":"v31","k2":300}', NULL); """
    qt_sql_string4 """ SELECT JSON_EXTRACT_STRING('{"k1":"v31","k2":{"sub_key": 1234.56}}', '\$.k2.sub_key'); """
    qt_sql_string5 """ SELECT JSON_EXTRACT_STRING(json_array("abc", 123, '2025-06-05 14:47:01'), '\$[2]'); """
    qt_sql_string6 """ SELECT JSON_EXTRACT_STRING('{"k1":"v31","k2": null}', '\$.k2'); """
    qt_sql_string7 """ SELECT JSON_EXTRACT_STRING('{"k1":"v31","k2":300}', '\$.k3'); """

    test {
        sql """ SELECT JSON_EXTRACT_STRING('{"id": 123, "name": "doris"}', '\$.'); """
        exception "Invalid Json Path for value: \$."
    }

    qt_fix_array_path """
        select 
            JSON_EXTRACT('[{"key": [123]}]', '\$[0].key') v1
            , JSON_EXTRACT('[{"key": [123]}]', '\$[0].key') v2
            , JSONB_EXTRACT('[{"key": [123]}]', '\$[0].key') v3
            , JSONB_EXTRACT('[{"key": [123]}]', '\$[0].key') v4;
    """
    qt_empty """
        select JSONB_EXTRACT('{}', '\$.*');
    """

    qt_empty2 """
        select JSONB_EXTRACT('{}', '\$[*]');
    """

    qt_empty3 """
        select JSONB_EXTRACT('[]', '\$.*');
    """

    qt_empty4 """
        select JSONB_EXTRACT('[]', '\$[*]');
    """

    qt_wildcard """
        select JSONB_EXTRACT('[{"key1": "v1", "key2": "v2"}, {"key1": "v3", "key2": "v4"}]', '\$[*].*');
    """

    qt_wildcard2 """
        select JSONB_EXTRACT('[[123, 345, 456], [456, 678]]', '\$[*].*');
    """

    qt_wildcard3 """
        select JSONB_EXTRACT('[[123, 345, 456], [456, 678]]', '\$[*][*]');
    """

    qt_wildcard4 """
        select JSONB_EXTRACT('[[123, 345, 456], [], {"key": "value"}, {}]', '\$[*][*]');
    """

    qt_wildcard5 """
        select JSONB_EXTRACT('[[123, 345, 456], [], {"key": "value", "key2": {"key3": 123}}, {}, {"key4": {"key5": ["a", "b", "c"]}}]', '\$[*].*');
    """

    qt_wildcard6 """
        select JSONB_EXTRACT('{"key1": "v1", "key2": {"key3": "v3"}, "key3": {"key4": "v4", "key5": 5}}', '\$.*.*');
    """

    qt_wildcard7 """
        select jsonb_extract('[[1,2,3], {"k": [4,5], "b": "123"}]', '\$[*]', '\$[1].*');
    """

    qt_array_last """
        select JSONB_EXTRACT('[1, 2, 3, 4, 5]', '\$[-1]') v1, JSONB_EXTRACT('[1, 2, 3, 4, 5]', '\$[last]') v2;
    """

    qt_array_last2 """
        select JSONB_EXTRACT('[1, 2, 3, 4, 5]', '\$[-2]') v1, JSONB_EXTRACT('[1, 2, 3, 4, 5]', '\$[last-1]') v2;
    """

    qt_array_last3 """
        select JSONB_EXTRACT('[1, 2, 3, 4, 5]', '\$[-2]') v1, JSONB_EXTRACT('[1, 2, 3, 4, 5]', '\$[last -    1]') v2;
    """

    test {
        sql """
            select JSONB_EXTRACT('[1, 2, 3, 4, 5]', '\$[last abc-1]') v;
        """
        exception "Invalid Json Path for value: \$[last abc-1]"
    }

    sql """
        drop table if exists json_extract_test;
    """

    sql """
        create table json_extract_test (
            id int,
            json_col json,
            json_path string
        ) distributed by hash(id) buckets 1 properties("replication_num" = "1");
    """

    sql """

        insert into json_extract_test values
        (1, '{"k1":"v31","k2":300}', '\$.k1'),
        (2, '{"k1":"v31","k2":{"sub_key": 1234.56}}', '\$.k2.sub_key'),
        (3, '{"k1":"v31","k2": null}', '\$.k2'),
        (4, '{"k1":"v31","k2":300}', '\$.k3'),
        (5, '{"id": 123, "name": "doris"}', '\$.'),
        (6, '{"k1":"v31","k2":300}', null),
        (7, '{"k1":"v31","k2":{"sub_key": 1234.56}}', null),
        (8, '{"k1":"v31","k2": null}', null),
        (9, '{"k1":"v31","k2":300}', null),
        (10, null, '\$.k1'),
        (11, null, '\$.k2.sub_key'),
        (12, null, '\$.k2'),
        (13, null, '\$.k3');
    """

    test {
        sql """
            select jsonb_extract(json_col, json_path) from json_extract_test;
        """
        exception "Invalid Json Path for value: \$."
    }

    qt_test_col_vector_vector """
        select jsonb_extract(json_col, json_path) from json_extract_test where id != 5 order by id;
    """

    qt_test_col_scalar_vector """
        select jsonb_extract('{"k1": "v1", "k2": 2, "k3": 333}', json_path) from json_extract_test where id != 5 order by id;
    """

    qt_test_col_vector_scalar """
        select jsonb_extract(json_col, '\$.k1') from json_extract_test where id != 5 order by id;
    """

    qt_test_col_vector_scalar_2 """
        select json_col, jsonb_extract(json_col, '\$.k1', '\$.k2', '\$.k3') from json_extract_test where id != 5 order by id;
    """

    qt_example1 """
        SELECT JSON_EXTRACT('{"k1":"v31","k2":300}', '\$.k1');
    """

    qt_example2 """
        select JSON_EXTRACT(null, '\$.k1');
    """

    qt_example3 """
        SELECT JSON_EXTRACT('{"k1":"v31","k2":300}', NULL);
    """

    qt_example4 """
        SELECT JSON_EXTRACT('{"k1":"v31","k2":{"sub_key": 1234.56}}', '\$.k2.sub_key');
    """

    qt_example5 """
        SELECT JSON_EXTRACT(json_array("abc", 123, '2025-06-05 14:47:01'), '\$[2]');
    """

    qt_example6 """
        SELECT JSON_EXTRACT('{"k1":"v31","k2": null}', '\$.k3');
    """

    qt_example7_1 """
        select JSON_EXTRACT('{"id": 123, "name": "doris"}', '\$.name', '\$.id', '\$.not_exists');
    """

    qt_example7_2 """
        select JSON_EXTRACT('{"id": 123, "name": "doris"}', '\$.name', '\$.id2', '\$.not_exists');
    """

    qt_example7_3 """
        select JSON_EXTRACT('{"id": 123, "name": "doris"}', '\$.k1', '\$.k2', '\$.not_exists');
    """

    qt_example8_1 """
        select json_extract('{"k": [1,2,3,4,5]}', '\$.k[*]');
    """

    qt_example8_2 """
        select json_extract('{"k": [1,2,3,4,5], "k2": "abc", "k3": {"k4": "v4"}}', '\$.*', '\$.k3.k4');
    """

    qt_example9 """
        select JSON_EXTRACT('{"id": 123, "name": null}', '\$.name') v, JSON_EXTRACT('{"id": 123, "name": null}', '\$.name') is null v2;
    """
}

