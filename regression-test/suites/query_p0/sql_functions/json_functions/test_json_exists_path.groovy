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

suite("test_json_exists_path") {
    qt_json_exists_path1 """ SELECT json_exists_path('{"k1":"v31","k2":300}', '\$.k1'); """
    qt_json_exists_path2 """ SELECT json_exists_path(null, '\$.k1'); """
    qt_json_exists_path3 """ SELECT json_exists_path('{"k1":"v31","k2":300}', NULL); """
    qt_json_exists_path4 """ SELECT json_exists_path('{"k1":"v31","k2":{"sub_key": 1234.56}}', '\$.k2.sub_key'); """
    qt_json_exists_path5 """ SELECT json_exists_path(json_array("abc", 123, '2025-06-05 14:47:01'), '\$[2]'); """
    qt_json_exists_path6 """ SELECT json_exists_path('{"k1":"v31","k2": null}', '\$.k2'); """
    qt_json_exists_path7 """ SELECT json_exists_path('{"k1":"v31","k2":300}', '\$.k3'); """

    test {
        sql """ SELECT json_exists_path('{"id": 123, "name": "doris"}', '\$.'); """
        exception "Invalid Json Path for value: \$."
    }

    qt_empty """
        select json_exists_path('{}', '\$.*');
    """

    qt_empty2 """
        select json_exists_path('{}', '\$[*]');
    """

    qt_empty3 """
        select json_exists_path('[]', '\$.*');
    """

    qt_empty4 """
        select json_exists_path('[]', '\$[*]');
    """

    qt_wildcard """
        select json_exists_path('[{"key1": "v1", "key2": "v2"}, {"key1": "v3", "key2": "v4"}]', '\$[*].*');
    """

    qt_wildcard2 """
        select json_exists_path('[[123, 345, 456], [456, 678]]', '\$[*].*');
    """

    qt_wildcard3 """
        select json_exists_path('[[123, 345, 456], [456, 678]]', '\$[*][*]');
    """

    qt_wildcard4 """
        select json_exists_path('[[123, 345, 456], [], {"key": "value"}, {}]', '\$[*][*]');
    """

    qt_wildcard5 """
        select json_exists_path('[[123, 345, 456], [], {"key": "value", "key2": {"key3": 123}}, {}, {"key4": {"key5": ["a", "b", "c"]}}]', '\$[*].*');
    """

    qt_wildcard6 """
        select json_exists_path('{"key1": "v1", "key2": {"key3": "v3"}, "key3": {"key4": "v4", "key5": 5}}', '\$.*.*');
    """

    qt_wildcard7 """
        select json_exists_path('{"key1": "v1", "key2": {"key3": "v3"}, "key3": {"key4": "v4", "key5": 5}}', '\$**.*');
    """

    qt_wildcard8 """
        select json_exists_path('[[1,2,3], {"k": [4,5], "b": "123"}]', ' \$**.k ');
    """

    qt_array_last """
        select json_exists_path('[1, 2, 3, 4, 5]', '\$[-1]') v1, json_exists_path('[1, 2, 3, 4, 5]', '\$[last]') v2;
    """

    qt_array_last2 """
        select json_exists_path('[1, 2, 3, 4, 5]', '\$[-2]') v1, json_exists_path('[1, 2, 3, 4, 5]', '\$[last-1]') v2;
    """

    qt_array_last3 """
        select json_exists_path('[1, 2, 3, 4, 5]', '\$[-2]') v1, json_exists_path('[1, 2, 3, 4, 5]', '\$[last -    1]') v2;
    """

    test {
        sql """
            select json_exists_path('[1, 2, 3, 4, 5]', '\$[last abc-1]') v;
        """
        exception "Invalid Json Path for value: \$[last abc-1]"
    }

    sql """
        drop table if exists json_exists_path_test;
    """

    sql """
        create table json_exists_path_test (
            id int,
            json_col json,
            json_path string,
            json_col_non_null json not null,
            json_path_non_null string not null
        ) distributed by hash(id) buckets 1 properties("replication_num" = "1");
    """

    sql """

        insert into json_exists_path_test values
        (1, '{"k1":"v31","k2":300}', '\$.k1', '{"k1":"v31","k2":300}', '\$.k1'),
        (2, '{"k1":"v31","k2":{"sub_key": 1234.56}}', '\$.k2.sub_key', '{"k1":"v31","k2":{"sub_key": 1234.56}}', '\$.k2.sub_key'),
        (3, '{"k1":"v31","k2": null}', '\$.k2', '{"k1":"v31","k2": null}', '\$.k2'),
        (4, '{"k1":"v31","k2":300}', '\$.k3', '{"k1":"v31","k2":300}', '\$.k3'),
        (5, '{"id": 123, "name": "doris"}', '\$.', '{"id": 123, "name": "doris"}', '\$.'),
        (6, '{"k1":"v31","k2":300}', null, '{"k1":"v31","k2":300}', '\$'),
        (7, '{"k1":"v31","k2":{"sub_key": 1234.56}}', null, '{"k1":"v31","k2":{"sub_key": 1234.56}}', '\$'),
        (8, '{"k1":"v31","k2": null}', null, '{"k1":"v31","k2": null}', '\$'),
        (9, '{"k1":"v31","k2":300}', null, '{"k1":"v31","k2":300}', '\$'),
        (10, null, '\$.k1', '{}', '\$.k1'),
        (11, null, '\$.k2.sub_key', '{}', '\$.k2.sub_key'),
        (12, null, '\$.k2', '{}', '\$.k2'),
        (13, null, '\$.k3', '{}', '\$.k3');
    """

    test {
        sql """
            select json_exists_path(json_col, json_path) from json_exists_path_test;
        """
        exception "Invalid Json Path for value: \$."
    }

    qt_test_col_vector_vector """
        select json_exists_path(json_col, json_path) from json_exists_path_test where id != 5 order by id;
    """

    qt_test_col_scalar_vector """
        select json_exists_path('{"k1": "v1", "k2": 2, "k3": 333}', json_path) from json_exists_path_test where id != 5 order by id;
    """

    qt_test_col_vector_scalar """
        select json_exists_path(json_col, '\$.k1') from json_exists_path_test where id != 5 order by id;
    """

    qt_test_all_null """
        select
            id
            , json_col
            , json_path
            , json_exists_path(json_col, json_path)
        from json_exists_path_test where json_col is null or json_path is null  order by id
    """

    test {
        sql """
            select json_exists_path(json_col_non_null, json_path_non_null) from json_exists_path_test where id = 5;
        """
        exception "Invalid Json Path for value: \$."
    }

    sql """
        drop table if exists json_exists_path_test2;
    """

    sql """
        create table json_exists_path_test2 (
            id int,
            json_col json not null,
            json_path string
        ) distributed by hash(id) buckets 1 properties("replication_num" = "1");
    """

    sql """
        insert into json_exists_path_test2 values
        (1, '{"k1":"v31","k2":300}', '\$.k1'),
        (2, '{"k1":"v31","k2":{"sub_key": 1234.56}}', null),
        (3, '{"k1":"v31","k2": null}', '\$.k2'),
        (4, '{"k1":"v31","k2":300}', null),
        (5, '{"id": 123, "name": "doris"}', '\$.'),
        (6, '{"k1":"v31","k2":300}', null),
        (7, '{"k1":"v31","k2":{"sub_key": 1234.56}}', null);
    """

    qt_test_all_null2 """
        select
            id
            , json_col
            , json_path
            , json_exists_path(json_col, json_path)
        from json_exists_path_test2 where id % 2 = 0 order by id
    """

    test {
        sql """
            select json_exists_path(json_col_non_null, '\$.') from json_exists_path_test where id = 5;
        """
        exception "Invalid Json Path for value: \$."
    }

    test {
        sql """
            select json_exists_path('{"k1": "v1", "k2": 2, "k3": 333}', json_path) from json_exists_path_test where id = 5;
        """
        exception "Invalid Json Path for value: \$."
    }
}

