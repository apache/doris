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

suite("test_strip_null_value") {
    sql """DROP TABLE IF EXISTS `test_strip_null_value_table`;"""
    sql """CREATE TABLE test_strip_null_value_table (
            id INT,
            json_value JSON,
            json_value_non_null JSON not null
        ) PROPERTIES ("replication_num"="1");"""

    sql """INSERT INTO test_strip_null_value_table VALUES
        (1, '{"name": "null", "age": 30, "a": "aaa", "b": "b", "c": null}', '{"name": "Alice2", "age": null, "a": 123, "c": null}'),
        (2, '{"name": "Bob", "age": null, "b": "bbbb", "c": 23423, "d": null}', '{"name": "Bob", "age": null, "b": "a123", "c": null, "d": 9993}'),
        (3, null, '{"name": "Jack", "age": 28, "a": null, "b": null, "c": null}'),
        (4, null, '{"name": "Jim", "age": 33, "a": 1234, "b": 4567, "d": 7890}');
    """

    qt_test """
        select id, 
            strip_null_value(json_extract(json_value, '\$.name')) striped,
            strip_null_value(json_extract(json_value, '\$.age')) as striped2
        from test_strip_null_value_table order by 1;
    """

    qt_test2 """
        select id,
            strip_null_value(json_extract(json_value_non_null, '\$.name')) striped,
            strip_null_value(json_extract(json_value_non_null, '\$.age')) striped2 
        from test_strip_null_value_table order by 1;
    """

    sql """DROP TABLE IF EXISTS `test_strip_null_value_paths_table`;"""
    sql """CREATE TABLE test_strip_null_value_paths_table (
            id INT,
            path string,
            path_not_null string not null
        ) PROPERTIES ("replication_num"="1");"""

    sql """INSERT INTO test_strip_null_value_paths_table VALUES
        (1, '\$.a', '\$.a'),
        (2, '\$.b', '\$.b'),
        (3, null, '\$.c'),
        (4, null, '\$.d');
    """

    qt_test2 """
        select
            id,
            strip_null_value(json_extract('{"a": "a", "b": null, "c": "c", "d": null}', path)) striped1, 
            strip_null_value(json_extract('{"a": "a", "b": null, "c": "c", "d": null}', path_not_null)) striped2
        from test_strip_null_value_paths_table order by 1;
    """

    qt_test3 """
        select
            t1.id,
            strip_null_value(json_extract(t1.json_value, t2.path)) striped1,
            strip_null_value(json_extract(t1.json_value_non_null, t2.path)) striped2,
            strip_null_value(json_extract(t1.json_value, t2.path_not_null)) striped3,
            strip_null_value(json_extract(t1.json_value_non_null, t2.path_not_null)) striped4
        from test_strip_null_value_table t1
        inner join test_strip_null_value_paths_table t2 on t1.id = t2.id
        order by t1.id;
    """

    qt_const """
        select strip_null_value(json_extract('{"a": "aaa", "b": null, "c": "ccc", "d": null}', '\$.a')) as striped1,
               strip_null_value(json_extract('{"a": "aaa", "b": null, "c": "ccc", "d": null}', '\$.b')) as striped2,
               strip_null_value(json_extract('{"a": "aaa", "b": null, "c": "ccc", "d": null}', '\$.c')) as striped3,
               strip_null_value(json_extract('{"a": "aaa", "b": null, "c": "ccc", "d": null}', '\$.d')) as striped4,
               strip_null_value(NULL) as striped5;
    """
}
