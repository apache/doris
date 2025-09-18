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
suite("test_json_function", "arrow_flight_sql") {
    sql "set batch_size = 4096;"

    qt_sql "SELECT get_json_double('{\"k1\":1.3, \"k2\":\"2\"}', \"\$.k1\");"
    qt_sql "SELECT get_json_double('{\"k1\":\"v1\", \"my.key\":[1.1, 2.2, 3.3]}', '\$.\"my.key\"[1]');"
    qt_sql "SELECT get_json_double('{\"k1.key\":{\"k2\":[1.1, 2.2]}}', '\$.\"k1.key\".k2[0]');"

    qt_sql "SELECT get_json_int('{\"k1\":1, \"k2\":\"2\"}', \"\$.k1\");"
    qt_sql "SELECT get_json_int('{\"k1\":\"v1\", \"my.key\":[1, 2, 3]}', '\$.\"my.key\"[1]');"
    qt_sql "SELECT get_json_int('{\"k1.key\":{\"k2\":[1, 2]}}', '\$.\"k1.key\".k2[0]');"
    qt_sql "SELECT get_json_bigint('{\"k1\":1678708107000, \"k2\":\"2\"}', \"\$.k1\");"
    qt_sql "SELECT get_json_bigint('{\"k1\":\"v1\", \"my.key\":[11678708107001, 1678708107002, 31678708107003]}', '\$.\"my.key\"[1]');"
    qt_sql "SELECT get_json_bigint('{\"k1.key\":{\"k2\":[1678708107001, 1678708107002]}}', '\$.\"k1.key\".k2[0]');"

    qt_sql "SELECT get_json_string('{\"k1\":\"v1\", \"k2\":\"v2\"}', \"\$.k1\");"
    qt_sql "SELECT get_json_string('{\"k1\":\"v1\", \"my.key\":[\"e1\", \"e2\", \"e3\"]}', '\$.\"my.key\"[1]');"
    qt_sql "SELECT get_json_string('{\"k1.key\":{\"k2\":[\"v1\", \"v2\"]}}', '\$.\"k1.key\".k2[0]');"

    qt_sql "SELECT json_array();"
    qt_sql "SELECT json_array(null);"
    qt_sql "SELECT json_array(1, \"abc\", NULL, TRUE, '10:00:00');"
    qt_sql "SELECT json_array(1, \"abc\", NULL, TRUE, '10:00:00', 1678708107000);"
    qt_sql "SELECT json_array(\"a\", null, \"c\");"

    qt_sql "SELECT json_object();"
    qt_sql "SELECT json_object('time','10:00:00');"
    qt_sql "SELECT json_object('id', 87, 'name', 'carrot');"
    qt_sql "SELECT json_object('id', 1678708107000, 'name', 'carrot');"
    qt_sql "SELECT json_array(\"a\", null, \"c\");"

    qt_sql "SELECT json_quote('null'), json_quote('\"null\"');"
    qt_sql "SELECT json_quote('[1, 2, 3, 1678708107000]');"
    qt_sql "SELECT json_quote(null);"
    qt_sql "SELECT json_quote(\"\\n\\b\\r\\t\");"
    qt_sql "SELECT json_quote('')"

    qt_sql "SELECT json_unquote('')"
    qt_sql "SELECT json_unquote('doris')"
    qt_sql "SELECT json_unquote('\"doris\"');"
    qt_sql "SELECT json_unquote('open-quoted\"');"
    qt_sql "SELECT json_unquote('\"open-quoted');"
    qt_sql "SELECT json_unquote(null);"
    qt_sql "SELECT json_unquote('Dorr\bis\tishere\n');"
    qt_sql "SELECT json_unquote('\"Dorr\\\\bis\\\\tishere\\\\n\"');"
    qt_sql "SELECT json_unquote('\"\\\\u0044\\\\u004F\\\\u0052\\\\u0049\\\\u0053\"');"

    qt_sql "SELECT json_extract('[1, 2, 3]', '\$.[1]');"
    qt_sql "SELECT json_extract('{\"id\": 123, \"name\": \"doris\"}', '\$.id', '\$.name');"
    qt_sql "SELECT json_extract('{\"id\": 123, \"name\": \"doris\"}', null, '\$.id');"
    qt_sql "SELECT json_extract(null, '\$.id');"
    qt_sql "SELECT json_extract('{\"k1\": \"v1\", \"k2\": { \"k21\": 6.6, \"k22\": [1, 2, 3] } }', '\$.k1', '\$.k2');"
    qt_sql "SELECT json_extract('{\"k1\": \"v1\", \"k2\": { \"k21\": 6.6, \"k22\": [1, 2, 3] } }', '\$.k2.k21', '\$.k2.k22', '\$.k2.k22[1]');"

    qt_sql "SELECT JSON_CONTAINS('{\"a\": 1, \"b\": 2, \"c\": {\"d\": 4}}','1','\$.a');"
    qt_sql "SELECT JSON_CONTAINS('{\"a\": 1, \"b\": 2, \"c\": {\"d\": 4}}','1','\$.b');"
    qt_sql "SELECT JSON_CONTAINS('{\"a\": 1, \"b\": 2, \"c\": {\"d\": 4}}','{\"d\": 4}','\$.a');"
    qt_sql "SELECT JSON_CONTAINS('{\"a\": 1, \"b\": 2, \"c\": {\"d\": 4}}','{\"d\": 4}','\$.c');"
    qt_sql "SELECT JSON_CONTAINS('{\"name\": \"John\", \"age\": 30, \"city\": \"New York\", \"hobbies\": [\"reading\", \"travelling\"]}', '{\"age\": 31, \"hobbies\": [\"reading\"]}', '\$');"
    qt_sql "SELECT JSON_CONTAINS('{\"name\": \"John\", \"age\": 30, \"projects\": [{\"name\": \"Project A\", \"year\": 2020}, {\"name\": \"Project B\", \"year\": 2021}]}', '{\"projects\": [{\"name\": \"Project A\"}]}', '\$');"
    qt_sql "SELECT JSON_CONTAINS('{\"name\": \"John\", \"age\": 30, \"address\": {\"city\": \"New York\", \"country\": \"USA\"}}', '{\"address\": {\"city\": \"New York\"}}', '\$');"
    qt_sql """SELECT JSON_CONTAINS('','1','\$.a')"""
    qt_sql """SELECT JSON_CONTAINS('""','1','\$.a')"""
    qt_sql """SELECT JSON_CONTAINS("",'1','\$.a')"""

    qt_sql """select k6, json_extract_string(cast(k7 as json), "\$.a") as x10 from test_query_db.baseall group by k6, x10 order by 1,2; """

    qt_sql "SELECT json_extract_no_quotes('[1, 2, 3]', '\$.[1]');"
    qt_sql "SELECT json_extract_no_quotes('{\"id\": 123, \"name\": \"doris\"}', '\$.name');"
    qt_sql "SELECT json_extract_no_quotes('{\"id\": 123, \"name\": \"doris\"}', '\$.id', null);"
    qt_sql "SELECT json_extract_no_quotes(null, '\$.id');"
    qt_sql "SELECT json_extract_no_quotes('{\"k1\": \"v1\", \"k2\": { \"k21\": 6.6, \"k22\": [1, 2, 3] } }', '\$.k1', '\$.k2');"
    qt_sql "SELECT json_extract_no_quotes('{\"k1\": \"v1\", \"k2\": { \"k21\": 6.6, \"k22\": [1, 2, 3] } }', '\$.k2.k21', '\$.k2.k22', '\$.k2.k22[1]');"

    // invalid json path
    qt_sql """select get_json_string('{"name\\k" : 123}', '\$.name\\k')"""
    qt_sql """select get_json_string('{"name\\k" : 123}', '\$.name\\\\k')"""
    qt_sql """select get_json_string('{"name\\k" : 123}', '\$.name\\\\\\k')"""

    sql "drop table if exists d_table;"
    sql """
        create table d_table (
            k1 varchar(100) null,
            k2 varchar(100) not null
        )
        duplicate key (k1)
        distributed BY hash(k1) buckets 3
        properties("replication_num" = "1");
    """
    sql """insert into d_table values
    ('{\"a\": 1, \"b\": 2, \"c\": {\"d\": 4}}', '{\"a\": 1, \"b\": 2, \"c\": {\"d\": 4}}'),
    ('{\"a\": 1, \"b\": 2, \"c\": {\"d\": 4}}', '{\"a\": 1, \"b\": 2, \"c\": {\"d\": 5}}'),
    ('{\"a\": 1, \"b\": 2, \"c\": {\"d\": 4}}', '{\"a\": 1, \"b\": 2, \"c\": {\"d\": 6}}'),
    (null,'{\"name\": \"John\", \"age\": 30, \"city\": \"New York\", \"hobbies\": [\"reading\", \"travelling\"]}');
    """
    qt_test """
      SELECT k1,k2,JSON_CONTAINS(k1, '\$'),JSON_CONTAINS(k2, '\$') from d_table order by k1,k2;
    """

    qt_json_contains1 """
      SELECT JSON_CONTAINS('{"age": 30, "name": "John", "hobbies": ["reading", "swimming"]}', '{"invalid": "format"}');
    """
    qt_json_contains2 """
      SELECT JSON_CONTAINS('{"age": 25, "name": "Alice", "hobbies": ["painting", "music"]}', '{"age": 25}');
    """
    qt_json_contains3 """
      SELECT JSON_CONTAINS('{"age": 25, "name": "Alice", "hobbies": ["painting", "music"]}', '{"age": "25"}');
    """
    qt_json_contains4 """
      SELECT JSON_CONTAINS('{"age": 25, "name": "Alice", "hobbies": ["painting", "music"]}', '"music"', '\$.hobbies[1]');
    """
    qt_json_contains5 """
      SELECT JSON_CONTAINS('{"age": 25, "name": "Alice", "hobbies": ["painting", "music"]}', '"music"', '\$.hobbies[0]');
    """
    qt_json_contains6 """
      SELECT JSON_CONTAINS(NULL, '"music"', '{"age": 25}');
    """

    qt_json_keys """
      SELECT JSON_KEYS('{"name": "John", "age": 30, "city": "New York"}');
    """
    qt_json_keys2 """
      SELECT JSON_KEYS('{"name": "John", "age": 30, "city": "New York", "hobbies": ["reading", "travelling"]}', '\$.hobbies');
    """

    qt_json_keys3 """
      SELECT JSON_KEYS(k1, '\$.c') FROM d_table order by k1;
    """

    test {
        sql """
            SELECT JSON_KEYS('{"name": "John", "age": 30, "city": "New York", "hobbies": ["reading", "travelling"]}', '\$.*');
        """

        exception "In this situation, path expressions may not contain the * and ** tokens or an array range."
    }

    test {
        sql """
            SELECT JSON_KEYS(k1, '\$.*.c') FROM d_table order by k1;
        """

        exception "In this situation, path expressions may not contain the * and ** tokens or an array range."
    }

    test {
        sql """
            select json_extract('[[1,2,3], {"k": [4,5], "b": "123"}]', '\$**');
        """

        exception "Json path error: Invalid Json Path for value: \$**"
    }

    qt_json_remove1 """
      SELECT JSON_REMOVE('{"a": 1, "b": 2, "c": 3}', '\$.b');
    """

    qt_json_remove2 """
      SELECT JSON_REMOVE('{"Name": "Homer", "Gender": "Male", "Age": 39}', '\$.Age');
    """

    qt_json_remove3 """
      SELECT JSON_REMOVE('{"Name": "Homer", "Age": 39}', '\$.Gender');
    """

    qt_json_remove4 """
      SELECT JSON_REMOVE('[1, 2, 3]', '\$[0]');
    """

    qt_json_remove5 """
      SELECT JSON_REMOVE('[1, 2, [3, 4, 5]]', '\$[2][1]');
    """

    qt_json_remove6 """
      SELECT JSON_REMOVE('[1, 2, 3, 4, 5]', '\$[1]', '\$[3]');
    """

    qt_json_remove7 """
      SELECT JSON_REMOVE('[1, 2, 3, 4, 5]', '\$[3]');
    """

    qt_json_remove8 """
      SELECT JSON_REMOVE('[1, 2, [3, 4, 5]]', '\$[0]', '\$[1][1]');
    """

    qt_json_remove9 """
      SELECT JSON_REMOVE('[1, 2, [3, 4, 5]]', '\$[2][1]', '\$[0]');
    """

    qt_json_remove10 """
      SELECT JSON_REMOVE('{"Person": {"Name": "Homer","Age": 39,"Hobbies": ["Eating", "Sleeping", "Base Jumping"]}}', '\$.Person.Age', '\$.Person.Hobbies[2]');
    """

    qt_json_remove11 """
      SELECT JSON_REMOVE(NULL, '\$.a');
    """

    qt_json_remove12 """
      SELECT JSON_REMOVE('{"a": 1, "b": 2}', NULL);
    """

    qt_json_remove13 """
      SELECT JSON_REMOVE(NULL, NULL);
    """

    qt_json_remove14 """
      SELECT JSON_REMOVE('{"a": 1, "b": 2}', '\$.a', NULL, '\$.b');
    """

    sql "drop table if exists json_remove_test_table;"
    sql """
        create table json_remove_test_table (
            id int,
            json_data varchar(500) null,
            path1 varchar(100),
            path2 varchar(100)
        )
        duplicate key (id)
        distributed BY hash(id) buckets 1
        properties("replication_num" = "1");
    """

    sql """insert into json_remove_test_table values
    (1, '{"name": "Alice", "age": 25, "skills": ["Java", "Python", "SQL"]}', '\$.age', '\$.skills[1]'),
    (2, '{"product": "laptop", "price": 999, "specs": {"cpu": "Intel", "ram": "16GB"}}', '\$.price', '\$.specs.ram'),
    (3, '{"users": [{"id": 1, "name": "Bob"}, {"id": 2, "name": "Charlie"}]}', '\$.users[0].id', '\$.users[1]'),
    (4, '{"empty": {}}', '\$.empty', '\$.nonexistent');
    """

    qt_json_remove15 """
      SELECT JSON_REMOVE(json_data, path1) FROM json_remove_test_table ORDER BY id;
    """

    qt_json_remove16 """
      SELECT JSON_REMOVE(json_data, path1, path2) FROM json_remove_test_table ORDER BY id;
    """

    // returning errors
    test {
        sql """
            SELECT JSON_REMOVE('{"a": 1}', '\$.*');
        """
        exception "In this situation, path expressions may not contain the * and ** tokens or an array range"
    }

    test {
        sql """
            SELECT JSON_REMOVE('{"a": 1}', '\$**');
        """
        exception "Json path error: Invalid Json Path for value: \$**"
    }

    // edge cases
    qt_json_remove17 """
      SELECT JSON_REMOVE('{}', '\$.nonexistent');
    """

    qt_json_remove18 """
      SELECT JSON_REMOVE('[]', '\$[0]');
    """

    qt_json_remove19 """
      SELECT JSON_REMOVE('123', '\$.key');
    """

    qt_json_remove20 """
      SELECT JSON_REMOVE('true', '\$.key');
    """

    sql "drop table if exists json_remove_test_table;"
}