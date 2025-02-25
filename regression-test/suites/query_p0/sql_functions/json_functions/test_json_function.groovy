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
}
