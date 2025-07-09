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
    qt_sql_string5 """ SELECT JSON_EXTRACT_STRING(json_array("abc", 123, '2025-06-05 14:47:01'), '\$.[2]'); """
    qt_sql_string6 """ SELECT JSON_EXTRACT_STRING('{"k1":"v31","k2": null}', '\$.k2'); """
    qt_sql_string7 """ SELECT JSON_EXTRACT_STRING('{"k1":"v31","k2":300}', '\$.k3'); """

    test {
        sql """ SELECT JSON_EXTRACT_STRING('{"id": 123, "name": "doris"}', '\$.'); """
        exception "Invalid Json Path for value: \$."
    }

    qt_fix_array_path """
        select 
            JSON_EXTRACT('[{"key": [123]}]', '\$[0].key') v1
            , JSON_EXTRACT('[{"key": [123]}]', '\$.[0].key') v2
            , JSONB_EXTRACT('[{"key": [123]}]', '\$[0].key') v3
            , JSONB_EXTRACT('[{"key": [123]}]', '\$.[0].key') v4;
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
}
