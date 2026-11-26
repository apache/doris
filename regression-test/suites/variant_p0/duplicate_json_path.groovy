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

suite("duplicate_json_path", "p0") {
    sql "DROP TABLE IF EXISTS duplicate_json_path"
    sql """
        CREATE TABLE duplicate_json_path (
            k int,
            v variant
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    // A key that repeats in one object is a parse error: parse_to_variant fails and
    // try_parse_to_variant returns NULL.
    test {
        sql """insert into duplicate_json_path values (1, parse_to_variant('{"a":42,"a":{"b":42}}'))"""
        exception "Duplicate Variant object key"
    }
    sql """insert into duplicate_json_path values
        (2, try_parse_to_variant('{"a" : 123, "a" : "123"}')),
        (3, try_parse_to_variant('{"a":{"b":5},"a":{"c":6}}')),
        (4, try_parse_to_variant('{"a":{"b":5},"c":6}'))"""

    // A dotted key and a nested key are different keys of the parsed value, but they are the
    // same path when the value is stored, so the write fails.
    test {
        sql """insert into duplicate_json_path values (5, parse_to_variant('{"a.b":1,"a":{"b":2}}'))"""
        exception "may contains duplicated entry"
    }

    // Load jobs parse like try_parse_to_variant, so a row with duplicate keys loads NULL.
    streamLoad {
        table "duplicate_json_path"
        set 'read_json_by_line', 'true'
        set 'format', 'json'
        file 'duplicate_json_path.json'
        time 10000

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(6, json.NumberTotalRows)
            assertEquals(6, json.NumberLoadedRows)
        }
    }

    // A dotted-key collision is found when the value is stored, not when it is parsed, so it
    // fails the whole load instead of loading NULL.
    streamLoad {
        table "duplicate_json_path"
        set 'read_json_by_line', 'true'
        set 'format', 'json'
        inputText '''{"k":16,"v":{"a":1}}
{"k":17,"v":{"a.b":8,"a":{"b":9}}}'''
        time 10000

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertTrue(json.Message.contains("may contains duplicated entry"), json.Message)
        }
    }

    order_qt_rows """select k, v, v is null from duplicate_json_path order by k"""
}
