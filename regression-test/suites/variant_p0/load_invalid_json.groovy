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

// A load job parses a string field into VARIANT like try_parse_to_variant: text that is not a
// JSON document loads SQL NULL. A JSON-format load passes a string value without its quotes, so a
// string that holds JSON text is parsed, while a plain string such as "hello" loads NULL.
suite("variant_load_invalid_json", "p0") {
    sql "DROP TABLE IF EXISTS variant_load_invalid_json"
    sql """
        CREATE TABLE variant_load_invalid_json (
            k int,
            v variant
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
    """

    streamLoad {
        table "variant_load_invalid_json"
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        inputText '''{"k":1,"v":{"a":1}}
{"k":2,"v":"{\\"a\\":2}"}
{"k":3,"v":"123"}
{"k":4,"v":"hello"}
{"k":5,"v":""}
{"k":6,"v":null}'''
        time 10000

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(6, json.NumberLoadedRows)
        }
    }

    streamLoad {
        table "variant_load_invalid_json"
        set 'column_separator', '|'
        inputText '''11|{"a":11}
12|not-json
13|
14|\\N'''
        time 10000

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(4, json.NumberLoadedRows)
        }
    }

    order_qt_rows """select k, v, variant_type(v) from variant_load_invalid_json order by k"""

    // A NOT NULL column filters the rows that parse to NULL.
    sql "DROP TABLE IF EXISTS variant_load_invalid_json_not_null"
    sql """
        CREATE TABLE variant_load_invalid_json_not_null (
            k int,
            v variant NOT NULL
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
    """

    streamLoad {
        table "variant_load_invalid_json_not_null"
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'max_filter_ratio', '0.5'
        inputText '''{"k":1,"v":{"a":1}}
{"k":2,"v":"hello"}'''
        time 10000

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(1, json.NumberLoadedRows)
            assertEquals(1, json.NumberFilteredRows)
        }
    }

    order_qt_not_null """select k, v from variant_load_invalid_json_not_null order by k"""
}
