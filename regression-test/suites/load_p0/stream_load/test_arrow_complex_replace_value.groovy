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

// Arrow keeps the declared ARRAY/MAP/STRUCT type in the scan slot, so the replace_value search of a
// complex column has to be cast to that type and compared with a type-aware equality. These cases
// pin the values that the complex display text cannot tell apart and a search that the default value
// format rejects.
suite("test_arrow_complex_replace_value", "p0") {
    sql "DROP TABLE IF EXISTS test_arrow_complex_replace_literal"
    sql "DROP TABLE IF EXISTS test_arrow_complex_replace_dynamic"
    sql "DROP TABLE IF EXISTS test_arrow_complex_replace_quote"
    sql "DROP TABLE IF EXISTS test_arrow_complex_replace_map_struct"

    sql """
        CREATE TABLE test_arrow_complex_replace_literal (
            `k` INT,
            `v` ARRAY<STRING>,
            `w` STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """

    // `["a", "b"]` is both a two element array and the single element a", "b. Only the first row may
    // be replaced.
    streamLoad {
        table "test_arrow_complex_replace_literal"
        set 'format', 'arrow'
        set 'columns', 'k, v, v = replace_value(\'["a", "b"]\', NULL), w'
        file 'arrow_complex_replace_search.arrow'
        time 10000
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(2, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
        }
    }
    sql "sync"
    order_qt_literal """
        SELECT k, v FROM test_arrow_complex_replace_literal ORDER BY k
    """

    sql """
        CREATE TABLE test_arrow_complex_replace_dynamic (
            `k` INT,
            `v` ARRAY<INT>,
            `w` STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """

    // The search is the w column. w has no space after the comma while the array display text has
    // one, so only parsing w into the column type matches the first row.
    streamLoad {
        table "test_arrow_complex_replace_dynamic"
        set 'format', 'arrow'
        set 'columns', 'k, v, v = replace_value(w, NULL), w'
        file 'arrow_complex_replace_dynamic.arrow'
        time 10000
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(2, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
        }
    }
    sql "sync"
    order_qt_dynamic """
        SELECT k, v FROM test_arrow_complex_replace_dynamic ORDER BY k
    """

    sql """
        CREATE TABLE test_arrow_complex_replace_quote (
            `k` INT,
            `v` ARRAY<STRING>
        ) ENGINE=OLAP
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """

    // A search is compared, not persisted, so the default value rule that rejects quotes must not
    // apply to it: a parsed ARRAY<STRING> element is allowed to contain a single quote.
    streamLoad {
        table "test_arrow_complex_replace_quote"
        set 'format', 'arrow'
        set 'columns', "k, v, v = replace_value('[\"it''s\"]', NULL)"
        file 'arrow_complex_replace_quote.arrow'
        time 10000
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(2, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
        }
    }
    sql "sync"
    order_qt_quote """
        SELECT k, v FROM test_arrow_complex_replace_quote ORDER BY k
    """

    sql """
        CREATE TABLE test_arrow_complex_replace_map_struct (
            `k` INT,
            `m` MAP<STRING, INT>,
            `s` STRUCT<f1: INT, f2: STRING>
        ) ENGINE=OLAP
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """

    // MAP and STRUCT cannot be compared directly, so the rewritten comparison uses the JSON
    // rendering of both sides.
    streamLoad {
        table "test_arrow_complex_replace_map_struct"
        set 'format', 'arrow'
        set 'columns', "k, m, m = replace_value('{\"a\": 1, \"b\": 2}', NULL), s, s = replace_value('{7, \"x\"}', NULL)"
        file 'arrow_complex_replace_map_struct.arrow'
        time 10000
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(2, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
        }
    }
    sql "sync"
    order_qt_map_struct """
        SELECT k, m, s FROM test_arrow_complex_replace_map_struct ORDER BY k
    """
}
