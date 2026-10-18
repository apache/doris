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
    sql "DROP TABLE IF EXISTS test_arrow_complex_replace_null_key"
    sql "DROP TABLE IF EXISTS test_arrow_complex_replace_empty_key"
    sql "DROP TABLE IF EXISTS test_arrow_complex_replace_long_key"
    sql "DROP TABLE IF EXISTS test_arrow_complex_replace_long_key_match"
    sql "DROP TABLE IF EXISTS test_arrow_complex_replace_invalid_search"
    sql "DROP TABLE IF EXISTS test_arrow_complex_replace_empty_search"
    sql "DROP TABLE IF EXISTS test_arrow_complex_replace_null_search"
    sql "DROP TABLE IF EXISTS test_arrow_complex_replace_nested_struct"
    sql "DROP TABLE IF EXISTS test_arrow_complex_replace_float_struct"
    sql "DROP TABLE IF EXISTS test_arrow_complex_replace_long_struct_name"
    sql "DROP TABLE IF EXISTS test_arrow_complex_replace_too_long_struct_name"

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

    // MAP and STRUCT cannot be compared directly, so the rewritten comparison compares the map keys,
    // the map values and the struct fields one by one.
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

    // A NULL search key must not be conflated with the empty string key: the comparison keeps the
    // key NULL bitmap, so {"": 1} is not equal to {null: 1} and the source row is kept as is.
    sql """
        CREATE TABLE test_arrow_complex_replace_null_key (
            `k` INT,
            `m` MAP<STRING, INT>,
            `w` STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    streamLoad {
        table "test_arrow_complex_replace_null_key"
        set 'format', 'arrow'
        set 'columns', "k, m, m = replace_value('{null:1}', '{\"z\":9}'), w"
        file 'arrow_complex_replace_null_key.arrow'
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
    order_qt_null_key """
        SELECT k, m FROM test_arrow_complex_replace_null_key ORDER BY k
    """

    // The empty string key is still comparable and matches only the row that really has it.
    sql """
        CREATE TABLE test_arrow_complex_replace_empty_key (
            `k` INT,
            `m` MAP<STRING, INT>,
            `w` STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    streamLoad {
        table "test_arrow_complex_replace_empty_key"
        set 'format', 'arrow'
        set 'columns', "k, m, m = replace_value('{\"\":1}', '{\"z\":9}'), w"
        file 'arrow_complex_replace_null_key.arrow'
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
    order_qt_empty_key """
        SELECT k, m FROM test_arrow_complex_replace_empty_key ORDER BY k
    """

    // A 256 byte key exceeds the JSONB object key limit, so the comparison must not go through a
    // JSON object: the rows are imported unchanged when the empty search does not match.
    sql """
        CREATE TABLE test_arrow_complex_replace_long_key (
            `k` INT,
            `m` MAP<STRING, INT>,
            `w` STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    streamLoad {
        table "test_arrow_complex_replace_long_key"
        set 'format', 'arrow'
        set 'columns', "k, m, m = replace_value('{}', '{\"z\":9}'), w"
        file 'arrow_complex_replace_long_key.arrow'
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
    order_qt_long_key """
        SELECT k, m FROM test_arrow_complex_replace_long_key ORDER BY k
    """

    // The same 256 byte key is still matchable by a search value.
    sql """
        CREATE TABLE test_arrow_complex_replace_long_key_match (
            `k` INT,
            `m` MAP<STRING, INT>,
            `w` STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    def longKey = 'x' * 256
    streamLoad {
        table "test_arrow_complex_replace_long_key_match"
        set 'format', 'arrow'
        set 'columns', "k, m, m = replace_value('{${longKey}:1}', '{\"z\":9}'), w"
        file 'arrow_complex_replace_long_key.arrow'
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
    order_qt_long_key_match """
        SELECT k, m FROM test_arrow_complex_replace_long_key_match ORDER BY k
    """

    // A non-NULL search that does not parse must fail the load instead of becoming SQL NULL, which
    // the mapping IF would read as "not equal" and replace every row.
    sql """
        CREATE TABLE test_arrow_complex_replace_invalid_search (
            `k` INT,
            `m` MAP<STRING, INT>,
            `w` STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    streamLoad {
        table "test_arrow_complex_replace_invalid_search"
        set 'format', 'arrow'
        set 'columns', "k, m, m = replace_value(w, '{\"z\":9}'), w"
        file 'arrow_complex_replace_invalid_search.arrow'
        time 10000
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
        }
    }

    // An empty dynamic search is not a map literal and must fail the same way.
    sql """
        CREATE TABLE test_arrow_complex_replace_empty_search (
            `k` INT,
            `m` MAP<STRING, INT>,
            `w` STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    streamLoad {
        table "test_arrow_complex_replace_empty_search"
        set 'format', 'arrow'
        set 'columns', "k, m, m = replace_value(w, '{\"z\":9}'), w"
        file 'arrow_complex_replace_empty_search.arrow'
        time 10000
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
        }
    }

    // A NULL dynamic search keeps the text format behavior: the comparison is unknown, so the
    // replacement applies; the other row matches w and is replaced too.
    sql """
        CREATE TABLE test_arrow_complex_replace_null_search (
            `k` INT,
            `m` MAP<STRING, INT>,
            `w` STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    streamLoad {
        table "test_arrow_complex_replace_null_search"
        set 'format', 'arrow'
        set 'columns', "k, m, m = replace_value(w, '{\"z\":9}'), w"
        file 'arrow_complex_replace_null_search.arrow'
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
    order_qt_null_search """
        SELECT k, m FROM test_arrow_complex_replace_null_search ORDER BY k
    """

    // element_at merges a NULL parent STRUCT into every field, so the field comparisons alone would
    // treat a NULL nested STRUCT like a non-NULL one whose fields are all NULL. Each row pairs a
    // source s with a dynamic search w to cover both mismatch directions, both NULL, equal and
    // different fields, a NULL source and a NULL search.
    sql """
        CREATE TABLE test_arrow_complex_replace_nested_struct (
            `k` INT,
            `s` STRUCT<n: STRUCT<x: INT>>,
            `w` STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    streamLoad {
        table "test_arrow_complex_replace_nested_struct"
        set 'format', 'arrow'
        set 'columns', "k, s, s = replace_value(w, NULL), w"
        file 'arrow_complex_replace_nested_struct.arrow'
        time 10000
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(7, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
        }
    }
    sql "sync"
    order_qt_nested_struct """
        SELECT k, s FROM test_arrow_complex_replace_nested_struct ORDER BY k
    """

    // An ARRAY<STRUCT> is compared through its JSON text. The text keeps the sign of zero while
    // numeric equality treats -0.0 and 0.0 as equal, so a float leaf has no lossless comparison and
    // the search is rejected when the load plan is built instead of missing a replacement.
    sql """
        CREATE TABLE test_arrow_complex_replace_float_struct (
            `k` INT,
            `v` ARRAY<STRUCT<f: DOUBLE>>
        ) ENGINE=OLAP
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    streamLoad {
        table "test_arrow_complex_replace_float_struct"
        set 'format', 'arrow'
        set 'columns', "k, v, v = replace_value('[]', NULL)"
        file 'arrow_complex_replace_float_struct.arrow'
        time 10000
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertTrue(json.Message.contains("no lossless comparison"))
        }
    }

    // A STRUCT field name is a JSONB object key, limited to 255 bytes. The non-empty row renders
    // the 255 byte key and still loads.
    def longStructName = 'x' * 255
    sql """
        CREATE TABLE test_arrow_complex_replace_long_struct_name (
            `k` INT,
            `v` ARRAY<STRUCT<`${longStructName}`: INT>>
        ) ENGINE=OLAP
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    streamLoad {
        table "test_arrow_complex_replace_long_struct_name"
        set 'format', 'arrow'
        set 'columns', "k, v, v = replace_value('[]', NULL)"
        file 'arrow_complex_replace_long_struct_name.arrow'
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
    order_qt_long_struct_name """
        SELECT k FROM test_arrow_complex_replace_long_struct_name ORDER BY k
    """

    // One more byte cannot be written as a JSONB key, so the plan is rejected before the data is
    // read instead of failing the import at runtime.
    def tooLongStructName = 'x' * 256
    sql """
        CREATE TABLE test_arrow_complex_replace_too_long_struct_name (
            `k` INT,
            `v` ARRAY<STRUCT<`${tooLongStructName}`: INT>>
        ) ENGINE=OLAP
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    streamLoad {
        table "test_arrow_complex_replace_too_long_struct_name"
        set 'format', 'arrow'
        set 'columns', "k, v, v = replace_value('[]', NULL)"
        file 'arrow_complex_replace_long_struct_name.arrow'
        time 10000
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertTrue(json.Message.contains("no lossless comparison"))
        }
    }
}
