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

// Arrow Stream Load into a VARIANT column from each accepted input shape. Every file holds the
// same five rows: two objects, a SQL NULL, an array and a string.
//   arrow_variant_json_text.arrow   large_utf8 JSON text
//   arrow_variant_binary.arrow      struct<value: large_binary, metadata: large_binary>
//   arrow_variant_dictionary.arrow  struct<metadata: dictionary<int8, binary>, value: binary>
suite("test_arrow_stream_load_variant", "p0") {
    sql "DROP TABLE IF EXISTS test_arrow_stream_load_variant_json"
    sql """
        CREATE TABLE test_arrow_stream_load_variant_json (
            id INT,
            v VARIANT
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    streamLoad {
        table "test_arrow_stream_load_variant_json"
        set 'format', 'arrow'
        file 'arrow_variant_json_text.arrow'
        time 10000
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(5, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
        }
    }
    sql "sync"
    order_qt_json_text """
        SELECT id, cast(v AS STRING), cast(v['name'] AS STRING), cast(v['n'] AS INT)
        FROM test_arrow_stream_load_variant_json ORDER BY id
    """

    sql "DROP TABLE IF EXISTS test_arrow_stream_load_variant_binary"
    sql """
        CREATE TABLE test_arrow_stream_load_variant_binary (
            id INT,
            v VARIANT
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    streamLoad {
        table "test_arrow_stream_load_variant_binary"
        set 'format', 'arrow'
        file 'arrow_variant_binary.arrow'
        time 10000
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(5, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
        }
    }
    sql "sync"
    order_qt_binary """
        SELECT id, cast(v AS STRING), cast(v['name'] AS STRING), cast(v['n'] AS INT)
        FROM test_arrow_stream_load_variant_binary ORDER BY id
    """

    sql "DROP TABLE IF EXISTS test_arrow_stream_load_variant_dictionary"
    sql """
        CREATE TABLE test_arrow_stream_load_variant_dictionary (
            id INT,
            v VARIANT
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    streamLoad {
        table "test_arrow_stream_load_variant_dictionary"
        set 'format', 'arrow'
        file 'arrow_variant_dictionary.arrow'
        time 10000
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(5, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
        }
    }
    sql "sync"
    order_qt_dictionary """
        SELECT id, cast(v AS STRING), cast(v['name'] AS STRING), cast(v['n'] AS INT)
        FROM test_arrow_stream_load_variant_dictionary ORDER BY id
    """
}
