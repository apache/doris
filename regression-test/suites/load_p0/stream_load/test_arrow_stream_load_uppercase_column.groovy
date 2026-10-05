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

suite("test_arrow_stream_load_uppercase_column", "p0") {
    sql """DROP TABLE IF EXISTS test_arrow_stream_load_uppercase_column_explicit"""
    sql """
        CREATE TABLE test_arrow_stream_load_uppercase_column_explicit (
            `id` INT,
            `EV` DOUBLE
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    streamLoad {
        table "test_arrow_stream_load_uppercase_column_explicit"
        set 'format', 'arrow'
        set 'columns', 'id,EV'
        file 'arrow_uppercase_column.arrow'
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
    order_qt_explicit """
        SELECT concat(cast(id AS STRING), ':', cast(EV AS STRING))
        FROM test_arrow_stream_load_uppercase_column_explicit
        ORDER BY id
    """

    sql """DROP TABLE IF EXISTS test_arrow_stream_load_uppercase_column_auto"""
    sql """
        CREATE TABLE test_arrow_stream_load_uppercase_column_auto (
            `id` INT,
            `EV` DOUBLE
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    streamLoad {
        table "test_arrow_stream_load_uppercase_column_auto"
        set 'format', 'arrow'
        file 'arrow_uppercase_column.arrow'
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
    order_qt_auto """
        SELECT concat(cast(id AS STRING), ':', cast(EV AS STRING))
        FROM test_arrow_stream_load_uppercase_column_auto
        ORDER BY id
    """
}
