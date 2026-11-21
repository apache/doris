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

// A table column absent from the Arrow stream loads as its default, as a field missing from a
// JSON row does: NULL for a nullable column, the column default when it has one, and a filtered
// row for a NOT NULL column without a default.
suite("test_arrow_stream_load_missing_columns", "p0") {
    def load = { tableName, fixture, expectedStatus, expectedLoaded, expectedFiltered ->
        streamLoad {
            table tableName
            set 'format', 'arrow'
            file "arrow_missing_columns/${fixture}.arrow"
            time 30000
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def response = parseJson(result)
                assertEquals(expectedStatus, response.Status, response.Message)
                assertEquals(3, response.NumberTotalRows)
                assertEquals(expectedLoaded, response.NumberLoadedRows)
                assertEquals(expectedFiltered, response.NumberFilteredRows)
            }
        }
    }

    sql "DROP TABLE IF EXISTS test_arrow_stream_load_missing_columns_defaults"
    sql """
        CREATE TABLE test_arrow_stream_load_missing_columns_defaults (
            k INT,
            v STRING,
            extra STRING NULL,
            tag VARCHAR(16) NOT NULL DEFAULT "loaded",
            n INT NULL DEFAULT "7"
        ) DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    load("test_arrow_stream_load_missing_columns_defaults", "kv", "Success", 3, 0)
    sql "sync"
    order_qt_defaults "SELECT * FROM test_arrow_stream_load_missing_columns_defaults ORDER BY k"

    // Each concatenated IPC stream resolves its own schema: only the first stream lacks extra.
    sql "DROP TABLE IF EXISTS test_arrow_stream_load_missing_columns_streams"
    sql """
        CREATE TABLE test_arrow_stream_load_missing_columns_streams (
            k INT,
            v STRING,
            extra STRING NULL DEFAULT "absent"
        ) DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    load("test_arrow_stream_load_missing_columns_streams", "kv_then_kv_extra", "Success", 3, 0)
    sql "sync"
    order_qt_streams "SELECT * FROM test_arrow_stream_load_missing_columns_streams ORDER BY k"

    // A NOT NULL column without a default has no value to load, so every row is filtered.
    sql "DROP TABLE IF EXISTS test_arrow_stream_load_missing_columns_not_null"
    sql """
        CREATE TABLE test_arrow_stream_load_missing_columns_not_null (
            k INT,
            v STRING,
            required STRING NOT NULL
        ) DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    load("test_arrow_stream_load_missing_columns_not_null", "kv", "Fail", 0, 3)
    sql "sync"
    order_qt_not_null "SELECT count(*) FROM test_arrow_stream_load_missing_columns_not_null"

    // A sender built before ALTER TABLE ADD COLUMN keeps loading.
    sql "DROP TABLE IF EXISTS test_arrow_stream_load_missing_columns_alter"
    sql """
        CREATE TABLE test_arrow_stream_load_missing_columns_alter (
            k INT,
            v STRING
        ) DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    load("test_arrow_stream_load_missing_columns_alter", "kv", "Success", 3, 0)
    sql "ALTER TABLE test_arrow_stream_load_missing_columns_alter ADD COLUMN extra STRING NULL"
    waitForSchemaChangeDone {
        sql """SHOW ALTER TABLE COLUMN WHERE tablename='test_arrow_stream_load_missing_columns_alter' ORDER BY createtime DESC LIMIT 1"""
        time 600
    }
    load("test_arrow_stream_load_missing_columns_alter", "kv", "Success", 3, 0)
    sql "sync"
    order_qt_alter "SELECT * FROM test_arrow_stream_load_missing_columns_alter ORDER BY k, v"
}
