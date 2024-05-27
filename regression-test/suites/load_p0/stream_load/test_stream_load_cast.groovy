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

suite("test_stream_load_cast", "p0") {
    def tableName = "test_stream_load_cast"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            k0 INT             NULL,
            k1 TINYINT         NULL,
            k2 SMALLINT        NULL,
            k3 BIGINT          NULL,
            k4 LARGEINT        NULL,
            k5 FLOAT           NULL,
            k6 DOUBLE          NULL,
            k7 DECIMAL(9,1)    NULL,
            k8 DATE            NULL,
            k9 DATEV2          NULL,
            k10 DATETIME       NULL,
            k11 DATETIMEV2     NULL,
        )
        DISTRIBUTED BY HASH(k1) BUCKETS 32
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    // test decimal cast to integer
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', 'k0,k1,k2,k3,k4'
        set 'strict_mode', 'false'

        file 'test_cast1.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(1, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
        }
    }
    sql "sync"
    qt_sql1 "select * from ${tableName}"
    sql "sync"
    sql "truncate table ${tableName}"
    sql "sync"

    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', 'k0,k1,k2,k3,k4'
        set 'strict_mode', 'true'

        file 'test_cast1.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertEquals(1, json.NumberTotalRows)
            assertEquals(1, json.NumberFilteredRows)
        }
    }

    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', 'k0,k1,k2,k3,k4'
        set 'strict_mode', 'false'
        set 'format', 'json'

        file 'test_cast1.json'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(1, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
        }
    }
    sql "sync"
    qt_sql2 "select * from ${tableName}"
    sql "sync"
    sql "truncate table ${tableName}"
    sql "sync"

    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', 'k0,k1,k2,k3,k4'
        set 'strict_mode', 'true'
        set 'format', 'json'

        file 'test_cast1.json'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertEquals(1, json.NumberTotalRows)
            assertEquals(1, json.NumberFilteredRows)
        }
    }

    // test invaild
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'strict_mode', 'false'

        file 'test_cast2.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(1, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
        }
    }
    sql "sync"
    qt_sql3 "select * from ${tableName}"
    sql "sync"
    sql "truncate table ${tableName}"
    sql "sync"

    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'strict_mode', 'true'

        file 'test_cast2.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertEquals(1, json.NumberTotalRows)
            assertEquals(1, json.NumberFilteredRows)
        }
    }

    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'strict_mode', 'false'
        set 'format', 'json'

        file 'test_cast2.json'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(1, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
        }
    }
    sql "sync"
    qt_sql4 "select * from ${tableName}"
    sql "sync"
    sql "truncate table ${tableName}"
    sql "sync"

    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'strict_mode', 'true'
        set 'format', 'json'

        file 'test_cast2.json'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertEquals(1, json.NumberTotalRows)
            assertEquals(1, json.NumberFilteredRows)
        }
    }

    // test over limit
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'strict_mode', 'false'

        file 'test_cast3.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(1, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
        }
    }
    sql "sync"
    qt_sql5 "select * from ${tableName}"
    sql "sync"
    sql "truncate table ${tableName}"
    sql "sync"

    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'strict_mode', 'true'

        file 'test_cast3.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertEquals(1, json.NumberTotalRows)
            assertEquals(1, json.NumberFilteredRows)
        }
    }

    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'strict_mode', 'false'
        set 'format', 'json'

        file 'test_cast3.json'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            if (json.ErrorURL && !json.ErrorURL.isEmpty()) {
                def (code, out, err) = curl("GET", json.ErrorURL)
                log.info("error url: " + out)
            }
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(1, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
        }
    }
    sql "sync"
    qt_sql6 "select * from ${tableName}"
    sql "sync"
    sql "truncate table ${tableName}"
    sql "sync"

    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'strict_mode', 'true'
        set 'format', 'json'

        file 'test_cast3.json'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertEquals(1, json.NumberTotalRows)
            assertEquals(1, json.NumberFilteredRows)
        }
    }
}