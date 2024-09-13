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

import java.util.Random;

suite("test_stream_load_with_data_quality", "p0") {
    if (!isCloudMode()) {
        return;
    }

    def tableName = "test_stream_load_with_data_quality"
    sql "DROP TABLE IF EXISTS ${tableName}"

    sql """
        CREATE TABLE ${tableName}
        (
            siteid INT DEFAULT '10',
            citycode SMALLINT NOT NULL,
            username VARCHAR(32) DEFAULT '',
            pv BIGINT SUM DEFAULT '0'
        )
        AGGREGATE KEY(siteid, citycode, username)
        DISTRIBUTED BY HASH(siteid) BUCKETS 1;
    """

    String label = UUID.randomUUID().toString().replaceAll("-", "")

    // meta-service max_num_aborted_txn is 100
    for (int i = 0; i < 100; i++) {
        streamLoad {
            set 'label', "${label}"
            set 'column_separator', ','
            table "${tableName}"
            time 10000
            file 'test_stream_load_with_data_quality.csv'
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("fail", json.Status.toLowerCase())
                assertTrue(json.Message.contains("too many filtered rows"))
                assertEquals(2, json.NumberTotalRows)
                assertEquals(2, json.NumberFilteredRows)
            }
        }
    }

    streamLoad {
        set 'label', "${label}"
        set 'column_separator', ','
        table "${tableName}"
        time 10000
        file 'test_stream_load_with_data_quality.csv'
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertTrue(json.Message.contains("too many aborted txn"))
        }
    }

    String dbName = "regression_test_load_p0_stream_load"
    test {
        sql "clean label ${label} from ${dbName};"
    }

    streamLoad {
        set 'label', "${label}"
        set 'column_separator', ','
        table "${tableName}"
        time 10000
        file 'test_stream_load_with_data_quality2.csv'
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
        }
    }

    test {
        sql "clean label ${label} from ${dbName};"
    }

    // meta-service max_num_aborted_txn is 100
    for (int i = 0; i < 99; i++) {
        streamLoad {
            set 'label', "${label}"
            set 'column_separator', ','
            table "${tableName}"
            time 10000
            file 'test_stream_load_with_data_quality.csv'
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("fail", json.Status.toLowerCase())
                assertTrue(json.Message.contains("too many filtered rows"))
                assertEquals(2, json.NumberTotalRows)
                assertEquals(2, json.NumberFilteredRows)
            }
        }
    }

    streamLoad {
        set 'label', "${label}"
        set 'column_separator', ','
        table "${tableName}"
        time 10000
        file 'test_stream_load_with_data_quality2.csv'
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
        }
    }

    streamLoad {
        set 'label', "${label}"
        set 'column_separator', ','
        table "${tableName}"
        time 10000
        file 'test_stream_load_with_data_quality2.csv'
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("label already exists", json.Status.toLowerCase())
            assertTrue(json.Message.contains("has already been used"))
        }
    }

    test {
        sql "clean label ${label} from ${dbName};"
    }

    streamLoad {
        set 'label', "${label}"
        set 'column_separator', ','
        table "${tableName}"
        time 10000
        file 'test_stream_load_with_data_quality2.csv'
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
        }
    }

    test {
        sql "clean label ${label} from ${dbName};"
    }

    sql "DROP TABLE IF EXISTS ${tableName}"
}

