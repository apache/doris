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

suite("test_stream_load_new_move_memtable", "p0") {

    sql """set enable_memtable_on_sink_node=true"""

    // 1. test column with currenttimestamp default value
    def tableName1 = "test_stream_load_new_current_timestamp_mm"

    try {
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName1} (
            id int,
            name CHAR(10),
            dt_1 DATETIME DEFAULT CURRENT_TIMESTAMP,
            dt_2 DATETIMEV2 DEFAULT CURRENT_TIMESTAMP,
            dt_3 DATETIMEV2(3) DEFAULT CURRENT_TIMESTAMP,
            dt_4 DATETIMEV2(6) DEFAULT CURRENT_TIMESTAMP
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        )
        """

        streamLoad {
            set 'column_separator', ','
            set 'columns', 'id, name'
            set 'memtable_on_sink_node', 'true'
            table "${tableName1}"
            time 10000
            file 'test_stream_load_new_current_timestamp.csv'
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(11, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }

        sql """ sync; """
        qt_sql1 "select id, name from ${tableName1}"
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName1}"
    }

    // 2. test change column order
    def tableName2 = "test_stream_load_new_change_column_order_mm"

    try {
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName2} (
            k1 int,
            k2 smallint,
            k3 CHAR(10),
            k4 bigint,
            k5 decimal(6, 3),
            k6 float
        )
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        )
        """

        streamLoad {
            set 'column_separator', ','
            set 'columns', 'k1, k3, k2, k4, k6, k5'
            set 'memtable_on_sink_node', 'true'
            table "${tableName2}"
            time 10000
            file 'test_stream_load_new_change_column_order.csv'
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(11, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }

        sql """ sync; """
        qt_sql2 "select * from ${tableName2}"
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName2}"
    }

    // 3. test with function
    def tableName3 = "test_stream_load_new_function_mm"

    try {
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName3} (
            id int,
            name CHAR(10),
            year int,
            month int,
            day int
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        )
        """

        streamLoad {
            set 'column_separator', ','
            set 'columns', 'id, name, tmp_c3, year = year(tmp_c3), month = month(tmp_c3), day = day(tmp_c3)'
            set 'memtable_on_sink_node', 'true'
            table "${tableName3}"
            time 10000
            file 'test_stream_load_new_function.csv'
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(11, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }

        sql """ sync; """
        qt_sql3 "select * from ${tableName3}"
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName3}"
    }

    // 4. test column number mismatch
    def tableName4 = "test_stream_load_new_column_number_mismatch_mm"

    try {
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName4} (
            k1 int NOT NULL,
            k2 CHAR(10) NOT NULL,
            k3 smallint NOT NULL
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        )
        """

        streamLoad {
            set 'column_separator', ','
            set 'memtable_on_sink_node', 'true'
            table "${tableName4}"
            time 10000
            file 'test_stream_load_new_column_number_mismatch.csv'
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("fail", json.Status.toLowerCase())
            }
        }
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("Distribution column(id) doesn't exist"), e.getMessage())
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName4}"
    }

    // 5. test with default value
    def tableName5 = "test_stream_load_new_default_value_mm"

    try {
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName5} (
            id int NOT NULL,
            name CHAR(10) NOT NULL,
            date DATE NOT NULL, 
            max_dwell_time INT DEFAULT "0",
            min_dwell_time INT DEFAULT "99999" 
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        )
        """

        streamLoad {
            set 'column_separator', ','
            set 'columns', 'id, name, date'
            set 'memtable_on_sink_node', 'true'
            table "${tableName5}"
            time 10000
            file 'test_stream_load_new_default_value.csv'
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(11, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }

        sql """ sync; """
        qt_sql5 "select * from ${tableName5}"
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName5}"
    }

    // 6. test some column type
    def tableName6 = "test_stream_load_new_column_type_mm"

    try {
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName6} (
            c_int int(11) NULL,
            c_char char(15) NULL,
            c_varchar varchar(100) NULL,
            c_bool boolean NULL,
            c_tinyint tinyint(4) NULL,
            c_smallint smallint(6) NULL,
            c_bigint bigint(20) NULL,
            c_largeint largeint(40) NULL,
            c_float float NULL,
            c_double double NULL,
            c_decimal decimal(6, 3) NULL,
            c_decimalv3 decimalv3(6, 3) NULL,
            c_date date NULL,
            c_datev2 datev2 NULL,
            c_datetime datetime NULL,
            c_datetimev2 datetimev2(0) NULL
        )
        DISTRIBUTED BY HASH(c_int) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        )
        """

        streamLoad {
            set 'column_separator', ','
            set 'memtable_on_sink_node', 'true'
            table "${tableName6}"
            time 10000
            file 'test_stream_load_new_column_type.csv'
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(11, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }

        sql """ sync; """
        qt_sql6 "select * from ${tableName6}"
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName6}"
    }

    // 7. test duplicate key
    def tableName7 = "test_stream_load_duplicate_key_mm"

    try {
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName7}
        (
            user_id LARGEINT NOT NULL,
            username VARCHAR(50) NOT NULL,
            city VARCHAR(20),
            age SMALLINT,
            sex TINYINT,
            phone LARGEINT,
            address VARCHAR(500),
            register_time DATETIME
        )
        DUPLICATE KEY(`user_id`, `username`)
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        )
        """

        streamLoad {
            set 'column_separator', ','
            set 'memtable_on_sink_node', 'true'
            table "${tableName7}"
            time 10000
            file 'test_stream_load_data_model.csv'
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(11, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }

        sql """ sync; """
        qt_sql7 "select * from ${tableName7}"
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName7}"
    }

    // 8. test merge on read unique key
    def tableName8 = "test_stream_load_unique_key_merge_on_read_mm"

    try {
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName8}
        (
            user_id LARGEINT NOT NULL,
            username VARCHAR(50) NOT NULL,
            city VARCHAR(20),
            age SMALLINT,
            sex TINYINT,
            phone LARGEINT,
            address VARCHAR(500),
            register_time DATETIME
        )
        UNIQUE KEY(`user_id`, `username`)
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        )
        """

        streamLoad {
            set 'column_separator', ','
            set 'memtable_on_sink_node', 'true'
            table "${tableName8}"
            time 10000
            file 'test_stream_load_data_model.csv'
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(11, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }

        sql """ sync; """
        qt_sql8 "select * from ${tableName8}"
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName8}"
    }

    // 9. test merge on write unique key
    def tableName9 = "test_stream_load_unique_key_merge_on_write_mm"

    try {
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName9}
        (
            user_id LARGEINT NOT NULL,
            username VARCHAR(50) NOT NULL,
            city VARCHAR(20),
            age SMALLINT,
            sex TINYINT,
            phone LARGEINT,
            address VARCHAR(500),
            register_time DATETIME
        )
        UNIQUE KEY(`user_id`, `username`)
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "enable_unique_key_merge_on_write" = "true"
        )
        """

        streamLoad {
            set 'column_separator', ','
            set 'memtable_on_sink_node', 'true'
            table "${tableName9}"
            time 10000
            file 'test_stream_load_data_model.csv'
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(11, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }

        sql """ sync; """
        qt_sql9 "select * from ${tableName9}"
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName9}"
    }

    // 10. test stream load multiple times
    def tableName10 = "test_stream_load_multiple_times_mm"
    Random rd = new Random()
    def disable_auto_compaction = "false"
    if (rd.nextBoolean()) {
        disable_auto_compaction = "true"
    }
    log.info("disable_auto_compaction: ${disable_auto_compaction}".toString())
    try {
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName10}
        (
            user_id LARGEINT NOT NULL,
            username VARCHAR(50) NOT NULL,
            money INT
        )
        DUPLICATE KEY(`user_id`, `username`)
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "disable_auto_compaction" = "${disable_auto_compaction}"
        )
        """
        for (int i = 0; i < 3; ++i) {
            streamLoad {
                set 'column_separator', ','
                set 'memtable_on_sink_node', 'true'
                table "${tableName10}"
                time 10000
                file 'test_stream_load_multiple_times.csv'
                check { result, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    log.info("Stream load result: ${result}".toString())
                    def json = parseJson(result)
                    assertEquals("success", json.Status.toLowerCase())
                    assertEquals(500, json.NumberTotalRows)
                    assertEquals(0, json.NumberFilteredRows)
                }
            }
        }

        sql """ sync; """
        qt_sql10 "select count(*) from ${tableName10}"
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName10}"
    }

    // 11. test stream load column separator
    def tableName11 = "test_stream_load_column_separator_mm"

    try {
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName11} (
            id int,
            name CHAR(10),
            dt_1 DATETIME DEFAULT CURRENT_TIMESTAMP,
            dt_2 DATETIMEV2 DEFAULT CURRENT_TIMESTAMP,
            dt_3 DATETIMEV2(3) DEFAULT CURRENT_TIMESTAMP,
            dt_4 DATETIMEV2(6) DEFAULT CURRENT_TIMESTAMP
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        )
        """

        streamLoad {
            set 'column_separator', '--'
            set 'columns', 'id, name'
            set 'memtable_on_sink_node', 'true'
            table "${tableName11}"
            time 10000
            file 'test_stream_load_new_column_separator.csv'
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(11, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }

        sql """ sync; """
        qt_sql11 "select id, name from ${tableName11}"
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName11}"
    }

    // 12. test stream load line delimiter
    def tableName12 = "test_stream_load_line_delimiter_mm"
    
    try {
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName12} (
            id int,
            name CHAR(10),
            dt_1 DATETIME DEFAULT CURRENT_TIMESTAMP,
            dt_2 DATETIMEV2 DEFAULT CURRENT_TIMESTAMP,
            dt_3 DATETIMEV2(3) DEFAULT CURRENT_TIMESTAMP,
            dt_4 DATETIMEV2(6) DEFAULT CURRENT_TIMESTAMP
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        )
        """

        streamLoad {
            set 'column_separator', ','
            set 'line_delimiter', '||'
            set 'columns', 'id, name'
            set 'memtable_on_sink_node', 'true'
            table "${tableName12}"
            time 10000
            file 'test_stream_load_new_line_delimiter.csv'
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(11, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }

        sql """ sync; """
        qt_sql12 "select id, name from ${tableName12}"
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName12}"
    }

    sql """set enable_memtable_on_sink_node=false"""
}

