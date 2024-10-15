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

suite("test_http_stream", "p0") {

    // csv desc
    // | c1  | c2   | c3     | c4      | c5      | c6       | c7     | c8       |
    // | int | char | varchar| boolean | tinyint | smallint | bigint | largeint |
    // | c9    | c10    | c11     | c12       | c13  | c14    | c15      | c16        |
    // | float | double | decimal | decimalv3 | date | datev2 | datetime | datetimev2 |

    // 1. test column with currenttimestamp default value
    def tableName1 = "test_http_stream_current_timestamp"
    def db = "regression_test_load_p0_http_stream"
    try {
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName1} (
            id int,
            name CHAR(10),
            dt_1 DATETIME DEFAULT CURRENT_TIMESTAMP,
            dt_2 DATETIMEV2 DEFAULT CURRENT_TIMESTAMP,
            dt_3 DATETIMEV2(3) DEFAULT CURRENT_TIMESTAMP,
            dt_4 DATETIMEV2(6) DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_dt_2 (`dt_2`) USING INVERTED,
            INDEX idx_dt_3 (`dt_3`) USING INVERTED
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        )
        """

        streamLoad {
            set 'version', '1'
            set 'sql', """
                    insert into ${db}.${tableName1} (id, name) select c1, c2 from http_stream("format"="csv")
                    """
            time 10000
            file 'test_http_stream.csv'
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("http_stream result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(11, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }

        qt_sql1 "select id, name from ${tableName1}"
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName1}"
    }

    // 2. test change column order
    def tableName2 = "test_http_stream_change_column_order"

    try {
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName2} (
            k1 int,
            k2 smallint NOT NULL,
            k3 CHAR(10),
            k4 bigint NOT NULL,
            k5 decimal(6, 3) NOT NULL,
            k6 float sum NOT NULL
        )
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        )
        """

        streamLoad {
            set 'version', '1'
            set 'sql', """
                    insert into ${db}.${tableName2} select c1, c6, c2, c7, c11, c9 from http_stream("format"="csv")
                    """
            time 10000
            file 'test_http_stream.csv'
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("http_stream result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(11, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }

        qt_sql2 "select * from ${tableName2}"
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName2}"
    }

    // 3. test with function
    def tableName3 = "test_http_stream_function"

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
            set 'version', '1'
            set 'sql', """
                    insert into ${db}.${tableName3} select c1, c2, year(c14), month(c14), day(c14) from http_stream("format"="csv")
                    """
            time 10000
            file 'test_http_stream.csv'
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("http_stream result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(11, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }

        qt_sql3 "select * from ${tableName3}"
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName3}"
    }

    // 4. test column number mismatch
    def tableName4 = "test_http_stream_column_number_mismatch"

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
            set 'version', '1'
            set 'sql', """
                    insert into  ${db}.${tableName4} select c1, c2, c6, c3 from http_stream("format"="csv")
                    """
            time 10000
            file 'test_http_stream.csv'
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("http_stream result: ${result}".toString())
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
    def tableName5 = "test_http_stream_default_value"

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
            set 'version', '1'
            set 'sql', """
                    insert into  ${db}.${tableName5} (id, name, date) select c1, c2, c13 from http_stream("format"="csv")
                    """
            time 10000
            file 'test_http_stream.csv'
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("http_stream result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(11, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }

        qt_sql5 "select * from ${tableName5}"
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName5}"
    }

    // 6. test some column type
    def tableName6 = "test_http_stream_column_type"

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
            set 'version', '1'
            set 'sql', """
                    insert into  ${db}.${tableName6} select * from http_stream("format"="csv")
                    """
            time 10000
            file 'test_http_stream.csv'
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("http_stream result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(11, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }

        qt_sql6 "select * from ${tableName6}"
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName6}"
    }

    // 7. test duplicate key
    def tableName7 = "test_http_stream_duplicate_key"

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
            register_time DATETIME,
            INDEX idx_username (`username`) USING INVERTED,
            INDEX idx_address (`address`) USING INVERTED
        )
        DUPLICATE KEY(`user_id`, `username`)
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        )
        """

        streamLoad {
            set 'version', '1'
            set 'sql', """
                    insert into  ${db}.${tableName7} select * from http_stream("format"="csv")
                    """
            time 10000
            file 'test_http_stream_data_model.csv'
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("http_stream result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(11, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }

        qt_sql7 "select * from ${tableName7}"
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName7}"
    }

    // 8. test merge on read unique key
    def tableName8 = "test_http_stream_unique_key_merge_on_read"

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
            set 'version', '1'
            set 'sql', """
                    insert into  ${db}.${tableName8} select * from http_stream("format"="csv")
                    """
            time 10000
            file 'test_http_stream_data_model.csv'
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("http_stream result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(11, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }

        qt_sql8 "select * from ${tableName8}"
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName8}"
    }

    // 9. test merge on write unique key
    def tableName9 = "test_http_stream_unique_key_merge_on_write"

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
            set 'version', '1'
            set 'sql', """
                    insert into  ${db}.${tableName9} select * from http_stream("format"="csv")
                    """
            time 10000
            file 'test_http_stream_data_model.csv'
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("http_stream result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(11, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }

        qt_sql9 "select * from ${tableName9}"
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName9}"
    }

    // 10. test stream load multiple times
    def tableName10 = "test_http_stream_multiple_times"
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
                set 'version', '1'
                set 'sql', """
                    insert into  ${db}.${tableName10} select * from http_stream("format"="csv")
                    """
                time 10000
                file 'test_http_stream_multiple_times.csv'
                check { result, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    log.info("http_stream result: ${result}".toString())
                    def json = parseJson(result)
                    assertEquals("success", json.Status.toLowerCase())
                    assertEquals(500, json.NumberTotalRows)
                    assertEquals(0, json.NumberFilteredRows)
                }
            }
        }

        qt_sql10 "select count(*) from ${tableName10}"
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName10}"
    }

    // 11. test column separator
    def tableName11 = "test_http_stream_column_separator"
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
            set 'version', '1'
            set 'sql', """
                    insert into ${db}.${tableName11} (id, name) select c1, c2 from http_stream("format"="csv", "column_separator"="--")
                    """
            time 10000
            file 'test_http_stream_column_separator.csv'
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("http_stream result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(11, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }

        qt_sql11 "select id, name from ${tableName11}"
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName11}"
    }

    // 12. test line delimiter
    def tableName12 = "test_http_stream_line_delimiter"
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
            set 'version', '1'
            set 'sql', """
                    insert into ${db}.${tableName12} (id, name) select c1, c2 from http_stream("format"="csv", "line_delimiter"="||", "column_separator" = ",")
                    """
            time 10000
            file 'test_http_stream_line_delimiter.csv'
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("http_stream result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(11, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }

        qt_sql12 "select id, name from ${tableName12}"
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName12}"
    }

    // 13. test functions and aggregation operations
    def tableName13 = "test_http_stream_agg"
    try {
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName13} (
            name CHAR(10),
            agg1 string
        )
        DISTRIBUTED BY HASH(name) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        )
        """

        streamLoad {
            set 'version', '1'
            set 'sql', """
                    insert into ${db}.${tableName13} (name, agg1) select c2, GROUP_CONCAT(c1, ' ') from http_stream("format"="csv")
                    group by c2
                    """
            time 10000
            file 'test_http_stream_multiple_times.csv'
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("http_stream result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(443, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }

        qt_sql13 "select name, agg1 from ${tableName13} order by name"
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName13}"
    }

    // 14. test label
    def tableName14 = "test_http_stream_label"
    def label = UUID.randomUUID().toString().replaceAll("-", "")

    try {
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName14} (
            id int,
            name CHAR(10)
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        )
        """

        streamLoad {
            set 'version', '1'
            set 'sql', """
                    insert into ${db}.${tableName14} WITH LABEL ${label} select c1, c2 from http_stream("format"="csv", "column_separator"="--")
                    """
            time 10000
            file 'test_http_stream_column_separator.csv'
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("http_stream result: ${result}".toString())
                def json = parseJson(result)
                if (!isGroupCommitMode()) {
                    assertEquals(label, json.Label.toLowerCase())
                }
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(11, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }

        qt_sql14 "select id, name from ${tableName14} order by id"
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName14}"
    }

    // 15. test timezone
    def tableName15 = "test_http_stream_timezone"

    try {
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName15} (
            id int,
            name CHAR(10),
            cc int
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        )
        """

        streamLoad {
            set 'version', '1'
            set 'timezone', 'America/Los_Angeles'
            set 'sql', """
                    insert into ${db}.${tableName15} select c1, c2 , UNIX_TIMESTAMP('1970-01-01 08:00:00') from http_stream("format"="csv", "column_separator"="--")
                    """
            time 10000
            file 'test_http_stream_column_separator.csv'
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("http_stream result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
            }
        }

        qt_sql15 "select id, name, cc from ${tableName15} order by id"
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName15}"
    }

    try {
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName15} (
            id int,
            name CHAR(10),
            cc int
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        )
        """

        // TODO Wait until http_stream function is perfected.
        streamLoad {
            set 'version', '1'
            set 'timezone', 'Test'
            set 'sql', """
                    insert into ${db}.${tableName15} select c1, c2 , UNIX_TIMESTAMP('1970-01-01 08:00:00') from http_stream("format"="csv", "column_separator"="--")
                    """
            time 10000
            file 'test_http_stream_column_separator.csv'
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("http_stream result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
            }
            qt_sql15_1 "select id, name, cc from ${tableName15} order by id"
        }
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName15}"
    }

    // 16. test strict_mode
    def tableName16 = "test_http_stream_strict_mode"

    try {
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName16} (
            id int,
            name CHAR(10),
            tyint1 tinyint,
            decimal1 decimal(6, 3) NULL
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        )
        """

        streamLoad {
            set 'version', '1'
            set 'strict_mode', 'true'
            set 'sql', """
                    insert into ${db}.${tableName16} select c1, c2, c3, c4 from http_stream("format"="csv", "column_separator"="--")
                    """
            time 10000
            file 'test_http_stream_column_strict_mode.csv'
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("http_stream result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(11, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }

        qt_sql16 "select id, name, tyint1, decimal1 from ${tableName16} order by id"

        sql """truncate table ${tableName16}"""
        sql """sync"""

        // TODO Waiting for the http_stream strict_mode problem to be fixed
        streamLoad {
            set 'version', '1'
            set 'strict_mode', 'test'
            set 'sql', """
                    insert into ${db}.${tableName16} select c1, c2, c3, c4 from http_stream("format"="csv", "column_separator"="--")
                    """
            time 10000
            file 'test_http_stream_column_strict_mode.csv'
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("http_stream result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
            }
            qt_sql16_1 "select id, name, tyint1, decimal1 from ${tableName16} order by id"
        }
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName16}"
    }

    def tableName17 = "test_http_stream_trim_double_quotes"

    try {
        sql """
       CREATE TABLE IF NOT EXISTS ${tableName17} (
        `k1` int(11) NULL,
        `k2` VARCHAR(20) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        DISTRIBUTED BY HASH(`k1`) BUCKETS 3
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        """

        streamLoad {
            set 'version', '1'
            set 'sql', """
                    insert into ${db}.${tableName17} select c1, c2 from http_stream("format"="csv", "column_separator"="|", "trim_double_quotes"="true")
                    """
            time 10000
            file '../stream_load/trim_double_quotes.csv'
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("http_stream result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
            }
        }

        qt_sql_trim_double_quotes "select * from ${tableName17} order by k1"
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName17}"
    }

    //  test enable_profile
    def tableName18 = "test_http_stream_enable_profile"

    try {
        sql """
       CREATE TABLE IF NOT EXISTS ${tableName18} (
        `k1` int(11) NULL,
        `k2` VARCHAR(20) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        DISTRIBUTED BY HASH(`k1`) BUCKETS 3
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        """

        streamLoad {
            set 'version', '1'
            set 'enable_profile','true'
            set 'sql', """
                    insert into ${db}.${tableName18} select c1, c2 from http_stream("format"="csv", "column_separator"="|", "trim_double_quotes"="true")
                    """
            time 10000
            file '../stream_load/trim_double_quotes.csv'
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("http_stream result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
            }
        }

        qt_sql18 "select * from ${tableName18} order by k1"
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName18}"
    }

    //  test load hll type
    def tableName19 = "test_http_stream_hll_type"

    try {
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName19} (
            type_id int,
            type_name varchar(10),
            pv_hash hll hll_union not null,
            pv_base64 hll hll_union not null
        )
        AGGREGATE KEY(type_id,type_name)
        DISTRIBUTED BY HASH(type_id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        )
        """

        streamLoad {
            set 'version', '1'
            set 'sql', """
                    insert into ${db}.${tableName19} select c1,c2,hll_hash(c1),hll_from_base64(c3) from http_stream("format"="csv", "column_separator"=",")
                    """
            time 10000
            file '../stream_load/test_stream_load_hll_type.csv'
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("http_stream result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
            }
        }

        qt_sql19 "select type_name, hll_union_agg(pv_hash), hll_union_agg(pv_base64) from ${tableName19} group by type_name  order by type_name"
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName19}"
    }

}

