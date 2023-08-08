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

suite("test_stream_load_with_sql", "p0") {

    // csv desc
    // | c1  | c2   | c3     | c4      | c5      | c6       | c7     | c8       |
    // | int | char | varchar| boolean | tinyint | smallint | bigint | largeint |
    // | c9    | c10    | c11     | c12       | c13  | c14    | c15      | c16        |
    // | float | double | decimal | decimalv3 | date | datev2 | datetime | datetimev2 |

    // 1. test column with currenttimestamp default value
    def tableName1 = "test_stream_load_with_sql_current_timestamp"
    def db = "regression_test_load_p0_stream_load_with_sql"
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

        // streamLoad {
        //     set 'version', '1'
        //     set 'sql', """
        //             insert into ${db}.${tableName1} (id, name) select c1, c2 from stream("format"="csv")
        //             """
        //     time 10000
        //     file 'test_stream_load_with_sql.csv'
        //     check { result, exception, startTime, endTime ->
        //         if (exception != null) {
        //             throw exception
        //         }
        //         log.info("Stream load result: ${result}".toString())
        //         def json = parseJson(result)
        //         assertEquals("success", json.Status.toLowerCase())
        //         assertEquals(11, json.NumberTotalRows)
        //         assertEquals(0, json.NumberFilteredRows)
        //     }
        // }

        // qt_sql1 "select id, name from ${tableName1}"
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName1}"
    }

    // 2. test change column order
    def tableName2 = "test_stream_load_with_sql_change_column_order"

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

        // streamLoad {
        //     set 'version', '1'
        //     set 'sql', """
        //             insert into ${db}.${tableName2} select c1, c6, c2, c7, c11, c9 from stream("format"="csv")
        //             """
        //     time 10000
        //     file 'test_stream_load_with_sql.csv'
        //     check { result, exception, startTime, endTime ->
        //         if (exception != null) {
        //             throw exception
        //         }
        //         log.info("Stream load result: ${result}".toString())
        //         def json = parseJson(result)
        //         assertEquals("success", json.Status.toLowerCase())
        //         assertEquals(11, json.NumberTotalRows)
        //         assertEquals(0, json.NumberFilteredRows)
        //     }
        // }

        // qt_sql2 "select * from ${tableName2}"
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName2}"
    }

    // 3. test with function
    def tableName3 = "test_stream_load_with_sql_function"

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

        // streamLoad {
        //     set 'version', '1'
        //     set 'sql', """
        //             insert into ${db}.${tableName3} select c1, c2, year(c14), month(c14), day(c14) from stream("format"="csv")
        //             """
        //     time 10000
        //     file 'test_stream_load_with_sql.csv'
        //     check { result, exception, startTime, endTime ->
        //         if (exception != null) {
        //             throw exception
        //         }
        //         log.info("Stream load result: ${result}".toString())
        //         def json = parseJson(result)
        //         assertEquals("success", json.Status.toLowerCase())
        //         assertEquals(11, json.NumberTotalRows)
        //         assertEquals(0, json.NumberFilteredRows)
        //     }
        // }

        // qt_sql3 "select * from ${tableName3}"
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName3}"
    }

    // 4. test column number mismatch
    def tableName4 = "test_stream_load_with_sql_column_number_mismatch"

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

        // streamLoad {
        //     set 'version', '1'
        //     set 'sql', """
        //             insert into  ${db}.${tableName4} select c1, c2, c6, c3 from stream("format"="csv")
        //             """
        //     time 10000
        //     file 'test_stream_load_with_sql.csv'
        //     check { result, exception, startTime, endTime ->
        //         if (exception != null) {
        //             throw exception
        //         }
        //         log.info("Stream load result: ${result}".toString())
        //         def json = parseJson(result)
        //         assertEquals("fail", json.Status.toLowerCase())
        //     }
        // }
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("Distribution column(id) doesn't exist"), e.getMessage())
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName4}"
    }

    // 5. test with default value
    def tableName5 = "test_stream_load_with_sql_default_value"

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

        // streamLoad {
        //     set 'version', '1'
        //     set 'sql', """
        //             insert into  ${db}.${tableName5} (id, name, date) select c1, c2, c13 from stream("format"="csv")
        //             """
        //     time 10000
        //     file 'test_stream_load_with_sql.csv'
        //     check { result, exception, startTime, endTime ->
        //         if (exception != null) {
        //             throw exception
        //         }
        //         log.info("Stream load result: ${result}".toString())
        //         def json = parseJson(result)
        //         assertEquals("success", json.Status.toLowerCase())
        //         assertEquals(11, json.NumberTotalRows)
        //         assertEquals(0, json.NumberFilteredRows)
        //     }
        // }

        // qt_sql5 "select * from ${tableName5}"
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName5}"
    }

    // 6. test some column type
    def tableName6 = "test_stream_load_with_sql_column_type"

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

        // streamLoad {
        //     set 'version', '1'
        //     set 'sql', """
        //             insert into  ${db}.${tableName6} select * from stream("format"="csv")
        //             """
        //     time 10000
        //     file 'test_stream_load_with_sql.csv'
        //     check { result, exception, startTime, endTime ->
        //         if (exception != null) {
        //             throw exception
        //         }
        //         log.info("Stream load result: ${result}".toString())
        //         def json = parseJson(result)
        //         assertEquals("success", json.Status.toLowerCase())
        //         assertEquals(11, json.NumberTotalRows)
        //         assertEquals(0, json.NumberFilteredRows)
        //     }
        // }

        // qt_sql6 "select * from ${tableName6}"
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName6}"
    }

    // 7. test duplicate key
    def tableName7 = "test_stream_load_with_sql_duplicate_key"

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

        // streamLoad {
        //     set 'version', '1'
        //     set 'sql', """
        //             insert into  ${db}.${tableName7} select * from stream("format"="csv")
        //             """
        //     time 10000
        //     file 'test_stream_load_with_sql_data_model.csv'
        //     check { result, exception, startTime, endTime ->
        //         if (exception != null) {
        //             throw exception
        //         }
        //         log.info("Stream load result: ${result}".toString())
        //         def json = parseJson(result)
        //         assertEquals("success", json.Status.toLowerCase())
        //         assertEquals(11, json.NumberTotalRows)
        //         assertEquals(0, json.NumberFilteredRows)
        //     }
        // }

        // qt_sql7 "select * from ${tableName7}"
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName7}"
    }

    // 8. test merge on read unique key
    def tableName8 = "test_stream_load_with_sql_unique_key_merge_on_read"

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

        // streamLoad {
        //     set 'version', '1'
        //     set 'sql', """
        //             insert into  ${db}.${tableName8} select * from stream("format"="csv")
        //             """
        //     time 10000
        //     file 'test_stream_load_with_sql_data_model.csv'
        //     check { result, exception, startTime, endTime ->
        //         if (exception != null) {
        //             throw exception
        //         }
        //         log.info("Stream load result: ${result}".toString())
        //         def json = parseJson(result)
        //         assertEquals("success", json.Status.toLowerCase())
        //         assertEquals(11, json.NumberTotalRows)
        //         assertEquals(0, json.NumberFilteredRows)
        //     }
        // }

        // qt_sql8 "select * from ${tableName8}"
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName8}"
    }

    // 9. test merge on write unique key
    def tableName9 = "test_stream_load_with_sql_unique_key_merge_on_write"

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

        // streamLoad {
        //     set 'version', '1'
        //     set 'sql', """
        //             insert into  ${db}.${tableName9} select * from stream("format"="csv")
        //             """
        //     time 10000
        //     file 'test_stream_load_with_sql_data_model.csv'
        //     check { result, exception, startTime, endTime ->
        //         if (exception != null) {
        //             throw exception
        //         }
        //         log.info("Stream load result: ${result}".toString())
        //         def json = parseJson(result)
        //         assertEquals("success", json.Status.toLowerCase())
        //         assertEquals(11, json.NumberTotalRows)
        //         assertEquals(0, json.NumberFilteredRows)
        //     }
        // }

        // qt_sql9 "select * from ${tableName9}"
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName9}"
    }

    // 10. test stream load multiple times
    def tableName10 = "test_stream_load_with_sql_multiple_times"
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
        // for (int i = 0; i < 3; ++i) {
        //     streamLoad {
        //         set 'version', '1'
        //         set 'sql', """
        //             insert into  ${db}.${tableName10} select * from stream("format"="csv")
        //             """
        //         time 10000
        //         file 'test_stream_load_with_sql_multiple_times.csv'
        //         check { result, exception, startTime, endTime ->
        //             if (exception != null) {
        //                 throw exception
        //             }
        //             log.info("Stream load result: ${result}".toString())
        //             def json = parseJson(result)
        //             assertEquals("success", json.Status.toLowerCase())
        //             assertEquals(500, json.NumberTotalRows)
        //             assertEquals(0, json.NumberFilteredRows)
        //         }
        //     }
        // }

        // qt_sql10 "select count(*) from ${tableName10}"
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName10}"
    }

    // 11. test column separator 
    def tableName11 = "test_stream_load_with_sql_column_separator"
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

        // streamLoad {
        //     set 'version', '1'
        //     set 'sql', """
        //             insert into ${db}.${tableName11} (id, name) select c1, c2 from stream("format"="csv", "column_separator"="--")
        //             """
        //     time 10000
        //     file 'test_stream_load_with_sql_column_separator.csv'
        //     check { result, exception, startTime, endTime ->
        //         if (exception != null) {
        //             throw exception
        //         }
        //         log.info("Stream load result: ${result}".toString())
        //         def json = parseJson(result)
        //         assertEquals("success", json.Status.toLowerCase())
        //         assertEquals(11, json.NumberTotalRows)
        //         assertEquals(0, json.NumberFilteredRows)
        //     }
        // }

        // qt_sql11 "select id, name from ${tableName11}"
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName11}"
    }

    // 12. test line delimiter 
    def tableName12 = "test_stream_load_with_sql_line_delimiter"
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

        // streamLoad {
        //     set 'version', '1'
        //     set 'sql', """
        //             insert into ${db}.${tableName12} (id, name) select c1, c2 from stream("format"="csv", "line_delimiter"="||")
        //             """
        //     time 10000
        //     file 'test_stream_load_with_sql_line_delimiter.csv'
        //     check { result, exception, startTime, endTime ->
        //         if (exception != null) {
        //             throw exception
        //         }
        //         log.info("Stream load result: ${result}".toString())
        //         def json = parseJson(result)
        //         assertEquals("success", json.Status.toLowerCase())
        //         assertEquals(11, json.NumberTotalRows)
        //         assertEquals(0, json.NumberFilteredRows)
        //     }
        // }

        // qt_sql12 "select id, name from ${tableName12}"
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName12}"
    }

    // 13. test parquet orc case
    def tableName13 = "test_parquet_orc_case"
    try {
        sql """ DROP TABLE IF EXISTS ${tableName13} """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName13} (
                `WatchId` char(128), 
                `JavaEnable` smallint, 
                `Title` string, 
                `GoodEvent` smallint, 
                `EventTime` datetime, 
                `EventDate` date, 
                `CounterId` bigint, 
                `ClientIp` bigint, 
                `ClientIp6` char(50), 
                `RegionId` bigint, 
                `UserId` string, 
                `CounterClass` tinyint, 
                `Os` smallint, 
                `UserAgent` smallint, 
                `Url` string, 
                `Referer` string, 
                `Urldomain` string, 
                `RefererDomain` string, 
                `Refresh` smallint, 
                `IsRobot` smallint, 
                `RefererCategories` string, 
                `UrlCategories` string, 
                `UrlRegions` string, 
                `RefererRegions` string, 
                `ResolutionWidth` int, 
                `ResolutionHeight` int, 
                `ResolutionDepth` smallint, 
                `FlashMajor` smallint, 
                `FlashMinor` smallint, 
                `FlashMinor2` string, 
                `NetMajor` smallint, 
                `NetMinor` smallint, 
                `UserAgentMajor` int, 
                `UserAgentMinor` char(4), 
                `CookieEnable` smallint, 
                `JavascriptEnable` smallint, 
                `IsMobile` smallint, 
                `MobilePhone` smallint, 
                `MobilePhoneModel` string, 
                `Params` string, 
                `IpNetworkId` bigint, 
                `TraficSourceId` tinyint, 
                `SearchEngineId` int, 
                `SearchPhrase` string, 
                `AdvEngineId` smallint, 
                `IsArtifical` smallint, 
                `WindowClientWidth` int, 
                `WindowClientHeight` int, 
                `ClientTimeZone` smallint, 
                `ClientEventTime` datetime, 
                `SilverLightVersion1` smallint, 
                `SilverlightVersion2` smallint, 
                `SilverlightVersion3` bigint, 
                `SilverlightVersion4` int, 
                `PageCharset` string, 
                `CodeVersion` bigint, 
                `IsLink` smallint, 
                `IsDownload` smallint, 
                `IsNotBounce` smallint, 
                `FUniqId` string, 
                `Hid` bigint, 
                `IsOldCounter` smallint, 
                `IsEvent` smallint, 
                `IsParameter` smallint, 
                `DontCountHits` smallint, 
                `WithHash` smallint, 
                `HitColor` char(2), 
                `UtcEventTime` datetime, 
                `Age` smallint, 
                `Sex` smallint, 
                `Income` smallint, 
                `Interests` int, 
                `Robotness` smallint, 
                `GeneralInterests` string, 
                `RemoteIp` bigint, 
                `RemoteIp6` char(50), 
                `WindowName` int, 
                `OpenerName` int, 
                `historylength` smallint, 
                `BrowserLanguage` char(4), 
                `BrowserCountry` char(4), 
                `SocialNetwork` string, 
                `SocialAction` string, 
                `HttpError` int, 
                `SendTiming` int, 
                `DnsTiming` int, 
                `ConnectTiming` int, 
                `ResponseStartTiming` int, 
                `ResponseEndTiming` int, 
                `FetchTiming` int, 
                `RedirectTiming` int, 
                `DomInteractiveTiming` int, 
                `DomContentLoadedTiming` int, 
                `DomCompleteTiming` int, 
                `LoadEventStartTiming` int, 
                `LoadEventEndTiming` int, 
                `NsToDomContentLoadedTiming` int, 
                `FirstPaintTiming` int, 
                `RedirectCount` tinyint, 
                `SocialSourceNetworkId` smallint, 
                `SocialSourcePage` string, 
                `ParamPrice` bigint, 
                `ParamOrderId` string, 
                `ParamCurrency` char(6), 
                `ParamCurrencyId` int, 
                `GoalsReached` string, 
                `OpenStatServiceName` string, 
                `OpenStatCampaignId` string, 
                `OpenStatAdId` string, 
                `OpenStatSourceId` string, 
                `UtmSource` string, 
                `UtmMedium` string, 
                `UtmCampaign` string, 
                `UtmContent` string, 
                `UtmTerm` string, 
                `FromTag` string, 
                `HasGclId` smallint, 
                `RefererHash` string, 
                `UrlHash` string, 
                `ClId` bigint, 
                `YclId` string, 
                `ShareService` string, 
                `ShareUrl` string, 
                `ShareTitle` string, 
                `ParsedParamsKey1` string, 
                `ParsedParamsKey2` string, 
                `ParsedParamsKey3` string, 
                `ParsedParamsKey4` string, 
                `ParsedParamsKey5` string, 
                `ParsedParamsValueDouble` double, 
                `IsLandId` char(40), 
                `RequestNum` bigint, 
                `RequestTry` smallint
            ) ENGINE=OLAP
            DUPLICATE KEY(`WatchId`, `JavaEnable`)
            DISTRIBUTED BY HASH(`WatchId`, `JavaEnable`) BUCKETS 3
            PROPERTIES ("replication_num" = "1");
        """

        // streamLoad {
        //     set 'version', '1'
        //     set 'sql', """
        //             insert into ${db}.${tableName13} select * from stream("format"="parquet")
        //             """
        //     time 10000
        //     set 'format', 'parquet'
        //     file 'test_stream_load_with_sql_parquet_case.parquet'
        //     check { result, exception, startTime, endTime ->
        //         if (exception != null) {
        //             throw exception
        //         }
        //         log.info("Stream load result: ${result}".toString())
        //         def json = parseJson(result)
        //         assertEquals("success", json.Status.toLowerCase())
        //     }
        // }
        // qt_sql13 "select * from ${tableName13} order by WatchId"
        sql """truncate table ${tableName13}"""

        // streamLoad {
        //     set 'version', '1'
        //     set 'sql', """
        //             insert into ${db}.${tableName13} select * from stream("format"="parquet")
        //             """
        //     time 10000
        //     set 'format', 'parquet'
        //     file 'test_stream_load_with_sql_parquet_case.parquet'
        //     check { result, exception, startTime, endTime ->
        //         if (exception != null) {
        //             throw exception
        //         }
        //         log.info("Stream load result: ${result}".toString())
        //         def json = parseJson(result)
        //         assertEquals("success", json.Status.toLowerCase())
        //     }
        // }
        // qt_sql13 "select * from ${tableName13} order by WatchId"
        sql """truncate table ${tableName13}"""

        // streamLoad {
        //     set 'version', '1'
        //     set 'sql', """
        //             insert into ${db}.${tableName13} select * from stream("format"="parquet")
        //             """
        //     time 10000
        //     set 'format', 'parquet'
        //     file 'test_stream_load_with_sql_parquet_case.parquet'
        //     check { result, exception, startTime, endTime ->
        //         if (exception != null) {
        //             throw exception
        //         }
        //         log.info("Stream load result: ${result}".toString())
        //         def json = parseJson(result)
        //         assertEquals("success", json.Status.toLowerCase())
        //     }
        // }
        // qt_sql13 "select * from ${tableName13} order by WatchId"
        sql """truncate table ${tableName13}"""

        // streamLoad {
        //     set 'version', '1'
        //     set 'sql', """
        //             insert into ${db}.${tableName13} select * from stream("format"="orc")
        //             """
        //     time 10000
        //     set 'format', 'orc'
        //     file 'test_stream_load_with_sql_orc_case.orc'
        //     check { result, exception, startTime, endTime ->
        //         if (exception != null) {
        //             throw exception
        //         }
        //         log.info("Stream load result: ${result}".toString())
        //         def json = parseJson(result)
        //         assertEquals("success", json.Status.toLowerCase())
        //     }
        // }
        // qt_sql13 "select * from ${tableName13} order by WatchId"
        sql """truncate table ${tableName13}"""

    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName13}"
    }
}

