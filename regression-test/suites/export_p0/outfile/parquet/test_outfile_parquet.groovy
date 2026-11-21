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

import org.apache.doris.regression.util.ExportTestHelper

import java.nio.file.Files

suite("test_outfile_parquet") {
    // open nereids
    sql """ set enable_nereids_planner=true """
    sql """ set enable_fallback_to_original_planner=false """

    def dbName = "test_outfile_parquet"
    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql "USE $dbName"
    def tableName = "outfile_parquet_test"
    def tableName2 = "outfile_parquet_test2"
    def helper = new ExportTestHelper(delegate)
    def prefix = "regression/test_outfile_parquet/${UUID.randomUUID()}/"
    def downloadDir = new File(System.getProperty("java.io.tmpdir"), "doris-outfile-${UUID.randomUUID()}")
    def negativePrefix = "${prefix}negative_timestamp/"
    try {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `user_id` INT NOT NULL COMMENT "用户id",
            `date` DATE NOT NULL COMMENT "数据灌入日期时间",
            `datetime` DATETIME NOT NULL COMMENT "数据灌入日期时间",
            `date_1` DATEV2 NOT NULL COMMENT "",
            `datetime_1` DATETIMEV2 NOT NULL COMMENT "",
            `datetime_2` DATETIMEV2(3) NOT NULL COMMENT "",
            `datetime_3` DATETIMEV2(6) NOT NULL COMMENT "",
            `city` VARCHAR(20) COMMENT "用户所在城市",
            `age` SMALLINT COMMENT "用户年龄",
            `sex` TINYINT COMMENT "用户性别",
            `bool_col` boolean COMMENT "",
            `int_col` int COMMENT "",
            `bigint_col` bigint COMMENT "",
            `largeint_col` int COMMENT "",
            `float_col` float COMMENT "",
            `double_col` double COMMENT "",
            `char_col` CHAR(10) COMMENT "",
            `decimal_col` decimal COMMENT ""
            )
            DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
        """
        StringBuilder sb = new StringBuilder()
        int i = 1
        for (; i < 10; i ++) {
            sb.append("""
                (${i}, '2017-10-01', '2017-10-01 00:00:00', '2017-10-01', '2017-10-01 00:00:00.111111', '2017-10-01 00:00:00.111111', '2017-10-01 00:00:00.111111', 'Beijing', ${i}, ${i % 128}, true, ${i}, ${i}, ${i}, ${i}.${i}, ${i}.${i}, 'char${i}', ${i}),
            """)
        }
        sb.append("""
                (${i}, '2017-10-01', '2017-10-01 00:00:00', '2017-10-01', '2017-10-01 00:00:00.111111', '2017-10-01 00:00:00.111111', '2017-10-01 00:00:00.111111', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
            """)
        sql """ INSERT INTO ${tableName} VALUES
             ${sb.toString()}
            """
        qt_select_default """ SELECT * FROM ${tableName} t ORDER BY user_id; """

        // check outfile
        sql """
            SELECT * FROM ${tableName} t ORDER BY user_id
            INTO OUTFILE "${helper.uri(prefix)}"
            FORMAT AS PARQUET
            PROPERTIES (${helper.storageProperties()});
        """

        def files = helper.downloadFiles(prefix, downloadDir)
        assert files.size() == 1

        sql """ DROP TABLE IF EXISTS ${tableName2} """
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName2} (
            `user_id` INT NOT NULL COMMENT "用户id",
            `date` DATE NOT NULL COMMENT "数据灌入日期时间",
            `datetime` DATETIME NOT NULL COMMENT "数据灌入日期时间",
            `date_1` DATEV2 NOT NULL COMMENT "",
            `datetime_1` DATETIMEV2 NOT NULL COMMENT "",
            `datetime_2` DATETIMEV2(3) NOT NULL COMMENT "",
            `datetime_3` DATETIMEV2(6) NOT NULL COMMENT "",
            `city` VARCHAR(20) COMMENT "用户所在城市",
            `age` SMALLINT COMMENT "用户年龄",
            `sex` TINYINT COMMENT "用户性别",
            `bool_col` boolean COMMENT "",
            `int_col` int COMMENT "",
            `bigint_col` bigint COMMENT "",
            `largeint_col` int COMMENT "",
            `float_col` float COMMENT "",
            `double_col` double COMMENT "",
            `char_col` CHAR(10) COMMENT "",
            `decimal_col` decimal COMMENT ""
            )
            DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
        """

        streamLoad {
            db dbName
            table tableName2
            set "format", "parquet"
            file files[0].absolutePath
        }
        qt_select_default """ SELECT * FROM ${tableName2} t ORDER BY user_id; """

        sql """ DROP TABLE IF EXISTS outfile_parquet_negative_timestamp """
        sql """
            CREATE TABLE outfile_parquet_negative_timestamp (
                `id` INT NOT NULL,
                `ts` DATETIMEV2(6) NOT NULL
            )
            DUPLICATE KEY (`id`, `ts`)
            PARTITION BY RANGE (`ts`) (
                PARTITION p_pre_epoch VALUES LESS THAN ("1970-01-01 00:00:00")
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES("replication_num" = "1")
        """

        sql """
            SELECT id, ts
            FROM (
                SELECT 1 AS id, CAST('1900-01-01 00:00:00.123456' AS DATETIMEV2(6)) AS ts
                UNION ALL
                SELECT 2 AS id, CAST('1969-12-31 23:59:58.500000' AS DATETIMEV2(6)) AS ts
                UNION ALL
                SELECT 3 AS id, CAST('1969-12-31 23:59:59.500000' AS DATETIMEV2(6)) AS ts
                UNION ALL
                SELECT 4 AS id, CAST('1969-12-31 23:59:59.999000' AS DATETIMEV2(6)) AS ts
                UNION ALL
                SELECT 5 AS id, CAST('1969-12-31 23:59:59.999999' AS DATETIMEV2(6)) AS ts
            ) t
            ORDER BY id
            INTO OUTFILE "${helper.uri(negativePrefix)}"
            FORMAT AS PARQUET
            PROPERTIES (${helper.storageProperties()}, "enable_int96_timestamps" = "false")
        """

        def negativeTimestampFiles = helper.downloadFiles(negativePrefix, downloadDir)
        assert negativeTimestampFiles.size() == 1
        streamLoad {
            db dbName
            table "outfile_parquet_negative_timestamp"
            set "format", "parquet"
            file negativeTimestampFiles[0].absolutePath
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                def loadResponse = parseJson(result)
                assertEquals("Success", loadResponse.Status)
                assertEquals(5, loadResponse.NumberTotalRows)
                assertEquals(5, loadResponse.NumberLoadedRows)
                assertEquals(0, loadResponse.NumberFilteredRows)
            }
        }
        order_qt_negative_timestamp """
            SELECT id, ts
            FROM outfile_parquet_negative_timestamp
            ORDER BY id
        """
    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
        try_sql("DROP TABLE IF EXISTS ${tableName2}")
        downloadDir.deleteDir()
        helper.deleteFiles(prefix)
    }
}
