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

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths

suite("test_outfile_csv_with_names") {
    def dbName = "test_outfile_csv_with_names"
    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql "USE $dbName"
    def tableName = "outfil_csv_with_names_test"
    def tableName2 = "outfil_csv_with_names_test2"
    def helper = new ExportTestHelper(delegate)
    def prefix = "regression/test_outfile_csv_with_names/${UUID.randomUUID()}/"
    def downloadDir = new File(System.getProperty("java.io.tmpdir"), "doris-outfile-${UUID.randomUUID()}")

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
            `decimal_col` decimal COMMENT "",
            `json_col` json COMMENT "",
            `ipv4_col` ipv4 COMMENT "",
            `ipv6_col` ipv6 COMMENT ""
            )
            DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
        """
        StringBuilder sb = new StringBuilder()
        int i = 1
        for (; i < 10; i ++) {
            sb.append("""
                (${i}, '2017-10-01', '2017-10-01 00:00:00', '2017-10-01', '2017-10-01 00:00:00.111111', '2017-10-01 00:00:00.111111', '2017-10-01 00:00:00.111111', 'Beijing', ${i}, ${i % 128}, true, ${i}, ${i}, ${i}, ${i}.${i}, ${i}.${i}, 'char${i}', ${i}, '{"a": ${i}, "b": "str${i}"}', '0.0.0.${i}', '::${i}'),
            """)
        }
        sb.append("""
                (${i}, '2017-10-01', '2017-10-01 00:00:00', '2017-10-01', '2017-10-01 00:00:00.111111', '2017-10-01 00:00:00.111111', '2017-10-01 00:00:00.111111', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
            """)
        sql """ INSERT INTO ${tableName} VALUES
             ${sb.toString()}
            """
        order_qt_select_default1 """ SELECT * FROM ${tableName} t ORDER BY user_id; """

        // check outfile
        sql """
            SELECT * FROM ${tableName} t ORDER BY user_id INTO OUTFILE "${helper.uri(prefix)}" FORMAT AS CSV_WITH_NAMES
            PROPERTIES (${helper.storageProperties()}, "column_separator" = "|");
        """

        def files = helper.downloadFiles(prefix, downloadDir)
        assert files.size() == 1
        // check column names
        String columnNames = """user_id|date|datetime|date_1|datetime_1|datetime_2|datetime_3|city|age|sex|bool_col|int_col|bigint_col|largeint_col|float_col|double_col|char_col|decimal_col|json_col|ipv4_col|ipv6_col"""

        List<String> outLines = Files.readAllLines(Paths.get(files[0].getAbsolutePath()), StandardCharsets.UTF_8);
        assertEquals(columnNames, outLines.get(0))

        // check data correctness
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
            `decimal_col` decimal COMMENT "",
            `json_col` json COMMENT "",
            `ipv4_col` ipv4 COMMENT "",
            `ipv6_col` ipv6 COMMENT ""
            )
            DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
        """

        streamLoad {
            db dbName
            table tableName2
            set "format", "csv_with_names"
            set "column_separator", "|"
            file files[0].absolutePath
        }
        order_qt_select_default2 """ SELECT * FROM ${tableName2} t ORDER BY user_id; """
    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
        try_sql("DROP TABLE IF EXISTS ${tableName2}")
        downloadDir.deleteDir()
        helper.deleteFiles(prefix)
    }
}

