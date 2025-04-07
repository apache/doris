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

import java.math.BigDecimal;

suite("test_prepared_stmt_metadata", "nonConcurrent") {
    def tableName = "tbl_prepared_stmt_metadata"
    def user = context.config.jdbcUser
    def password = context.config.jdbcPassword
    // def url = context.config.jdbcUrl + "&useServerPrepStmts=true"
    String url = getServerPrepareJdbcUrl(context.config.jdbcUrl, "regression_test_prepared_stmt_p0")
    sql """set global max_prepared_stmt_count = 1024"""
    def result1 = connect(user, password, url) {
        sql """DROP TABLE IF EXISTS ${tableName} """
        sql """
             CREATE TABLE IF NOT EXISTS ${tableName} (
                     `k1` tinyint NULL COMMENT "",
                     `k2` smallint NULL COMMENT "",
                     `k3` int NULL COMMENT "",
                     `k4` bigint NULL COMMENT "",
                     `k5` decimalv3(27, 9) NULL COMMENT "",
                     `k6` varchar(30) NULL COMMENT "",
                     `k7` date NULL COMMENT "",
                     `k8` datetime NULL COMMENT "",
                     `k9` float NULL COMMENT "",
                     `k10` datetimev2 NULL COMMENT "",
                     `k11` datev2 NULL COMMENT ""
                   ) ENGINE=OLAP
                   DUPLICATE KEY(`k1`, `k2`, `k3`)
                   DISTRIBUTED BY HASH(`k1`, `k2`, `k3`) BUCKETS 1
                   PROPERTIES (
                   "replication_allocation" = "tag.location.default: 1",
                   "light_schema_change" = "true",
                   "storage_format" = "V2"
                   )
               """

        sql """ INSERT INTO ${tableName} VALUES(1, 1300, 55356821, 15982329875, 119291.11, "abcd", null, "2020-01-01 12:36:38", null, "1022-01-01 11:30:38", "1022-01-01 11:30:38") """
        sql """ INSERT INTO ${tableName} VALUES(2, 1301, 56052706, 14285329801, 12222.99121135, "doris", "2023-01-02", "2021-01-01 12:36:38", 522.762, "2022-01-01 11:30:38", "2022-01-01 11:30:38") """
        sql """ INSERT INTO ${tableName} VALUES(3, 1302, 55702967, 17754445280, 1.392932911, "superman", "2024-01-02", "2022-01-01 12:36:38", 52.862, "3022-01-01 11:30:38", "3022-01-01 11:30:38") """
        sql """ INSERT INTO ${tableName} VALUES(4, 1303, 56054326, 15669391193, 12919291.129191137, "xxabcd", "2025-01-02", "2023-01-01 12:36:38", 552.872, "4022-01-01 11:30:38", "4022-01-01 11:30:38") """
        sql """ INSERT INTO ${tableName} VALUES(5, 1304, 36548425, 15229335116, 991129292901.11138, "dd", "2120-01-02", "2024-01-01 12:36:38", 652.692, "5022-01-01 11:30:38", "5022-01-01 11:30:38") """
        sql """ INSERT INTO ${tableName} VALUES(6, 1305, 56054803, 18031831909, 100320.11139, "haha abcd", "2220-01-02", "2025-01-01 12:36:38", 2.7692, "6022-01-01 11:30:38", "6022-01-01 11:30:38") """
        sql """ INSERT INTO ${tableName} VALUES(7, 1306, 56055112, 13777918563, 120939.11130, "a    abcd", "2030-01-02", "2026-01-01 12:36:38", 22.822, "7022-01-01 11:30:38", "7022-01-01 11:30:38") """
        sql """sync"""

        String[] columnNames = ["k1", "k2", "k3", "k4", "k5", "k6", "k7", "k8", "k9", "k10", "k11"]
        String[] columnTypeNames = ["TINYINT", "SMALLINT", "INT", "BIGINT", "DECIMAL", "CHAR", "DATE", "DATETIME", "FLOAT", "DATETIME", "DATE"]
        qt_sql """select * from  ${tableName} order by 1, 2, 3"""

        def stmt_read = prepareStatement "select * from ${tableName} WHERE `k1` IN (?, ?, ?, ?, ?) order by 1"
        def md = stmt_read.getMetaData()
        int columnCount = md.getColumnCount()
        assertEquals(11, columnCount)
        for (int i = 1; i <= columnCount; i++) {
            String columnName = md.getColumnName(i)
            String columnTypeName = md.getColumnTypeName(i)
            assertEquals(columnNames[i-1], columnName)
            assertEquals(columnTypeNames[i-1], columnTypeName)
        }
        stmt_read.setByte(1, (byte) 1)
        stmt_read.setByte(2, (byte) 2)
        stmt_read.setByte(3, (byte) 3)
        stmt_read.setByte(4, (byte) 4)
        stmt_read.setByte(5, (byte) 5)
        qe_stmt_read1_1 stmt_read

        stmt_read = prepareStatement "select k2, k3 from ${tableName} WHERE `k1` = ?"
        md = stmt_read.getMetaData()
        columnCount = md.getColumnCount()
        assertEquals(2, columnCount)
        for (int i = 1; i <= columnCount; i++) {
            String columnName = md.getColumnName(i)
            String columnTypeName = md.getColumnTypeName(i)
            assertEquals(columnNames[i], columnName)
            assertEquals(columnTypeNames[i], columnTypeName)
        }
        stmt_read.setByte(1, (byte) 2)
        qe_stmt_read1_2 stmt_read

        //        stmt_read = prepareStatement "select * from tbl_prepared_stmt_metadata WHERE `k1` > 0 order by k1 limit ?"
        //        md = stmt_read.getMetaData()
        //        columnCount = md.getColumnCount()
        //        assertEquals(11, columnCount)
        //        for (int i = 1; i <= columnCount; i++) {
        //            String columnName = md.getColumnName(i)
        //            String columnTypeName = md.getColumnTypeName(i)
        //            assertEquals(columnNames[i-1], columnName)
        //            assertEquals(columnTypeNames[i-1], columnTypeName)
        //        }
        //        stmt_read.setInt(1, 3)
        //        qe_stmt_read1_3 stmt_read

        columnNames = ["job_id", "catalog_name", "db_name", "tbl_name", "col_name", "job_type", "analysis_type", "message", "last_exec_time_in_ms", "state", "progress", "schedule_type", "start_time", "end_time", "priority", "enable_partition"]
        columnTypeNames = ["CHAR", "TINYTEXT", "TINYTEXT", "TINYTEXT", "TINYTEXT", "CHAR", "CHAR", "TINYTEXT", "TINYTEXT", "CHAR", "TINYTEXT", "TINYTEXT", "TINYTEXT", "TINYTEXT", "TINYTEXT", "CHAR"]
        stmt_read = prepareStatement "show analyze"
        md = stmt_read.getMetaData()
        columnCount = md.getColumnCount()
        assertEquals(16, columnCount)
        for (int i = 1; i <= columnCount; i++) {
            String columnName = md.getColumnName(i)
            String columnTypeName = md.getColumnTypeName(i)
            logger.info("column meta " + i + " " + columnName + " " + columnTypeName)
            assertEquals(columnNames[i-1], columnName)
            assertEquals(columnTypeNames[i-1], columnTypeName)
        }
    }
}
