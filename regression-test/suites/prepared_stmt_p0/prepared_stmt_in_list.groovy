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

suite("test_prepared_stmt_in_list", "nonConcurrent") {
    def tableName = "tbl_prepared_stmt_in_list"
    def user = context.config.jdbcUser
    def password = context.config.jdbcPassword
    def url = context.config.jdbcUrl + "&useServerPrepStmts=true"
    sql """set global max_prepared_stmt_count = 1024"""
    def result1 = connect(user=user, password=password, url=url) {
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

        qt_sql """select * from  ${tableName} order by 1, 2, 3"""

        java.text.SimpleDateFormat formater = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        def stmt_read1 = prepareStatement "select * from ${tableName} WHERE `k1` IN (?, ?, ?, ?, ?) order by 1"
        stmt_read1.setByte(1, (byte) 1)
        stmt_read1.setByte(2, (byte) 2)
        stmt_read1.setByte(3, (byte) 3)
        stmt_read1.setByte(4, (byte) 4)
        stmt_read1.setByte(5, (byte) 5)
        qe_stmt_read1_1 stmt_read1

        stmt_read1.setString(1, '1')
        stmt_read1.setString(2, '2')
        stmt_read1.setString(3, '3')
        stmt_read1.setString(4, '4')
        stmt_read1.setString(5, '5')
        qe_stmt_read1_2 stmt_read1

        def stmt_read2 = prepareStatement "select * from ${tableName} WHERE `k2` IN (?, ?, ?, ?, ?) order by 1"
        stmt_read2.setShort(1, (short) 1300)
        stmt_read2.setShort(2, (short) 1301)
        stmt_read2.setShort(3, (short) 1302)
        stmt_read2.setShort(4, (short) 1303)
        stmt_read2.setShort(5, (short) 1304)
        qe_stmt_read2_1 stmt_read2

        stmt_read2.setString(1, '1300')
        stmt_read2.setString(2, '1301')
        stmt_read2.setString(3, '1302')
        stmt_read2.setString(4, '1303')
        stmt_read2.setString(5, '1304')
        qe_stmt_read2_2 stmt_read2

        def stmt_read3 = prepareStatement "select * from ${tableName} WHERE `k3` IN (?, ?, ?, ?, ?) order by 1"
        stmt_read3.setInt(1, 55356821)
        stmt_read3.setInt(2, 56054326)
        stmt_read3.setInt(3, 36548425)
        stmt_read3.setInt(4, 56054803)
        stmt_read3.setInt(5, 56055112)
        qe_stmt_read3_1 stmt_read3

        stmt_read3.setString(1, '55356821')
        stmt_read3.setString(2, '56054326')
        stmt_read3.setString(3, '36548425')
        stmt_read3.setString(4, '56054803')
        stmt_read3.setString(5, '56055112')
        qe_stmt_read3_2 stmt_read3

        def stmt_read4 = prepareStatement "select * from ${tableName} WHERE `k4` IN (?, ?, ?, ?, ?) order by 1"
        stmt_read4.setLong(1, 15982329875)
        stmt_read4.setLong(2, 14285329801)
        stmt_read4.setLong(3, 17754445280)
        stmt_read4.setLong(4, 15669391193)
        stmt_read4.setLong(5, 15229335116)
        qe_stmt_read4_1 stmt_read4

        stmt_read4.setString(1, '15982329875')
        stmt_read4.setString(2, '14285329801')
        stmt_read4.setString(3, '17754445280')
        stmt_read4.setString(4, '15669391193')
        stmt_read4.setString(5, '15229335116')
        qe_stmt_read4_2 stmt_read4

        def stmt_read5 = prepareStatement "select * from ${tableName} WHERE `k5` IN (?, ?, ?, ?, ?) order by 1"
        stmt_read5.setBigDecimal(1, new BigDecimal("119291.11"))
        stmt_read5.setBigDecimal(2, new BigDecimal("12222.99121135"))
        stmt_read5.setBigDecimal(3, new BigDecimal("1.392932911"))
        stmt_read5.setBigDecimal(4, new BigDecimal("12919291.129191137"))
        stmt_read5.setBigDecimal(5, new BigDecimal("991129292901.11138"))
        qe_stmt_read5_1 stmt_read5

        def stmt_read6 = prepareStatement "select * from ${tableName} WHERE `k6` IN (?, ?, ?, ?, ?) order by 1"
        stmt_read6.setString(1, 'abcd')
        stmt_read6.setString(2, 'doris')
        stmt_read6.setString(3, 'superman')
        stmt_read6.setString(4, 'xxabcd')
        stmt_read6.setString(5, 'haha abcd')
        qe_stmt_read6_1 stmt_read6

        def stmt_read7 = prepareStatement "select * from ${tableName} WHERE `k7` IN (?, ?, ?, ?, ?) order by 1"
        stmt_read7.setDate(1, java.sql.Date.valueOf("2023-01-02"))
        stmt_read7.setDate(2, java.sql.Date.valueOf("2024-01-02"))
        stmt_read7.setDate(3, java.sql.Date.valueOf("2025-01-02"))
        stmt_read7.setDate(4, java.sql.Date.valueOf("2120-01-02"))
        stmt_read7.setDate(5, java.sql.Date.valueOf("2030-01-02"))
        qe_stmt_read7_1 stmt_read7

        stmt_read7.setString(1, '2023-01-02')
        stmt_read7.setString(2, '2024-01-02')
        stmt_read7.setString(3, '2025-01-02')
        stmt_read7.setString(4, '2120-01-02')
        stmt_read7.setString(5, '2030-01-02')
        qe_stmt_read7_2 stmt_read7

        def stmt_read8 = prepareStatement "select * from ${tableName} WHERE `k8` IN (?, ?, ?, ?, ?) order by 1"
        stmt_read8.setTimestamp(1, new java.sql.Timestamp(formater.parse("2020-01-01 12:36:38").getTime()))
        stmt_read8.setTimestamp(2, new java.sql.Timestamp(formater.parse("2021-01-01 12:36:38").getTime()))
        stmt_read8.setTimestamp(3, new java.sql.Timestamp(formater.parse("2022-01-01 12:36:38").getTime()))
        stmt_read8.setTimestamp(4, new java.sql.Timestamp(formater.parse("2023-01-01 12:36:38").getTime()))
        stmt_read8.setTimestamp(5, new java.sql.Timestamp(formater.parse("2024-01-01 12:36:38").getTime()))
        qe_stmt_read8_1 stmt_read8

        stmt_read8.setString(1, '2020-01-01 12:36:38')
        stmt_read8.setString(2, '2021-01-01 12:36:38')
        stmt_read8.setString(3, '2022-01-01 12:36:38')
        stmt_read8.setString(4, '2023-01-01 12:36:38')
        stmt_read8.setString(5, '2024-01-01 12:36:38')
        qe_stmt_read8_2 stmt_read8

        def stmt_read9 = prepareStatement "select * from ${tableName} WHERE `k9` IN (?, ?, ?, ?, ?) order by 1"
        stmt_read9.setFloat(1, 522.762)
        stmt_read9.setFloat(2, 52.862)
        stmt_read9.setFloat(3, 552.872)
        stmt_read9.setFloat(4, 652.692)
        stmt_read9.setFloat(5, 2.7692)
        qe_stmt_read9_1 stmt_read9

        def stmt_read10 = prepareStatement "select * from ${tableName} WHERE `k10` IN (?, ?, ?, ?, ?) order by 1"
        stmt_read10.setTimestamp(1, new java.sql.Timestamp(formater.parse("2022-01-01 11:30:38").getTime()))
        stmt_read10.setTimestamp(2, new java.sql.Timestamp(formater.parse("3022-01-01 11:30:38").getTime()))
        stmt_read10.setTimestamp(3, new java.sql.Timestamp(formater.parse("4022-01-01 11:30:38").getTime()))
        stmt_read10.setTimestamp(4, new java.sql.Timestamp(formater.parse("5022-01-01 11:30:38").getTime()))
        stmt_read10.setTimestamp(5, new java.sql.Timestamp(formater.parse("6022-01-01 11:30:38").getTime()))
        qe_stmt_read10_1 stmt_read10

        stmt_read10.setString(1, '2022-01-01 11:30:38')
        stmt_read10.setString(2, '3022-01-01 11:30:38')
        stmt_read10.setString(3, '4022-01-01 11:30:38')
        stmt_read10.setString(4, '5022-01-01 11:30:38')
        stmt_read10.setString(5, '6022-01-01 11:30:38')
        qe_stmt_read10_2 stmt_read10
   }
}
