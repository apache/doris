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

suite("test_dup_schema_value_drop", "p0") {
    def tbName1 = "test_dup_schema_value_drop"
    def tbName2 = "test_dup_schema_value_drop_1"
    sql """ DROP TABLE IF EXISTS ${tbName1} """
    def initTable1 = ""
    def initTableData1 = ""
    def getTableStatusSql = " SHOW ALTER TABLE COLUMN WHERE IndexName='${tbName1}' ORDER BY createtime DESC LIMIT 1  "
    def errorMessage = ""
    def insertSql = "insert into ${tbName1} values(923456689, 'Alice', '四川省', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00');"


    /**
     *  Test the duplicate model by drop a value type
     */


    sql """ DROP TABLE IF EXISTS ${tbName1} """
    def initTable = " CREATE TABLE IF NOT EXISTS ${tbName1}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `score` DECIMAL(38,10) COMMENT \"分数\",\n" +
            "              `city` CHAR(20) COMMENT \"用户所在城市\",\n" +
            "              `age` SMALLINT COMMENT \"用户年龄\",\n" +
            "              `sex` TINYINT COMMENT \"用户性别\",\n" +
            "              `phone` LARGEINT COMMENT \"用户电话\",\n" +
            "              `is_ok` BOOLEAN COMMENT \"是否完成\",\n" +
            "              `t_int` INT COMMENT \"测试int\",\n" +
            "              `t_bigint` BIGINT COMMENT \"测试BIGINT\",\n" +
            "              `t_date` DATE COMMENT \"测试DATE\",\n" +
            "              `t_datev2` DATEV2 COMMENT \"测试DATEV2\",\n" +
            "              `t_datetimev2` DATETIMEV2 COMMENT \"测试DATETIMEV2\",\n" +
            "              `t_datetime` DATETIME COMMENT \"用户注册时间\",\n" +
            "              `t_string` STRING COMMENT \"测试string\",\n" +
            "              `m` Map<STRING, INT> NULL COMMENT \"\",\n" +
            "              `j` JSON NULL COMMENT \"\"\n" +
            "          )\n" +
            "          duplicate KEY(`user_id`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    def initTableData = "insert into ${tbName1} values(1, 'John Doe', 95.5, 'New York', 25, 1, 1234567890, true, 10, 1000000000, '2024-06-11', '2024-06-11', '2024-06-11 08:30:00', '2024-06-11 08:30:00', 'Test String 1', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (2, 'Jane Smith', 85.2, 'Los Angeles', 30, 2, 9876543210, false, 20, 2000000000, '2024-06-12', '2024-06-12', '2024-06-12 09:45:00', '2024-06-12 09:45:00', 'Test String 2', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (3, 'Mike Johnson', 77.8, 'Chicago', 35, 1, 1112223334, true, 30, 3000000000, '2024-06-13', '2024-06-13', '2024-06-13 11:15:00', '2024-06-13 11:15:00', 'Test String 3', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (4, 'Emily Brown', 92.0, 'San Francisco', 28, 2, 5556667778, true, 40, 4000000000, '2024-06-14', '2024-06-14', '2024-06-14 13:30:00', '2024-06-14 13:30:00', 'Test String 4', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (5, 'David Wilson', 88.9, 'Seattle', 32, 1, 9998887776, false, 50, 5000000000, '2024-06-15', '2024-06-15', '2024-06-15 15:45:00', '2024-06-15 15:45:00', 'Test String 5', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]');"

    // Test the duplicate model by drop a value type from BOOLEAN
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} DROP  column is_ok  """
    insertSql = "insert into ${tbName1} values(6, 'Sophia Lee', 91.3, 'Boston', 29, 2, 7778889990,  60, 6000000000, '2024-06-16', '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00', 'Test String 6', {'a': 500, 'b': 200}, '{\"k1\":\"v1\", \"k2\": 200}'); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName1}")


    sql """ DROP TABLE IF EXISTS ${tbName2} """
    initTable1 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `score` DECIMAL(38,10) COMMENT \"分数\",\n" +
            "              `city` CHAR(20) COMMENT \"用户所在城市\",\n" +
            "              `age` SMALLINT COMMENT \"用户年龄\",\n" +
            "              `sex` TINYINT COMMENT \"用户性别\",\n" +
            "              `phone` LARGEINT COMMENT \"用户电话\",\n" +
            "              `t_int` INT COMMENT \"测试int\",\n" +
            "              `t_bigint` BIGINT COMMENT \"测试BIGINT\",\n" +
            "              `t_date` DATE COMMENT \"测试DATE\",\n" +
            "              `t_datev2` DATEV2 COMMENT \"测试DATEV2\",\n" +
            "              `t_datetimev2` DATETIMEV2 COMMENT \"测试DATETIMEV2\",\n" +
            "              `t_datetime` DATETIME COMMENT \"用户注册时间\",\n" +
            "              `t_string` STRING COMMENT \"测试string\",\n" +
            "              `m` Map<STRING, INT> NULL COMMENT \"\",\n" +
            "              `j` JSON NULL COMMENT \"\"\n" +
            "          )\n" +
            "          duplicate KEY(`user_id`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData1 = "insert into ${tbName2} values(1, 'John Doe', 95.5, 'New York', 25, 1, 1234567890,  10, 1000000000, '2024-06-11', '2024-06-11', '2024-06-11 08:30:00', '2024-06-11 08:30:00', 'Test String 1', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (2, 'Jane Smith', 85.2, 'Los Angeles', 30, 2, 9876543210,  20, 2000000000, '2024-06-12', '2024-06-12', '2024-06-12 09:45:00', '2024-06-12 09:45:00', 'Test String 2', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (3, 'Mike Johnson', 77.8, 'Chicago', 35, 1, 1112223334,  30, 3000000000, '2024-06-13', '2024-06-13', '2024-06-13 11:15:00', '2024-06-13 11:15:00', 'Test String 3', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (4, 'Emily Brown', 92.0, 'San Francisco', 28, 2, 5556667778,  40, 4000000000, '2024-06-14', '2024-06-14', '2024-06-14 13:30:00', '2024-06-14 13:30:00', 'Test String 4', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (6, 'Sophia Lee', 91.3, 'Boston', 29, 2, 7778889990,  60, 6000000000, '2024-06-16', '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00', 'Test String 6', {'a': 500, 'b': 200}, '{\\\"k1\\\":\\\"v1\\\", \\\"k2\\\": 200}')," +
            "               (5, 'David Wilson', 88.9, 'Seattle', 32, 1, 9998887776,  50, 5000000000, '2024-06-15', '2024-06-15', '2024-06-15 15:45:00', '2024-06-15 15:45:00', 'Test String 5', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]');"

    sql initTable1
    sql initTableData1
    checkTableData("${tbName1}", "${tbName2}", "t_int")
    sql """ DROP TABLE IF EXISTS ${tbName1} """


    // Test the duplicate model by drop a value type from TINYINT
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} DROP  column sex  """
    insertSql = "insert into ${tbName1} values(6, 'Sophia Lee', 91.3, 'Boston', 29,  7778889990, true, 60, 6000000000, '2024-06-16', '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00', 'Test String 6', {'a': 500, 'b': 200}, '{\"k1\":\"v1\", \"k2\": 200}'); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName1}")


    sql """ DROP TABLE IF EXISTS ${tbName2} """
    initTable1 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `score` DECIMAL(38,10) COMMENT \"分数\",\n" +
            "              `city` CHAR(20) COMMENT \"用户所在城市\",\n" +
            "              `age` SMALLINT COMMENT \"用户年龄\",\n" +
            "              `phone` LARGEINT COMMENT \"用户电话\",\n" +
            "              `is_ok` BOOLEAN COMMENT \"是否完成\",\n" +
            "              `t_int` INT COMMENT \"测试int\",\n" +
            "              `t_bigint` BIGINT COMMENT \"测试BIGINT\",\n" +
            "              `t_date` DATE COMMENT \"测试DATE\",\n" +
            "              `t_datev2` DATEV2 COMMENT \"测试DATEV2\",\n" +
            "              `t_datetimev2` DATETIMEV2 COMMENT \"测试DATETIMEV2\",\n" +
            "              `t_datetime` DATETIME COMMENT \"用户注册时间\",\n" +
            "              `t_string` STRING COMMENT \"测试string\",\n" +
            "              `m` Map<STRING, INT> NULL COMMENT \"\",\n" +
            "              `j` JSON NULL COMMENT \"\"\n" +
            "          )\n" +
            "          duplicate KEY(`user_id`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData1 = "insert into ${tbName2} values(1, 'John Doe', 95.5, 'New York', 25,  1234567890, true, 10, 1000000000, '2024-06-11', '2024-06-11', '2024-06-11 08:30:00', '2024-06-11 08:30:00', 'Test String 1', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (2, 'Jane Smith', 85.2, 'Los Angeles', 30,  9876543210, false, 20, 2000000000, '2024-06-12', '2024-06-12', '2024-06-12 09:45:00', '2024-06-12 09:45:00', 'Test String 2', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (3, 'Mike Johnson', 77.8, 'Chicago', 35,  1112223334, true, 30, 3000000000, '2024-06-13', '2024-06-13', '2024-06-13 11:15:00', '2024-06-13 11:15:00', 'Test String 3', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (4, 'Emily Brown', 92.0, 'San Francisco', 28,  5556667778, true, 40, 4000000000, '2024-06-14', '2024-06-14', '2024-06-14 13:30:00', '2024-06-14 13:30:00', 'Test String 4', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (6, 'Sophia Lee', 91.3, 'Boston', 29,  7778889990, true, 60, 6000000000, '2024-06-16', '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00', 'Test String 6', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (5, 'David Wilson', 88.9, 'Seattle', 32,  9998887776, false, 50, 5000000000, '2024-06-15', '2024-06-15', '2024-06-15 15:45:00', '2024-06-15 15:45:00', 'Test String 5', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]');"

    sql initTable1
    sql initTableData1
    checkTableData("${tbName1}", "${tbName2}", "phone")
    sql """ DROP TABLE IF EXISTS ${tbName1} """


    // Test the duplicate model by drop a value type from SMALLINT
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} DROP  column age  """
    insertSql = "insert into ${tbName1} values(6, 'Sophia Lee', 91.3, 'Boston', 2, 7778889990, true, 60, 6000000000, '2024-06-16', '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00', 'Test String 6', {'a': 500, 'b': 200}, '{\"k1\":\"v1\", \"k2\": 200}'); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName1}")


    sql """ DROP TABLE IF EXISTS ${tbName2} """
    initTable1 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `score` DECIMAL(38,10) COMMENT \"分数\",\n" +
            "              `city` CHAR(20) COMMENT \"用户所在城市\",\n" +
            "              `sex` TINYINT COMMENT \"用户性别\",\n" +
            "              `phone` LARGEINT COMMENT \"用户电话\",\n" +
            "              `is_ok` BOOLEAN COMMENT \"是否完成\",\n" +
            "              `t_int` INT COMMENT \"测试int\",\n" +
            "              `t_bigint` BIGINT COMMENT \"测试BIGINT\",\n" +
            "              `t_date` DATE COMMENT \"测试DATE\",\n" +
            "              `t_datev2` DATEV2 COMMENT \"测试DATEV2\",\n" +
            "              `t_datetimev2` DATETIMEV2 COMMENT \"测试DATETIMEV2\",\n" +
            "              `t_datetime` DATETIME COMMENT \"用户注册时间\",\n" +
            "              `t_string` STRING COMMENT \"测试string\",\n" +
            "              `m` Map<STRING, INT> NULL COMMENT \"\",\n" +
            "              `j` JSON NULL COMMENT \"\"\n" +
            "          )\n" +
            "          duplicate KEY(`user_id`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData1 = "insert into ${tbName2} values(1, 'John Doe', 95.5, 'New York',  1, 1234567890, true, 10, 1000000000, '2024-06-11', '2024-06-11', '2024-06-11 08:30:00', '2024-06-11 08:30:00', 'Test String 1', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (2, 'Jane Smith', 85.2, 'Los Angeles',  2, 9876543210, false, 20, 2000000000, '2024-06-12', '2024-06-12', '2024-06-12 09:45:00', '2024-06-12 09:45:00', 'Test String 2', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (3, 'Mike Johnson', 77.8, 'Chicago',  1, 1112223334, true, 30, 3000000000, '2024-06-13', '2024-06-13', '2024-06-13 11:15:00', '2024-06-13 11:15:00', 'Test String 3', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (4, 'Emily Brown', 92.0, 'San Francisco',  2, 5556667778, true, 40, 4000000000, '2024-06-14', '2024-06-14', '2024-06-14 13:30:00', '2024-06-14 13:30:00', 'Test String 4', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (6, 'Sophia Lee', 91.3, 'Boston',  2, 7778889990, true, 60, 6000000000, '2024-06-16', '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00', 'Test String 6', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (5, 'David Wilson', 88.9, 'Seattle',  1, 9998887776, false, 50, 5000000000, '2024-06-15', '2024-06-15', '2024-06-15 15:45:00', '2024-06-15 15:45:00', 'Test String 5', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]');"

    sql initTable1
    sql initTableData1
    checkTableData("${tbName1}", "${tbName2}", "sex")
    sql """ DROP TABLE IF EXISTS ${tbName1} """


    // Test the duplicate model by drop a value type from INT
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} DROP  column t_int  """
    insertSql = "insert into ${tbName1} values(6, 'Sophia Lee', 91.3, 'Boston', 29, 2, 7778889990, true,  6000000000, '2024-06-16', '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00', 'Test String 6', {'a': 500, 'b': 200}, '{\"k1\":\"v1\", \"k2\": 200}'); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName1}")


    sql """ DROP TABLE IF EXISTS ${tbName2} """
    initTable1 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `score` DECIMAL(38,10) COMMENT \"分数\",\n" +
            "              `city` CHAR(20) COMMENT \"用户所在城市\",\n" +
            "              `age` SMALLINT COMMENT \"用户年龄\",\n" +
            "              `sex` TINYINT COMMENT \"用户性别\",\n" +
            "              `phone` LARGEINT COMMENT \"用户电话\",\n" +
            "              `is_ok` BOOLEAN COMMENT \"是否完成\",\n" +
            "              `t_bigint` BIGINT COMMENT \"测试BIGINT\",\n" +
            "              `t_date` DATE COMMENT \"测试DATE\",\n" +
            "              `t_datev2` DATEV2 COMMENT \"测试DATEV2\",\n" +
            "              `t_datetimev2` DATETIMEV2 COMMENT \"测试DATETIMEV2\",\n" +
            "              `t_datetime` DATETIME COMMENT \"用户注册时间\",\n" +
            "              `t_string` STRING COMMENT \"测试string\",\n" +
            "              `m` Map<STRING, INT> NULL COMMENT \"\",\n" +
            "              `j` JSON NULL COMMENT \"\"\n" +
            "          )\n" +
            "          duplicate KEY(`user_id`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData1 = "insert into ${tbName2} values(1, 'John Doe', 95.5, 'New York', 25, 1, 1234567890, true,  1000000000, '2024-06-11', '2024-06-11', '2024-06-11 08:30:00', '2024-06-11 08:30:00', 'Test String 1', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (2, 'Jane Smith', 85.2, 'Los Angeles', 30, 2, 9876543210, false,  2000000000, '2024-06-12', '2024-06-12', '2024-06-12 09:45:00', '2024-06-12 09:45:00', 'Test String 2', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (3, 'Mike Johnson', 77.8, 'Chicago', 35, 1, 1112223334, true,  3000000000, '2024-06-13', '2024-06-13', '2024-06-13 11:15:00', '2024-06-13 11:15:00', 'Test String 3', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (4, 'Emily Brown', 92.0, 'San Francisco', 28, 2, 5556667778, true,  4000000000, '2024-06-14', '2024-06-14', '2024-06-14 13:30:00', '2024-06-14 13:30:00', 'Test String 4', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (6, 'Sophia Lee', 91.3, 'Boston', 29, 2, 7778889990, true,  6000000000, '2024-06-16', '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00', 'Test String 6', {'a': 500, 'b': 200}, '{\\\"k1\\\":\\\"v1\\\", \\\"k2\\\": 200}')," +
            "               (5, 'David Wilson', 88.9, 'Seattle', 32, 1, 9998887776, false,  5000000000, '2024-06-15', '2024-06-15', '2024-06-15 15:45:00', '2024-06-15 15:45:00', 'Test String 5', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]');"

    sql initTable1
    sql initTableData1
    checkTableData("${tbName1}", "${tbName2}", "t_bigint")
    sql """ DROP TABLE IF EXISTS ${tbName1} """


    // Test the duplicate model by drop a value type from BIGINT
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} DROP  column t_bigint  """
    insertSql = "insert into ${tbName1} values(6, 'Sophia Lee', 91.3, 'Boston', 29, 2, 7778889990, true, 60,  '2024-06-16', '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00', 'Test String 6', {'a': 500, 'b': 200}, '{\"k1\":\"v1\", \"k2\": 200}'); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName1}")


    sql """ DROP TABLE IF EXISTS ${tbName2} """
    initTable1 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `score` DECIMAL(38,10) COMMENT \"分数\",\n" +
            "              `city` CHAR(20) COMMENT \"用户所在城市\",\n" +
            "              `age` SMALLINT COMMENT \"用户年龄\",\n" +
            "              `sex` TINYINT COMMENT \"用户性别\",\n" +
            "              `phone` LARGEINT COMMENT \"用户电话\",\n" +
            "              `is_ok` BOOLEAN COMMENT \"是否完成\",\n" +
            "              `t_int` INT COMMENT \"测试int\",\n" +
            "              `t_date` DATE COMMENT \"测试DATE\",\n" +
            "              `t_datev2` DATEV2 COMMENT \"测试DATEV2\",\n" +
            "              `t_datetimev2` DATETIMEV2 COMMENT \"测试DATETIMEV2\",\n" +
            "              `t_datetime` DATETIME COMMENT \"用户注册时间\",\n" +
            "              `t_string` STRING COMMENT \"测试string\",\n" +
            "              `m` Map<STRING, INT> NULL COMMENT \"\",\n" +
            "              `j` JSON NULL COMMENT \"\"\n" +
            "          )\n" +
            "          duplicate KEY(`user_id`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData1 = "insert into ${tbName2} values(1, 'John Doe', 95.5, 'New York', 25, 1, 1234567890, true, 10,  '2024-06-11', '2024-06-11', '2024-06-11 08:30:00', '2024-06-11 08:30:00', 'Test String 1', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (2, 'Jane Smith', 85.2, 'Los Angeles', 30, 2, 9876543210, false, 20,  '2024-06-12', '2024-06-12', '2024-06-12 09:45:00', '2024-06-12 09:45:00', 'Test String 2', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (3, 'Mike Johnson', 77.8, 'Chicago', 35, 1, 1112223334, true, 30,  '2024-06-13', '2024-06-13', '2024-06-13 11:15:00', '2024-06-13 11:15:00', 'Test String 3', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (4, 'Emily Brown', 92.0, 'San Francisco', 28, 2, 5556667778, true, 40,  '2024-06-14', '2024-06-14', '2024-06-14 13:30:00', '2024-06-14 13:30:00', 'Test String 4', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (6, 'Sophia Lee', 91.3, 'Boston', 29, 2, 7778889990, true, 60,  '2024-06-16', '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00', 'Test String 6', {'a': 500, 'b': 200}, '{\\\"k1\\\":\\\"v1\\\", \\\"k2\\\": 200}')," +
            "               (5, 'David Wilson', 88.9, 'Seattle', 32, 1, 9998887776, false, 50,  '2024-06-15', '2024-06-15', '2024-06-15 15:45:00', '2024-06-15 15:45:00', 'Test String 5', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]');"

    sql initTable1
    sql initTableData1
    checkTableData("${tbName1}", "${tbName2}", "t_date")
    sql """ DROP TABLE IF EXISTS ${tbName1} """


    // Test the duplicate model by drop a value type from LARGEINT
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} DROP  column phone  """
    insertSql = "insert into ${tbName1} values(6, 'Sophia Lee', 91.3, 'Boston', 29, 2, true, 60, 6000000000, '2024-06-16', '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00', 'Test String 6', {'a': 500, 'b': 200}, '{\"k1\":\"v1\", \"k2\": 200}'); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName1}")


    sql """ DROP TABLE IF EXISTS ${tbName2} """
    initTable1 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `score` DECIMAL(38,10) COMMENT \"分数\",\n" +
            "              `city` CHAR(20) COMMENT \"用户所在城市\",\n" +
            "              `age` SMALLINT COMMENT \"用户年龄\",\n" +
            "              `sex` TINYINT COMMENT \"用户性别\",\n" +
            "              `is_ok` BOOLEAN COMMENT \"是否完成\",\n" +
            "              `t_int` INT COMMENT \"测试int\",\n" +
            "              `t_bigint` BIGINT COMMENT \"测试BIGINT\",\n" +
            "              `t_date` DATE COMMENT \"测试DATE\",\n" +
            "              `t_datev2` DATEV2 COMMENT \"测试DATEV2\",\n" +
            "              `t_datetimev2` DATETIMEV2 COMMENT \"测试DATETIMEV2\",\n" +
            "              `t_datetime` DATETIME COMMENT \"用户注册时间\",\n" +
            "              `t_string` STRING COMMENT \"测试string\",\n" +
            "              `m` Map<STRING, INT> NULL COMMENT \"\",\n" +
            "              `j` JSON NULL COMMENT \"\"\n" +
            "          )\n" +
            "          duplicate KEY(`user_id`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData1 = "insert into ${tbName2} values(1, 'John Doe', 95.5, 'New York', 25, 1,  true, 10, 1000000000, '2024-06-11', '2024-06-11', '2024-06-11 08:30:00', '2024-06-11 08:30:00', 'Test String 1', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (2, 'Jane Smith', 85.2, 'Los Angeles', 30, 2,  false, 20, 2000000000, '2024-06-12', '2024-06-12', '2024-06-12 09:45:00', '2024-06-12 09:45:00', 'Test String 2', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (3, 'Mike Johnson', 77.8, 'Chicago', 35, 1,  true, 30, 3000000000, '2024-06-13', '2024-06-13', '2024-06-13 11:15:00', '2024-06-13 11:15:00', 'Test String 3', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (4, 'Emily Brown', 92.0, 'San Francisco', 28, 2,  true, 40, 4000000000, '2024-06-14', '2024-06-14', '2024-06-14 13:30:00', '2024-06-14 13:30:00', 'Test String 4', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (6, 'Sophia Lee', 91.3, 'Boston', 29, 2, true, 60, 6000000000, '2024-06-16', '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00', 'Test String 6', {'a': 500, 'b': 200}, '{\\\"k1\\\":\\\"v1\\\", \\\"k2\\\": 200}')," +
            "               (5, 'David Wilson', 88.9, 'Seattle', 32, 1,  false, 50, 5000000000, '2024-06-15', '2024-06-15', '2024-06-15 15:45:00', '2024-06-15 15:45:00', 'Test String 5', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]');"

    sql initTable1
    sql initTableData1
    checkTableData("${tbName1}", "${tbName2}", "is_ok")
    sql """ DROP TABLE IF EXISTS ${tbName1} """


    // Test the duplicate model by drop a value type from DATE
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} DROP  column t_date  """
    insertSql = "insert into ${tbName1} values(6, 'Sophia Lee', 91.3, 'Boston', 29, 2, 7778889990, true, 60, 6000000000,  '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00', 'Test String 6', {'a': 500, 'b': 200}, '{\"k1\":\"v1\", \"k2\": 200}'); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName1}")


    sql """ DROP TABLE IF EXISTS ${tbName2} """
    initTable1 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `score` DECIMAL(38,10) COMMENT \"分数\",\n" +
            "              `city` CHAR(20) COMMENT \"用户所在城市\",\n" +
            "              `age` SMALLINT COMMENT \"用户年龄\",\n" +
            "              `sex` TINYINT COMMENT \"用户性别\",\n" +
            "              `phone` LARGEINT COMMENT \"用户电话\",\n" +
            "              `is_ok` BOOLEAN COMMENT \"是否完成\",\n" +
            "              `t_int` INT COMMENT \"测试int\",\n" +
            "              `t_bigint` BIGINT COMMENT \"测试BIGINT\",\n" +
            "              `t_datev2` DATEV2 COMMENT \"测试DATEV2\",\n" +
            "              `t_datetimev2` DATETIMEV2 COMMENT \"测试DATETIMEV2\",\n" +
            "              `t_datetime` DATETIME COMMENT \"用户注册时间\",\n" +
            "              `t_string` STRING COMMENT \"测试string\",\n" +
            "              `m` Map<STRING, INT> NULL COMMENT \"\",\n" +
            "              `j` JSON NULL COMMENT \"\"\n" +
            "          )\n" +
            "          duplicate KEY(`user_id`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData1 = "insert into ${tbName2} values(1, 'John Doe', 95.5, 'New York', 25, 1, 1234567890, true, 10, 1000000000,  '2024-06-11', '2024-06-11 08:30:00', '2024-06-11 08:30:00', 'Test String 1', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (2, 'Jane Smith', 85.2, 'Los Angeles', 30, 2, 9876543210, false, 20, 2000000000,  '2024-06-12', '2024-06-12 09:45:00', '2024-06-12 09:45:00', 'Test String 2', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (3, 'Mike Johnson', 77.8, 'Chicago', 35, 1, 1112223334, true, 30, 3000000000,  '2024-06-13', '2024-06-13 11:15:00', '2024-06-13 11:15:00', 'Test String 3', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (4, 'Emily Brown', 92.0, 'San Francisco', 28, 2, 5556667778, true, 40, 4000000000,  '2024-06-14', '2024-06-14 13:30:00', '2024-06-14 13:30:00', 'Test String 4', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (6, 'Sophia Lee', 91.3, 'Boston', 29, 2, 7778889990, true, 60, 6000000000,  '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00', 'Test String 6', {'a': 500, 'b': 200}, '{\\\"k1\\\":\\\"v1\\\", \\\"k2\\\": 200}')," +
            "               (5, 'David Wilson', 88.9, 'Seattle', 32, 1, 9998887776, false, 50, 5000000000,  '2024-06-15', '2024-06-15 15:45:00', '2024-06-15 15:45:00', 'Test String 5', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]');"

    sql initTable1
    sql initTableData1
    checkTableData("${tbName1}", "${tbName2}", "t_datev2")
    sql """ DROP TABLE IF EXISTS ${tbName1} """


    // Test the duplicate model by drop a value type from DATEV2
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} DROP  column t_datev2  """
    insertSql = "insert into ${tbName1} values(6, 'Sophia Lee', 91.3, 'Boston', 29, 2, 7778889990, true, 60, 6000000000,  '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00', 'Test String 6', {'a': 500, 'b': 200}, '{\"k1\":\"v1\", \"k2\": 200}'); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName1}")


    sql """ DROP TABLE IF EXISTS ${tbName2} """
    initTable1 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `score` DECIMAL(38,10) COMMENT \"分数\",\n" +
            "              `city` CHAR(20) COMMENT \"用户所在城市\",\n" +
            "              `age` SMALLINT COMMENT \"用户年龄\",\n" +
            "              `sex` TINYINT COMMENT \"用户性别\",\n" +
            "              `phone` LARGEINT COMMENT \"用户电话\",\n" +
            "              `is_ok` BOOLEAN COMMENT \"是否完成\",\n" +
            "              `t_int` INT COMMENT \"测试int\",\n" +
            "              `t_bigint` BIGINT COMMENT \"测试BIGINT\",\n" +
            "              `t_date` DATE COMMENT \"测试DATEV2\",\n" +
            "              `t_datetimev2` DATETIMEV2 COMMENT \"测试DATETIMEV2\",\n" +
            "              `t_datetime` DATETIME COMMENT \"用户注册时间\",\n" +
            "              `t_string` STRING COMMENT \"测试string\",\n" +
            "              `m` Map<STRING, INT> NULL COMMENT \"\",\n" +
            "              `j` JSON NULL COMMENT \"\"\n" +
            "          )\n" +
            "          duplicate KEY(`user_id`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData1 = "insert into ${tbName2} values(1, 'John Doe', 95.5, 'New York', 25, 1, 1234567890, true, 10, 1000000000,  '2024-06-11', '2024-06-11 08:30:00', '2024-06-11 08:30:00', 'Test String 1', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (2, 'Jane Smith', 85.2, 'Los Angeles', 30, 2, 9876543210, false, 20, 2000000000,  '2024-06-12', '2024-06-12 09:45:00', '2024-06-12 09:45:00', 'Test String 2', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (3, 'Mike Johnson', 77.8, 'Chicago', 35, 1, 1112223334, true, 30, 3000000000,  '2024-06-13', '2024-06-13 11:15:00', '2024-06-13 11:15:00', 'Test String 3', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (4, 'Emily Brown', 92.0, 'San Francisco', 28, 2, 5556667778, true, 40, 4000000000,  '2024-06-14', '2024-06-14 13:30:00', '2024-06-14 13:30:00', 'Test String 4', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (6, 'Sophia Lee', 91.3, 'Boston', 29, 2, 7778889990, true, 60, 6000000000,  '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00', 'Test String 6', {'a': 500, 'b': 200}, '{\\\"k1\\\":\\\"v1\\\", \\\"k2\\\": 200}')," +
            "               (5, 'David Wilson', 88.9, 'Seattle', 32, 1, 9998887776, false, 50, 5000000000,  '2024-06-15', '2024-06-15 15:45:00', '2024-06-15 15:45:00', 'Test String 5', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]');"

    sql initTable1
    sql initTableData1
    checkTableData("${tbName1}", "${tbName2}", "t_datetimev2")
    sql """ DROP TABLE IF EXISTS ${tbName1} """


    // Test the duplicate model by drop a value type from t_datetimev2
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} DROP  column t_datetimev2  """
    insertSql = "insert into ${tbName1} values(6, 'Sophia Lee', 91.3, 'Boston', 29, 2, 7778889990, true, 60, 6000000000,  '2024-06-16', '2024-06-16',  '2024-06-16 17:00:00', 'Test String 6', {'a': 500, 'b': 200}, '{\"k1\":\"v1\", \"k2\": 200}'); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName1}")


    sql """ DROP TABLE IF EXISTS ${tbName2} """
    initTable1 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `score` DECIMAL(38,10) COMMENT \"分数\",\n" +
            "              `city` CHAR(20) COMMENT \"用户所在城市\",\n" +
            "              `age` SMALLINT COMMENT \"用户年龄\",\n" +
            "              `sex` TINYINT COMMENT \"用户性别\",\n" +
            "              `phone` LARGEINT COMMENT \"用户电话\",\n" +
            "              `is_ok` BOOLEAN COMMENT \"是否完成\",\n" +
            "              `t_int` INT COMMENT \"测试int\",\n" +
            "              `t_bigint` BIGINT COMMENT \"测试BIGINT\",\n" +
            "              `t_date` DATE COMMENT \"测试DATE\",\n" +
            "              `t_datev2` DATEV2 COMMENT \"测试DATEV2\",\n" +
            "              `t_datetime` DATETIME COMMENT \"用户注册时间\",\n" +
            "              `t_string` STRING COMMENT \"测试string\",\n" +
            "              `m` Map<STRING, INT> NULL COMMENT \"\",\n" +
            "              `j` JSON NULL COMMENT \"\"\n" +
            "          )\n" +
            "          duplicate KEY(`user_id`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData1 = "insert into ${tbName2} values(1, 'John Doe', 95.5, 'New York', 25, 1, 1234567890, true, 10, 1000000000, '2024-06-11', '2024-06-11',  '2024-06-11 08:30:00', 'Test String 1', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (2, 'Jane Smith', 85.2, 'Los Angeles', 30, 2, 9876543210, false, 20, 2000000000, '2024-06-12', '2024-06-12',  '2024-06-12 09:45:00', 'Test String 2', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (3, 'Mike Johnson', 77.8, 'Chicago', 35, 1, 1112223334, true, 30, 3000000000, '2024-06-13', '2024-06-13',  '2024-06-13 11:15:00', 'Test String 3', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (4, 'Emily Brown', 92.0, 'San Francisco', 28, 2, 5556667778, true, 40, 4000000000, '2024-06-14', '2024-06-14',  '2024-06-14 13:30:00', 'Test String 4', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (6, 'Sophia Lee', 91.3, 'Boston', 29, 2, 7778889990, true, 60, 6000000000,  '2024-06-16', '2024-06-16',  '2024-06-16 17:00:00', 'Test String 6', {'a': 500, 'b': 200}, '{\\\"k1\\\":\\\"v1\\\", \\\"k2\\\": 200}')," +
            "               (5, 'David Wilson', 88.9, 'Seattle', 32, 1, 9998887776, false, 50, 5000000000, '2024-06-15', '2024-06-15',  '2024-06-15 15:45:00', 'Test String 5', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]');"

    sql initTable1
    sql initTableData1
    checkTableData("${tbName1}", "${tbName2}", "t_datetime")
    sql """ DROP TABLE IF EXISTS ${tbName1} """


    // Test the duplicate model by drop a value type from t_datetimev2
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} DROP  column t_datetimev2  """
    insertSql = "insert into ${tbName1} values(6, 'Sophia Lee', 91.3, 'Boston', 29, 2, 7778889990, true, 60, 6000000000,  '2024-06-16', '2024-06-16',  '2024-06-16 17:00:00', 'Test String 6', {'a': 500, 'b': 200}, '{\"k1\":\"v1\", \"k2\": 200}'); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName1}")


    sql """ DROP TABLE IF EXISTS ${tbName2} """
    initTable1 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `score` DECIMAL(38,10) COMMENT \"分数\",\n" +
            "              `city` CHAR(20) COMMENT \"用户所在城市\",\n" +
            "              `age` SMALLINT COMMENT \"用户年龄\",\n" +
            "              `sex` TINYINT COMMENT \"用户性别\",\n" +
            "              `phone` LARGEINT COMMENT \"用户电话\",\n" +
            "              `is_ok` BOOLEAN COMMENT \"是否完成\",\n" +
            "              `t_int` INT COMMENT \"测试int\",\n" +
            "              `t_bigint` BIGINT COMMENT \"测试BIGINT\",\n" +
            "              `t_date` DATE COMMENT \"测试DATE\",\n" +
            "              `t_datev2` DATEV2 COMMENT \"测试DATEV2\",\n" +
            "              `t_datetimev2` DATETIMEv2 COMMENT \"用户注册时间\",\n" +
            "              `t_string` STRING COMMENT \"测试string\",\n" +
            "              `m` Map<STRING, INT> NULL COMMENT \"\",\n" +
            "              `j` JSON NULL COMMENT \"\"\n" +
            "          )\n" +
            "          duplicate KEY(`user_id`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData1 = "insert into ${tbName2} values(1, 'John Doe', 95.5, 'New York', 25, 1, 1234567890, true, 10, 1000000000, '2024-06-11', '2024-06-11',  '2024-06-11 08:30:00', 'Test String 1', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (2, 'Jane Smith', 85.2, 'Los Angeles', 30, 2, 9876543210, false, 20, 2000000000, '2024-06-12', '2024-06-12',  '2024-06-12 09:45:00', 'Test String 2', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (3, 'Mike Johnson', 77.8, 'Chicago', 35, 1, 1112223334, true, 30, 3000000000, '2024-06-13', '2024-06-13',  '2024-06-13 11:15:00', 'Test String 3', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (4, 'Emily Brown', 92.0, 'San Francisco', 28, 2, 5556667778, true, 40, 4000000000, '2024-06-14', '2024-06-14',  '2024-06-14 13:30:00', 'Test String 4', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (6, 'Sophia Lee', 91.3, 'Boston', 29, 2, 7778889990, true, 60, 6000000000,  '2024-06-16', '2024-06-16',  '2024-06-16 17:00:00', 'Test String 6', {'a': 500, 'b': 200}, '{\\\"k1\\\":\\\"v1\\\", \\\"k2\\\": 200}')," +
            "               (5, 'David Wilson', 88.9, 'Seattle', 32, 1, 9998887776, false, 50, 5000000000, '2024-06-15', '2024-06-15',  '2024-06-15 15:45:00', 'Test String 5', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]');"

    sql initTable1
    sql initTableData1
    checkTableData("${tbName1}", "${tbName2}", "t_string")
    sql """ DROP TABLE IF EXISTS ${tbName1} """


    // Test the duplicate model by drop a value type from t_datetime
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} DROP  column t_datetime  """
    insertSql = "insert into ${tbName1} values(6, 'Sophia Lee', 91.3, 'Boston', 29, 2, 7778889990, true, 60, 6000000000,  '2024-06-16', '2024-06-16',  '2024-06-16 17:00:00', 'Test String 6', {'a': 500, 'b': 200}, '{\"k1\":\"v1\", \"k2\": 200}'); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName1}")


    sql """ DROP TABLE IF EXISTS ${tbName2} """
    initTable1 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `score` DECIMAL(38,10) COMMENT \"分数\",\n" +
            "              `city` CHAR(20) COMMENT \"用户所在城市\",\n" +
            "              `age` SMALLINT COMMENT \"用户年龄\",\n" +
            "              `sex` TINYINT COMMENT \"用户性别\",\n" +
            "              `phone` LARGEINT COMMENT \"用户电话\",\n" +
            "              `is_ok` BOOLEAN COMMENT \"是否完成\",\n" +
            "              `t_int` INT COMMENT \"测试int\",\n" +
            "              `t_bigint` BIGINT COMMENT \"测试BIGINT\",\n" +
            "              `t_date` DATE COMMENT \"测试DATE\",\n" +
            "              `t_datev2` DATEV2 COMMENT \"测试DATEV2\",\n" +
            "              `t_datetimev2` DATETIMEv2 COMMENT \"用户注册时间\",\n" +
            "              `t_string` STRING COMMENT \"测试string\",\n" +
            "              `m` Map<STRING, INT> NULL COMMENT \"\",\n" +
            "              `j` JSON NULL COMMENT \"\"\n" +
            "          )\n" +
            "          duplicate KEY(`user_id`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData1 = "insert into ${tbName2} values(1, 'John Doe', 95.5, 'New York', 25, 1, 1234567890, true, 10, 1000000000, '2024-06-11', '2024-06-11',  '2024-06-11 08:30:00', 'Test String 1', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (2, 'Jane Smith', 85.2, 'Los Angeles', 30, 2, 9876543210, false, 20, 2000000000, '2024-06-12', '2024-06-12',  '2024-06-12 09:45:00', 'Test String 2', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (3, 'Mike Johnson', 77.8, 'Chicago', 35, 1, 1112223334, true, 30, 3000000000, '2024-06-13', '2024-06-13',  '2024-06-13 11:15:00', 'Test String 3', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (4, 'Emily Brown', 92.0, 'San Francisco', 28, 2, 5556667778, true, 40, 4000000000, '2024-06-14', '2024-06-14',  '2024-06-14 13:30:00', 'Test String 4', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (6, 'Sophia Lee', 91.3, 'Boston', 29, 2, 7778889990, true, 60, 6000000000,  '2024-06-16', '2024-06-16',  '2024-06-16 17:00:00', 'Test String 6', {'a': 500, 'b': 200}, '{\\\"k1\\\":\\\"v1\\\", \\\"k2\\\": 200}')," +
            "               (5, 'David Wilson', 88.9, 'Seattle', 32, 1, 9998887776, false, 50, 5000000000, '2024-06-15', '2024-06-15',  '2024-06-15 15:45:00', 'Test String 5', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]');"

    sql initTable1
    sql initTableData1
    checkTableData("${tbName1}", "${tbName2}", "t_string")
    sql """ DROP TABLE IF EXISTS ${tbName1} """


    // Test the duplicate model by drop a value type from CHAR
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} DROP  column city  """
    insertSql = "insert into ${tbName1} values(6, 'Sophia Lee', 91.3, 29, 2, 7778889990, true, 60, 6000000000, '2024-06-16', '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00', 'Test String 6', {'a': 500, 'b': 200}, '{\"k1\":\"v1\", \"k2\": 200}'); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName1}")


    sql """ DROP TABLE IF EXISTS ${tbName2} """
    initTable1 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `score` DECIMAL(38,10) COMMENT \"分数\",\n" +
            "              `age` SMALLINT COMMENT \"用户年龄\",\n" +
            "              `sex` TINYINT COMMENT \"用户性别\",\n" +
            "              `phone` LARGEINT COMMENT \"用户电话\",\n" +
            "              `is_ok` BOOLEAN COMMENT \"是否完成\",\n" +
            "              `t_int` INT COMMENT \"测试int\",\n" +
            "              `t_bigint` BIGINT COMMENT \"测试BIGINT\",\n" +
            "              `t_date` DATE COMMENT \"测试DATE\",\n" +
            "              `t_datev2` DATEV2 COMMENT \"测试DATEV2\",\n" +
            "              `t_datetimev2` DATETIMEV2 COMMENT \"测试DATETIMEV2\",\n" +
            "              `t_datetime` DATETIME COMMENT \"用户注册时间\",\n" +
            "              `t_string` STRING COMMENT \"测试string\",\n" +
            "              `m` Map<STRING, INT> NULL COMMENT \"\",\n" +
            "              `j` JSON NULL COMMENT \"\"\n" +
            "          )\n" +
            "          duplicate KEY(`user_id`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData1 = "insert into ${tbName2} values(1, 'John Doe', 95.5,  25, 1, 1234567890, true, 10, 1000000000, '2024-06-11', '2024-06-11', '2024-06-11 08:30:00', '2024-06-11 08:30:00', 'Test String 1', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (2, 'Jane Smith', 85.2,  30, 2, 9876543210, false, 20, 2000000000, '2024-06-12', '2024-06-12', '2024-06-12 09:45:00', '2024-06-12 09:45:00', 'Test String 2', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (3, 'Mike Johnson', 77.8,  35, 1, 1112223334, true, 30, 3000000000, '2024-06-13', '2024-06-13', '2024-06-13 11:15:00', '2024-06-13 11:15:00', 'Test String 3', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (4, 'Emily Brown', 92.0,  28, 2, 5556667778, true, 40, 4000000000, '2024-06-14', '2024-06-14', '2024-06-14 13:30:00', '2024-06-14 13:30:00', 'Test String 4', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (6, 'Sophia Lee', 91.3, 29, 2, 7778889990, true, 60, 6000000000, '2024-06-16', '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00', 'Test String 6', {'a': 500, 'b': 200}, '{\\\"k1\\\":\\\"v1\\\", \\\"k2\\\": 200}')," +
            "               (5, 'David Wilson', 88.9,  32, 1, 9998887776, false, 50, 5000000000, '2024-06-15', '2024-06-15', '2024-06-15 15:45:00', '2024-06-15 15:45:00', 'Test String 5', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]');"

    sql initTable1
    sql initTableData1
    checkTableData("${tbName1}", "${tbName2}", "age")
    sql """ DROP TABLE IF EXISTS ${tbName1} """


    // Test the duplicate model by drop a value type from VARCHAR
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} DROP  column username  """
    insertSql = "insert into ${tbName1} values(6, 91.3, 'Boston', 29, 2, 7778889990, true, 60, 6000000000, '2024-06-16', '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00', 'Test String 6', {'a': 500, 'b': 200}, '{\"k1\":\"v1\", \"k2\": 200}'); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName1}")


    sql """ DROP TABLE IF EXISTS ${tbName2} """
    initTable1 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `score` DECIMAL(38,10) COMMENT \"分数\",\n" +
            "              `city` CHAR(20) COMMENT \"用户所在城市\",\n" +
            "              `age` SMALLINT COMMENT \"用户年龄\",\n" +
            "              `sex` TINYINT COMMENT \"用户性别\",\n" +
            "              `phone` LARGEINT COMMENT \"用户电话\",\n" +
            "              `is_ok` BOOLEAN COMMENT \"是否完成\",\n" +
            "              `t_int` INT COMMENT \"测试int\",\n" +
            "              `t_bigint` BIGINT COMMENT \"测试BIGINT\",\n" +
            "              `t_date` DATE COMMENT \"测试DATE\",\n" +
            "              `t_datev2` DATEV2 COMMENT \"测试DATEV2\",\n" +
            "              `t_datetimev2` DATETIMEV2 COMMENT \"测试DATETIMEV2\",\n" +
            "              `t_datetime` DATETIME COMMENT \"用户注册时间\",\n" +
            "              `t_string` STRING COMMENT \"测试string\",\n" +
            "              `m` Map<STRING, INT> NULL COMMENT \"\",\n" +
            "              `j` JSON NULL COMMENT \"\"\n" +
            "          )\n" +
            "          duplicate KEY(`user_id`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData1 = "insert into ${tbName2} values(1, 95.5, 'New York', 25, 1, 1234567890, true, 10, 1000000000, '2024-06-11', '2024-06-11', '2024-06-11 08:30:00', '2024-06-11 08:30:00', 'Test String 1', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (2, 85.2, 'Los Angeles', 30, 2, 9876543210, false, 20, 2000000000, '2024-06-12', '2024-06-12', '2024-06-12 09:45:00', '2024-06-12 09:45:00', 'Test String 2', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (3, 77.8, 'Chicago', 35, 1, 1112223334, true, 30, 3000000000, '2024-06-13', '2024-06-13', '2024-06-13 11:15:00', '2024-06-13 11:15:00', 'Test String 3', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (4, 92.0, 'San Francisco', 28, 2, 5556667778, true, 40, 4000000000, '2024-06-14', '2024-06-14', '2024-06-14 13:30:00', '2024-06-14 13:30:00', 'Test String 4', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (6, 91.3, 'Boston', 29, 2, 7778889990, true, 60, 6000000000, '2024-06-16', '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00', 'Test String 6', {'a': 500, 'b': 200}, '{\\\"k1\\\":\\\"v1\\\", \\\"k2\\\": 200}')," +
            "               (5, 88.9, 'Seattle', 32, 1, 9998887776, false, 50, 5000000000, '2024-06-15', '2024-06-15', '2024-06-15 15:45:00', '2024-06-15 15:45:00', 'Test String 5', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]');"

    sql initTable1
    sql initTableData1
    checkTableData("${tbName1}", "${tbName2}", "score")
    sql """ DROP TABLE IF EXISTS ${tbName1} """


    // Test the duplicate model by drop a value type from STRING
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} DROP  column t_string  """
    insertSql = "insert into ${tbName1} values(6, 'Sophia Lee', 91.3, 'Boston', 29, 2, 7778889990, true, 60, 6000000000, '2024-06-16', '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00', {'a': 500, 'b': 200}, '{\"k1\":\"v1\", \"k2\": 200}'); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName1}")


    sql """ DROP TABLE IF EXISTS ${tbName2} """
    initTable1 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `score` DECIMAL(38,10) COMMENT \"分数\",\n" +
            "              `city` CHAR(20) COMMENT \"用户所在城市\",\n" +
            "              `age` SMALLINT COMMENT \"用户年龄\",\n" +
            "              `sex` TINYINT COMMENT \"用户性别\",\n" +
            "              `phone` LARGEINT COMMENT \"用户电话\",\n" +
            "              `is_ok` BOOLEAN COMMENT \"是否完成\",\n" +
            "              `t_int` INT COMMENT \"测试int\",\n" +
            "              `t_bigint` BIGINT COMMENT \"测试BIGINT\",\n" +
            "              `t_date` DATE COMMENT \"测试DATE\",\n" +
            "              `t_datev2` DATEV2 COMMENT \"测试DATEV2\",\n" +
            "              `t_datetimev2` DATETIMEV2 COMMENT \"测试DATETIMEV2\",\n" +
            "              `t_datetime` DATETIME COMMENT \"用户注册时间\",\n" +
            "              `m` Map<STRING, INT> NULL COMMENT \"\",\n" +
            "              `j` JSON NULL COMMENT \"\"\n" +
            "          )\n" +
            "          duplicate KEY(`user_id`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData1 = "insert into ${tbName2} values(1, 'John Doe', 95.5, 'New York', 25, 1, 1234567890, true, 10, 1000000000, '2024-06-11', '2024-06-11', '2024-06-11 08:30:00', '2024-06-11 08:30:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (2, 'Jane Smith', 85.2, 'Los Angeles', 30, 2, 9876543210, false, 20, 2000000000, '2024-06-12', '2024-06-12', '2024-06-12 09:45:00', '2024-06-12 09:45:00', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (3, 'Mike Johnson', 77.8, 'Chicago', 35, 1, 1112223334, true, 30, 3000000000, '2024-06-13', '2024-06-13', '2024-06-13 11:15:00', '2024-06-13 11:15:00', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (4, 'Emily Brown', 92.0, 'San Francisco', 28, 2, 5556667778, true, 40, 4000000000, '2024-06-14', '2024-06-14', '2024-06-14 13:30:00', '2024-06-14 13:30:00', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (6, 'Sophia Lee', 91.3, 'Boston', 29, 2, 7778889990, true, 60, 6000000000, '2024-06-16', '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00', {'a': 500, 'b': 200}, '{\\\"k1\\\":\\\"v1\\\", \\\"k2\\\": 200}')," +
            "               (5, 'David Wilson', 88.9, 'Seattle', 32, 1, 9998887776, false, 50, 5000000000, '2024-06-15', '2024-06-15', '2024-06-15 15:45:00', '2024-06-15 15:45:00', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]');"

    sql initTable1
    sql initTableData1
    checkTableData("${tbName1}", "${tbName2}", "score")
    sql """ DROP TABLE IF EXISTS ${tbName1} """


    // Test the duplicate model by drop a value type from Map
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} DROP  column m  """
    insertSql = "insert into ${tbName1} values(6, 'Sophia Lee', 91.3, 'Boston', 29, 2, 7778889990, true, 60, 6000000000, '2024-06-16', '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00', 'Test String 6', '{\"k1\":\"v1\", \"k2\": 200}'); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName1}")


    sql """ DROP TABLE IF EXISTS ${tbName2} """
    initTable1 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `score` DECIMAL(38,10) COMMENT \"分数\",\n" +
            "              `city` CHAR(20) COMMENT \"用户所在城市\",\n" +
            "              `age` SMALLINT COMMENT \"用户年龄\",\n" +
            "              `sex` TINYINT COMMENT \"用户性别\",\n" +
            "              `phone` LARGEINT COMMENT \"用户电话\",\n" +
            "              `is_ok` BOOLEAN COMMENT \"是否完成\",\n" +
            "              `t_int` INT COMMENT \"测试int\",\n" +
            "              `t_bigint` BIGINT COMMENT \"测试BIGINT\",\n" +
            "              `t_date` DATE COMMENT \"测试DATE\",\n" +
            "              `t_datev2` DATEV2 COMMENT \"测试DATEV2\",\n" +
            "              `t_datetimev2` DATETIMEV2 COMMENT \"测试DATETIMEV2\",\n" +
            "              `t_datetime` DATETIME COMMENT \"用户注册时间\",\n" +
            "              `t_string` STRING COMMENT \"测试string\",\n" +
            "              `j` JSON NULL COMMENT \"\"\n" +
            "          )\n" +
            "          duplicate KEY(`user_id`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData1 = "insert into ${tbName2} values(1, 'John Doe', 95.5, 'New York', 25, 1, 1234567890, true, 10, 1000000000, '2024-06-11', '2024-06-11', '2024-06-11 08:30:00', '2024-06-11 08:30:00', 'Test String 1', '[\"abc\", \"def\"]')," +
            "               (2, 'Jane Smith', 85.2, 'Los Angeles', 30, 2, 9876543210, false, 20, 2000000000, '2024-06-12', '2024-06-12', '2024-06-12 09:45:00', '2024-06-12 09:45:00', 'Test String 2', '[\"abc\", \"def\"]')," +
            "               (3, 'Mike Johnson', 77.8, 'Chicago', 35, 1, 1112223334, true, 30, 3000000000, '2024-06-13', '2024-06-13', '2024-06-13 11:15:00', '2024-06-13 11:15:00', 'Test String 3', '[\"abc\", \"def\"]')," +
            "               (4, 'Emily Brown', 92.0, 'San Francisco', 28, 2, 5556667778, true, 40, 4000000000, '2024-06-14', '2024-06-14', '2024-06-14 13:30:00', '2024-06-14 13:30:00', 'Test String 4', '[\"abc\", \"def\"]')," +
            "               (6, 'Sophia Lee', 91.3, 'Boston', 29, 2, 7778889990, true, 60, 6000000000, '2024-06-16', '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00', 'Test String 6', '{\\\"k1\\\":\\\"v1\\\", \\\"k2\\\": 200}')," +
            "               (5, 'David Wilson', 88.9, 'Seattle', 32, 1, 9998887776, false, 50, 5000000000, '2024-06-15', '2024-06-15', '2024-06-15 15:45:00', '2024-06-15 15:45:00', 'Test String 5', '[\"abc\", \"def\"]');"

    sql initTable1
    sql initTableData1
    checkTableData("${tbName1}", "${tbName2}", "score")
    sql """ DROP TABLE IF EXISTS ${tbName1} """


    // Test the duplicate model by drop a value type from JSON
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} DROP  column j  """
    insertSql = "insert into ${tbName1} values(6, 'Sophia Lee', 91.3, 'Boston', 29, 2, 7778889990, true, 60, 6000000000, '2024-06-16', '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00', 'Test String 6', {'a': 100, 'b': 200}); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName1}")


    sql """ DROP TABLE IF EXISTS ${tbName2} """
    initTable1 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `score` DECIMAL(38,10) COMMENT \"分数\",\n" +
            "              `city` CHAR(20) COMMENT \"用户所在城市\",\n" +
            "              `age` SMALLINT COMMENT \"用户年龄\",\n" +
            "              `sex` TINYINT COMMENT \"用户性别\",\n" +
            "              `phone` LARGEINT COMMENT \"用户电话\",\n" +
            "              `is_ok` BOOLEAN COMMENT \"是否完成\",\n" +
            "              `t_int` INT COMMENT \"测试int\",\n" +
            "              `t_bigint` BIGINT COMMENT \"测试BIGINT\",\n" +
            "              `t_date` DATE COMMENT \"测试DATE\",\n" +
            "              `t_datev2` DATEV2 COMMENT \"测试DATEV2\",\n" +
            "              `t_datetimev2` DATETIMEV2 COMMENT \"测试DATETIMEV2\",\n" +
            "              `t_datetime` DATETIME COMMENT \"用户注册时间\",\n" +
            "              `t_string` STRING COMMENT \"测试string\",\n" +
            "              `m` Map<STRING, INT> NULL COMMENT \"\",\n" +
            "          )\n" +
            "          duplicate KEY(`user_id`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData1 = "insert into ${tbName2} values(1, 'John Doe', 95.5, 'New York', 25, 1, 1234567890, true, 10, 1000000000, '2024-06-11', '2024-06-11', '2024-06-11 08:30:00', '2024-06-11 08:30:00', 'Test String 1', {'a': 100, 'b': 200})," +
            "               (2, 'Jane Smith', 85.2, 'Los Angeles', 30, 2, 9876543210, false, 20, 2000000000, '2024-06-12', '2024-06-12', '2024-06-12 09:45:00', '2024-06-12 09:45:00', 'Test String 2',  {'a': 100, 'b': 200})," +
            "               (3, 'Mike Johnson', 77.8, 'Chicago', 35, 1, 1112223334, true, 30, 3000000000, '2024-06-13', '2024-06-13', '2024-06-13 11:15:00', '2024-06-13 11:15:00', 'Test String 3',  {'a': 100, 'b': 200})," +
            "               (4, 'Emily Brown', 92.0, 'San Francisco', 28, 2, 5556667778, true, 40, 4000000000, '2024-06-14', '2024-06-14', '2024-06-14 13:30:00', '2024-06-14 13:30:00', 'Test String 4', {'a': 100, 'b': 200})," +
            "               (6, 'Sophia Lee', 91.3, 'Boston', 29, 2, 7778889990, true, 60, 6000000000, '2024-06-16', '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00', 'Test String 6',  {'a': 100, 'b': 200})," +
            "               (5, 'David Wilson', 88.9, 'Seattle', 32, 1, 9998887776, false, 50, 5000000000, '2024-06-15', '2024-06-15', '2024-06-15 15:45:00', '2024-06-15 15:45:00', 'Test String 5',  {'a': 100, 'b': 200});"

    sql initTable1
    sql initTableData1
    checkTableData("${tbName1}", "${tbName2}", "user_id")
    sql """ DROP TABLE IF EXISTS ${tbName1} """




    initTable = " CREATE TABLE IF NOT EXISTS ${tbName1}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `t_decimal` DECIMAL(38,10) COMMENT \"测试decimal\",\n" +
            "              `t_float` FLOAT COMMENT \"测试float\",\n" +
            "              `t_double` DOUBLE COMMENT \"测试double\"\n" +
            "          )\n" +
            "          duplicate KEY(`user_id`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData = "insert into ${tbName1} values(1, 123.4567890123, 123.45, 1234.5678901234)," +
            "               (2, 234.5678901234, 234.56, 2345.6789012345)," +
            "               (3, 345.6789012345, 345.67, 3456.7890123456)," +
            "               (4, 456.7890123456, 456.78, 4567.8901234567)," +
            "               (5, 567.8901234567, 567.89, 5678.9012345678);"


    // Test the duplicate model by drop a value type from DECIMAL
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} DROP  column t_decimal  """
    insertSql = "insert into ${tbName1} values(6,  678.90, 6789.0123456789); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName1}")


    sql """ DROP TABLE IF EXISTS ${tbName2} """
    initTable1 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `t_float` FLOAT COMMENT \"测试float\",\n" +
            "              `t_double` DOUBLE COMMENT \"测试double\"\n" +
            "          )\n" +
            "          duplicate KEY(`user_id`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData1 = "insert into ${tbName2} values(1,  123.45, 1234.5678901234)," +
            "               (2,  234.56, 2345.6789012345)," +
            "               (3,  345.67, 3456.7890123456)," +
            "               (4,  456.78, 4567.8901234567)," +
            "               (6,  678.90, 6789.0123456789)," +
            "               (5,  567.89, 5678.9012345678);"

    sql initTable1
    sql initTableData1
    checkTableData("${tbName1}", "${tbName2}", "t_float")
    sql """ DROP TABLE IF EXISTS ${tbName1} """


    // Test the duplicate model by drop a value type from FLOAT
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} DROP  column t_float  """
    insertSql = "insert into ${tbName1} values(6, 678.9012345678, 6789.0123456789); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName1}")


    sql """ DROP TABLE IF EXISTS ${tbName2} """
    initTable1 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `t_decimal` DECIMAL(38,10) COMMENT \"测试decimal\",\n" +
            "              `t_double` DOUBLE COMMENT \"测试double\"\n" +
            "          )\n" +
            "          duplicate KEY(`user_id`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData1 = "insert into ${tbName2} values(1, 123.4567890123,  1234.5678901234)," +
            "               (2, 234.5678901234,  2345.6789012345)," +
            "               (3, 345.6789012345,  3456.7890123456)," +
            "               (4, 456.7890123456,  4567.8901234567)," +
            "               (6, 678.9012345678, 6789.0123456789)," +
            "               (5, 567.8901234567,  5678.9012345678);"

    sql initTable1
    sql initTableData1
    checkTableData("${tbName1}", "${tbName2}", "t_double")
    sql """ DROP TABLE IF EXISTS ${tbName1} """


    // Test the duplicate model by drop a value type from DOUBLE
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} DROP  column t_double  """
    insertSql = "insert into ${tbName1} values(6, 678.9012345678, 678.90); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName1}")


    sql """ DROP TABLE IF EXISTS ${tbName2} """
    initTable1 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `t_decimal` DECIMAL(38,10) COMMENT \"测试decimal\",\n" +
            "              `t_float` FLOAT COMMENT \"测试float\",\n" +
            "          )\n" +
            "          duplicate KEY(`user_id`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData1 = "insert into ${tbName2} values(1, 123.4567890123, 123.45)," +
            "               (2, 234.5678901234, 234.56)," +
            "               (3, 345.6789012345, 345.67)," +
            "               (4, 456.7890123456, 456.78)," +
            "               (6, 678.9012345678, 678.90)," +
            "               (5, 567.8901234567, 567.89);"

    sql initTable1
    sql initTableData1
    checkTableData("${tbName1}", "${tbName2}", "user_id")
    sql """ DROP TABLE IF EXISTS ${tbName1} """


    initTable = " CREATE TABLE IF NOT EXISTS ${tbName1}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `c_array` ARRAY<int(11)> COMMENT \"测试ARRAY\",\n" +
            "              `s_info` STRUCT<s_id:int(11), s_name:string, s_address:string> COMMENT \"测试STRUCT\"\n" +
            "          )\n" +
            "          duplicate KEY(`user_id`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData = "insert into ${tbName1} values(1, [1,7,8], struct(1, 'sn1', 'sa1'))," +
            "               (2,  [2,7,8], struct(2, 'sn2', 'sa2'))," +
            "               (3,  [3,7,8], struct(3, 'sn3', 'sa3'))," +
            "               (4,  [4,7,8], struct(4, 'sn4', 'sa4'))," +
            "               (5,  [5,7,8], struct(5, 'sn5', 'sa5'));"


    // Test the duplicate model by drop a value type from ARRAY
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} DROP  column c_array  """
    insertSql = "insert into ${tbName1} values(6,  struct(6, 'sn6', 'sa6')); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName1}")


    sql """ DROP TABLE IF EXISTS ${tbName2} """
    initTable1 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `s_info` STRUCT<s_id:int(11), s_name:string, s_address:string> COMMENT \"测试STRUCT\"\n" +
            "          )\n" +
            "          duplicate KEY(`user_id`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData1 = "insert into ${tbName2} values(1, struct(1, 'sn1', 'sa1'))," +
            "               (2, struct(1, 'sn2', 'sa2'))," +
            "               (3, struct(1, 'sn3', 'sa3'))," +
            "               (4, struct(1, 'sn4', 'sa4'))," +
            "               (6, struct(1, 'sn6', 'sa6'))," +
            "               (5, struct(5, 'sn5', 'sa5'));"

    sql initTable1
    sql initTableData1
    checkTableData("${tbName1}", "${tbName2}", "user_id")
    sql """ DROP TABLE IF EXISTS ${tbName1} """


    // Test the duplicate model by drop a value type from STRUCT
    errorMessage = "errCode = 2, detailMessage = can not cast from origin type STRUCT<StructField ( name=col1, dataType=TINYINT, nullable=true ),StructField ( name=col2, dataType=VARCHAR(3), nullable=true ),StructField ( name=col3, dataType=VARCHAR(3), nullable=true )> to target type=ARRAY<INT>"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} DROP  column s_info  """
        insertSql = "insert into ${tbName1} values(6,  struct(6, 'sn6', 'sa6')); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


}
