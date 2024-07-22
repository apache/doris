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

suite("test_schema_reordering_dup", "p0") {
    def tbName = "test_schema_reordering_dup"
    def tbName2 = "test_schema_reordering_dup_0"
    def initTable1 = ""
    def initTableData1 = ""
    def getTableStatusSql = " SHOW ALTER TABLE COLUMN WHERE IndexName='${tbName}' ORDER BY createtime DESC LIMIT 1  "
    def errorMessage = ""
    def insertSql = ""


    /**
     *  Test the dup model by change a value order
     */


    sql """ DROP TABLE IF EXISTS ${tbName} """
    def initTable = " CREATE TABLE IF NOT EXISTS ${tbName}\n" +
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
            "              `j` JSON NULL COMMENT \"\",\n" +
            "              `t_decimal` DECIMAL(38,10) COMMENT \"测试decimal\",\n" +
            "              `t_float` FLOAT COMMENT \"测试float\"\n" +
            "          )\n" +
            "          DUPLICATE KEY(`user_id`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    def initTableData = "insert into ${tbName} values(1, 'John Doe', 95.5, 'New York', 25, 1, 1234567890, true, 10, 1000000000, '2024-06-11', '2024-06-11', '2024-06-11 08:30:00', '2024-06-11 08:30:00', 'Test String 1', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99)," +
            "               (2, 'Jane Smith', 85.2, 'Los Angeles', 30, 2, 9876543210, false, 20, 2000000000, '2024-06-12', '2024-06-12', '2024-06-12 09:45:00', '2024-06-12 09:45:00', 'Test String 2', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99)," +
            "               (3, 'Mike Johnson', 77.8, 'Chicago', 35, 1, 1112223334, true, 30, 3000000000, '2024-06-13', '2024-06-13', '2024-06-13 11:15:00', '2024-06-13 11:15:00', 'Test String 3', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99)," +
            "               (4, 'Emily Brown', 92.0, 'San Francisco', 28, 2, 5556667778, true, 40, 4000000000, '2024-06-14', '2024-06-14', '2024-06-14 13:30:00', '2024-06-14 13:30:00', 'Test String 4', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99)," +
            "               (5, 'David Wilson', 88.9, 'Seattle', 32, 1, 9998887776, false, 50, 5000000000, '2024-06-15', '2024-06-15', '2024-06-15 15:45:00', '2024-06-15 15:45:00', 'Test String 5', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99);"

    /**
     * add VALUE change order
     */
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName} ADD COLUMN new_col INT  DEFAULT "5555" AFTER t_string  """
    insertSql = "insert into ${tbName} values(9, 'David Wilson', 87.9, 'Seattle', 22, 1, 9998887776, true, 50, 5000000000, '2024-06-15', '2024-06-15', '2024-06-15 15:45:00', '2024-06-15 15:45:00', 'Test String 9', 6666, {'a': 500, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 60
    }, insertSql, false, "${tbName}")


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
            "              `new_col` INT COMMENT \"new_col\",\n" +
            "              `m` Map<STRING, INT> NULL COMMENT \"\",\n" +
            "              `j` JSON NULL COMMENT \"\",\n" +
            "              `t_decimal` DECIMAL(38,10) COMMENT \"测试decimal\",\n" +
            "              `t_float` FLOAT COMMENT \"测试float\"\n" +
            "          )\n" +
            "          DUPLICATE KEY(`user_id`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData1 = "insert into ${tbName2} values(1, 'John Doe', 95.5, 'New York', 25, 1, 1234567890, true, 10, 1000000000, '2024-06-11', '2024-06-11', '2024-06-11 08:30:00', '2024-06-11 08:30:00', 'Test String 1', 5555, {'a': 100, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99)," +
            "               (2, 'Jane Smith', 85.2, 'Los Angeles', 30, 2, 9876543210, false, 20, 2000000000, '2024-06-12', '2024-06-12', '2024-06-12 09:45:00', '2024-06-12 09:45:00', 'Test String 2', 5555, {'a': 200, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99)," +
            "               (3, 'Mike Johnson', 77.8, 'Chicago', 35, 1, 1112223334, true, 30, 3000000000, '2024-06-13', '2024-06-13', '2024-06-13 11:15:00', '2024-06-13 11:15:00', 'Test String 3', 5555, {'a': 300, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99)," +
            "               (4, 'Emily Brown', 92.0, 'San Francisco', 28, 2, 5556667778, true, 40, 4000000000, '2024-06-14', '2024-06-14', '2024-06-14 13:30:00', '2024-06-14 13:30:00', 'Test String 4', 5555, {'a': 400, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99)," +
            "               (9, 'David Wilson', 87.9, 'Seattle', 22, 1, 9998887776, true, 50, 5000000000, '2024-06-15', '2024-06-15', '2024-06-15 15:45:00', '2024-06-15 15:45:00', 'Test String 9', 6666, {'a': 500, 'b': 200}, '[\\\"abc\\\", \\\"def\\\"]',95.5, 9.99)," +
            "               (5, 'David Wilson', 88.9, 'Seattle', 32, 1, 9998887776, false, 50, 5000000000, '2024-06-15', '2024-06-15', '2024-06-15 15:45:00', '2024-06-15 15:45:00', 'Test String 5', 5555, {'a': 500, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99);"

    sql initTable1
    sql initTableData1
    checkTableData("${tbName}", "${tbName2}", "new_col")
    sql """ DROP TABLE IF EXISTS ${tbName} """


    /**
     * modify value change order
     */
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName} MODIFY  column t_int BIGINT AFTER t_string """
    insertSql = "insert into ${tbName} values(9, 'David Wilson', 87.9, 'Seattle', 22, 1, 9998887776, true, 5000000000, '2024-06-15', '2024-06-15', '2024-06-15 15:45:00', '2024-06-15 15:45:00', 'Test String 9', 50, {'a': 500, 'b': 200}, '[\\\"abc\\\", \\\"def\\\"]',95.5, 9.99); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 60
    }, insertSql, false, "${tbName}")

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
            "              `t_int` BIGINT COMMENT \"INT\",\n" +
            "              `m` Map<STRING, INT> NULL COMMENT \"\",\n" +
            "              `j` JSON NULL COMMENT \"\",\n" +
            "              `t_decimal` DECIMAL(38,10) COMMENT \"测试decimal\",\n" +
            "              `t_float` FLOAT COMMENT \"测试float\"\n" +
            "          )\n" +
            "          DUPLICATE KEY(`user_id`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"


    initTableData1 = "insert into ${tbName2} values(1, 'John Doe', 95.5, 'New York', 25, 1, 1234567890, true, 1000000000, '2024-06-11', '2024-06-11', '2024-06-11 08:30:00', '2024-06-11 08:30:00', 'Test String 1', 10, {'a': 100, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99)," +
            "               (2, 'Jane Smith', 85.2, 'Los Angeles', 30, 2, 9876543210, false, 2000000000, '2024-06-12', '2024-06-12', '2024-06-12 09:45:00', '2024-06-12 09:45:00', 'Test String 2', 20, {'a': 200, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99)," +
            "               (3, 'Mike Johnson', 77.8, 'Chicago', 35, 1, 1112223334, true,  3000000000, '2024-06-13', '2024-06-13', '2024-06-13 11:15:00', '2024-06-13 11:15:00', 'Test String 3', 30, {'a': 300, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99)," +
            "               (4, 'Emily Brown', 92.0, 'San Francisco', 28, 2, 5556667778, true,  4000000000, '2024-06-14', '2024-06-14', '2024-06-14 13:30:00', '2024-06-14 13:30:00', 'Test String 4', 40, {'a': 400, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99)," +
            "               (9, 'David Wilson', 87.9, 'Seattle', 22, 1, 9998887776, true, 5000000000, '2024-06-15', '2024-06-15', '2024-06-15 15:45:00', '2024-06-15 15:45:00', 'Test String 9', 50, {'a': 500, 'b': 200}, '[\\\\\\\"abc\\\\\\\", \\\\\\\"def\\\\\\\"]',95.5, 9.99)," +
            "               (5, 'David Wilson', 88.9, 'Seattle', 32, 1, 9998887776, false,  5000000000, '2024-06-15', '2024-06-15', '2024-06-15 15:45:00', '2024-06-15 15:45:00', 'Test String 5', 50, {'a': 500, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99);"

    sql initTable1
    sql initTableData1
    checkTableData("${tbName}", "${tbName2}", "t_int")
    sql """ DROP TABLE IF EXISTS ${tbName} """


    /**
     * dup add key change order
     */
    initTable = " CREATE TABLE IF NOT EXISTS ${tbName}\n" +
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
            "              `j` JSON NULL COMMENT \"\",\n" +
            "              `t_decimal` DECIMAL(38,10) COMMENT \"测试decimal\",\n" +
            "              `t_float` FLOAT COMMENT \"测试float\"\n" +
            "          )\n" +
            "          DUPLICATE KEY(`user_id`, `username`, `score`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData = "insert into ${tbName} values(1, 'John Doe', 95.5, 'New York', 25, 1, 1234567890, true, 10, 1000000000, '2024-06-11', '2024-06-11', '2024-06-11 08:30:00', '2024-06-11 08:30:00', 'Test String 1', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99)," +
            "               (2, 'Jane Smith', 85.2, 'Los Angeles', 30, 2, 9876543210, false, 20, 2000000000, '2024-06-12', '2024-06-12', '2024-06-12 09:45:00', '2024-06-12 09:45:00', 'Test String 2', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99)," +
            "               (3, 'Mike Johnson', 77.8, 'Chicago', 35, 1, 1112223334, true, 30, 3000000000, '2024-06-13', '2024-06-13', '2024-06-13 11:15:00', '2024-06-13 11:15:00', 'Test String 3', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99)," +
            "               (4, 'Emily Brown', 92.0, 'San Francisco', 28, 2, 5556667778, true, 40, 4000000000, '2024-06-14', '2024-06-14', '2024-06-14 13:30:00', '2024-06-14 13:30:00', 'Test String 4', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99)," +
            "               (5, 'David Wilson', 88.9, 'Seattle', 32, 1, 9998887776, false, 50, 5000000000, '2024-06-15', '2024-06-15', '2024-06-15 15:45:00', '2024-06-15 15:45:00', 'Test String 5', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99);"

    sql initTable
    sql initTableData
    sql """ alter  table ${tbName} add  column province VARCHAR(20) KEY DEFAULT "广东省" AFTER username """
    insertSql = "insert into ${tbName} values(4, 'Emily Brown', '四川省', 92.0, 'San Francisco', 28, 2, 5556667778, true, 40, 4000000000, '2024-06-14', '2024-06-14', '2024-06-14 13:30:00', '2024-06-14 13:30:00', 'Test String 4', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99);"
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 60
    }, insertSql, false, "${tbName}")

    sql """ DROP TABLE IF EXISTS ${tbName2} """
    initTable1 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `province` VARCHAR(20) NOT NULL COMMENT \"省份\",\n" +
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
            "              `j` JSON NULL COMMENT \"\",\n" +
            "              `t_decimal` DECIMAL(38,10) COMMENT \"测试decimal\",\n" +
            "              `t_float` FLOAT COMMENT \"测试float\"\n" +
            "          )\n" +
            "          DUPLICATE KEY(`user_id`, `username`, `province`, `score`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData1 = "insert into ${tbName2} values(1, 'John Doe', '广东省',95.5, 'New York', 25, 1, 1234567890, true, 10, 1000000000, '2024-06-11', '2024-06-11', '2024-06-11 08:30:00', '2024-06-11 08:30:00', 'Test String 1', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99)," +
            "               (2, 'Jane Smith', '广东省', 85.2, 'Los Angeles', 30, 2, 9876543210, false, 20, 2000000000, '2024-06-12', '2024-06-12', '2024-06-12 09:45:00', '2024-06-12 09:45:00', 'Test String 2', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99)," +
            "               (3, 'Mike Johnson', '广东省', 77.8, 'Chicago', 35, 1, 1112223334, true, 30, 3000000000, '2024-06-13', '2024-06-13', '2024-06-13 11:15:00', '2024-06-13 11:15:00', 'Test String 3', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99)," +
            "               (4, 'Emily Brown','广东省' , 92.0, 'San Francisco', 28, 2, 5556667778, true, 40, 4000000000, '2024-06-14', '2024-06-14', '2024-06-14 13:30:00', '2024-06-14 13:30:00', 'Test String 4', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99)," +
            "               (9, 'Emily Brown', '四川省', 92.0, 'San Francisco', 28, 2, 5556667778, true, 40, 4000000000, '2024-06-14', '2024-06-14', '2024-06-14 13:30:00', '2024-06-14 13:30:00', 'Test String 4', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99)," +
            "               (5, 'David Wilson', '广东省', 88.9, 'Seattle', 32, 1, 9998887776, false, 50, 5000000000, '2024-06-15', '2024-06-15', '2024-06-15 15:45:00', '2024-06-15 15:45:00', 'Test String 5', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99);"

    sql initTable1
    sql initTableData1
    checkTableData("${tbName}", "${tbName2}", "province")
    sql """ DROP TABLE IF EXISTS ${tbName} """


    /**
     * dup modify key order
     *
     */
    initTable = " CREATE TABLE IF NOT EXISTS ${tbName}\n" +
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
            "              `j` JSON NULL COMMENT \"\",\n" +
            "              `t_decimal` DECIMAL(38,10) COMMENT \"测试decimal\",\n" +
            "              `t_float` FLOAT COMMENT \"测试float\"\n" +
            "          )\n" +
            "          DUPLICATE KEY(`user_id`, `username`, `score`, `city`, `age`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData = "insert into ${tbName} values(1, 'John Doe', 95.5, 'New York', 25, 1, 1234567890, true, 10, 1000000000, '2024-06-11', '2024-06-11', '2024-06-11 08:30:00', '2024-06-11 08:30:00', 'Test String 1', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99)," +
            "               (2, 'Jane Smith', 85.2, 'Los Angeles', 30, 2, 9876543210, false, 20, 2000000000, '2024-06-12', '2024-06-12', '2024-06-12 09:45:00', '2024-06-12 09:45:00', 'Test String 2', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99)," +
            "               (3, 'Mike Johnson', 77.8, 'Chicago', 35, 1, 1112223334, true, 30, 3000000000, '2024-06-13', '2024-06-13', '2024-06-13 11:15:00', '2024-06-13 11:15:00', 'Test String 3', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99)," +
            "               (4, 'Emily Brown', 92.0, 'San Francisco', 28, 2, 5556667778, true, 40, 4000000000, '2024-06-14', '2024-06-14', '2024-06-14 13:30:00', '2024-06-14 13:30:00', 'Test String 4', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99)," +
            "               (5, 'David Wilson', 88.9, 'Seattle', 32, 1, 9998887776, false, 50, 5000000000, '2024-06-15', '2024-06-15', '2024-06-15 15:45:00', '2024-06-15 15:45:00', 'Test String 5', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99);"

    sql initTable
    sql initTableData
    sql """ alter  table ${tbName} MODIFY  column age INT KEY  AFTER username """
    insertSql = "insert into ${tbName} values(9, 'Emily Brown', 28, 92.0, 'San Francisco', 2, 5556667778, true, 40, 4000000000, '2024-06-14', '2024-06-14', '2024-06-14 13:30:00', '2024-06-14 13:30:00', 'Test String 4', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99);"
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 60
    }, insertSql, false, "${tbName}")


    sql """ DROP TABLE IF EXISTS ${tbName2} """
    initTable1 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `age` INT COMMENT \"用户年龄\",\n" +
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
            "              `j` JSON NULL COMMENT \"\",\n" +
            "              `t_decimal` DECIMAL(38,10) COMMENT \"测试decimal\",\n" +
            "              `t_float` FLOAT COMMENT \"测试float\"\n" +
            "          )\n" +
            "          DUPLICATE KEY(`user_id`, `username`, `age`, `score`, `city` )\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData1 = "insert into ${tbName2} values(1, 'John Doe', 25, 95.5, 'New York',  1, 1234567890, true, 10, 1000000000, '2024-06-11', '2024-06-11', '2024-06-11 08:30:00', '2024-06-11 08:30:00', 'Test String 1', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99)," +
            "               (2, 'Jane Smith', 30, 85.2, 'Los Angeles',  2, 9876543210, false, 20, 2000000000, '2024-06-12', '2024-06-12', '2024-06-12 09:45:00', '2024-06-12 09:45:00', 'Test String 2', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99)," +
            "               (3, 'Mike Johnson', 35, 77.8, 'Chicago',  1, 1112223334, true, 30, 3000000000, '2024-06-13', '2024-06-13', '2024-06-13 11:15:00', '2024-06-13 11:15:00', 'Test String 3', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99)," +
            "               (4, 'Emily Brown', 28, 92.0, 'San Francisco',  2, 5556667778, true, 40, 4000000000, '2024-06-14', '2024-06-14', '2024-06-14 13:30:00', '2024-06-14 13:30:00', 'Test String 4', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99)," +
            "               (9, 'Emily Brown', 28, 92.0, 'San Francisco', 2, 5556667778, true, 40, 4000000000, '2024-06-14', '2024-06-14', '2024-06-14 13:30:00', '2024-06-14 13:30:00', 'Test String 4', {'a': 400, 'b': 200}, '[\\\"abc\\\", \\\"def\\\"]',95.5, 9.99)," +
            "               (5, 'David Wilson', 32, 88.9, 'Seattle',  1, 9998887776, false, 50, 5000000000, '2024-06-15', '2024-06-15', '2024-06-15 15:45:00', '2024-06-15 15:45:00', 'Test String 5', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99);"
    sql initTable1
    sql initTableData1
    checkTableData("${tbName}", "${tbName2}", "age")
    sql """ DROP TABLE IF EXISTS ${tbName} """


    /**
     *  dup modify vale AFTER key
     *
     */
    initTable = " CREATE TABLE IF NOT EXISTS ${tbName}\n" +
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
            "              `j` JSON NULL COMMENT \"\",\n" +
            "              `t_decimal` DECIMAL(38,10) COMMENT \"测试decimal\",\n" +
            "              `t_float` FLOAT COMMENT \"测试float\"\n" +
            "          )\n" +
            "          DUPLICATE KEY(`user_id`, `username`, `score`, `city`, `age`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData = "insert into ${tbName} values(1, 'John Doe', 95.5, 'New York', 25, 1, 1234567890, true, 10, 1000000000, '2024-06-11', '2024-06-11', '2024-06-11 08:30:00', '2024-06-11 08:30:00', 'Test String 1', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99)," +
            "               (2, 'Jane Smith', 85.2, 'Los Angeles', 30, 2, 9876543210, false, 20, 2000000000, '2024-06-12', '2024-06-12', '2024-06-12 09:45:00', '2024-06-12 09:45:00', 'Test String 2', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99)," +
            "               (3, 'Mike Johnson', 77.8, 'Chicago', 35, 1, 1112223334, true, 30, 3000000000, '2024-06-13', '2024-06-13', '2024-06-13 11:15:00', '2024-06-13 11:15:00', 'Test String 3', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99)," +
            "               (4, 'Emily Brown', 92.0, 'San Francisco', 28, 2, 5556667778, true, 40, 4000000000, '2024-06-14', '2024-06-14', '2024-06-14 13:30:00', '2024-06-14 13:30:00', 'Test String 4', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99)," +
            "               (5, 'David Wilson', 88.9, 'Seattle', 32, 1, 9998887776, false, 50, 5000000000, '2024-06-15', '2024-06-15', '2024-06-15 15:45:00', '2024-06-15 15:45:00', 'Test String 5', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99);"
    errorMessage = "errCode = 2, detailMessage = Can not change aggregation type"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName} MODIFY  column t_int BIGINT KEY  AFTER username """
        insertSql = "insert into ${tbName} values(4, 'Emily Brown', 40, 92.0, 'San Francisco', 28, 2, 5556667778, true,  4000000000, '2024-06-14', '2024-06-14', '2024-06-14 13:30:00', '2024-06-14 13:30:00', 'Test String 4', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99);"
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 60
        }, insertSql, true, "${tbName}")
    }, errorMessage)




    /**
     *  TODO Data doubling dup drop partition key
     *
     */
    initTable = " CREATE TABLE IF NOT EXISTS ${tbName}\n" +
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
            "              `j` JSON NULL COMMENT \"\",\n" +
            "              `t_decimal` DECIMAL(38,10) COMMENT \"测试decimal\",\n" +
            "              `t_float` FLOAT COMMENT \"测试float\"\n" +
            "          )\n" +
            "          DUPLICATE KEY(`user_id`, `username`, `score`, `city`, `age`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData = "insert into ${tbName} values(1, 'John Doe', 95.5, 'New York', 25, 1, 1234567890, true, 10, 1000000000, '2024-06-11', '2024-06-11', '2024-06-11 08:30:00', '2024-06-11 08:30:00', 'Test String 1', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99)," +
            "               (2, 'Jane Smith', 85.2, 'Los Angeles', 30, 2, 9876543210, false, 20, 2000000000, '2024-06-12', '2024-06-12', '2024-06-12 09:45:00', '2024-06-12 09:45:00', 'Test String 2', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99)," +
            "               (3, 'Mike Johnson', 77.8, 'Chicago', 35, 1, 1112223334, true, 30, 3000000000, '2024-06-13', '2024-06-13', '2024-06-13 11:15:00', '2024-06-13 11:15:00', 'Test String 3', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99)," +
            "               (4, 'Emily Brown', 92.0, 'San Francisco', 28, 2, 5556667778, true, 40, 4000000000, '2024-06-14', '2024-06-14', '2024-06-14 13:30:00', '2024-06-14 13:30:00', 'Test String 4', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99)," +
            "               (5, 'David Wilson', 88.9, 'Seattle', 32, 1, 9998887776, false, 50, 5000000000, '2024-06-15', '2024-06-15', '2024-06-15 15:45:00', '2024-06-15 15:45:00', 'Test String 5', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99);"
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName} DROP  column age  """
        insertSql = "insert into ${tbName} values(4, 'Emily Brown', 92.0, 'San Francisco', 28, 2, 5556667778, true,  4000000000, '2024-06-14', '2024-06-14', '2024-06-14 13:30:00', '2024-06-14 13:30:00', 'Test String 4', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99);"
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 60
        }, insertSql, false, "${tbName}")
    sql """ DROP TABLE IF EXISTS ${tbName} """



    /**
     *  dup drop DISTRIBUTED key
     *
     */
    initTable = " CREATE TABLE IF NOT EXISTS ${tbName}\n" +
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
            "              `j` JSON NULL COMMENT \"\",\n" +
            "              `t_decimal` DECIMAL(38,10) COMMENT \"测试decimal\",\n" +
            "              `t_float` FLOAT COMMENT \"测试float\"\n" +
            "          )\n" +
            "          DUPLICATE KEY(`user_id` )\n" +
            "          DISTRIBUTED BY HASH(`username`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData = "insert into ${tbName} values(1, 'John Doe', 95.5, 'New York', 25, 1, 1234567890, true, 10, 1000000000, '2024-06-11', '2024-06-11', '2024-06-11 08:30:00', '2024-06-11 08:30:00', 'Test String 1', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99)," +
            "               (2, 'Jane Smith', 85.2, 'Los Angeles', 30, 2, 9876543210, false, 20, 2000000000, '2024-06-12', '2024-06-12', '2024-06-12 09:45:00', '2024-06-12 09:45:00', 'Test String 2', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99)," +
            "               (3, 'Mike Johnson', 77.8, 'Chicago', 35, 1, 1112223334, true, 30, 3000000000, '2024-06-13', '2024-06-13', '2024-06-13 11:15:00', '2024-06-13 11:15:00', 'Test String 3', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99)," +
            "               (4, 'Emily Brown', 92.0, 'San Francisco', 28, 2, 5556667778, true, 40, 4000000000, '2024-06-14', '2024-06-14', '2024-06-14 13:30:00', '2024-06-14 13:30:00', 'Test String 4', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99)," +
            "               (5, 'David Wilson', 88.9, 'Seattle', 32, 1, 9998887776, false, 50, 5000000000, '2024-06-15', '2024-06-15', '2024-06-15 15:45:00', '2024-06-15 15:45:00', 'Test String 5', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99);"
    errorMessage = "errCode = 2, detailMessage = Table regression_test_schema_change_p0.test_schema_reordering_dup check failed"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName} DROP  column username  """
        insertSql = "insert into ${tbName} values(4, 'Emily Brown', 92.0, 'San Francisco', 28, 2, 5556667778, true,  4000000000, '2024-06-14', '2024-06-14', '2024-06-14 13:30:00', '2024-06-14 13:30:00', 'Test String 4', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]',95.5, 9.99);"
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 60
        }, insertSql, true, "${tbName}")
    }, errorMessage)

}
