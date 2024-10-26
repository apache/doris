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

suite("test_agg_schema_key_drop", "p0") {
    def tbName1 = "test_agg_schema_key_drop"
    def tbName2 = "test_agg_schema_key_drop_1"
    sql """ DROP TABLE IF EXISTS ${tbName1} """
    def getTableStatusSql = " SHOW ALTER TABLE COLUMN WHERE IndexName='${tbName1}' ORDER BY createtime DESC LIMIT 1  "
    def errorMessage = ""
    def insertSql = "insert into ${tbName1} values(923456689, 'Alice', '四川省', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00');"


    /**
     *  Test the aggregate model by dorp a key type
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
            "              `t_datetime` DATETIME COMMENT \"用户注册时间\"\n" +
            "          )\n" +
            "          aggregate KEY(`user_id`, `username`,  `score`, `city`, `age`, `sex`, `phone`,`is_ok`, `t_int`, `t_bigint`, `t_date`, `t_datev2`, `t_datetimev2`,  `t_datetime`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    def initTableData = "insert into ${tbName1} values(1, 'John Doe', 95.5, 'New York', 25, 1, 1234567890, true, 10, 1000000000, '2024-06-11', '2024-06-11', '2024-06-11 08:30:00', '2024-06-11 08:30:00')," +
            "               (2, 'Jane Smith', 85.2, 'Los Angeles', 30, 2, 9876543210, false, 20, 2000000000, '2024-06-12', '2024-06-12', '2024-06-12 09:45:00', '2024-06-12 09:45:00')," +
            "               (3, 'Mike Johnson', 77.8, 'Chicago', 35, 1, 1112223334, true, 30, 3000000000, '2024-06-13', '2024-06-13', '2024-06-13 11:15:00', '2024-06-13 11:15:00')," +
            "               (4, 'Emily Brown', 92.0, 'San Francisco', 28, 2, 5556667778, true, 40, 4000000000, '2024-06-14', '2024-06-14', '2024-06-14 13:30:00', '2024-06-14 13:30:00')," +
            "               (5, 'David Wilson', 88.9, 'Seattle', 32, 1, 9998887776, false, 50, 5000000000, '2024-06-15', '2024-06-15', '2024-06-15 15:45:00', '2024-06-15 15:45:00');"

    def initTable1 = ""
    def initTableData1 = ""
    //Test the aggregate model by drop a key type from BOOLEAN
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} DROP  column is_ok  """
    insertSql = "insert into ${tbName1} values(6, 'Sophia Lee', 91.3, 'Boston', 29, 2, 7778889990,  60, 6000000000, '2024-06-16', '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00'); "
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
            "              `t_datetime` DATETIME COMMENT \"用户注册时间\"\n" +
            "          )\n" +
            "          aggregate KEY(`user_id`, `username`,  `score`, `city`, `age`, `sex`, `phone`, `t_int`, `t_bigint`, `t_date`, `t_datev2`, `t_datetimev2`,  `t_datetime`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData1 = "insert into ${tbName2} values(1, 'John Doe', 95.5, 'New York', 25, 1, 1234567890,  10, 1000000000, '2024-06-11', '2024-06-11', '2024-06-11 08:30:00', '2024-06-11 08:30:00')," +
            "               (2, 'Jane Smith', 85.2, 'Los Angeles', 30, 2, 9876543210,  20, 2000000000, '2024-06-12', '2024-06-12', '2024-06-12 09:45:00', '2024-06-12 09:45:00')," +
            "               (3, 'Mike Johnson', 77.8, 'Chicago', 35, 1, 1112223334,  30, 3000000000, '2024-06-13', '2024-06-13', '2024-06-13 11:15:00', '2024-06-13 11:15:00')," +
            "               (4, 'Emily Brown', 92.0, 'San Francisco', 28, 2, 5556667778,  40, 4000000000, '2024-06-14', '2024-06-14', '2024-06-14 13:30:00', '2024-06-14 13:30:00')," +
            "               (6, 'Sophia Lee', 91.3, 'Boston', 29, 2, 7778889990,  60, 6000000000, '2024-06-16', '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00')," +
            "               (5, 'David Wilson', 88.9, 'Seattle', 32, 1, 9998887776,  50, 5000000000, '2024-06-15', '2024-06-15', '2024-06-15 15:45:00', '2024-06-15 15:45:00');"

    sql initTable1
    sql initTableData1
    checkTableData("${tbName1}", "${tbName2}", "t_int")
    sql """ DROP TABLE IF EXISTS ${tbName1} """


    //Test the aggregate model by drop a key type from TINYINT
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} DROP  column sex  """
    insertSql = "insert into ${tbName1} values(6, 'Sophia Lee', 91.3, 'Boston', 29,  7778889990, true, 60, 6000000000, '2024-06-16', '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00'); "
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
            "              `t_datetime` DATETIME COMMENT \"用户注册时间\"\n" +
            "          )\n" +
            "          aggregate KEY(`user_id`, `username`,  `score`, `city`, `age`,  `phone`,`is_ok`, `t_int`, `t_bigint`, `t_date`, `t_datev2`, `t_datetimev2`,  `t_datetime`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData1 = "insert into ${tbName2} values(1, 'John Doe', 95.5, 'New York', 25,  1234567890, true, 10, 1000000000, '2024-06-11', '2024-06-11', '2024-06-11 08:30:00', '2024-06-11 08:30:00')," +
            "               (2, 'Jane Smith', 85.2, 'Los Angeles', 30,  9876543210, false, 20, 2000000000, '2024-06-12', '2024-06-12', '2024-06-12 09:45:00', '2024-06-12 09:45:00')," +
            "               (3, 'Mike Johnson', 77.8, 'Chicago', 35,  1112223334, true, 30, 3000000000, '2024-06-13', '2024-06-13', '2024-06-13 11:15:00', '2024-06-13 11:15:00')," +
            "               (4, 'Emily Brown', 92.0, 'San Francisco', 28,  5556667778, true, 40, 4000000000, '2024-06-14', '2024-06-14', '2024-06-14 13:30:00', '2024-06-14 13:30:00')," +
            "               (6, 'Sophia Lee', 91.3, 'Boston', 29,  7778889990, true, 60, 6000000000, '2024-06-16', '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00')," +
            "               (5, 'David Wilson', 88.9, 'Seattle', 32,  9998887776, false, 50, 5000000000, '2024-06-15', '2024-06-15', '2024-06-15 15:45:00', '2024-06-15 15:45:00');"
    sql initTable1
    sql initTableData1
    checkTableData("${tbName1}", "${tbName2}", "phone")
    sql """ DROP TABLE IF EXISTS ${tbName1} """


    //Test the aggregate model by drop a key type from SMALLINT
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} DROP  column age  """
    insertSql = "insert into ${tbName1} values(6, 'Sophia Lee', 91.3, 'Boston', 2, 7778889990, true, 60, 6000000000, '2024-06-16', '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00'); "
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
            "              `t_datetime` DATETIME COMMENT \"用户注册时间\"\n" +
            "          )\n" +
            "          aggregate KEY(`user_id`, `username`,  `score`, `city`, `sex`, `phone`,`is_ok`, `t_int`, `t_bigint`, `t_date`, `t_datev2`, `t_datetimev2`,  `t_datetime`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData1 = "insert into ${tbName2} values(1, 'John Doe', 95.5, 'New York',  1, 1234567890, true, 10, 1000000000, '2024-06-11', '2024-06-11', '2024-06-11 08:30:00', '2024-06-11 08:30:00')," +
            "               (2, 'Jane Smith', 85.2, 'Los Angeles',  2, 9876543210, false, 20, 2000000000, '2024-06-12', '2024-06-12', '2024-06-12 09:45:00', '2024-06-12 09:45:00')," +
            "               (3, 'Mike Johnson', 77.8, 'Chicago',  1, 1112223334, true, 30, 3000000000, '2024-06-13', '2024-06-13', '2024-06-13 11:15:00', '2024-06-13 11:15:00')," +
            "               (4, 'Emily Brown', 92.0, 'San Francisco',  2, 5556667778, true, 40, 4000000000, '2024-06-14', '2024-06-14', '2024-06-14 13:30:00', '2024-06-14 13:30:00')," +
            "               (6, 'Sophia Lee', 91.3, 'Boston', 2, 7778889990, true, 60, 6000000000, '2024-06-16', '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00')," +
            "               (5, 'David Wilson', 88.9, 'Seattle',  1, 9998887776, false, 50, 5000000000, '2024-06-15', '2024-06-15', '2024-06-15 15:45:00', '2024-06-15 15:45:00');"

    sql initTable1
    sql initTableData1
    checkTableData("${tbName1}", "${tbName2}", "sex")
    sql """ DROP TABLE IF EXISTS ${tbName1} """


    //TODO Test the aggregate model by drop a key type from INT
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} DROP  column t_int  """
    insertSql = "insert into ${tbName1} values(6, 'Sophia Lee', 91.3, 'Boston', 29, 2, 7778889990, true,  6000000000, '2024-06-16', '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00'); "
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
            "              `t_datetime` DATETIME COMMENT \"用户注册时间\"\n" +
            "          )\n" +
            "          aggregate KEY(`user_id`, `username`,  `score`, `city`, `age`, `sex`, `phone`,`is_ok`, `t_bigint`, `t_date`, `t_datev2`, `t_datetimev2`,  `t_datetime`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData1 = "insert into ${tbName2} values(1, 'John Doe', 95.5, 'New York', 25, 1, 1234567890, true,  1000000000, '2024-06-11', '2024-06-11', '2024-06-11 08:30:00', '2024-06-11 08:30:00')," +
            "               (2, 'Jane Smith', 85.2, 'Los Angeles', 30, 2, 9876543210, false,  2000000000, '2024-06-12', '2024-06-12', '2024-06-12 09:45:00', '2024-06-12 09:45:00')," +
            "               (3, 'Mike Johnson', 77.8, 'Chicago', 35, 1, 1112223334, true,  3000000000, '2024-06-13', '2024-06-13', '2024-06-13 11:15:00', '2024-06-13 11:15:00')," +
            "               (4, 'Emily Brown', 92.0, 'San Francisco', 28, 2, 5556667778, true,  4000000000, '2024-06-14', '2024-06-14', '2024-06-14 13:30:00', '2024-06-14 13:30:00')," +
            "               (6, 'Sophia Lee', 91.3, 'Boston', 29, 2, 7778889990, true,  6000000000, '2024-06-16', '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00')," +
            "               (5, 'David Wilson', 88.9, 'Seattle', 32, 1, 9998887776, false,  5000000000, '2024-06-15', '2024-06-15', '2024-06-15 15:45:00', '2024-06-15 15:45:00');"
    sql initTable1
    sql initTableData1
    checkTableData("${tbName1}", "${tbName2}", "t_bigint")
    sql """ DROP TABLE IF EXISTS ${tbName1} """


    //TODO Test the aggregate model by drop a key type from BIGINT
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} DROP  column t_bigint  """
    insertSql = "insert into ${tbName1} values(6, 'Sophia Lee', 91.3, 'Boston', 29, 2, 7778889990, true, 60,  '2024-06-16', '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00'); "
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
            "              `t_datetime` DATETIME COMMENT \"用户注册时间\"\n" +
            "          )\n" +
            "          aggregate KEY(`user_id`, `username`,  `score`, `city`, `age`, `sex`, `phone`,`is_ok`, `t_int`,  `t_date`, `t_datev2`, `t_datetimev2`,  `t_datetime`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData1 = "insert into ${tbName2} values(1, 'John Doe', 95.5, 'New York', 25, 1, 1234567890, true, 10,  '2024-06-11', '2024-06-11', '2024-06-11 08:30:00', '2024-06-11 08:30:00')," +
            "               (2, 'Jane Smith', 85.2, 'Los Angeles', 30, 2, 9876543210, false, 20,  '2024-06-12', '2024-06-12', '2024-06-12 09:45:00', '2024-06-12 09:45:00')," +
            "               (3, 'Mike Johnson', 77.8, 'Chicago', 35, 1, 1112223334, true, 30,  '2024-06-13', '2024-06-13', '2024-06-13 11:15:00', '2024-06-13 11:15:00')," +
            "               (4, 'Emily Brown', 92.0, 'San Francisco', 28, 2, 5556667778, true, 40,  '2024-06-14', '2024-06-14', '2024-06-14 13:30:00', '2024-06-14 13:30:00')," +
            "               (6, 'Sophia Lee', 91.3, 'Boston', 29, 2, 7778889990, true, 60,  '2024-06-16', '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00')," +
            "               (5, 'David Wilson', 88.9, 'Seattle', 32, 1, 9998887776, false, 50,  '2024-06-15', '2024-06-15', '2024-06-15 15:45:00', '2024-06-15 15:45:00');"
    sql initTable1
    sql initTableData1
    checkTableData("${tbName1}", "${tbName2}", "t_date")
    sql """ DROP TABLE IF EXISTS ${tbName1} """


    //Test the aggregate model by drop a key type from LARGEINT
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} DROP  column phone  """
    insertSql = "insert into ${tbName1} values(6, 'Sophia Lee', 91.3, 'Boston', 29, 2, true, 60, 6000000000, '2024-06-16', '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00'); "
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
            "              `t_datetime` DATETIME COMMENT \"用户注册时间\"\n" +
            "          )\n" +
            "          aggregate KEY(`user_id`, `username`,  `score`, `city`, `age`, `sex`, `is_ok`, `t_int`, `t_bigint`, `t_date`, `t_datev2`, `t_datetimev2`,  `t_datetime`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData1 = "insert into ${tbName2} values(1, 'John Doe', 95.5, 'New York', 25, 1,  true, 10, 1000000000, '2024-06-11', '2024-06-11', '2024-06-11 08:30:00', '2024-06-11 08:30:00')," +
            "               (2, 'Jane Smith', 85.2, 'Los Angeles', 30, 2,  false, 20, 2000000000, '2024-06-12', '2024-06-12', '2024-06-12 09:45:00', '2024-06-12 09:45:00')," +
            "               (3, 'Mike Johnson', 77.8, 'Chicago', 35, 1,  true, 30, 3000000000, '2024-06-13', '2024-06-13', '2024-06-13 11:15:00', '2024-06-13 11:15:00')," +
            "               (4, 'Emily Brown', 92.0, 'San Francisco', 28, 2,  true, 40, 4000000000, '2024-06-14', '2024-06-14', '2024-06-14 13:30:00', '2024-06-14 13:30:00')," +
            "               (6, 'Sophia Lee', 91.3, 'Boston', 29, 2, true, 60, 6000000000, '2024-06-16', '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00')," +
            "               (5, 'David Wilson', 88.9, 'Seattle', 32, 1,  false, 50, 5000000000, '2024-06-15', '2024-06-15', '2024-06-15 15:45:00', '2024-06-15 15:45:00');"
    sql initTable1
    sql initTableData1
    checkTableData("${tbName1}", "${tbName2}", "is_ok")
    sql """ DROP TABLE IF EXISTS ${tbName1} """


    //Test the aggregate model by drop a key type from DATE
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} DROP  column t_date  """
    insertSql = "insert into ${tbName1} values(6, 'Sophia Lee', 91.3, 'Boston', 29, 2, 7778889990, true, 60, 6000000000,  '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00'); "
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
            "              `t_datetime` DATETIME COMMENT \"用户注册时间\"\n" +
            "          )\n" +
            "          aggregate KEY(`user_id`, `username`,  `score`, `city`, `age`, `sex`, `phone`,`is_ok`, `t_int`, `t_bigint`,  `t_datev2`, `t_datetimev2`,  `t_datetime`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData1 = "insert into ${tbName2} values(1, 'John Doe', 95.5, 'New York', 25, 1, 1234567890, true, 10, 1000000000,  '2024-06-11', '2024-06-11 08:30:00', '2024-06-11 08:30:00')," +
            "               (2, 'Jane Smith', 85.2, 'Los Angeles', 30, 2, 9876543210, false, 20, 2000000000, '2024-06-12',  '2024-06-12 09:45:00', '2024-06-12 09:45:00')," +
            "               (3, 'Mike Johnson', 77.8, 'Chicago', 35, 1, 1112223334, true, 30, 3000000000, '2024-06-13',  '2024-06-13 11:15:00', '2024-06-13 11:15:00')," +
            "               (4, 'Emily Brown', 92.0, 'San Francisco', 28, 2, 5556667778, true, 40, 4000000000, '2024-06-14',  '2024-06-14 13:30:00', '2024-06-14 13:30:00')," +
            "               (6, 'Sophia Lee', 91.3, 'Boston', 29, 2, 7778889990, true, 60, 6000000000,  '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00')," +
            "               (5, 'David Wilson', 88.9, 'Seattle', 32, 1, 9998887776, false, 50, 5000000000, '2024-06-15',  '2024-06-15 15:45:00', '2024-06-15 15:45:00');"
    sql initTable1
    sql initTableData1
    checkTableData("${tbName1}", "${tbName2}", "t_datev2")
    sql """ DROP TABLE IF EXISTS ${tbName1} """


    //Test the aggregate model by drop a key type from DATEV2
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} DROP  column t_datev2  """
    insertSql = "insert into ${tbName1} values(6, 'Sophia Lee', 91.3, 'Boston', 29, 2, 7778889990, true, 60, 6000000000,  '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00'); "
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
            "              `t_datetimev2` DATETIMEV2 COMMENT \"测试DATETIMEV2\",\n" +
            "              `t_datetime` DATETIME COMMENT \"用户注册时间\"\n" +
            "          )\n" +
            "          aggregate KEY(`user_id`, `username`,  `score`, `city`, `age`, `sex`, `phone`,`is_ok`, `t_int`, `t_bigint`,  `t_date`, `t_datetimev2`,  `t_datetime`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData1 = "insert into ${tbName2} values(1, 'John Doe', 95.5, 'New York', 25, 1, 1234567890, true, 10, 1000000000,  '2024-06-11', '2024-06-11 08:30:00', '2024-06-11 08:30:00')," +
            "               (2, 'Jane Smith', 85.2, 'Los Angeles', 30, 2, 9876543210, false, 20, 2000000000, '2024-06-12',  '2024-06-12 09:45:00', '2024-06-12 09:45:00')," +
            "               (3, 'Mike Johnson', 77.8, 'Chicago', 35, 1, 1112223334, true, 30, 3000000000, '2024-06-13',  '2024-06-13 11:15:00', '2024-06-13 11:15:00')," +
            "               (4, 'Emily Brown', 92.0, 'San Francisco', 28, 2, 5556667778, true, 40, 4000000000, '2024-06-14',  '2024-06-14 13:30:00', '2024-06-14 13:30:00')," +
            "               (6, 'Sophia Lee', 91.3, 'Boston', 29, 2, 7778889990, true, 60, 6000000000,  '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00')," +
            "               (5, 'David Wilson', 88.9, 'Seattle', 32, 1, 9998887776, false, 50, 5000000000, '2024-06-15',  '2024-06-15 15:45:00', '2024-06-15 15:45:00');"
    sql initTable1
    sql initTableData1
    checkTableData("${tbName1}", "${tbName2}", "t_datetimev2")
    sql """ DROP TABLE IF EXISTS ${tbName1} """

    //Test the aggregate model by drop a key type from t_datetimev2
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} DROP  column t_datetimev2  """
    insertSql = "insert into ${tbName1} values(6, 'Sophia Lee', 91.3, 'Boston', 29, 2, 7778889990, true, 60, 6000000000,  '2024-06-16', '2024-06-16',  '2024-06-16 17:00:00'); "
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
            "              `t_datetime` DATETIME COMMENT \"用户注册时间\"\n" +
            "          )\n" +
            "          aggregate KEY(`user_id`, `username`,  `score`, `city`, `age`, `sex`, `phone`,`is_ok`, `t_int`, `t_bigint`,  `t_date`, `t_datev2`, `t_datetime`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData1 = "insert into ${tbName2} values(1, 'John Doe', 95.5, 'New York', 25, 1, 1234567890, true, 10, 1000000000, '2024-06-11', '2024-06-11',  '2024-06-11 08:30:00')," +
            "               (2, 'Jane Smith', 85.2, 'Los Angeles', 30, 2, 9876543210, false, 20, 2000000000, '2024-06-12', '2024-06-12', '2024-06-12 09:45:00')," +
            "               (3, 'Mike Johnson', 77.8, 'Chicago', 35, 1, 1112223334, true, 30, 3000000000, '2024-06-13', '2024-06-13',  '2024-06-13 11:15:00')," +
            "               (4, 'Emily Brown', 92.0, 'San Francisco', 28, 2, 5556667778, true, 40, 4000000000, '2024-06-14', '2024-06-14',  '2024-06-14 13:30:00')," +
            "               (6, 'Sophia Lee', 91.3, 'Boston', 29, 2, 7778889990, true, 60, 6000000000,  '2024-06-16', '2024-06-16',  '2024-06-16 17:00:00')," +
            "               (5, 'David Wilson', 88.9, 'Seattle', 32, 1, 9998887776, false, 50, 5000000000, '2024-06-15', '2024-06-15',  '2024-06-15 15:45:00');"
    sql initTable1
    sql initTableData1
    checkTableData("${tbName1}", "${tbName2}", "t_datetime")
    sql """ DROP TABLE IF EXISTS ${tbName1} """


    //Test the aggregate model by drop a key type from t_datetime
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} DROP  column t_datetime  """
    insertSql = "insert into ${tbName1} values(6, 'Sophia Lee', 91.3, 'Boston', 29, 2, 7778889990, true, 60, 6000000000,  '2024-06-16', '2024-06-16',  '2024-06-16 17:00:00'); "
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
            "              `t_datetimev2` DATETIMEV2 COMMENT \"用户注册时间\"\n" +
            "          )\n" +
            "          aggregate KEY(`user_id`, `username`,  `score`, `city`, `age`, `sex`, `phone`,`is_ok`, `t_int`, `t_bigint`,  `t_date`, `t_datev2`, `t_datetimev2`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData1 = "insert into ${tbName2} values(1, 'John Doe', 95.5, 'New York', 25, 1, 1234567890, true, 10, 1000000000, '2024-06-11', '2024-06-11',  '2024-06-11 08:30:00')," +
            "               (2, 'Jane Smith', 85.2, 'Los Angeles', 30, 2, 9876543210, false, 20, 2000000000, '2024-06-12', '2024-06-12', '2024-06-12 09:45:00')," +
            "               (3, 'Mike Johnson', 77.8, 'Chicago', 35, 1, 1112223334, true, 30, 3000000000, '2024-06-13', '2024-06-13',  '2024-06-13 11:15:00')," +
            "               (4, 'Emily Brown', 92.0, 'San Francisco', 28, 2, 5556667778, true, 40, 4000000000, '2024-06-14', '2024-06-14',  '2024-06-14 13:30:00')," +
            "               (6, 'Sophia Lee', 91.3, 'Boston', 29, 2, 7778889990, true, 60, 6000000000,  '2024-06-16', '2024-06-16',  '2024-06-16 17:00:00')," +
            "               (5, 'David Wilson', 88.9, 'Seattle', 32, 1, 9998887776, false, 50, 5000000000, '2024-06-15', '2024-06-15',  '2024-06-15 15:45:00');"
    sql initTable1
    sql initTableData1
    checkTableData("${tbName1}", "${tbName2}", "t_datetimev2")
    sql """ DROP TABLE IF EXISTS ${tbName1} """


    //Test the aggregate model by drop a key type from CHAR
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} DROP  column city  """
        insertSql = "insert into ${tbName1} values(6, 'Sophia Lee', 91.3, 29, 2, 7778889990, true, 60, 6000000000, '2024-06-16', '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00'); "
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
            "              `t_datetime` DATETIME COMMENT \"用户注册时间\"\n" +
            "          )\n" +
            "          aggregate KEY(`user_id`, `username`,  `score`,  `age`, `sex`, `phone`,`is_ok`, `t_int`, `t_bigint`, `t_date`, `t_datev2`, `t_datetimev2`,  `t_datetime`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData1 = "insert into ${tbName2} values(1, 'John Doe', 95.5,  25, 1, 1234567890, true, 10, 1000000000, '2024-06-11', '2024-06-11', '2024-06-11 08:30:00', '2024-06-11 08:30:00')," +
            "               (2, 'Jane Smith', 85.2,  30, 2, 9876543210, false, 20, 2000000000, '2024-06-12', '2024-06-12', '2024-06-12 09:45:00', '2024-06-12 09:45:00')," +
            "               (3, 'Mike Johnson', 77.8,  35, 1, 1112223334, true, 30, 3000000000, '2024-06-13', '2024-06-13', '2024-06-13 11:15:00', '2024-06-13 11:15:00')," +
            "               (4, 'Emily Brown', 92.0,  28, 2, 5556667778, true, 40, 4000000000, '2024-06-14', '2024-06-14', '2024-06-14 13:30:00', '2024-06-14 13:30:00')," +
            "               (6, 'Sophia Lee', 91.3, 29, 2, 7778889990, true, 60, 6000000000, '2024-06-16', '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00')," +
            "               (5, 'David Wilson', 88.9,  32, 1, 9998887776, false, 50, 5000000000, '2024-06-15', '2024-06-15', '2024-06-15 15:45:00', '2024-06-15 15:45:00');"
    sql initTable1
    sql initTableData1
    checkTableData("${tbName1}", "${tbName2}", "age")
    sql """ DROP TABLE IF EXISTS ${tbName1} """


    //Test the aggregate model by drop a key type from VARCHAR
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} DROP  column username  """
        insertSql = "insert into ${tbName1} values(6, 91.3, 'Boston', 29, 2, 7778889990, true, 60, 6000000000, '2024-06-16', '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, false, "${tbName1}")

    sql """ DROP TABLE IF EXISTS ${tbName2} """
    initTable1 =  " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
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
            "              `t_datetime` DATETIME COMMENT \"用户注册时间\"\n" +
            "          )\n" +
            "          aggregate KEY(`user_id`,  `score`, `city`, `age`, `sex`, `phone`,`is_ok`, `t_int`, `t_bigint`, `t_date`, `t_datev2`, `t_datetimev2`,  `t_datetime`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData1 = "insert into ${tbName2} values(1,  95.5, 'New York', 25, 1, 1234567890, true, 10, 1000000000, '2024-06-11', '2024-06-11', '2024-06-11 08:30:00', '2024-06-11 08:30:00')," +
            "               (2,  85.2, 'Los Angeles', 30, 2, 9876543210, false, 20, 2000000000, '2024-06-12', '2024-06-12', '2024-06-12 09:45:00', '2024-06-12 09:45:00')," +
            "               (3,  77.8, 'Chicago', 35, 1, 1112223334, true, 30, 3000000000, '2024-06-13', '2024-06-13', '2024-06-13 11:15:00', '2024-06-13 11:15:00')," +
            "               (4,  92.0, 'San Francisco', 28, 2, 5556667778, true, 40, 4000000000, '2024-06-14', '2024-06-14', '2024-06-14 13:30:00', '2024-06-14 13:30:00')," +
            "               (6, 91.3, 'Boston', 29, 2, 7778889990, true, 60, 6000000000, '2024-06-16', '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00')," +
            "               (5,  88.9, 'Seattle', 32, 1, 9998887776, false, 50, 5000000000, '2024-06-15', '2024-06-15', '2024-06-15 15:45:00', '2024-06-15 15:45:00');"
    sql initTable1
    sql initTableData1
    checkTableData("${tbName1}", "${tbName2}", "age")
    sql """ DROP TABLE IF EXISTS ${tbName1} """


}
