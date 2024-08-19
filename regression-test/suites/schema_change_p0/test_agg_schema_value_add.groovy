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

suite("test_agg_schema_value_add", "p0") {
    def tbName1 = "test_agg_model_schema_value_add"
    def tbName2 = "test_agg_model_schema_value_add_1"
    //Test the AGGREGATE model by adding a value column
    sql """ DROP TABLE IF EXISTS ${tbName1} """
    def initTable = " CREATE TABLE IF NOT EXISTS ${tbName1}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `city` VARCHAR(20) REPLACE_IF_NOT_NULL DEFAULT \"广州\" COMMENT \"用户所在城市\",\n" +
            "              `age` SMALLINT SUM  COMMENT \"用户年龄\",\n" +
            "              `sex` TINYINT MAX  COMMENT \"用户性别\",\n" +
            "              `phone` LARGEINT MAX  COMMENT \"用户电话\",\n" +
            "              `address` VARCHAR(500)  REPLACE DEFAULT \"青海省西宁市城东区\"COMMENT \"用户地址\",\n" +
            "              `register_time` DATETIME REPLACE DEFAULT \"1970-01-01 00:00:00\" COMMENT \"用户注册时间\"\n" +
            "          )\n" +
            "          AGGREGATE KEY(`user_id`, `username`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    def initTableData = "insert into ${tbName1} values(123456789, 'Alice', 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
            "               (234567890, 'Bob', 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
            "               (345678901, 'Carol', 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
            "               (456789012, 'Dave', 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
            "               (567890123, 'Eve', 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
            "               (678901234, 'Frank', 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
            "               (789012345, 'Grace', 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"
    def initTable1 = ""
    def initTableData1 = ""
    //Test the AGGREGATE model by adding a value column with VARCHAR

    def getTableStatusSql = " SHOW ALTER TABLE COLUMN WHERE IndexName='${tbName1}' ORDER BY createtime DESC LIMIT 1  "
    def errorMessage = ""
    def insertSql = ""
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} add  column province VARCHAR(20)  REPLACE_IF_NOT_NULL DEFAULT "广东省" AFTER username """
    insertSql = "insert into ${tbName1} values(923456689, 'Alice', '四川省', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00');"
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName1}")

    sql """ DROP TABLE IF EXISTS ${tbName2} """
    initTable1 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `province` VARCHAR(20) REPLACE_IF_NOT_NULL COMMENT \"省份\",\n" +
            "              `city` VARCHAR(20) REPLACE_IF_NOT_NULL DEFAULT \"广州\" COMMENT \"用户所在城市\",\n" +
            "              `age` SMALLINT SUM  COMMENT \"用户年龄\",\n" +
            "              `sex` TINYINT MAX  COMMENT \"用户性别\",\n" +
            "              `phone` LARGEINT MAX  COMMENT \"用户电话\",\n" +
            "              `address` VARCHAR(500)  REPLACE DEFAULT \"青海省西宁市城东区\"COMMENT \"用户地址\",\n" +
            "              `register_time` DATETIME REPLACE DEFAULT \"1970-01-01 00:00:00\" COMMENT \"用户注册时间\"\n" +
            "          )\n" +
            "          AGGREGATE KEY(`user_id`, `username`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData1 = "insert into ${tbName2} values(123456789, 'Alice', '广东省', 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
            "               (234567890, 'Bob', '广东省', 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
            "               (345678901, 'Carol', '广东省', 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
            "               (456789012, 'Dave', '广东省', 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
            "               (567890123, 'Eve', '广东省', 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
            "               (678901234, 'Frank', '广东省', 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
            "               (923456689, 'Alice', '四川省', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
            "               (789012345, 'Grace', '广东省', 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"
    sql initTable1
    sql initTableData1
    checkTableData("${tbName1}", "${tbName2}", "province")
    sql """ DROP TABLE IF EXISTS ${tbName1} """

    //Test the AGGREGATE model by adding a value column with BOOLEAN
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} add  column special_area BOOLEAN  REPLACE DEFAULT "0" AFTER username """
    insertSql = "insert into ${tbName1} values(923456689, 'Alice', 1, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName1}")

    sql """ DROP TABLE IF EXISTS ${tbName2} """
    initTable1 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `special_area` BOOLEAN REPLACE_IF_NOT_NULL COMMENT \"特区\",\n" +
            "              `city` VARCHAR(20) REPLACE_IF_NOT_NULL DEFAULT \"广州\" COMMENT \"用户所在城市\",\n" +
            "              `age` SMALLINT SUM  COMMENT \"用户年龄\",\n" +
            "              `sex` TINYINT MAX  COMMENT \"用户性别\",\n" +
            "              `phone` LARGEINT MAX  COMMENT \"用户电话\",\n" +
            "              `address` VARCHAR(500)  REPLACE DEFAULT \"青海省西宁市城东区\"COMMENT \"用户地址\",\n" +
            "              `register_time` DATETIME REPLACE DEFAULT \"1970-01-01 00:00:00\" COMMENT \"用户注册时间\"\n" +
            "          )\n" +
            "          AGGREGATE KEY(`user_id`, `username`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData1 = "insert into ${tbName2} values(123456789, 'Alice', '0', 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
            "               (234567890, 'Bob', '0', 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
            "               (345678901, 'Carol', '0', 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
            "               (456789012, 'Dave', '0', 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
            "               (567890123, 'Eve', '0', 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
            "               (678901234, 'Frank', '0', 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
            "               (923456689, 'Alice', '1', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
            "               (789012345, 'Grace', '0', 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"
    sql initTable1
    sql initTableData1
    checkTableData("${tbName1}", "${tbName2}", "special_area")
    sql """ DROP TABLE IF EXISTS ${tbName1} """

    //Test the AGGREGATE model by adding a value column with TINYINT
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} add  column special_area TINYINT  REPLACE_IF_NOT_NULL DEFAULT "0" AFTER username """
    insertSql = " insert into ${tbName1} values(923456689, 'Alice', 1, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName1}")

    sql """ DROP TABLE IF EXISTS ${tbName2} """
    initTable1 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `special_area` TINYINT REPLACE_IF_NOT_NULL DEFAULT \"0\" COMMENT \"特区\",\n" +
            "              `city` VARCHAR(20) REPLACE_IF_NOT_NULL DEFAULT \"广州\" COMMENT \"用户所在城市\",\n" +
            "              `age` SMALLINT SUM  COMMENT \"用户年龄\",\n" +
            "              `sex` TINYINT MAX  COMMENT \"用户性别\",\n" +
            "              `phone` LARGEINT MAX  COMMENT \"用户电话\",\n" +
            "              `address` VARCHAR(500)  REPLACE DEFAULT \"青海省西宁市城东区\"COMMENT \"用户地址\",\n" +
            "              `register_time` DATETIME REPLACE DEFAULT \"1970-01-01 00:00:00\" COMMENT \"用户注册时间\"\n" +
            "          )\n" +
            "          AGGREGATE KEY(`user_id`, `username`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData1 = "insert into ${tbName2} values(123456789, 'Alice', '0', 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
            "               (234567890, 'Bob', '0', 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
            "               (345678901, 'Carol', '0', 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
            "               (456789012, 'Dave', '0', 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
            "               (567890123, 'Eve', '0', 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
            "               (678901234, 'Frank', '0', 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
            "               (923456689, 'Alice', '1', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
            "               (789012345, 'Grace', '0', 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"
    sql initTable1
    sql initTableData1
    checkTableData("${tbName1}", "${tbName2}", "special_area")
    sql """ DROP TABLE IF EXISTS ${tbName1} """


    //Test the AGGREGATE model by adding a value column with SMALLINT
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} add  column area_num SMALLINT  REPLACE_IF_NOT_NULL DEFAULT "999" AFTER username """
    insertSql = " insert into ${tbName1} values(923456689, 'Alice', 567, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName1}")


    sql """ DROP TABLE IF EXISTS ${tbName2} """
    initTable1 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `area_num` SMALLINT  REPLACE_IF_NOT_NULL DEFAULT \"999\" COMMENT \"地区编号\",\n" +
            "              `city` VARCHAR(20) REPLACE_IF_NOT_NULL DEFAULT \"广州\" COMMENT \"用户所在城市\",\n" +
            "              `age` SMALLINT SUM  COMMENT \"用户年龄\",\n" +
            "              `sex` TINYINT MAX  COMMENT \"用户性别\",\n" +
            "              `phone` LARGEINT MAX  COMMENT \"用户电话\",\n" +
            "              `address` VARCHAR(500)  REPLACE DEFAULT \"青海省西宁市城东区\"COMMENT \"用户地址\",\n" +
            "              `register_time` DATETIME REPLACE DEFAULT \"1970-01-01 00:00:00\" COMMENT \"用户注册时间\"\n" +
            "          )\n" +
            "          AGGREGATE KEY(`user_id`, `username`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData1 = "insert into ${tbName2} values(123456789, 'Alice', 999, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
            "               (234567890, 'Bob', 999, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
            "               (345678901, 'Carol', 999, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
            "               (456789012, 'Dave', 999, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
            "               (567890123, 'Eve', 999, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
            "               (678901234, 'Frank', 999, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
            "               (923456689, 'Alice', 567, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
            "               (789012345, 'Grace', 999, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"
    sql initTable1
    sql initTableData1
    checkTableData("${tbName1}", "${tbName2}", "area_num")
    sql """ DROP TABLE IF EXISTS ${tbName1} """


    //Test the AGGREGATE model by adding a value column with INT
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} add  column house_price INT  DEFAULT "999" AFTER username """
    insertSql = " insert into ${tbName1} values(923456689, 'Alice', 22536, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName1}")

    sql """ DROP TABLE IF EXISTS ${tbName2} """
    initTable1 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `house_price` INT  REPLACE_IF_NOT_NULL DEFAULT \"999\" COMMENT \"房子价格\",\n" +
            "              `city` VARCHAR(20) REPLACE_IF_NOT_NULL DEFAULT \"广州\" COMMENT \"用户所在城市\",\n" +
            "              `age` SMALLINT SUM  COMMENT \"用户年龄\",\n" +
            "              `sex` TINYINT MAX  COMMENT \"用户性别\",\n" +
            "              `phone` LARGEINT MAX  COMMENT \"用户电话\",\n" +
            "              `address` VARCHAR(500)  REPLACE DEFAULT \"青海省西宁市城东区\"COMMENT \"用户地址\",\n" +
            "              `register_time` DATETIME REPLACE DEFAULT \"1970-01-01 00:00:00\" COMMENT \"用户注册时间\"\n" +
            "          )\n" +
            "          AGGREGATE KEY(`user_id`, `username`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData1 = "insert into ${tbName2} values(123456789, 'Alice', 999, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
            "               (234567890, 'Bob', 999, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
            "               (345678901, 'Carol', 999, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
            "               (456789012, 'Dave', 999, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
            "               (567890123, 'Eve', 999, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
            "               (678901234, 'Frank', 999, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
            "               (923456689, 'Alice', 22536, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 11:56:00')," +
            "               (789012345, 'Grace', 999, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"
    sql initTable1
    sql initTableData1
    checkTableData("${tbName1}", "${tbName2}", "house_price")
    sql """ DROP TABLE IF EXISTS ${tbName1} """

    //Test the AGGREGATE model by adding a value column with BIGINT
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} add  column house_price1 BIGINT  DEFAULT "99999991" AFTER username """
    insertSql = " insert into ${tbName1} values(923456689, 'Alice', 88889494646, 'Yaan',  25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName1}")

    sql """ DROP TABLE IF EXISTS ${tbName2} """
    initTable1 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `house_price1` BIGINT  REPLACE_IF_NOT_NULL COMMENT \"房子价格\",\n" +
            "              `city` VARCHAR(20) REPLACE_IF_NOT_NULL DEFAULT \"广州\" COMMENT \"用户所在城市\",\n" +
            "              `age` SMALLINT SUM  COMMENT \"用户年龄\",\n" +
            "              `sex` TINYINT MAX  COMMENT \"用户性别\",\n" +
            "              `phone` LARGEINT MAX  COMMENT \"用户电话\",\n" +
            "              `address` VARCHAR(500)  REPLACE DEFAULT \"青海省西宁市城东区\"COMMENT \"用户地址\",\n" +
            "              `register_time` DATETIME REPLACE DEFAULT \"1970-01-01 00:00:00\" COMMENT \"用户注册时间\"\n" +
            "          )\n" +
            "          AGGREGATE KEY(`user_id`, `username`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData1 = "insert into ${tbName2} values(123456789, 'Alice', 99999991, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
            "               (234567890, 'Bob', 99999991, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
            "               (345678901, 'Carol', 99999991, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
            "               (456789012, 'Dave', 99999991, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
            "               (567890123, 'Eve', 99999991, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
            "               (678901234, 'Frank', 99999991, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
            "               (923456689, 'Alice', 88889494646, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
            "               (789012345, 'Grace', 99999991, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"
    sql initTable1
    sql initTableData1
    checkTableData("${tbName1}", "${tbName2}", "house_price1")
    sql """ DROP TABLE IF EXISTS ${tbName1} """


    //Test the AGGREGATE model by adding a value column with LARGEINT
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} add  column car_price LARGEINT  REPLACE_IF_NOT_NULL DEFAULT "9999" AFTER username """
    insertSql = " insert into ${tbName1} values(923456689, 'Alice', 555888555, 'Yaan',  25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00');"
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName1}")

    sql """ DROP TABLE IF EXISTS ${tbName2} """
    initTable1 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `car_price` LARGEINT  REPLACE_IF_NOT_NULL COMMENT \"车价格\",\n" +
            "              `city` VARCHAR(20) REPLACE_IF_NOT_NULL DEFAULT \"广州\" COMMENT \"用户所在城市\",\n" +
            "              `age` SMALLINT SUM  COMMENT \"用户年龄\",\n" +
            "              `sex` TINYINT MAX  COMMENT \"用户性别\",\n" +
            "              `phone` LARGEINT MAX  COMMENT \"用户电话\",\n" +
            "              `address` VARCHAR(500)  REPLACE DEFAULT \"青海省西宁市城东区\"COMMENT \"用户地址\",\n" +
            "              `register_time` DATETIME REPLACE DEFAULT \"1970-01-01 00:00:00\" COMMENT \"用户注册时间\"\n" +
            "          )\n" +
            "          AGGREGATE KEY(`user_id`, `username`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData1 = "insert into ${tbName2} values(123456789, 'Alice', 9999, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
            "               (234567890, 'Bob', 9999, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
            "               (345678901, 'Carol', 9999, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
            "               (456789012, 'Dave', 9999, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
            "               (567890123, 'Eve', 9999, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
            "               (678901234, 'Frank', 9999, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
            "               (923456689, 'Alice', 555888555, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
            "               (789012345, 'Grace', 9999, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"
    sql initTable1
    sql initTableData1
    checkTableData("${tbName1}", "${tbName2}", "car_price")
    sql """ DROP TABLE IF EXISTS ${tbName1} """


    //TODO Test the AGGREGATE model by adding a value column with FLOAT
    errorMessage = "errCode = 2, detailMessage = Float or double can not used as a key, use decimal instead."
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} add  column phone FLOAT  DEFAULT "166.68" AFTER username """
        insertSql = " insert into ${tbName1} values(923456689, 'Alice', 189.98, 'Yaan',  25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00');"
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //Test the AGGREGATE model by adding a value column with DOUBLE
    errorMessage = "errCode = 2, detailMessage = Float or double can not used as a key, use decimal instead."
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} add  column watch DOUBLE  DEFAULT "166.689" AFTER username """
        insertSql = " insert into ${tbName1} values(923456689, 'Alice', 189.479, 'Yaan',  25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, false, "${tbName1}")
    }, errorMessage)


    //Test the AGGREGATE model by adding a value column with DECIMAL
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} add  column watch DECIMAL(38,10)  DEFAULT "16899.6464689" AFTER username """
    insertSql = " insert into ${tbName1} values(923456689, 'Alice', 16499.6464689, 'Yaan',  25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00');"
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName1}")


    sql """ DROP TABLE IF EXISTS ${tbName2} """
    initTable1 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `watch` DECIMAL(38,10)  REPLACE_IF_NOT_NULL COMMENT \"车价格\",\n" +
            "              `city` VARCHAR(20) REPLACE_IF_NOT_NULL DEFAULT \"广州\" COMMENT \"用户所在城市\",\n" +
            "              `age` SMALLINT SUM  COMMENT \"用户年龄\",\n" +
            "              `sex` TINYINT MAX  COMMENT \"用户性别\",\n" +
            "              `phone` LARGEINT MAX  COMMENT \"用户电话\",\n" +
            "              `address` VARCHAR(500)  REPLACE DEFAULT \"青海省西宁市城东区\"COMMENT \"用户地址\",\n" +
            "              `register_time` DATETIME REPLACE DEFAULT \"1970-01-01 00:00:00\" COMMENT \"用户注册时间\"\n" +
            "          )\n" +
            "          AGGREGATE KEY(`user_id`, `username`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData1 = "insert into ${tbName2} values(123456789, 'Alice', 16899.6464689, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
            "               (234567890, 'Bob', 16899.6464689, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
            "               (345678901, 'Carol', 16899.6464689, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
            "               (456789012, 'Dave', 16899.6464689, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
            "               (567890123, 'Eve', 16899.6464689, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
            "               (678901234, 'Frank', 16899.6464689, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
            "               (923456689, 'Alice', 16499.6464689, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
            "               (789012345, 'Grace', 16899.6464689, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"
    sql initTable1
    sql initTableData1
    checkTableData("${tbName1}", "${tbName2}", "watch")
    sql """ DROP TABLE IF EXISTS ${tbName1} """


    //Test the AGGREGATE model by adding a value column with DATE
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} add  column watch DATE  REPLACE_IF_NOT_NULL DEFAULT "1997-01-01" AFTER username """
    insertSql = " insert into ${tbName1} values(923456689, 'Alice', \"2024-01-01\", 'Yaan',  25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName1}")

    sql """ DROP TABLE IF EXISTS ${tbName2} """
    initTable1 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `watch` DATE  REPLACE_IF_NOT_NULL COMMENT \"手表日期\",\n" +
            "              `city` VARCHAR(20) REPLACE_IF_NOT_NULL DEFAULT \"广州\" COMMENT \"用户所在城市\",\n" +
            "              `age` SMALLINT SUM  COMMENT \"用户年龄\",\n" +
            "              `sex` TINYINT MAX  COMMENT \"用户性别\",\n" +
            "              `phone` LARGEINT MAX  COMMENT \"用户电话\",\n" +
            "              `address` VARCHAR(500)  REPLACE DEFAULT \"青海省西宁市城东区\"COMMENT \"用户地址\",\n" +
            "              `register_time` DATETIME REPLACE DEFAULT \"1970-01-01 00:00:00\" COMMENT \"用户注册时间\"\n" +
            "          )\n" +
            "          AGGREGATE KEY(`user_id`, `username`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData1 = "insert into ${tbName2} values(123456789, 'Alice', '1997-01-01', 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
            "               (234567890, 'Bob', '1997-01-01', 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
            "               (345678901, 'Carol', '1997-01-01', 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
            "               (456789012, 'Dave', '1997-01-01', 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
            "               (567890123, 'Eve', '1997-01-01', 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
            "               (678901234, 'Frank', '1997-01-01', 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
            "               (923456689, 'Alice', '2024-01-01', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
            "               (789012345, 'Grace', '1997-01-01', 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"
    sql initTable1
    sql initTableData1
    checkTableData("${tbName1}", "${tbName2}", "watch")
    sql """ DROP TABLE IF EXISTS ${tbName1} """


    //Test the AGGREGATE model by adding a value column with DATETIME
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} add  column anniversary DATETIME  REPLACE_IF_NOT_NULL DEFAULT "1997-01-01 00:00:00" AFTER username """
    insertSql = " insert into ${tbName1} values(923456689, 'Alice', \"2024-01-04 09:00:00\", 'Yaan',  25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName1}")


    sql """ DROP TABLE IF EXISTS ${tbName2} """
    initTable1 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `anniversary` DATETIME  REPLACE_IF_NOT_NULL COMMENT \"手表日期\",\n" +
            "              `city` VARCHAR(20) REPLACE_IF_NOT_NULL DEFAULT \"广州\" COMMENT \"用户所在城市\",\n" +
            "              `age` SMALLINT SUM  COMMENT \"用户年龄\",\n" +
            "              `sex` TINYINT MAX  COMMENT \"用户性别\",\n" +
            "              `phone` LARGEINT MAX  COMMENT \"用户电话\",\n" +
            "              `address` VARCHAR(500)  REPLACE DEFAULT \"青海省西宁市城东区\"COMMENT \"用户地址\",\n" +
            "              `register_time` DATETIME REPLACE DEFAULT \"1970-01-01 00:00:00\" COMMENT \"用户注册时间\"\n" +
            "          )\n" +
            "          AGGREGATE KEY(`user_id`, `username`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData1 = "insert into ${tbName2} values(123456789, 'Alice', '1997-01-01 00:00:00', 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
            "               (234567890, 'Bob', '1997-01-01 00:00:00', 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
            "               (345678901, 'Carol', '1997-01-01 00:00:00', 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
            "               (456789012, 'Dave', '1997-01-01 00:00:00', 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
            "               (567890123, 'Eve', '1997-01-01 00:00:00', 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
            "               (678901234, 'Frank', '1997-01-01 00:00:00', 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
            "               (923456689, 'Alice', '2024-01-04 09:00:00', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
            "               (789012345, 'Grace', '1997-01-01 00:00:00', 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"
    sql initTable1
    sql initTableData1
    checkTableData("${tbName1}", "${tbName2}", "anniversary")
    sql """ DROP TABLE IF EXISTS ${tbName1} """


    //Test the AGGREGATE model by adding a value column with CHAR
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} add  column teacher CHAR  REPLACE_IF_NOT_NULL DEFAULT "F" AFTER username """
    insertSql = " insert into ${tbName1} values(923456689, 'Alice', 'T', 'Yaan',  25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00');  "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName1}")


    sql """ DROP TABLE IF EXISTS ${tbName2} """
    initTable1 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `teacher` CHAR  REPLACE_IF_NOT_NULL COMMENT \"老师\",\n" +
            "              `city` VARCHAR(20) REPLACE_IF_NOT_NULL DEFAULT \"广州\" COMMENT \"用户所在城市\",\n" +
            "              `age` SMALLINT SUM  COMMENT \"用户年龄\",\n" +
            "              `sex` TINYINT MAX  COMMENT \"用户性别\",\n" +
            "              `phone` LARGEINT MAX  COMMENT \"用户电话\",\n" +
            "              `address` VARCHAR(500)  REPLACE DEFAULT \"青海省西宁市城东区\"COMMENT \"用户地址\",\n" +
            "              `register_time` DATETIME REPLACE DEFAULT \"1970-01-01 00:00:00\" COMMENT \"用户注册时间\"\n" +
            "          )\n" +
            "          AGGREGATE KEY(`user_id`, `username`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData1 = "insert into ${tbName2} values(123456789, 'Alice', 'F', 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
            "               (234567890, 'Bob', 'F', 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
            "               (345678901, 'Carol', 'F', 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
            "               (456789012, 'Dave', 'F', 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
            "               (567890123, 'Eve', 'F', 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
            "               (678901234, 'Frank', 'F', 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
            "               (923456689, 'Alice', 'T', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
            "               (789012345, 'Grace', 'F', 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"
    sql initTable1
    sql initTableData1
    checkTableData("${tbName1}", "${tbName2}", "teacher")
    sql """ DROP TABLE IF EXISTS ${tbName1} """


    //Test the AGGREGATE model by adding a value column with STRING
    errorMessage = "errCode = 2, detailMessage = String Type should not be used in key column[comment]."
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} add  column comment STRING  DEFAULT "我是小说家" AFTER username """
        insertSql = " insert into ${tbName1} values(923456689, 'Alice', '我是侦探家', 'Yaan',  25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00');  "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //TODO Test the AGGREGATE model by adding a value column with HLL
    errorMessage = "errCode = 2, detailMessage = can not cast from origin type VARCHAR(1) to target type=HLL"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} add  column comment HLL HLL_UNION   AFTER username """
        insertSql = " insert into ${tbName1} values(923456689, 'Alice', '2', 'Yaan',  25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00');  "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //TODO Test the AGGREGATE model by adding a value column with bitmap
    sql """ DROP TABLE IF EXISTS ${tbName1} """
    errorMessage = "errCode = 2, detailMessage = Column count doesn't match value count"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} add  column device_id   bitmap BITMAP_UNION  AFTER username """
        insertSql = " insert into ${tbName1} values(923456689, 'Alice', to_bitmap(243), 'Yaan',  25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00');  "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //Test the AGGREGATE model by adding a value column with Map
    sql """ DROP TABLE IF EXISTS ${tbName1} """
    errorMessage = "errCode = 2, detailMessage = Map can only be used in the non-key column of the duplicate table at present."
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} add  column m   Map<STRING, INT>   AFTER username """
        insertSql = " insert into ${tbName1} values(923456689, 'Alice', {'a': 100, 'b': 200}, 'Yaan',  25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00');  "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, false, "${tbName1}")
    }, errorMessage)


    //Test the AGGREGATE model by adding a value column with JSON
    errorMessage = "errCode = 2, detailMessage = JSONB or VARIANT type should not be used in key column[j]."
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} add  column   j  JSON   AFTER username """
        insertSql = " insert into ${tbName1} values(923456689, 'Alice', '{\"k1\":\"v31\", \"k2\": 300}', 'Yaan',  25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, false, "${tbName1}")
    }, errorMessage)


    //Test the AGGREGATE model by adding a value column with ARRAY
    errorMessage = "errCode = 2, detailMessage = Array can only be used in the non-key column of the duplicate table at present."
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} add  column   c_array  ARRAY<int(11)>   AFTER username """
        insertSql = " insert into ${tbName1} values(923456689, 'Alice', [6,7,8], 'Yaan',  25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, false, "${tbName1}")
    }, errorMessage)


    //Test the AGGREGATE model by adding a value column with STRUCT
    errorMessage = "errCode = 2, detailMessage = Struct can only be used in the non-key column of the duplicate table at present."
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} add  column  s_info  STRUCT<s_id:int(11), s_name:string, s_address:string>   AFTER username """
        insertSql = " insert into ${tbName1} values(923456689, 'Alice', [6,7,8], 'Yaan',  25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, false, "${tbName1}")
    }, errorMessage)


}
