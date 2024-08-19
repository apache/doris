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

suite("test_dup_schema_value_modify2", "p0") {
    def tbName1 = "test_dup_model_value_change2"
    def tbName2 = "test_dup_model_value_change_2"
    //Test the dup model by adding a value column
    sql """ DROP TABLE IF EXISTS ${tbName1} """
    def initTable = " CREATE TABLE IF NOT EXISTS ${tbName1}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `city` VARCHAR(20) COMMENT \"用户所在城市\",\n" +
            "              `age` SMALLINT COMMENT \"用户年龄\",\n" +
            "              `sex` TINYINT COMMENT \"用户性别\",\n" +
            "              `phone` LARGEINT COMMENT \"用户电话\",\n" +
            "              `address` VARCHAR(500) COMMENT \"用户地址\",\n" +
            "              `register_time` DATETIME COMMENT \"用户注册时间\"\n" +
            "          )\n" +
            "          DUPLICATE KEY(`user_id`, `username`)\n" +
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

    //Test the dup model by adding a value column with VARCHAR
    sql initTable
    sql initTableData
    def getTableStatusSql = " SHOW ALTER TABLE COLUMN WHERE IndexName='${tbName1}' ORDER BY createtime DESC LIMIT 1  "
    def errorMessage = ""
    def insertSql = "insert into ${tbName1} values(923456689, 'Alice', '四川省', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00');"


    /**
     *  Test the dup model by modify a value type from CHAR to other type
     */
    sql """ DROP TABLE IF EXISTS ${tbName1} """
    initTable = " CREATE TABLE IF NOT EXISTS ${tbName1}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `score` DECIMAL(38,10) COMMENT \"分数\",\n" +
            "              `city` CHAR(20) COMMENT \"用户所在城市\",\n" +
            "              `age` SMALLINT COMMENT \"用户年龄\",\n" +
            "              `sex` TINYINT COMMENT \"用户性别\",\n" +
            "              `phone` LARGEINT COMMENT \"用户电话\",\n" +
            "              `address` VARCHAR(500) COMMENT \"用户地址\",\n" +
            "              `register_time` DATETIME COMMENT \"用户注册时间\",\n" +
            "              `m` Map<STRING, INT> NULL COMMENT \"\",\n" +
            "              `j` JSON NULL COMMENT \"\"\n" +
            "          )\n" +
            "          DUPLICATE KEY(`user_id`, `username`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData = "insert into ${tbName1} values(123456789, 'Alice', 1.83, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (234567890, 'Bob', 1.89, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (345678901, 'Carol', 2.6689, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (456789012, 'Dave', 3.9456, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (567890123, 'Eve', 4.223, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (678901234, 'Frank', 2.5454, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00', {'a': 600, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (789012345, 'Grace', 2.19656, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00', {'a': 700, 'b': 200}, '[\"abc\", \"def\"]');"

    //TODO Test the dup model by modify a value type from CHAR  to BOOLEAN
    errorMessage = "errCode = 2, detailMessage = Can not change CHAR to BOOLEAN"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column city BOOLEAN  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 1.2, false, 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '{\"k1\":\"v1\", \"k2\": 200}'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    // TODO Test the dup model by modify a value type from CHAR  to TINYINT
    errorMessage = "expected:<[FINISH]ED> but was:<[CANCELL]ED>"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column city TINYINT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 2.2, 1, 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '{\"k1\":\"v1\", \"k2\": 200}'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 120
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //Test the dup model by modify a value type from CHAR  to SMALLINT
    errorMessage = "expected:<[FINISH]ED> but was:<[CANCELL]ED>"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column city SMALLINT   """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 3, 3, 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, [\"abc\", \"def\"]); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)

    //Test the dup model by modify a value type from CHAR  to INT
    errorMessage = "expected:<[FINISH]ED> but was:<[CANCELL]ED>"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column city INT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 4.1, 23, 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, [\"abc\", \"def\"]); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)


    //Test the dup model by modify a value type from CHAR  to BIGINT
    errorMessage = "expected:<[FINISH]ED> but was:<[CANCELL]ED>"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column city BIGINT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 5.6, 2423, 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, [\"abc\", \"def\"]); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)

    //Test the dup model by modify a value type from  CHAR to LARGEINT
    errorMessage = "expected:<[FINISH]ED> but was:<[CANCELL]ED>"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column city LARGEINT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 2.36, 4561, 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, [\"abc\", \"def\"]); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //Test the dup model by modify a value type from CHAR  to FLOAT
    errorMessage = "expected:<[FINISH]ED> but was:<[CANCELL]ED>"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column city FLOAT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 1.23, 1.25, 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //TODO Test the dup model by modify a value type from CHAR  to DECIMAL
    errorMessage = "errCode = 2, detailMessage = Can not change CHAR to DECIMAL128"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column city DECIMAL(38,0)  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 1.23, 56.98, 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)


    //TODO Test the dup model by modify a value type from CHAR  to DATE
    errorMessage = "errCode = 2, detailMessage = Can not change CHAR to DATEV2"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column city DATE  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 5.6, '2003-12-31', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)

    //TODO Test the dup model by modify a value type from CHAR  to DATEV2
    errorMessage = "errCode = 2, detailMessage = Can not change CHAR to DATEV2"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column city DATEV2  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 6.3, '2003-12-31', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)


    //TODO Test the dup model by modify a value type from CHAR  to DATETIME
    errorMessage = "errCode = 2, detailMessage = Can not change CHAR to DATETIMEV2"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column city DATETIME  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 9.63, '2003-12-31 20:12:12', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)

    //TODO Test the dup model by modify a value type from CHAR  to DATETIME
    errorMessage = "errCode = 2, detailMessage = Can not change CHAR to DATETIMEV2"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column city DATETIMEV2  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 5.69, '2003-12-31 20:12:12', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)


    //TODO Data doubling Test the dup model by modify a value type from CHAR  to VARCHAR
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} MODIFY  column city VARCHAR(100)  """
    insertSql = "insert into ${tbName1} values(923456689, 'Alice', 5.69, 'Yaan1', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
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
            "              `city` VARCHAR(100) COMMENT \"用户所在城市\",\n" +
            "              `age` SMALLINT COMMENT \"用户年龄\",\n" +
            "              `sex` TINYINT COMMENT \"用户性别\",\n" +
            "              `phone` LARGEINT COMMENT \"用户电话\",\n" +
            "              `address` VARCHAR(500) COMMENT \"用户地址\",\n" +
            "              `register_time` DATETIME COMMENT \"用户注册时间\",\n" +
            "              `m` Map<STRING, INT> NULL COMMENT \"\",\n" +
            "              `j` JSON NULL COMMENT \"\"\n" +
            "          )\n" +
            "          DUPLICATE KEY(`user_id`, `username`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData1 = "insert into ${tbName2} values(123456789, 'Alice', 1.83, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (234567890, 'Bob', 1.89, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (345678901, 'Carol', 2.6689, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (456789012, 'Dave', 3.9456, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (567890123, 'Eve', 4.223, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (678901234, 'Frank', 2.5454, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00', {'a': 600, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (923456689, 'Alice', 5.69, 'Yaan1', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\\\"abc\\\", \\\"def\\\"]')," +
            "               (789012345, 'Grace', 2.19656, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00', {'a': 700, 'b': 200}, '[\"abc\", \"def\"]');"


    sql initTable1
    sql initTableData1
//    checkTableData("${tbName1}","${tbName2}","city")
    sql """ DROP TABLE IF EXISTS ${tbName1} """

    //Test the dup model by modify a value type from CHAR  to STRING
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} MODIFY  column city STRING  """
    insertSql = "insert into ${tbName1} values(923456689, 'Alice', 6.59, 'Yaan2', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
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
            "              `city` STRING COMMENT \"用户所在城市\",\n" +
            "              `age` SMALLINT COMMENT \"用户年龄\",\n" +
            "              `sex` TINYINT COMMENT \"用户性别\",\n" +
            "              `phone` LARGEINT COMMENT \"用户电话\",\n" +
            "              `address` VARCHAR(500) COMMENT \"用户地址\",\n" +
            "              `register_time` DATETIME COMMENT \"用户注册时间\",\n" +
            "              `m` Map<STRING, INT> NULL COMMENT \"\",\n" +
            "              `j` JSON NULL COMMENT \"\"\n" +
            "          )\n" +
            "          DUPLICATE KEY(`user_id`, `username`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData1 = "insert into ${tbName2} values(123456789, 'Alice', 1.83, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (234567890, 'Bob', 1.89, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (345678901, 'Carol', 2.6689, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (456789012, 'Dave', 3.9456, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (567890123, 'Eve', 4.223, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (678901234, 'Frank', 2.5454, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00', {'a': 600, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (923456689, 'Alice', 5.69, 'Yaan2', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\\\"abc\\\", \\\"def\\\"]')," +
            "               (789012345, 'Grace', 2.19656, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00', {'a': 700, 'b': 200}, '[\"abc\", \"def\"]');"


    sql initTable1
    sql initTableData1
    checkTableData("${tbName1}","${tbName2}","city")
    sql """ DROP TABLE IF EXISTS ${tbName1} """



    //TODO Test the dup model by modify a  value type from CHAR  to map
    //Test the dup model by modify a value type from CHAR  to map
    errorMessage = "errCode = 2, detailMessage = Can not change CHAR to MAP"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column city Map<STRING, INT>  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 5.98, {'a': 100, 'b': 200}, 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //Test the dup model by modify a value type from CHAR  to JSON
    errorMessage = "errCode = 2, detailMessage = Can not change CHAR to JSON"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column city JSON  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 8.47, '{'a': 100, 'b': 200}', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)



    /**
     *  Test the dup model by modify a value type from VARCHAR to other type
     */
    sql """ DROP TABLE IF EXISTS ${tbName1} """
    initTable = " CREATE TABLE IF NOT EXISTS ${tbName1}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `score` DECIMAL(38,10) COMMENT \"分数\",\n" +
            "              `city` VARCHAR(255) COMMENT \"用户所在城市\",\n" +
            "              `age` SMALLINT COMMENT \"用户年龄\",\n" +
            "              `sex` TINYINT COMMENT \"用户性别\",\n" +
            "              `phone` LARGEINT COMMENT \"用户电话\",\n" +
            "              `address` VARCHAR(500) COMMENT \"用户地址\",\n" +
            "              `register_time` DATETIME COMMENT \"用户注册时间\",\n" +
            "              `m` Map<STRING, INT> NULL COMMENT \"\",\n" +
            "              `j` JSON NULL COMMENT \"\"\n" +
            "          )\n" +
            "          DUPLICATE KEY(`user_id`, `username`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData = "insert into ${tbName1} values(123456789, 'Alice', 1.83, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (234567890, 'Bob', 1.89, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (345678901, 'Carol', 2.6689, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (456789012, 'Dave', 3.9456, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (567890123, 'Eve', 4.223, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (678901234, 'Frank', 2.5454, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00', {'a': 600, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (789012345, 'Grace', 2.19656, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00', {'a': 700, 'b': 200}, '[\"abc\", \"def\"]');"

    //TODO Test the dup model by modify a value type from VARCHAR  to BOOLEAN
    errorMessage = "errCode = 2, detailMessage = Can not change VARCHAR to BOOLEAN"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column city BOOLEAN  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 1.2, false, 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '{\"k1\":\"v1\", \"k2\": 200}'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    // TODO Test the dup model by modify a value type from VARCHAR  to TINYINT
    errorMessage = "expected:<[FINISH]ED> but was:<[CANCELL]ED>"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column city TINYINT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 2.2, 1, 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '{\"k1\":\"v1\", \"k2\": 200}'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 120
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //Test the dup model by modify a value type from VARCHAR  to SMALLINT
    errorMessage = "expected:<[FINISH]ED> but was:<[CANCELL]ED>"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column city SMALLINT   """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 3, 3, 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, [\"abc\", \"def\"]); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)

    //Test the dup model by modify a value type from VARCHAR  to INT
    errorMessage = "expected:<[FINISH]ED> but was:<[CANCELL]ED>"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column city INT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 4.1, 23, 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, [\"abc\", \"def\"]); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)


    //Test the dup model by modify a value type from VARCHAR  to BIGINT
    errorMessage = "expected:<[FINISH]ED> but was:<[CANCELL]ED>"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column city BIGINT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 5.6, 2423, 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, [\"abc\", \"def\"]); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)

    //Test the dup model by modify a value type from  VARCHAR to LARGEINT
    errorMessage = "expected:<[FINISH]ED> but was:<[CANCELL]ED>"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column city LARGEINT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 2.36, 4561, 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, [\"abc\", \"def\"]); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //Test the dup model by modify a value type from VARCHAR  to FLOAT
    errorMessage = "expected:<[FINISH]ED> but was:<[CANCELL]ED>"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column city FLOAT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 1.23, 1.25, 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //TODO Test the dup model by modify a value type from VARCHAR  to DECIMAL
    errorMessage = "errCode = 2, detailMessage = Can not change VARCHAR to DECIMAL128"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column city DECIMAL(38,0)  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 1.23, 56.98, 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)


    //TODO Test the dup model by modify a value type from VARCHAR  to DATE
    errorMessage = "expected:<[FINISH]ED> but was:<[CANCELL]ED>"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column city DATE  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 5.6, '2003-12-31', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)

    //TODO Test the dup model by modify a value type from VARCHAR  to DATEV2
    errorMessage = "expected:<[FINISH]ED> but was:<[CANCELL]ED>"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column city DATEV2  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 6.3, '2003-12-31', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)


    //TODO Test the dup model by modify a value type from VARCHAR  to DATETIME
    errorMessage = "errCode = 2, detailMessage = Can not change VARCHAR to DATETIMEV2"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column city DATETIME  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 9.63, '2003-12-31 20:12:12', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)

    //TODO Test the dup model by modify a value type from VARCHAR  to DATETIME
    errorMessage = "errCode = 2, detailMessage = Can not change VARCHAR to DATETIMEV2"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column city DATETIMEV2  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 5.69, '2003-12-31 20:12:12', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)


    //Test the dup model by modify a value type from VARCHAR  to CHAR
    errorMessage = "errCode = 2, detailMessage = Can not change VARCHAR to CHAR"
    expectException({
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} MODIFY  column city CHAR(3)  """
    insertSql = "insert into ${tbName1} values(923456689, 'Alice', 5.69, 'Yaan1', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName1}")
    }, errorMessage)


    //TODO Data doubling Test the dup model by modify a value type from VARCHAR  to STRING
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} MODIFY  column city STRING  """
    insertSql = "insert into ${tbName1} values(923456689, 'Alice', 6.59, 'Yaan2', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
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
            "              `city` STRING COMMENT \"用户所在城市\",\n" +
            "              `age` SMALLINT COMMENT \"用户年龄\",\n" +
            "              `sex` TINYINT COMMENT \"用户性别\",\n" +
            "              `phone` LARGEINT COMMENT \"用户电话\",\n" +
            "              `address` VARCHAR(500) COMMENT \"用户地址\",\n" +
            "              `register_time` DATETIME COMMENT \"用户注册时间\",\n" +
            "              `m` Map<STRING, INT> NULL COMMENT \"\",\n" +
            "              `j` JSON NULL COMMENT \"\"\n" +
            "          )\n" +
            "          DUPLICATE KEY(`user_id`, `username`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData1 = "insert into ${tbName2} values(123456789, 'Alice', 1.83, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (234567890, 'Bob', 1.89, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (345678901, 'Carol', 2.6689, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (456789012, 'Dave', 3.9456, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (567890123, 'Eve', 4.223, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (678901234, 'Frank', 2.5454, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00', {'a': 600, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (923456689, 'Alice', 5.69, 'Yaan2', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\\\"abc\\\", \\\"def\\\"]')," +
            "               (789012345, 'Grace', 2.19656, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00', {'a': 700, 'b': 200}, '[\"abc\", \"def\"]');"


    sql initTable1
    sql initTableData1
//    checkTableData("${tbName1}","${tbName2}","city")
    sql """ DROP TABLE IF EXISTS ${tbName1} """



    //TODO Test the dup model by modify a  value type from VARCHAR  to map
    //Test the dup model by modify a value type from VARCHAR  to STRING
    errorMessage = "errCode = 2, detailMessage = Can not change VARCHAR to MAP"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column city Map<STRING, INT>  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 5.98, {'a': 100, 'b': 200}, 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //Test the dup model by modify a value type from VARCHAR  to JSON
    errorMessage = "expected:<[FINISH]ED> but was:<[CANCELL]ED>"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column city JSON  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 8.47, '{'a': 100, 'b': 200}', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    /**
     *  Test the dup model by modify a value type from STRING to other type
     */
    sql """ DROP TABLE IF EXISTS ${tbName1} """
    initTable = " CREATE TABLE IF NOT EXISTS ${tbName1}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `score` DECIMAL(38,10) COMMENT \"分数\",\n" +
            "              `city` STRING COMMENT \"用户所在城市\",\n" +
            "              `age` SMALLINT COMMENT \"用户年龄\",\n" +
            "              `sex` TINYINT COMMENT \"用户性别\",\n" +
            "              `phone` LARGEINT COMMENT \"用户电话\",\n" +
            "              `address` VARCHAR(500) COMMENT \"用户地址\",\n" +
            "              `register_time` DATETIME COMMENT \"用户注册时间\",\n" +
            "              `m` Map<STRING, INT> NULL COMMENT \"\",\n" +
            "              `j` JSON NULL COMMENT \"\"\n" +
            "          )\n" +
            "          DUPLICATE KEY(`user_id`, `username`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData = "insert into ${tbName1} values(123456789, 'Alice', 1.83, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (234567890, 'Bob', 1.89, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (345678901, 'Carol', 2.6689, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (456789012, 'Dave', 3.9456, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (567890123, 'Eve', 4.223, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (678901234, 'Frank', 2.5454, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00', {'a': 600, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (789012345, 'Grace', 2.19656, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00', {'a': 700, 'b': 200}, '[\"abc\", \"def\"]');"

    //TODO Test the dup model by modify a value type from STRING  to BOOLEAN
    errorMessage = "errCode = 2, detailMessage = Can not change STRING to BOOLEAN"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column city BOOLEAN  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 1.2, false, 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '{\"k1\":\"v1\", \"k2\": 200}'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    // TODO Test the dup model by modify a value type from STRING  to TINYINT
    errorMessage = "errCode = 2, detailMessage = Can not change STRING to TINYINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column city TINYINT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 2.2, 1, 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '{\"k1\":\"v1\", \"k2\": 200}'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 120
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //Test the dup model by modify a value type from STRING  to SMALLINT
    errorMessage = "errCode = 2, detailMessage = Can not change STRING to SMALLINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column city SMALLINT   """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 3, 3, 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, [\"abc\", \"def\"]); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)

    //Test the dup model by modify a value type from STRING  to INT
    errorMessage = "errCode = 2, detailMessage = Can not change STRING to INT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column city INT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 4.1, 23, 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, [\"abc\", \"def\"]); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)


    //Test the dup model by modify a value type from STRING  to BIGINT
    errorMessage = "errCode = 2, detailMessage = Can not change STRING to BIGINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column city BIGINT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 5.6, 2423, 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, [\"abc\", \"def\"]); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)

    //Test the dup model by modify a value type from  STRING to LARGEINT
    errorMessage = "errCode = 2, detailMessage = Can not change STRING to LARGEINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column city LARGEINT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 2.36, 4561, 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, [\"abc\", \"def\"]); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //Test the dup model by modify a value type from STRING  to FLOAT
    errorMessage = "errCode = 2, detailMessage = Can not change STRING to FLOAT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column city FLOAT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 1.23, 1.25, 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //TODO Test the dup model by modify a value type from STRING  to DECIMAL
    errorMessage = "errCode = 2, detailMessage = Can not change STRING to DECIMAL128"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column city DECIMAL(38,0)  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 1.23, 56.98, 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)


    //TODO Test the dup model by modify a value type from STRING  to DATE
    errorMessage = "errCode = 2, detailMessage = Can not change STRING to DATEV2"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column city DATE  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 5.6, '2003-12-31', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)

    //TODO Test the dup model by modify a value type from STRING  to DATEV2
    errorMessage = "errCode = 2, detailMessage = Can not change STRING to DATEV2"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column city DATEV2  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 6.3, '2003-12-31', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)


    //TODO Test the dup model by modify a value type from STRING  to DATETIME
    errorMessage = "errCode = 2, detailMessage = Can not change STRING to DATETIMEV2"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column city DATETIME  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 9.63, '2003-12-31 20:12:12', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)

    //TODO Test the dup model by modify a value type from STRING  to DATETIME
    errorMessage = "errCode = 2, detailMessage = Can not change STRING to DATETIMEV2"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column city DATETIMEV2  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 5.69, '2003-12-31 20:12:12', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)


    //Test the dup model by modify a value type from STRING  to CHAR
    errorMessage = "errCode = 2, detailMessage = Can not change STRING to CHAR"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column city CHAR(3)  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 5.69, 'Yaan1', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //Test the dup model by modify a value type from  STRING to VARCHAR
    errorMessage = "errCode = 2, detailMessage = Can not change STRING to VARCHAR"
    expectException({
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} MODIFY  column city VARCHAR(256)  """
    insertSql = "insert into ${tbName1} values(923456689, 'Alice', 6.59, 'Yaan2', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //TODO Test the dup model by modify a  value type from STRING  to map
    //Test the dup model by modify a value type from STRING  to STRING
    errorMessage = "errCode = 2, detailMessage = Can not change STRING to MAP"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column city Map<STRING, INT>  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 5.98, {'a': 100, 'b': 200}, 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //Test the dup model by modify a value type from STRING  to JSON
    errorMessage = "expected:<[FINISH]ED> but was:<[CANCELL]ED>"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column city JSON  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 8.47, '{'a': 100, 'b': 200}', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)




}
