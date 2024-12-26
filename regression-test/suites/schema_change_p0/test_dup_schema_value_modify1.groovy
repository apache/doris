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

suite("test_dup_schema_value_modify1", "p0") {
    def tbName1 = "unique_model_value_change1"
    def tbName2 = "unique_model_value_change_1"
    def on_write = getRandomBoolean()
    println String.format("current enable_unique_key_merge_on_write is : %s ", on_write)
    //Test the unique model by adding a value column
    sql """ DROP TABLE IF EXISTS ${tbName1} """
    def getTableStatusSql = " SHOW ALTER TABLE COLUMN WHERE IndexName='${tbName1}' ORDER BY createtime DESC LIMIT 1  "
    def errorMessage = ""
    /**
     *  Test the unique model by modify a value type
     */
    def initTable2 = ""
    def initTableData2 = ""
    sql """ DROP TABLE IF EXISTS ${tbName1} """
    def initTable = " CREATE TABLE IF NOT EXISTS ${tbName1}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `is_teacher` BOOLEAN COMMENT \"是否是老师\",\n" +
            "              `city` VARCHAR(20) COMMENT \"用户所在城市\",\n" +
            "              `age` SMALLINT COMMENT \"用户年龄\",\n" +
            "              `sex` TINYINT COMMENT \"用户性别\",\n" +
            "              `phone` LARGEINT COMMENT \"用户电话\",\n" +
            "              `address` VARCHAR(500) COMMENT \"用户地址\",\n" +
            "              `register_time` DATETIME COMMENT \"用户注册时间\"\n" +
            "          )\n" +
            "          UNIQUE KEY(`user_id`, `username`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    def initTableData = "insert into ${tbName1} values(123456789, 'Alice', 0, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
            "               (234567890, 'Bob', 0, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
            "               (345678901, 'Carol', 1, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
            "               (456789012, 'Dave', 0, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
            "               (567890123, 'Eve', 0, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
            "               (678901234, 'Frank', 1, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
            "               (789012345, 'Grace', 0, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"

    def insertSql = ""

    /**
     *  Test the dup model by modify a value type from FLOAT to other type
     */
    sql """ DROP TABLE IF EXISTS ${tbName1} """
    initTable = " CREATE TABLE IF NOT EXISTS ${tbName1}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `score` FLOAT COMMENT \"分数\",\n" +
            "              `city` VARCHAR(20) COMMENT \"用户所在城市\",\n" +
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

    initTableData = "insert into ${tbName1} values(123456789, 'Alice', 1.8, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (234567890, 'Bob', 1.89, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (345678901, 'Carol', 2.6, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (456789012, 'Dave', 3.9, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (567890123, 'Eve', 4.2, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (678901234, 'Frank', 2.5, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00', {'a': 600, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (789012345, 'Grace', 2.1, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00', {'a': 700, 'b': 200}, '[\"abc\", \"def\"]');"

    //TODO Test the dup model by modify a value type from FLOAT  to BOOLEAN
    errorMessage = "errCode = 2, detailMessage = Can not change FLOAT to BOOLEAN"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column score BOOLEAN  """
        insertSql = "insert into ${tbName1} values(993456689, 'Alice', false, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '{\"k1\":\"v1\", \"k2\": 200}'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    // TODO Test the dup model by modify a value type from FLOAT  to TINYINT
    errorMessage = "errCode = 2, detailMessage = Can not change FLOAT to TINYINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column score TINYINT  """
        insertSql = "insert into ${tbName1} values(993456689, 'Alice', 2, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '{\"k1\":\"v1\", \"k2\": 200}'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //TODO Test the dup model by modify a value type from FLOAT  to SMALLINT
    errorMessage = "errCode = 2, detailMessage = Can not change FLOAT to SMALLINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column score SMALLINT   """
        insertSql = "insert into ${tbName1} values(993456689, 'Alice', 3, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, [\"abc\", \"def\"]); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)

    //TODO Test the dup model by modify a value type from FLOAT  to INT
    errorMessage = "errCode = 2, detailMessage = Can not change FLOAT to INT"
    expectException({
        sql initTable
        sql initTableData

        sql """ alter  table ${tbName1} MODIFY  column score INT  """
        insertSql = "insert into ${tbName1} values(993456689, 'Alice', 4, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, [\"abc\", \"def\"]); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //TODO Test the dup model by modify a value type from FLOAT  to BIGINT
    errorMessage = "errCode = 2, detailMessage = Can not change FLOAT to BIGINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column score BIGINT  """
        insertSql = "insert into ${tbName1} values(993456689, 'Alice', 545645, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, [\"abc\", \"def\"]); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)

    //TODO Test the dup model by modify a value type from  FLOAT to LARGEINT
    errorMessage = "errCode = 2, detailMessage = Can not change FLOAT to LARGEINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column score LARGEINT  """
        insertSql = "insert into ${tbName1} values(993456689, 'Alice', 156546, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, [\"abc\", \"def\"]); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //TODO Loss of accuracy Test the dup model by modify a value type from FLOAT  to DOUBLE

    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} MODIFY  column score DOUBLE  """
    insertSql = "insert into ${tbName1} values(993456689, 'Alice', 1.23, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName1}")

    sql """ DROP TABLE IF EXISTS ${tbName2} """
    initTable2 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `score` DOUBLE COMMENT \"分数\",\n" +
            "              `city` VARCHAR(20) COMMENT \"用户所在城市\",\n" +
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

    initTableData2 = "insert into ${tbName2} values(123456789, 'Alice', 1.8, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (234567890, 'Bob', 1.89, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (345678901, 'Carol', 2.6, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (456789012, 'Dave', 3.9, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (567890123, 'Eve', 4.2, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (678901234, 'Frank', 2.5, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00', {'a': 600, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (993456689, 'Alice', 1.23, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\\\"abc\\\", \\\"def\\\"]')," +
            "               (789012345, 'Grace', 2.1, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00', {'a': 700, 'b': 200}, '[\"abc\", \"def\"]');"

    sql initTable2
    sql initTableData2
    sql """ DROP TABLE IF EXISTS ${tbName1} """


    //TODO Test the dup model by modify a value type from FLOAT  to DECIMAL
    errorMessage = "errCode = 2, detailMessage = Can not change FLOAT to DECIMAL128"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column score DECIMAL(38,0)  """
        insertSql = "insert into ${tbName1} values(993456689, 'Alice', 1.23, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)


    //TODO Test the dup model by modify a value type from FLOAT  to DATE
    errorMessage = "errCode = 2, detailMessage = Can not change FLOAT to DATEV2"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column score DATE  """
        insertSql = "insert into ${tbName1} values(993456689, 'Alice', '2003-12-31', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)

    //TODO Test the dup model by modify a value type from FLOAT  to DATE
    errorMessage = "errCode = 2, detailMessage = Can not change FLOAT to DATEV2"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column score DATEV2  """
        insertSql = "insert into ${tbName1} values(993456689, 'Alice', '2003-12-31', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)


    //TODO Test the dup model by modify a value type from FLOAT  to DATETIME
    errorMessage = "errCode = 2, detailMessage = Can not change FLOAT to DATETIMEV2"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column score DATETIME  """
        insertSql = "insert into ${tbName1} values(993456689, 'Alice', '2003-12-31 20:12:12', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)

    //TODO Test the dup model by modify a value type from FLOAT  to DATETIME
    errorMessage = "errCode = 2, detailMessage = Can not change FLOAT to DATETIMEV2"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column score DATETIMEV2  """
        insertSql = "insert into ${tbName1} values(993456689, 'Alice', '2003-12-31 20:12:12', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)

    //TODO Test the dup model by modify a  value type from FLOAT  to CHAR
    errorMessage = "errCode = 2, detailMessage = Can not change FLOAT to CHAR"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column score CHAR(15)  """
        insertSql = "insert into ${tbName1} values(993456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)

    //TODO Data doublingTest the dup model by modify a  value type from FLOAT  to VARCHAR
    //Test the dup model by modify a value type from FLOAT  to VARCHAR
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} MODIFY  column score VARCHAR(100)  """
    insertSql = "insert into ${tbName1} values(993456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, true, "${tbName1}")


    //Test the dup model by modify a value type from FLOAT  to STRING
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} MODIFY  column score STRING  """
    insertSql = "insert into ${tbName1} values(993456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName1}")

    sql """ DROP TABLE IF EXISTS ${tbName2} """
    initTable2 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `score` STRING COMMENT \"分数\",\n" +
            "              `city` VARCHAR(20) COMMENT \"用户所在城市\",\n" +
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

    initTableData2 = "insert into ${tbName2} values(123456789, 'Alice', 1.8, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (234567890, 'Bob', 1.89, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (345678901, 'Carol', 2.6, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (456789012, 'Dave', 3.9, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (567890123, 'Eve', 4.2, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (678901234, 'Frank', 2.5, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00', {'a': 600, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (993456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\\\"abc\\\", \\\"def\\\"]')," +
            "               (789012345, 'Grace', 2.1, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00', {'a': 700, 'b': 200}, '[\"abc\", \"def\"]');"

    sql initTable2
    sql initTableData2
    checkTableData("${tbName1}", "${tbName2}", "score")
    sql """ DROP TABLE IF EXISTS ${tbName1} """


    //TODO Test the dup model by modify a  value type from FLOAT  to map
    //Test the dup model by modify a value type from FLOAT  to STRING
    errorMessage = "errCode = 2, detailMessage = Can not change FLOAT to MAP"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column score Map<STRING, INT>  """
        insertSql = "insert into ${tbName1} values(993456689, 'Alice', {'a': 100, 'b': 200}, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //TODO Test the dup model by modify a value type from FLOAT  to JSON
    errorMessage = "errCode = 2, detailMessage = Can not change FLOAT to JSON"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column score JSON  """
        insertSql = "insert into ${tbName1} values(993456689, 'Alice', '{'a': 100, 'b': 200}', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)

    /**
     *  Test the unique model by modify a value type from DOUBLE to other type
     */
    sql """ DROP TABLE IF EXISTS ${tbName1} """
    initTable = " CREATE TABLE IF NOT EXISTS ${tbName1}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `score` DOUBLE COMMENT \"分数\",\n" +
            "              `city` VARCHAR(20) COMMENT \"用户所在城市\",\n" +
            "              `age` SMALLINT COMMENT \"用户年龄\",\n" +
            "              `sex` TINYINT COMMENT \"用户性别\",\n" +
            "              `phone` LARGEINT COMMENT \"用户电话\",\n" +
            "              `address` VARCHAR(500) COMMENT \"用户地址\",\n" +
            "              `register_time` DATETIME COMMENT \"用户注册时间\",\n" +
            "              `m` Map<STRING, INT> NULL COMMENT \"\",\n" +
            "              `j` JSON NULL COMMENT \"\"\n" +
            "          )\n" +
            "          UNIQUE KEY(`user_id`, `username`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData = "insert into ${tbName1} values(123456789, 'Alice', 1.8, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (234567890, 'Bob', 1.89, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (345678901, 'Carol', 2.6, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (456789012, 'Dave', 3.9, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (567890123, 'Eve', 4.2, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (678901234, 'Frank', 2.5, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00', {'a': 600, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (789012345, 'Grace', 2.1, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00', {'a': 700, 'b': 200}, '[\"abc\", \"def\"]');"

    //TODO Test the unique model by modify a value type from DOUBLE  to BOOLEAN
    errorMessage = "errCode = 2, detailMessage = Can not change DOUBLE to BOOLEAN"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column score BOOLEAN  """
        insertSql = "insert into ${tbName1} values(993456689, 'Alice', false, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '{\"k1\":\"v1\", \"k2\": 200}'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    // TODO Test the unique model by modify a value type from DOUBLE  to TINYINT
    errorMessage = "errCode = 2, detailMessage = Can not change DOUBLE to TINYINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column score TINYINT  """
        insertSql = "insert into ${tbName1} values(993456689, 'Alice', 2, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '{\"k1\":\"v1\", \"k2\": 200}'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //Test the unique model by modify a value type from DOUBLE  to SMALLINT
    errorMessage = "errCode = 2, detailMessage = Can not change DOUBLE to SMALLINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column score SMALLINT   """
        insertSql = "insert into ${tbName1} values(993456689, 'Alice', 3, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, [\"abc\", \"def\"]); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)

    //Test the unique model by modify a value type from DOUBLE  to INT
    errorMessage = "errCode = 2, detailMessage = Can not change DOUBLE to INT"
    expectException({
        sql initTable
        sql initTableData

        sql """ alter  table ${tbName1} MODIFY  column score INT  """
        insertSql = "insert into ${tbName1} values(993456689, 'Alice', 4, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, [\"abc\", \"def\"]); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //Test the unique model by modify a value type from DOUBLE  to BIGINT
    errorMessage = "errCode = 2, detailMessage = Can not change DOUBLE to BIGINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column score BIGINT  """
        insertSql = "insert into ${tbName1} values(993456689, 'Alice', 545645, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, [\"abc\", \"def\"]); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)

    //Test the unique model by modify a value type from  DOUBLE to LARGEINT
    errorMessage = "errCode = 2, detailMessage = Can not change DOUBLE to LARGEINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column score LARGEINT  """
        insertSql = "insert into ${tbName1} values(993456689, 'Alice', 156546, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, [\"abc\", \"def\"]); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //Test the unique model by modify a value type from DOUBLE  to FLOAT
    errorMessage = "errCode = 2, detailMessage = Can not change DOUBLE to FLOAT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column score FLOAT  """
        insertSql = "insert into ${tbName1} values(993456689, 'Alice', 1.23, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //TODO Test the unique model by modify a value type from DOUBLE  to DECIMAL
    errorMessage = "errCode = 2, detailMessage = Can not change DOUBLE to DECIMAL128"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column score DECIMAL(38,0)  """
        insertSql = "insert into ${tbName1} values(993456689, 'Alice', 1.23, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)


    //TODO Test the unique model by modify a value type from DOUBLE  to DATE
    errorMessage = "errCode = 2, detailMessage = Can not change DOUBLE to DATEV2"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column score DATE  """
        insertSql = "insert into ${tbName1} values(993456689, 'Alice', '2003-12-31', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)

    //TODO Test the unique model by modify a value type from DOUBLE  to DATE
    errorMessage = "errCode = 2, detailMessage = Can not change DOUBLE to DATEV2"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column score DATEV2  """
        insertSql = "insert into ${tbName1} values(993456689, 'Alice', '2003-12-31', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)


    //TODO Test the unique model by modify a value type from DOUBLE  to DATETIME
    errorMessage = "errCode = 2, detailMessage = Can not change DOUBLE to DATETIMEV2"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column score DATETIME  """
        insertSql = "insert into ${tbName1} values(993456689, 'Alice', '2003-12-31 20:12:12', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)

    //TODO Test the unique model by modify a value type from DOUBLE  to DATETIME
    errorMessage = "errCode = 2, detailMessage = Can not change DOUBLE to DATETIMEV2"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column score DATETIMEV2  """
        insertSql = "insert into ${tbName1} values(993456689, 'Alice', '2003-12-31 20:12:12', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)

    //TODO Test the unique model by modify a  value type from DOUBLE  to CHAR
    errorMessage = "errCode = 2, detailMessage = Can not change DOUBLE to CHAR"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column score CHAR(15)  """
        insertSql = "insert into ${tbName1} values(993456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)

    //Test the unique model by modify a value type from DOUBLE  to VARCHAR
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} MODIFY  column score VARCHAR(100)  """
    insertSql = "insert into ${tbName1} values(993456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName1}")

    sql """ DROP TABLE IF EXISTS ${tbName2} """
    initTable2 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `score` VARCHAR(100) COMMENT \"分数\",\n" +
            "              `city` VARCHAR(20) COMMENT \"用户所在城市\",\n" +
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

    initTableData2 = "insert into ${tbName2} values(123456789, 'Alice', 1.8, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (234567890, 'Bob', 1.89, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (345678901, 'Carol', 2.6, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (456789012, 'Dave', 3.9, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (567890123, 'Eve', 4.2, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (678901234, 'Frank', 2.5, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00', {'a': 600, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (993456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\\\"abc\\\", \\\"def\\\"]')," +
            "               (789012345, 'Grace', 2.1, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00', {'a': 700, 'b': 200}, '[\"abc\", \"def\"]');"

    sql initTable2
    sql initTableData2
    checkTableData("${tbName1}", "${tbName2}", "score")
    sql """ DROP TABLE IF EXISTS ${tbName1} """


    //Test the unique model by modify a value type from DOUBLE  to STRING
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} MODIFY  column score STRING  """
    insertSql = "insert into ${tbName1} values(993456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName1}")


    sql """ DROP TABLE IF EXISTS ${tbName2} """
    initTable2 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `score` STRING COMMENT \"分数\",\n" +
            "              `city` VARCHAR(20) COMMENT \"用户所在城市\",\n" +
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

    initTableData2 = "insert into ${tbName2} values(123456789, 'Alice', 1.8, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (234567890, 'Bob', 1.89, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (345678901, 'Carol', 2.6, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (456789012, 'Dave', 3.9, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (567890123, 'Eve', 4.2, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (678901234, 'Frank', 2.5, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00', {'a': 600, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (993456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\\\"abc\\\", \\\"def\\\"]')," +
            "               (789012345, 'Grace', 2.1, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00', {'a': 700, 'b': 200}, '[\"abc\", \"def\"]');"

    sql initTable2
    sql initTableData2
    checkTableData("${tbName1}", "${tbName2}", "score")
    sql """ DROP TABLE IF EXISTS ${tbName1} """


    //TODO Test the unique model by modify a  value type from DOUBLE  to map
    //Test the unique model by modify a value type from DOUBLE  to STRING
    errorMessage = "errCode = 2, detailMessage = Can not change DOUBLE to MAP"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column score Map<STRING, INT>  """
        insertSql = "insert into ${tbName1} values(993456689, 'Alice', {'a': 100, 'b': 200}, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //Test the unique model by modify a value type from DOUBLE  to JSON
    errorMessage = "errCode = 2, detailMessage = Can not change DOUBLE to JSON"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column score JSON  """
        insertSql = "insert into ${tbName1} values(993456689, 'Alice', '{'a': 100, 'b': 200}', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    /**
     *  Test the unique model by modify a value type from DECIMAL to other type
     */
    sql """ DROP TABLE IF EXISTS ${tbName1} """
    initTable = " CREATE TABLE IF NOT EXISTS ${tbName1}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `score` DECIMAL(38,10) COMMENT \"分数\",\n" +
            "              `city` VARCHAR(20) COMMENT \"用户所在城市\",\n" +
            "              `age` SMALLINT COMMENT \"用户年龄\",\n" +
            "              `sex` TINYINT COMMENT \"用户性别\",\n" +
            "              `phone` LARGEINT COMMENT \"用户电话\",\n" +
            "              `address` VARCHAR(500) COMMENT \"用户地址\",\n" +
            "              `register_time` DATETIME COMMENT \"用户注册时间\",\n" +
            "              `m` Map<STRING, INT> NULL COMMENT \"\",\n" +
            "              `j` JSON NULL COMMENT \"\"\n" +
            "          )\n" +
            "          UNIQUE KEY(`user_id`, `username`)\n" +
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

    //TODO Test the unique model by modify a value type from DECIMAL128  to BOOLEAN
    errorMessage = "errCode = 2, detailMessage = Can not change DECIMAL128 to BOOLEAN"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column score BOOLEAN  """
        insertSql = "insert into ${tbName1} values(993456689, 'Alice', false, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '{\"k1\":\"v1\", \"k2\": 200}'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    // TODO Test the unique model by modify a value type from DECIMAL128  to TINYINT
    errorMessage = "errCode = 2, detailMessage = Can not change DECIMAL128 to TINYINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column score TINYINT  """
        insertSql = "insert into ${tbName1} values(993456689, 'Alice', 2, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '{\"k1\":\"v1\", \"k2\": 200}'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //Test the unique model by modify a value type from DECIMAL128  to SMALLINT
    errorMessage = "errCode = 2, detailMessage = Can not change DECIMAL128 to SMALLINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column score SMALLINT   """
        insertSql = "insert into ${tbName1} values(993456689, 'Alice', 3, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, [\"abc\", \"def\"]); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)

    //Test the unique model by modify a value type from DECIMAL128  to INT
    errorMessage = "errCode = 2, detailMessage = Can not change DECIMAL128 to INT"
    expectException({
        sql initTable
        sql initTableData

        sql """ alter  table ${tbName1} MODIFY  column score INT  """
        insertSql = "insert into ${tbName1} values(993456689, 'Alice', 4, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, [\"abc\", \"def\"]); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //Test the unique model by modify a value type from DECIMAL128  to BIGINT
    errorMessage = "errCode = 2, detailMessage = Can not change DECIMAL128 to BIGINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column score BIGINT  """
        insertSql = "insert into ${tbName1} values(993456689, 'Alice', 545645, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, [\"abc\", \"def\"]); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)

    //Test the unique model by modify a value type from  DECIMAL128 to LARGEINT
    errorMessage = "errCode = 2, detailMessage = Can not change DECIMAL128 to LARGEINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column score LARGEINT  """
        insertSql = "insert into ${tbName1} values(993456689, 'Alice', 156546, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, [\"abc\", \"def\"]); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //Test the unique model by modify a value type from DECIMAL128  to FLOAT
    errorMessage = "errCode = 2, detailMessage = Can not change DECIMAL128 to FLOAT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column score FLOAT  """
        insertSql = "insert into ${tbName1} values(993456689, 'Alice', 1.23, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //TODO Data accuracy loss Test the unique model by modify a value type from DECIMAL128  to DECIMAL
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} MODIFY  column score DECIMAL(38,0)  """
    insertSql = "insert into ${tbName1} values(993456689, 'Alice', 1.23, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, true, "${tbName1}")



    //TODO Test the unique model by modify a value type from DECIMAL128  to DATE
    errorMessage = "errCode = 2, detailMessage = Can not change DECIMAL128 to DATEV2"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column score DATE  """
        insertSql = "insert into ${tbName1} values(993456689, 'Alice', '2003-12-31', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)

    //TODO Test the unique model by modify a value type from DECIMAL128  to DATE
    errorMessage = "errCode = 2, detailMessage = Can not change DECIMAL128 to DATEV2"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column score DATEV2  """
        insertSql = "insert into ${tbName1} values(993456689, 'Alice', '2003-12-31', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)


    //TODO Test the unique model by modify a value type from DECIMAL128  to DATETIME
    errorMessage = "errCode = 2, detailMessage = Can not change DECIMAL128 to DATETIMEV2"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column score DATETIME  """
        insertSql = "insert into ${tbName1} values(993456689, 'Alice', '2003-12-31 20:12:12', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)

    //TODO Test the unique model by modify a value type from DECIMAL128  to DATETIME
    errorMessage = "errCode = 2, detailMessage = Can not change DECIMAL128 to DATETIMEV2"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column score DATETIMEV2  """
        insertSql = "insert into ${tbName1} values(993456689, 'Alice', '2003-12-31 20:12:12', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)

    //TODO Test the unique model by modify a  value type from DECIMAL128  to CHAR
    errorMessage = "errCode = 2, detailMessage = Can not change DECIMAL128 to CHAR"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column score CHAR(15)  """
        insertSql = "insert into ${tbName1} values(993456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)

    //Test Data accuracy loss the unique model by modify a value type from DECIMAL128  to VARCHAR
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} MODIFY  column score VARCHAR(100)  """
    insertSql = "insert into ${tbName1} values(993456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName1}")

    sql """ DROP TABLE IF EXISTS ${tbName2} """
    initTable2 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `score` VARCHAR(100) COMMENT \"分数\",\n" +
            "              `city` VARCHAR(20) COMMENT \"用户所在城市\",\n" +
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

    initTableData2 = "insert into ${tbName2} values(123456789, 'Alice', 1.83, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (234567890, 'Bob', 1.89, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (345678901, 'Carol', 2.6689, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (456789012, 'Dave', 3.9456, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (567890123, 'Eve', 4.223, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (678901234, 'Frank', 2.5454, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00', {'a': 600, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (993456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\\\"abc\\\", \\\"def\\\"]')," +
            "               (789012345, 'Grace', 2.19656, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00', {'a': 700, 'b': 200}, '[\"abc\", \"def\"]');"

    sql initTable2
    sql initTableData2
    sql """ DROP TABLE IF EXISTS ${tbName1} """


    //TODO Data accuracy loss Test the unique model by modify a value type from DECIMAL128  to STRING
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} MODIFY  column score STRING  """
    insertSql = "insert into ${tbName1} values(993456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName1}")


    sql """ DROP TABLE IF EXISTS ${tbName2} """
    initTable2 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `score` STRING COMMENT \"分数\",\n" +
            "              `city` VARCHAR(20) COMMENT \"用户所在城市\",\n" +
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

    initTableData2 = "insert into ${tbName2} values(123456789, 'Alice', 1.83, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (234567890, 'Bob', 1.89, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (345678901, 'Carol', 2.6689, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (456789012, 'Dave', 3.9456, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (567890123, 'Eve', 4.223, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (678901234, 'Frank', 2.5454, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00', {'a': 600, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (993456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\\\"abc\\\", \\\"def\\\"]')," +
            "               (789012345, 'Grace', 2.19656, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00', {'a': 700, 'b': 200}, '[\"abc\", \"def\"]');"

    sql initTable2
    sql initTableData2
    sql """ DROP TABLE IF EXISTS ${tbName1} """


    //TODO Test the unique model by modify a  value type from DECIMAL128  to map
    //Test the unique model by modify a value type from DECIMAL128  to STRING
    errorMessage = "errCode = 2, detailMessage = Can not change DECIMAL128 to MAP"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column score Map<STRING, INT>  """
        insertSql = "insert into ${tbName1} values(993456689, 'Alice', {'a': 100, 'b': 200}, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //Test the unique model by modify a value type from DECIMAL128  to JSON
    errorMessage = "errCode = 2, detailMessage = Can not change DECIMAL128 to JSON"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column score JSON  """
        insertSql = "insert into ${tbName1} values(993456689, 'Alice', '{'a': 100, 'b': 200}', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)



    /**
     *  Test the unique model by modify a value type from DATE to other type
     */
    sql """ DROP TABLE IF EXISTS ${tbName1} """
    initTable = " CREATE TABLE IF NOT EXISTS ${tbName1}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `score` FLOAT COMMENT \"分数\",\n" +
            "              `city` VARCHAR(20) COMMENT \"用户所在城市\",\n" +
            "              `age` SMALLINT COMMENT \"用户年龄\",\n" +
            "              `sex` TINYINT COMMENT \"用户性别\",\n" +
            "              `phone` LARGEINT COMMENT \"用户电话\",\n" +
            "              `address` VARCHAR(500) COMMENT \"用户地址\",\n" +
            "              `register_time` DATE COMMENT \"用户注册时间\",\n" +
            "              `m` Map<STRING, INT> NULL COMMENT \"\",\n" +
            "              `j` JSON NULL COMMENT \"\"\n" +
            "          )\n" +
            "          UNIQUE KEY(`user_id`, `username`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\",\n" +
            "          \"enable_unique_key_merge_on_write\" = \"${on_write}\"\n"  +
            "          );"

    initTableData = "insert into ${tbName1} values(123456789, 'Alice', 1.8, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (234567890, 'Bob', 1.89, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (345678901, 'Carol', 2.6, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (456789012, 'Dave', 3.9, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (567890123, 'Eve', 4.2, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (678901234, 'Frank', 2.5, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06', {'a': 600, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (789012345, 'Grace', 2.1, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07', {'a': 700, 'b': 200}, '[\"abc\", \"def\"]');"

    //TODO Test the unique model by modify a value type from DATE  to BOOLEAN
    errorMessage = "errCode = 2, detailMessage = Can not change DATEV2 to BOOLEAN"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time BOOLEAN  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 6.4, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', false, {'a': 100, 'b': 200}, '{\"k1\":\"v1\", \"k2\": 200}'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    // TODO Test the unique model by modify a value type from DATE  to TINYINT
    errorMessage = "errCode = 2, detailMessage = Can not change DATEV2 to TINYINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time TINYINT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 2.2, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', 1', {'a': 100, 'b': 200}, '{\"k1\":\"v1\", \"k2\": 200}'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //Test the unique model by modify a value type from DATE  to SMALLINT
    errorMessage = "errCode = 2, detailMessage = Can not change DATEV2 to SMALLINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time SMALLINT   """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 3, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', 2, {'a': 100, 'b': 200}, [\"abc\", \"def\"]); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)

    //Test the unique model by modify a value type from DATE  to INT
    errorMessage = "errCode = 2, detailMessage = Can not change DATEV2 to INT"
    expectException({
        sql initTable
        sql initTableData

        sql """ alter  table ${tbName1} MODIFY  column register_time INT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 4, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', 156, {'a': 100, 'b': 200}, [\"abc\", \"def\"]); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //Test the unique model by modify a value type from DATE  to BIGINT
    errorMessage = "errCode = 2, detailMessage = Can not change DATEV2 to BIGINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time BIGINT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 545645, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '15662', {'a': 100, 'b': 200}, [\"abc\", \"def\"]); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)

    //Test the unique model by modify a value type from  DATE to LARGEINT
    errorMessage = "errCode = 2, detailMessage = Can not change DATEV2 to LARGEINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time LARGEINT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 156546, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', 15898, {'a': 100, 'b': 200}, [\"abc\", \"def\"]); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //Test the unique model by modify a value type from DATE  to DOUBLE
    errorMessage = "errCode = 2, detailMessage = Can not change DATEV2 to DOUBLE"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time DOUBLE  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 1.23, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', 3.65, {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //TODO Test the unique model by modify a value type from DATE  to DECIMAL
    errorMessage = "errCode = 2, detailMessage = Can not change DATEV2 to DECIMAL128"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time DECIMAL(38,0)  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 1.23, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', 3.6598, {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)


    //TODO Test the unique model by modify a value type from DATE  to FLOAT
    errorMessage = "errCode = 2, detailMessage = Can not change DATEV2 to FLOAT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time FLOAT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 2.6, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', 1.65, {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)


    //Test the unique model by modify a value type from DATE  to DATETIME
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} MODIFY  column register_time DATETIME  """
    insertSql = "insert into ${tbName1} values(923456689, 'Alice', 1.2, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2003-12-31 20:12:12', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName1}")

    sql """ DROP TABLE IF EXISTS ${tbName2} """
    initTable2 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `score` FLOAT COMMENT \"分数\",\n" +
            "              `city` VARCHAR(20) COMMENT \"用户所在城市\",\n" +
            "              `age` SMALLINT COMMENT \"用户年龄\",\n" +
            "              `sex` TINYINT COMMENT \"用户性别\",\n" +
            "              `phone` LARGEINT COMMENT \"用户电话\",\n" +
            "              `address` VARCHAR(500) COMMENT \"用户地址\",\n" +
            "              `register_time` DATETIME COMMENT \"用户注册时间\",\n" +
            "              `m` Map<STRING, INT> NULL COMMENT \"\",\n" +
            "              `j` JSON NULL COMMENT \"\"\n" +
            "          )\n" +
            "          UNIQUE KEY(`user_id`, `username`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\",\n" +
            "          \"enable_unique_key_merge_on_write\" = \"${on_write}\"\n"  +
            "          );"

    initTableData2 = "insert into ${tbName2} values(123456789, 'Alice', 1.8, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (234567890, 'Bob', 1.89, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 00:00', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (345678901, 'Carol', 2.6, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 00:00', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (456789012, 'Dave', 3.9, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 00:00', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (567890123, 'Eve', 4.2, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 00:00', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (678901234, 'Frank', 2.5, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 00:00', {'a': 600, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (923456689, 'Alice', 1.2, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2003-12-31 20:12:12', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (789012345, 'Grace', 2.1, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 00:00', {'a': 700, 'b': 200}, '[\"abc\", \"def\"]');"

    sql initTable2
    sql initTableData2
    checkTableData("${tbName1}","${tbName2}","register_time")
    sql """ DROP TABLE IF EXISTS ${tbName1} """


    //TODO Test the unique model by modify a value type from DATE  to DATETIME
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} MODIFY  column register_time DATETIMEV2  """
    insertSql = "insert into ${tbName1} values(923456689, 'Alice', 5.69, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2003-12-31 20:12:12', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName1}")


    sql """ DROP TABLE IF EXISTS ${tbName2} """
    initTable2 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `score` FLOAT COMMENT \"分数\",\n" +
            "              `city` VARCHAR(20) COMMENT \"用户所在城市\",\n" +
            "              `age` SMALLINT COMMENT \"用户年龄\",\n" +
            "              `sex` TINYINT COMMENT \"用户性别\",\n" +
            "              `phone` LARGEINT COMMENT \"用户电话\",\n" +
            "              `address` VARCHAR(500) COMMENT \"用户地址\",\n" +
            "              `register_time` DATETIMEV2 COMMENT \"用户注册时间\",\n" +
            "              `m` Map<STRING, INT> NULL COMMENT \"\",\n" +
            "              `j` JSON NULL COMMENT \"\"\n" +
            "          )\n" +
            "          UNIQUE KEY(`user_id`, `username`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\",\n" +
            "          \"enable_unique_key_merge_on_write\" = \"${on_write}\"\n"  +
            "          );"

    initTableData2 = "insert into ${tbName2} values(123456789, 'Alice', 1.8, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (234567890, 'Bob', 1.89, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 00:00', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (345678901, 'Carol', 2.6, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 00:00', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (456789012, 'Dave', 3.9, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 00:00', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (567890123, 'Eve', 4.2, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 00:00', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (678901234, 'Frank', 2.5, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 00:00', {'a': 600, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (923456689, 'Alice', 1.2, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2003-12-31 20:12:12', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (789012345, 'Grace', 2.1, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 00:00', {'a': 700, 'b': 200}, '[\"abc\", \"def\"]');"

    sql initTable2
    sql initTableData2
    checkTableData("${tbName1}","${tbName2}","register_time")
    sql """ DROP TABLE IF EXISTS ${tbName1} """



    //TODO Test the unique model by modify a  value type from DATE  to CHAR
    errorMessage = "errCode = 2, detailMessage = Can not change DATEV2 to CHAR"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time CHAR(15)  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 1.69, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', 'cs1', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)

    //TODO Test the unique model by modify a  value type from DATE  to VARCHAR
    //Test the unique model by modify a value type from DATE  to VARCHAR
    errorMessage = "errCode = 2, detailMessage = Can not change DATEV2 to VARCHAR"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time VARCHAR(100)  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 8.45, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', 'asd', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)

    //TODO Test the unique model by modify a  value type from DATE  to STRING
    //Test the unique model by modify a value type from DATE  to STRING
    errorMessage = "errCode = 2, detailMessage = Can not change DATEV2 to STRING"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time STRING  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 1.89, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //TODO Test the unique model by modify a  value type from DATE  to map
    //Test the unique model by modify a value type from DATE  to STRING
    errorMessage = "errCode = 2, detailMessage = Can not change DATEV2 to MAP"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time Map<STRING, INT>  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 1.49, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', {'a': 100, 'b': 200}, {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //Test the unique model by modify a value type from DATE  to JSON
    errorMessage = "errCode = 2, detailMessage = Can not change DATEV2 to JSON"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time JSON  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 1.34, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '{'a': 100, 'b': 200}', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)



    /**
     *  Test the unique model by modify a value type from DATEV2 to other type
     */
    sql """ DROP TABLE IF EXISTS ${tbName1} """
    initTable = " CREATE TABLE IF NOT EXISTS ${tbName1}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `score` FLOAT COMMENT \"分数\",\n" +
            "              `city` VARCHAR(20) COMMENT \"用户所在城市\",\n" +
            "              `age` SMALLINT COMMENT \"用户年龄\",\n" +
            "              `sex` TINYINT COMMENT \"用户性别\",\n" +
            "              `phone` LARGEINT COMMENT \"用户电话\",\n" +
            "              `address` VARCHAR(500) COMMENT \"用户地址\",\n" +
            "              `register_time` DATEV2 COMMENT \"用户注册时间\",\n" +
            "              `m` Map<STRING, INT> NULL COMMENT \"\",\n" +
            "              `j` JSON NULL COMMENT \"\"\n" +
            "          )\n" +
            "          UNIQUE KEY(`user_id`, `username`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\",\n" +
            "          \"enable_unique_key_merge_on_write\" = \"${on_write}\"\n"  +
            "          );"

    initTableData = "insert into ${tbName1} values(123456789, 'Alice', 1.8, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (234567890, 'Bob', 1.89, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (345678901, 'Carol', 2.6, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (456789012, 'Dave', 3.9, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (567890123, 'Eve', 4.2, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (678901234, 'Frank', 2.5, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06', {'a': 600, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (789012345, 'Grace', 2.1, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07', {'a': 700, 'b': 200}, '[\"abc\", \"def\"]');"

    //TODO Test the unique model by modify a value type from DATEV2  to BOOLEAN
    errorMessage = "errCode = 2, detailMessage = Can not change DATEV2 to BOOLEAN"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time BOOLEAN  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 6.4, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', false, {'a': 100, 'b': 200}, '{\"k1\":\"v1\", \"k2\": 200}'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    // TODO Test the unique model by modify a value type from DATEV2  to TINYINT
    errorMessage = "errCode = 2, detailMessage = Can not change DATEV2 to TINYINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time TINYINT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 2.2, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', 1', {'a': 100, 'b': 200}, '{\"k1\":\"v1\", \"k2\": 200}'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //Test the unique model by modify a value type from DATEV2  to SMALLINT
    errorMessage = "errCode = 2, detailMessage = Can not change DATEV2 to SMALLINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time SMALLINT   """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 3, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', 2, {'a': 100, 'b': 200}, [\"abc\", \"def\"]); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)

    //Test the unique model by modify a value type from DATEV2  to INT
    errorMessage = "errCode = 2, detailMessage = Can not change DATEV2 to INT"
    expectException({
        sql initTable
        sql initTableData

        sql """ alter  table ${tbName1} MODIFY  column register_time INT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 4, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', 156, {'a': 100, 'b': 200}, [\"abc\", \"def\"]); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //Test the unique model by modify a value type from DATEV2  to BIGINT
    errorMessage = "errCode = 2, detailMessage = Can not change DATEV2 to BIGINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time BIGINT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 545645, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '15662', {'a': 100, 'b': 200}, [\"abc\", \"def\"]); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)

    //Test the unique model by modify a value type from  DATEV2 to LARGEINT
    errorMessage = "errCode = 2, detailMessage = Can not change DATEV2 to LARGEINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time LARGEINT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 156546, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', 15898, {'a': 100, 'b': 200}, [\"abc\", \"def\"]); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //Test the unique model by modify a value type from DATEV2  to DOUBLE
    errorMessage = "errCode = 2, detailMessage = Can not change DATEV2 to DOUBLE"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time DOUBLE  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 1.23, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', 3.65, {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //TODO Test the unique model by modify a value type from DATEV2  to DECIMAL
    errorMessage = "errCode = 2, detailMessage = Can not change DATEV2 to DECIMAL128"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time DECIMAL(38,0)  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 1.23, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', 3.6598, {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)


    //TODO Test the unique model by modify a value type from DATEV2  to FLOAT
    errorMessage = "errCode = 2, detailMessage = Can not change DATEV2 to FLOAT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time FLOAT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 2.6, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', 1.65, {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)


    //TODO Test the unique model by modify a value type from DATEV2  to DATETIME
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} MODIFY  column register_time DATETIME  """
    insertSql = "insert into ${tbName1} values(923456689, 'Alice', 1.2, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2003-12-31 20:12:12', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName1}")

    sql """ DROP TABLE IF EXISTS ${tbName2} """
    initTable2 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `score` FLOAT COMMENT \"分数\",\n" +
            "              `city` VARCHAR(20) COMMENT \"用户所在城市\",\n" +
            "              `age` SMALLINT COMMENT \"用户年龄\",\n" +
            "              `sex` TINYINT COMMENT \"用户性别\",\n" +
            "              `phone` LARGEINT COMMENT \"用户电话\",\n" +
            "              `address` VARCHAR(500) COMMENT \"用户地址\",\n" +
            "              `register_time` DATETIME COMMENT \"用户注册时间\",\n" +
            "              `m` Map<STRING, INT> NULL COMMENT \"\",\n" +
            "              `j` JSON NULL COMMENT \"\"\n" +
            "          )\n" +
            "          UNIQUE KEY(`user_id`, `username`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\",\n" +
            "          \"enable_unique_key_merge_on_write\" = \"${on_write}\"\n"  +
            "          );"

    initTableData2 = "insert into ${tbName2} values(123456789, 'Alice', 1.8, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (234567890, 'Bob', 1.89, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 00:00', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (345678901, 'Carol', 2.6, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 00:00', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (456789012, 'Dave', 3.9, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 00:00', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (567890123, 'Eve', 4.2, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 00:00', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (678901234, 'Frank', 2.5, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 00:00', {'a': 600, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (923456689, 'Alice', 1.2, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2003-12-31 20:12:12', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (789012345, 'Grace', 2.1, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 00:00', {'a': 700, 'b': 200}, '[\"abc\", \"def\"]');"

    sql initTable2
    sql initTableData2
    checkTableData("${tbName1}","${tbName2}","register_time")
    sql """ DROP TABLE IF EXISTS ${tbName1} """



    //TODO Test the unique model by modify a value type from DATEV2  to DATETIME
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} MODIFY  column register_time DATETIMEV2  """
    insertSql = "insert into ${tbName1} values(923456689, 'Alice', 5.69, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2003-12-31 20:12:12', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName1}")

    sql """ DROP TABLE IF EXISTS ${tbName2} """
    initTable2 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `score` FLOAT COMMENT \"分数\",\n" +
            "              `city` VARCHAR(20) COMMENT \"用户所在城市\",\n" +
            "              `age` SMALLINT COMMENT \"用户年龄\",\n" +
            "              `sex` TINYINT COMMENT \"用户性别\",\n" +
            "              `phone` LARGEINT COMMENT \"用户电话\",\n" +
            "              `address` VARCHAR(500) COMMENT \"用户地址\",\n" +
            "              `register_time` DATETIMEV2 COMMENT \"用户注册时间\",\n" +
            "              `m` Map<STRING, INT> NULL COMMENT \"\",\n" +
            "              `j` JSON NULL COMMENT \"\"\n" +
            "          )\n" +
            "          UNIQUE KEY(`user_id`, `username`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\",\n" +
            "          \"enable_unique_key_merge_on_write\" = \"${on_write}\"\n"  +
            "          );"

    initTableData2 = "insert into ${tbName2} values(123456789, 'Alice', 1.8, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (234567890, 'Bob', 1.89, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 00:00', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (345678901, 'Carol', 2.6, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 00:00', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (456789012, 'Dave', 3.9, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 00:00', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (567890123, 'Eve', 4.2, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 00:00', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (678901234, 'Frank', 2.5, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 00:00', {'a': 600, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (923456689, 'Alice', 1.2, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2003-12-31 20:12:12', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (789012345, 'Grace', 2.1, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 00:00', {'a': 700, 'b': 200}, '[\"abc\", \"def\"]');"

    sql initTable2
    sql initTableData2
    checkTableData("${tbName1}","${tbName2}","register_time")
    sql """ DROP TABLE IF EXISTS ${tbName1} """



    //TODO Test the unique model by modify a  value type from DATEV2  to CHAR
    errorMessage = "errCode = 2, detailMessage = Can not change DATEV2 to CHAR"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time CHAR(15)  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 1.69, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', 'cs1', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)

    //TODO Test the unique model by modify a  value type from DATEV2  to VARCHAR
    //Test the unique model by modify a value type from DATE  to VARCHAR
    errorMessage = "errCode = 2, detailMessage = Can not change DATEV2 to VARCHAR"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time VARCHAR(100)  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 8.45, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', 'asd', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)

    //TODO Test the unique model by modify a  value type from DATEV2  to STRING
    //Test the unique model by modify a value type from DATE  to STRING
    errorMessage = "errCode = 2, detailMessage = Can not change DATEV2 to STRING"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time STRING  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 1.89, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //TODO Test the unique model by modify a  value type from DATEV2  to map
    //Test the unique model by modify a value type from DATE  to STRING
    errorMessage = "errCode = 2, detailMessage = Can not change DATEV2 to MAP"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time Map<STRING, INT>  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 1.49, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', {'a': 100, 'b': 200}, {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //Test the unique model by modify a value type from DATEV2  to JSON
    errorMessage = "errCode = 2, detailMessage = Can not change DATEV2 to JSON"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time JSON  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 1.34, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '{'a': 100, 'b': 200}', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)




    /**
     *  Test the unique model by modify a value type from DATETIME to other type
     */
    sql """ DROP TABLE IF EXISTS ${tbName1} """
    initTable = " CREATE TABLE IF NOT EXISTS ${tbName1}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `score` FLOAT COMMENT \"分数\",\n" +
            "              `city` VARCHAR(20) COMMENT \"用户所在城市\",\n" +
            "              `age` SMALLINT COMMENT \"用户年龄\",\n" +
            "              `sex` TINYINT COMMENT \"用户性别\",\n" +
            "              `phone` LARGEINT COMMENT \"用户电话\",\n" +
            "              `address` VARCHAR(500) COMMENT \"用户地址\",\n" +
            "              `register_time` DATETIME COMMENT \"用户注册时间\",\n" +
            "              `m` Map<STRING, INT> NULL COMMENT \"\",\n" +
            "              `j` JSON NULL COMMENT \"\"\n" +
            "          )\n" +
            "          UNIQUE KEY(`user_id`, `username`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\",\n" +
            "          \"enable_unique_key_merge_on_write\" = \"${on_write}\"\n"  +
            "          );"

    initTableData = "insert into ${tbName1} values(123456789, 'Alice', 1.8, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:48:26', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (234567890, 'Bob', 1.89, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 10:48:26', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (345678901, 'Carol', 2.6, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 10:48:26', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (456789012, 'Dave', 3.9, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 10:48:26', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (567890123, 'Eve', 4.2, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 10:48:26', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (678901234, 'Frank', 2.5, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 10:48:26', {'a': 600, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (789012345, 'Grace', 2.1, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 10:48:26', {'a': 700, 'b': 200}, '[\"abc\", \"def\"]');"

    //TODO Test the unique model by modify a value type from DATETIME  to BOOLEAN
    errorMessage = "errCode = 2, detailMessage = Can not change DATETIMEV2 to BOOLEAN"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time BOOLEAN  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 6.4, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', false, {'a': 100, 'b': 200}, '{\"k1\":\"v1\", \"k2\": 200}'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    // TODO Test the unique model by modify a value type from DATETIME  to TINYINT
    errorMessage = "errCode = 2, detailMessage = Can not change DATETIMEV2 to TINYINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time TINYINT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 2.2, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', 1', {'a': 100, 'b': 200}, '{\"k1\":\"v1\", \"k2\": 200}'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //Test the unique model by modify a value type from DATETIME  to SMALLINT
    errorMessage = "errCode = 2, detailMessage = Can not change DATETIMEV2 to SMALLINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time SMALLINT   """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 3, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', 2, {'a': 100, 'b': 200}, [\"abc\", \"def\"]); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)

    //Test the unique model by modify a value type from DATETIME  to INT
    errorMessage = "errCode = 2, detailMessage = Can not change DATETIMEV2 to INT"
    expectException({
        sql initTable
        sql initTableData

        sql """ alter  table ${tbName1} MODIFY  column register_time INT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 4, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', 156, {'a': 100, 'b': 200}, [\"abc\", \"def\"]); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //Test the unique model by modify a value type from DATETIME  to BIGINT
    errorMessage = "errCode = 2, detailMessage = Can not change DATETIMEV2 to BIGINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time BIGINT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 545645, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '15662', {'a': 100, 'b': 200}, [\"abc\", \"def\"]); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)

    //Test the unique model by modify a value type from  DATETIME to LARGEINT
    errorMessage = "errCode = 2, detailMessage = Can not change DATETIMEV2 to LARGEINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time LARGEINT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 156546, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', 15898, {'a': 100, 'b': 200}, [\"abc\", \"def\"]); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //Test the unique model by modify a value type from DATETIME  to DOUBLE
    errorMessage = "errCode = 2, detailMessage = Can not change DATETIMEV2 to DOUBLE"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time DOUBLE  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 1.23, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', 3.65, {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //TODO Test the unique model by modify a value type from DATETIME  to DECIMAL
    errorMessage = "errCode = 2, detailMessage = Can not change DATETIMEV2 to DECIMAL128"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time DECIMAL(38,0)  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 1.23, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', 3.6598, {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)


    //TODO Test the unique model by modify a value type from DATETIME  to FLOAT
    errorMessage = "errCode = 2, detailMessage = Can not change DATETIMEV2 to FLOAT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time FLOAT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 2.6, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', 1.65, {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)


    //TODO Test the unique model by modify a value type from DATETIME  to DATE
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} MODIFY  column register_time DATE  """
    insertSql = "insert into ${tbName1} values(923456689, 'Alice', 1.2, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2003-12-31', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName1}")

    sql """ DROP TABLE IF EXISTS ${tbName2} """
    initTable2 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `score` FLOAT COMMENT \"分数\",\n" +
            "              `city` VARCHAR(20) COMMENT \"用户所在城市\",\n" +
            "              `age` SMALLINT COMMENT \"用户年龄\",\n" +
            "              `sex` TINYINT COMMENT \"用户性别\",\n" +
            "              `phone` LARGEINT COMMENT \"用户电话\",\n" +
            "              `address` VARCHAR(500) COMMENT \"用户地址\",\n" +
            "              `register_time` DATE COMMENT \"用户注册时间\",\n" +
            "              `m` Map<STRING, INT> NULL COMMENT \"\",\n" +
            "              `j` JSON NULL COMMENT \"\"\n" +
            "          )\n" +
            "          UNIQUE KEY(`user_id`, `username`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\",\n" +
            "          \"enable_unique_key_merge_on_write\" = \"${on_write}\"\n"  +
            "          );"

    initTableData2 = "insert into ${tbName2} values(123456789, 'Alice', 1.8, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (234567890, 'Bob', 1.89, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (345678901, 'Carol', 2.6, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (456789012, 'Dave', 3.9, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (567890123, 'Eve', 4.2, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (678901234, 'Frank', 2.5, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06', {'a': 600, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (923456689, 'Alice', 1.2, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2003-12-31', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (789012345, 'Grace', 2.1, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07', {'a': 700, 'b': 200}, '[\"abc\", \"def\"]');"

    sql initTable2
    sql initTableData2
    checkTableData("${tbName1}","${tbName2}","register_time")
    sql """ DROP TABLE IF EXISTS ${tbName1} """


    //TODO Test the unique model by modify a value type from DATETIME  to DATEV2
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} MODIFY  column register_time DATEV2  """
    insertSql = "insert into ${tbName1} values(923456689, 'Alice', 5.69, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2003-12-31', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName1}")

    sql """ DROP TABLE IF EXISTS ${tbName2} """
    initTable2 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `score` FLOAT COMMENT \"分数\",\n" +
            "              `city` VARCHAR(20) COMMENT \"用户所在城市\",\n" +
            "              `age` SMALLINT COMMENT \"用户年龄\",\n" +
            "              `sex` TINYINT COMMENT \"用户性别\",\n" +
            "              `phone` LARGEINT COMMENT \"用户电话\",\n" +
            "              `address` VARCHAR(500) COMMENT \"用户地址\",\n" +
            "              `register_time` DATEV2 COMMENT \"用户注册时间\",\n" +
            "              `m` Map<STRING, INT> NULL COMMENT \"\",\n" +
            "              `j` JSON NULL COMMENT \"\"\n" +
            "          )\n" +
            "          UNIQUE KEY(`user_id`, `username`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\",\n" +
            "          \"enable_unique_key_merge_on_write\" = \"${on_write}\"\n"  +
            "          );"

    initTableData2 = "insert into ${tbName2} values(123456789, 'Alice', 1.8, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (234567890, 'Bob', 1.89, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (345678901, 'Carol', 2.6, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (456789012, 'Dave', 3.9, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (567890123, 'Eve', 4.2, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (678901234, 'Frank', 2.5, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06', {'a': 600, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (923456689, 'Alice', 1.2, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2003-12-31', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (789012345, 'Grace', 2.1, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07', {'a': 700, 'b': 200}, '[\"abc\", \"def\"]');"

    sql initTable2
    sql initTableData2
    checkTableData("${tbName1}","${tbName2}","register_time")
    sql """ DROP TABLE IF EXISTS ${tbName1} """


    //TODO Test the unique model by modify a  value type from DATETIME  to CHAR
    errorMessage = "errCode = 2, detailMessage = Can not change DATETIMEV2 to CHAR"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time CHAR(15)  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 1.69, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', 'cs1', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)

    //TODO Test the unique model by modify a  value type from DATETIME  to VARCHAR
    //Test the unique model by modify a value type from DATETIME  to VARCHAR
    errorMessage = "errCode = 2, detailMessage = Can not change DATETIMEV2 to VARCHAR"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time VARCHAR(100)  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 8.45, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', 'asd', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)

    //TODO Test the unique model by modify a  value type from DATETIME  to STRING
    //Test the unique model by modify a value type from DATETIME  to STRING
    errorMessage = "errCode = 2, detailMessage = Can not change DATETIMEV2 to STRING"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time STRING  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 1.89, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //TODO Test the unique model by modify a  value type from DATETIME  to map
    //Test the unique model by modify a value type from DATETIME  to STRING
    errorMessage = "errCode = 2, detailMessage = Can not change DATETIMEV2 to MAP"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time Map<STRING, INT>  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 1.49, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', {'a': 100, 'b': 200}, {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //Test the unique model by modify a value type from DATETIME  to JSON
    errorMessage = "errCode = 2, detailMessage = Can not change DATETIMEV2 to JSON"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time JSON  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 1.34, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '{'a': 100, 'b': 200}', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    /**
     *  Test the unique model by modify a value type from DATETIMEV2 to other type
     */
    sql """ DROP TABLE IF EXISTS ${tbName1} """
    initTable = " CREATE TABLE IF NOT EXISTS ${tbName1}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `score` FLOAT COMMENT \"分数\",\n" +
            "              `city` VARCHAR(20) COMMENT \"用户所在城市\",\n" +
            "              `age` SMALLINT COMMENT \"用户年龄\",\n" +
            "              `sex` TINYINT COMMENT \"用户性别\",\n" +
            "              `phone` LARGEINT COMMENT \"用户电话\",\n" +
            "              `address` VARCHAR(500) COMMENT \"用户地址\",\n" +
            "              `register_time` DATETIME COMMENT \"用户注册时间\",\n" +
            "              `m` Map<STRING, INT> NULL COMMENT \"\",\n" +
            "              `j` JSON NULL COMMENT \"\"\n" +
            "          )\n" +
            "          UNIQUE KEY(`user_id`, `username`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\",\n" +
            "          \"enable_unique_key_merge_on_write\" = \"${on_write}\"\n"  +
            "          );"

    initTableData = "insert into ${tbName1} values(123456789, 'Alice', 1.8, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:48:26', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (234567890, 'Bob', 1.89, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 10:48:26', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (345678901, 'Carol', 2.6, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 10:48:26', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (456789012, 'Dave', 3.9, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 10:48:26', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (567890123, 'Eve', 4.2, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 10:48:26', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (678901234, 'Frank', 2.5, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 10:48:26', {'a': 600, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (789012345, 'Grace', 2.1, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 10:48:26', {'a': 700, 'b': 200}, '[\"abc\", \"def\"]');"

    //TODO Test the unique model by modify a value type from DATETIMEV2  to BOOLEAN
    errorMessage = "errCode = 2, detailMessage = Can not change DATETIMEV2 to BOOLEAN"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time BOOLEAN  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 6.4, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', false, {'a': 100, 'b': 200}, '{\"k1\":\"v1\", \"k2\": 200}'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    // TODO Test the unique model by modify a value type from DATETIMEV2  to TINYINT
    errorMessage = "errCode = 2, detailMessage = Can not change DATETIMEV2 to TINYINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time TINYINT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 2.2, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', 1', {'a': 100, 'b': 200}, '{\"k1\":\"v1\", \"k2\": 200}'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //Test the unique model by modify a value type from DATETIMEV2  to SMALLINT
    errorMessage = "errCode = 2, detailMessage = Can not change DATETIMEV2 to SMALLINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time SMALLINT   """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 3, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', 2, {'a': 100, 'b': 200}, [\"abc\", \"def\"]); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)

    //Test the unique model by modify a value type from DATETIMEV2  to INT
    errorMessage = "errCode = 2, detailMessage = Can not change DATETIMEV2 to INT"
    expectException({
        sql initTable
        sql initTableData

        sql """ alter  table ${tbName1} MODIFY  column register_time INT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 4, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', 156, {'a': 100, 'b': 200}, [\"abc\", \"def\"]); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //Test the unique model by modify a value type from DATETIMEV2  to BIGINT
    errorMessage = "errCode = 2, detailMessage = Can not change DATETIMEV2 to BIGINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time BIGINT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 545645, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '15662', {'a': 100, 'b': 200}, [\"abc\", \"def\"]); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)

    //Test the unique model by modify a value type from  DATETIMEV2 to LARGEINT
    errorMessage = "errCode = 2, detailMessage = Can not change DATETIMEV2 to LARGEINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time LARGEINT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 156546, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', 15898, {'a': 100, 'b': 200}, [\"abc\", \"def\"]); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //Test the unique model by modify a value type from DATETIMEV2  to DOUBLE
    errorMessage = "errCode = 2, detailMessage = Can not change DATETIMEV2 to DOUBLE"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time DOUBLE  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 1.23, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', 3.65, {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //TODO Test the unique model by modify a value type from DATETIMEV2  to DECIMAL
    errorMessage = "errCode = 2, detailMessage = Can not change DATETIMEV2 to DECIMAL128"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time DECIMAL(38,0)  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 1.23, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', 3.6598, {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)


    //TODO Test the unique model by modify a value type from DATETIMEV2  to FLOAT
    errorMessage = "errCode = 2, detailMessage = Can not change DATETIMEV2 to FLOAT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time FLOAT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 2.6, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', 1.65, {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)


    // Test the unique model by modify a value type from DATETIMEV2  to DATE
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} MODIFY  column register_time DATE  """
    insertSql = "insert into ${tbName1} values(923456689, 'Alice', 1.2, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2003-12-31', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName1}")

    sql """ DROP TABLE IF EXISTS ${tbName2} """
    initTable2 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `score` FLOAT COMMENT \"分数\",\n" +
            "              `city` VARCHAR(20) COMMENT \"用户所在城市\",\n" +
            "              `age` SMALLINT COMMENT \"用户年龄\",\n" +
            "              `sex` TINYINT COMMENT \"用户性别\",\n" +
            "              `phone` LARGEINT COMMENT \"用户电话\",\n" +
            "              `address` VARCHAR(500) COMMENT \"用户地址\",\n" +
            "              `register_time` DATE COMMENT \"用户注册时间\",\n" +
            "              `m` Map<STRING, INT> NULL COMMENT \"\",\n" +
            "              `j` JSON NULL COMMENT \"\"\n" +
            "          )\n" +
            "          UNIQUE KEY(`user_id`, `username`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\",\n" +
            "          \"enable_unique_key_merge_on_write\" = \"${on_write}\"\n"  +
            "          );"

    initTableData2 = "insert into ${tbName2} values(123456789, 'Alice', 1.8, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (234567890, 'Bob', 1.89, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (345678901, 'Carol', 2.6, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (456789012, 'Dave', 3.9, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (567890123, 'Eve', 4.2, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (678901234, 'Frank', 2.5, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06', {'a': 600, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (923456689, 'Alice', 1.2, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2003-12-31', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (789012345, 'Grace', 2.1, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07', {'a': 700, 'b': 200}, '[\"abc\", \"def\"]');"

    sql initTable2
    sql initTableData2
    checkTableData("${tbName1}","${tbName2}","register_time")
    sql """ DROP TABLE IF EXISTS ${tbName1} """



    //Test the unique model by modify a value type from DATETIMEV2  to DATEV2
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName1} MODIFY  column register_time DATEV2  """
    insertSql = "insert into ${tbName1} values(923456689, 'Alice', 5.69, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2003-12-31', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName1}")

    sql """ DROP TABLE IF EXISTS ${tbName2} """
    initTable2 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `score` FLOAT COMMENT \"分数\",\n" +
            "              `city` VARCHAR(20) COMMENT \"用户所在城市\",\n" +
            "              `age` SMALLINT COMMENT \"用户年龄\",\n" +
            "              `sex` TINYINT COMMENT \"用户性别\",\n" +
            "              `phone` LARGEINT COMMENT \"用户电话\",\n" +
            "              `address` VARCHAR(500) COMMENT \"用户地址\",\n" +
            "              `register_time` DATEV2 COMMENT \"用户注册时间\",\n" +
            "              `m` Map<STRING, INT> NULL COMMENT \"\",\n" +
            "              `j` JSON NULL COMMENT \"\"\n" +
            "          )\n" +
            "          UNIQUE KEY(`user_id`, `username`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\",\n" +
            "          \"enable_unique_key_merge_on_write\" = \"${on_write}\"\n"  +
            "          );"

    initTableData2 = "insert into ${tbName2} values(123456789, 'Alice', 1.8, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (234567890, 'Bob', 1.89, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (345678901, 'Carol', 2.6, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (456789012, 'Dave', 3.9, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (567890123, 'Eve', 4.2, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (678901234, 'Frank', 2.5, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06', {'a': 600, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (923456689, 'Alice', 1.2, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2003-12-31', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (789012345, 'Grace', 2.1, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07', {'a': 700, 'b': 200}, '[\"abc\", \"def\"]');"

    sql initTable2
    sql initTableData2
    checkTableData("${tbName1}","${tbName2}","register_time")
    sql """ DROP TABLE IF EXISTS ${tbName1} """




    //TODO Test the unique model by modify a  value type from DATETIMEV2  to CHAR
    errorMessage = "errCode = 2, detailMessage = Can not change DATETIMEV2 to CHAR"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time CHAR(15)  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 1.69, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', 'cs1', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)

    //TODO Test the unique model by modify a  value type from DATETIMEV2  to VARCHAR
    //Test the unique model by modify a value type from DATETIME  to VARCHAR
    errorMessage = "errCode = 2, detailMessage = Can not change DATETIMEV2 to VARCHAR"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time VARCHAR(100)  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 8.45, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', 'asd', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)

    //TODO Test the unique model by modify a  value type from DATETIMEV2  to STRING
    //Test the unique model by modify a value type from DATETIME  to STRING
    errorMessage = "errCode = 2, detailMessage = Can not change DATETIMEV2 to STRING"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time STRING  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 1.89, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //TODO Test the unique model by modify a  value type from DATETIMEV2  to map
    //Test the unique model by modify a value type from DATETIME  to STRING
    errorMessage = "errCode = 2, detailMessage = Can not change DATETIMEV2 to MAP"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time Map<STRING, INT>  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 1.49, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', {'a': 100, 'b': 200}, {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //Test the unique model by modify a value type from DATETIMEV2  to JSON
    errorMessage = "errCode = 2, detailMessage = Can not change DATETIMEV2 to JSON"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column register_time JSON  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 1.34, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '{'a': 100, 'b': 200}', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)



}