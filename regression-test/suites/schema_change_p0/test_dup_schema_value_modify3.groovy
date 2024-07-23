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

suite("test_dup_schema_value_modify3", "p0") {
    def tbName1 = "dup_model_value_change3"
    def tbName2 = "dup_model_value_change_3"
    //Test the dup model by adding a value column
    sql """ DROP TABLE IF EXISTS ${tbName1} """
    def getTableStatusSql = " SHOW ALTER TABLE COLUMN WHERE IndexName='${tbName1}' ORDER BY createtime DESC LIMIT 1  "
    def errorMessage = ""
    /**
     *  Test the dup model by modify a value type
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
            "          DUPLICATE KEY(`user_id`, `username`)\n" +
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


    /**
     *  Test the dup model by modify a value type from MAP to other type
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

    //TODO Test the dup model by modify a value type from MAP  to BOOLEAN
    errorMessage = "errCode = 2, detailMessage = Can not change MAP to BOOLEAN"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column m BOOLEAN  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 1.2, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', false, '{\"k1\":\"v1\", \"k2\": 200}'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    // TODO Test the dup model by modify a value type from MAP  to TINYINT
    errorMessage = "errCode = 2, detailMessage = Can not change MAP to TINYINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column m TINYINT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 2.2, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', 1, '{\"k1\":\"v1\", \"k2\": 200}'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 120
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //Test the dup model by modify a value type from MAP  to SMALLINT
    errorMessage = "errCode = 2, detailMessage = Can not change MAP to SMALLINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column m SMALLINT   """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 3, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', 3, [\"abc\", \"def\"]); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)

    //Test the dup model by modify a value type from MAP  to INT
    errorMessage = "errCode = 2, detailMessage = Can not change MAP to INT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column m INT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 4.1, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', 23, [\"abc\", \"def\"]); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)


    //Test the dup model by modify a value type from MAP  to BIGINT
    errorMessage = "errCode = 2, detailMessage = Can not change MAP to BIGINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column m BIGINT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 5.6, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', 4564, [\"abc\", \"def\"]); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)

    //Test the dup model by modify a value type from  MAP to LARGEINT
    errorMessage = "errCode = 2, detailMessage = Can not change MAP to LARGEINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column m LARGEINT """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 2.36, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', 43643734, [\"abc\", \"def\"]); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //Test the dup model by modify a value type from MAP  to FLOAT
    errorMessage = "errCode = 2, detailMessage = Can not change MAP to FLOAT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column m FLOAT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 1.23, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', 5.6, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //TODO Test the dup model by modify a value type from MAP  to DECIMAL
    errorMessage = "errCode = 2, detailMessage = Can not change MAP to DECIMAL128"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column m DECIMAL(38,0)  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 1.23, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', 895.666, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)


    //TODO Test the dup model by modify a value type from MAP  to DATE
    errorMessage = "errCode = 2, detailMessage = Can not change MAP to DATEV2"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column m DATE  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 5.6, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', '2003-12-31', '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)

    //TODO Test the dup model by modify a value type from MAP  to DATEV2
    errorMessage = "errCode = 2, detailMessage = Can not change MAP to DATEV2"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column m DATEV2  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 6.3, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', '2003-12-31', '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)


    //TODO Test the dup model by modify a value type from MAP  to DATETIME
    errorMessage = "errCode = 2, detailMessage = Can not change MAP to DATETIMEV2"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column m DATETIME  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 9.63, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', '2003-12-31 20:12:12', '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)

    //TODO Test the dup model by modify a value type from MAP  to DATETIME
    errorMessage = "errCode = 2, detailMessage = Can not change MAP to DATETIMEV2"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column m DATETIMEV2  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 5.69, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', '2003-12-31 20:12:12', '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)


    //Test the dup model by modify a value type from MAP  to VARCHAR
    errorMessage = "errCode = 2, detailMessage = Can not change MAP to VARCHAR"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column m VARCHAR(100)  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 5.69, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', 'sdfghjk', '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, false, "${tbName1}")
    }, errorMessage)

    //Test the dup model by modify a value type from MAP  to STRING
    errorMessage = "errCode = 2, detailMessage = Can not change MAP to STRING"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column m STRING  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 6.59, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', 'wertyu', '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, false, "${tbName1}")
    }, errorMessage)

    //Test the dup model by modify a value type from MAP  to JSON
    errorMessage = "errCode = 2, detailMessage = Can not change MAP to JSON"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column m JSON  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 8.47, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', '{'a': 100, 'b': 200}', '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    /**
     *  Test the dup model by modify a value type from JSON to other type
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

    //TODO Test the dup model by modify a value type from JSON  to BOOLEAN
    errorMessage = "errCode = 2, detailMessage = Can not change JSON to BOOLEAN"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column j BOOLEAN  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 1.2, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', , false); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    // TODO Test the dup model by modify a value type from JSON  to TINYINT
    errorMessage = "errCode = 2, detailMessage = Can not change JSON to TINYINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column j TINYINT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 2.2, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, 1); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 120
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //Test the dup model by modify a value type from JSON  to SMALLINT
    errorMessage = "errCode = 2, detailMessage = Can not change JSON to SMALLINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column j SMALLINT   """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 3, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, 21); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)

    //Test the dup model by modify a value type from JSON  to INT
    errorMessage = "errCode = 2, detailMessage = Can not change JSON to INT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column j INT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 4.1, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, 25); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)


    //Test the dup model by modify a value type from JSON  to BIGINT
    errorMessage = "errCode = 2, detailMessage = Can not change JSON to BIGINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column j BIGINT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 5.6, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, 32523); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)

    //Test the dup model by modify a value type from  JSON to LARGEINT
    errorMessage = "errCode = 2, detailMessage = Can not change JSON to LARGEINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column j LARGEINT """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 2.36, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, 356436); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //Test the dup model by modify a value type from JSON  to FLOAT
    errorMessage = "errCode = 2, detailMessage = Can not change JSON to FLOAT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column j FLOAT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 1.23, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, 86.5); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //TODO Test the dup model by modify a value type from JSON  to DECIMAL
    errorMessage = "errCode = 2, detailMessage = Can not change JSON to DECIMAL128"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column j DECIMAL(38,0)  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 1.23, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, 896.2356); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)


    //TODO Test the dup model by modify a value type from JSON  to DATE
    errorMessage = "errCode = 2, detailMessage = Can not change JSON to DATEV2"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column j DATE  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 5.6, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, '2003-12-31'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)

    //TODO Test the dup model by modify a value type from JSON  to DATEV2
    errorMessage = "errCode = 2, detailMessage = Can not change JSON to DATEV2"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column j DATEV2  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 6.3, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, '2003-12-31'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)


    //TODO Test the dup model by modify a value type from JSON  to DATETIME
    errorMessage = "errCode = 2, detailMessage = Can not change JSON to DATETIMEV2"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column j DATETIME  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 9.63, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, '2003-12-31 20:12:12'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)

    //TODO Test the dup model by modify a value type from JSON  to DATETIME
    errorMessage = "errCode = 2, detailMessage = Can not change JSON to DATETIMEV2"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column j DATETIMEV2  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 5.69, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, '2003-12-31 20:12:12'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)


    //Test the dup model by modify a value type from JSON  to VARCHAR
    errorMessage = "errCode = 2, detailMessage = Can not change JSON to VARCHAR"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column j VARCHAR(100)  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 5.69, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, 'erwtewxa'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, false, "${tbName1}")
    }, errorMessage)

    //Test the dup model by modify a value type from JSON  to STRING
    errorMessage = "errCode = 2, detailMessage = Can not change JSON to STRING"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column j STRING  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 6.59, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, '36tgeryda'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, false, "${tbName1}")
    }, errorMessage)

    //Test the dup model by modify a value type from JSON  to MAP
    errorMessage = "expected:<[FINISH]ED> but was:<[CANCELL]ED>"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column j JSON  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 8.47, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, {'a': 700, 'b': 200}); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)

    /**
     * Test the dup model by modify a value type from array to other type
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
            "              `j` JSON NULL COMMENT \"\",\n" +
            "              `array` ARRAY<int(11)> NULL COMMENT \"\",\n" +
            "              `STRUCT` STRUCT<s_id:int(11), s_name:string, s_address:string> NULL COMMENT \"\"\n" +
            "          )\n" +
            "          DUPLICATE KEY(`user_id`, `username`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData = "insert into ${tbName1} values(123456789, 'Alice', 1.83, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]', [6,7,8], {1, 'sn1', 'sa1'}), " +
            "               (234567890, 'Bob', 1.89, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]', [6,7,8], {1, 'sn1', 'sa1'}), " +
            "               (345678901, 'Carol', 2.6689, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]', [6,7,8], {1, 'sn1', 'sa1'}), " +
            "               (456789012, 'Dave', 3.9456, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]', [6,7,8], {1, 'sn1', 'sa1'}), " +
            "               (567890123, 'Eve', 4.223, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]', [6,7,8], {1, 'sn1', 'sa1'}), " +
            "               (678901234, 'Frank', 2.5454, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00', {'a': 600, 'b': 200}, '[\"abc\", \"def\"]', [6,7,8], {1, 'sn1', 'sa1'}), " +
            "               (789012345, 'Grace', 2.19656, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00', {'a': 700, 'b': 200}, '[\"abc\", \"def\"]', [6,7,8], {1, 'sn1', 'sa1'});"

    //TODO Test the dup model by modify a value type from array  to BOOLEAN
    errorMessage = "errCode = 2, detailMessage = Can not change ARRAY to BOOLEAN"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column array BOOLEAN  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 1.2, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, '[\"abc\", \"def\"]', false, {1, 'sn1', 'sa1'}); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    // TODO Test the dup model by modify a value type from ARRAY  to TINYINT
    errorMessage = "errCode = 2, detailMessage = Can not change ARRAY to TINYINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column array TINYINT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 2.2, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, '[\\\"abc\\\", \\\"def\\\"]', 1, {1, 'sn1', 'sa1'}); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 120
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //Test the dup model by modify a value type from ARRAY  to SMALLINT
    errorMessage = "errCode = 2, detailMessage = Can not change ARRAY to SMALLINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column array SMALLINT   """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 3, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, '[\\\"abc\\\", \\\"def\\\"]', 21, {1, 'sn1', 'sa1'}); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)

    //Test the dup model by modify a value type from ARRAY  to INT
    errorMessage = "errCode = 2, detailMessage = Can not change ARRAY to INT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column array INT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 4.1, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, '[\\\"abc\\\", \\\"def\\\"]', 25, {1, 'sn1', 'sa1'}); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)


    //Test the dup model by modify a value type from ARRAY  to BIGINT
    errorMessage = "errCode = 2, detailMessage = Can not change ARRAY to BIGINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column array BIGINT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 5.6, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, '[\\\"abc\\\", \\\"def\\\"]', 32454, {1, 'sn1', 'sa1'}); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)

    //Test the dup model by modify a value type from  ARRAY to LARGEINT
    errorMessage = "errCode = 2, detailMessage = Can not change ARRAY to LARGEINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column array LARGEINT """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 2.36, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, '[\\\"abc\\\", \\\"def\\\"]', 34235, {1, 'sn1', 'sa1'}); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //Test the dup model by modify a value type from ARRAY  to FLOAT
    errorMessage = "errCode = 2, detailMessage = Can not change ARRAY to FLOAT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column array FLOAT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 1.23, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, '[\\\"abc\\\", \\\"def\\\"]', 45,8, {1, 'sn1', 'sa1'}); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //TODO Test the dup model by modify a value type from ARRAY  to DECIMAL
    errorMessage = "errCode = 2, detailMessage = Can not change ARRAY to DECIMAL128"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column array DECIMAL(38,0)  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 1.23, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, '[\\\"abc\\\", \\\"def\\\"]', 677.908, {1, 'sn1', 'sa1'}); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)


    //TODO Test the dup model by modify a value type from ARRAY  to DATE
    errorMessage = "errCode = 2, detailMessage = Can not change ARRAY to DATEV2"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column array DATE  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 5.6, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, '[\\\"abc\\\", \\\"def\\\"]', '2023-10-23', {1, 'sn1', 'sa1'}); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)

    //TODO Test the dup model by modify a value type from ARRAY  to DATEV2
    errorMessage = "errCode = 2, detailMessage = Can not change ARRAY to DATEV2"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column array DATEV2  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 6.3, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, '[\\\"abc\\\", \\\"def\\\"]', '2023-10-23', {1, 'sn1', 'sa1'}); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)


    //TODO Test the dup model by modify a value type from ARRAY  to DATETIME
    errorMessage = "errCode = 2, detailMessage = Can not change ARRAY to DATETIMEV2"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column array DATETIME  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 9.63, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, '[\\\"abc\\\", \\\"def\\\"]', '2023-10-23 15:00:26', {1, 'sn1', 'sa1'}); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)

    //TODO Test the dup model by modify a value type from ARRAY  to DATETIME
    errorMessage = "errCode = 2, detailMessage = Can not change ARRAY to DATETIMEV2"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column array DATETIMEV2  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 5.69, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, '[\\\"abc\\\", \\\"def\\\"]', '2023-10-26 15:54:21', {1, 'sn1', 'sa1'}); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)


    //Test the dup model by modify a value type from ARRAY  to VARCHAR
    errorMessage = "errCode = 2, detailMessage = Can not change ARRAY to VARCHAR"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column array VARCHAR(100)  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 5.69, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, '[\\\"abc\\\", \\\"def\\\"]', 'wrwertew', {1, 'sn1', 'sa1'}); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, false, "${tbName1}")
    }, errorMessage)

    //Test the dup model by modify a value type from ARRAY  to STRING
    errorMessage = "errCode = 2, detailMessage = Can not change ARRAY to STRING"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column array STRING  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 6.59, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, '[\\\"abc\\\", \\\"def\\\"]', 'eterytergfds', {1, 'sn1', 'sa1'}); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, false, "${tbName1}")
    }, errorMessage)

    //Test the dup model by modify a value type from ARRAY  to JSON
    errorMessage = "errCode = 2, detailMessage = Can not change ARRAY to JSON"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column array JSON  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 8.47, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, '[\\\"abc\\\", \\\"def\\\"]', {'a': 700, 'b':500}, {1, 'sn1', 'sa1'}); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)

    //Test the dup model by modify a value type from ARRAY  to JSON
    errorMessage = "errCode = 2, detailMessage = Can not change ARRAY to MAP"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column array Map<STRING, INT>  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 8.47, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, '[\"abc\", \"def\"]', '[\"abc\", \"def\"]', {1, 'sn1', 'sa1'}); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //Test the dup model by modify a value type from ARRAY  to JSON
    errorMessage = "errCode = 2, detailMessage = Can not change ARRAY to STRUCT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column array STRUCT<s_id:int(11), s_name:string, s_address:string>  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 8.47, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, '[\"abc\", \"def\"]', '[\"abc\", \"def\"]', {1, 'sn1', 'sa1'}); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    /**
     * Test the dup model by modify a value type from STRUCT to other type
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
            "              `j` JSON NULL COMMENT \"\",\n" +
            "              `array` ARRAY<int(11)> NULL COMMENT \"\",\n" +
            "              `STRUCT` STRUCT<s_id:int(11), s_name:string, s_address:string> NULL COMMENT \"\"\n" +
            "          )\n" +
            "          DUPLICATE KEY(`user_id`, `username`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
            "          );"

    initTableData = "insert into ${tbName1} values(123456789, 'Alice', 1.83, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]', [6,7,8], {1, 'sn1', 'sa1'}), " +
            "               (234567890, 'Bob', 1.89, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]', [6,7,8], {1, 'sn1', 'sa1'}), " +
            "               (345678901, 'Carol', 2.6689, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]', [6,7,8], {1, 'sn1', 'sa1'}), " +
            "               (456789012, 'Dave', 3.9456, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]', [6,7,8], {1, 'sn1', 'sa1'}), " +
            "               (567890123, 'Eve', 4.223, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]', [6,7,8], {1, 'sn1', 'sa1'}), " +
            "               (678901234, 'Frank', 2.5454, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00', {'a': 600, 'b': 200}, '[\"abc\", \"def\"]', [6,7,8], {1, 'sn1', 'sa1'}), " +
            "               (789012345, 'Grace', 2.19656, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00', {'a': 700, 'b': 200}, '[\"abc\", \"def\"]', [6,7,8], {1, 'sn1', 'sa1'});"

    //TODO Test the dup model by modify a value type from STRUCT  to BOOLEAN
    errorMessage = "errCode = 2, detailMessage = Can not change STRUCT to BOOLEAN"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column STRUCT BOOLEAN  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 1.2, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, '[\"abc\", \"def\"]', [6,7,8], false); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    // TODO Test the dup model by modify a value type from STRUCT  to TINYINT
    errorMessage = "errCode = 2, detailMessage = Can not change STRUCT to TINYINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column STRUCT TINYINT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 2.2, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, '[\\\"abc\\\", \\\"def\\\"]', [6,7,8], 1); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 120
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //Test the dup model by modify a value type from STRUCT  to SMALLINT
    errorMessage = "errCode = 2, detailMessage = Can not change STRUCT to SMALLINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column STRUCT SMALLINT   """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 3, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, '[\\\"abc\\\", \\\"def\\\"]', [6,7,8], 21); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)

    //Test the dup model by modify a value type from STRUCT  to INT
    errorMessage = "errCode = 2, detailMessage = Can not change STRUCT to INT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column STRUCT INT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 4.1, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, '[\\\"abc\\\", \\\"def\\\"]', [6,7,8], 21); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)


    //Test the dup model by modify a value type from STRUCT  to BIGINT
    errorMessage = "errCode = 2, detailMessage = Can not change STRUCT to BIGINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column STRUCT BIGINT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 5.6, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, '[\\\"abc\\\", \\\"def\\\"]', [6,7,8], 32454); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)

    //Test the dup model by modify a value type from  STRUCT to LARGEINT
    errorMessage = "errCode = 2, detailMessage = Can not change STRUCT to LARGEINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column STRUCT LARGEINT """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 2.36, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, '[\\\"abc\\\", \\\"def\\\"]', [6,7,8], 34235); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //Test the dup model by modify a value type from STRUCT  to FLOAT
    errorMessage = "errCode = 2, detailMessage = Can not change STRUCT to FLOAT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column STRUCT FLOAT  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 1.23, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, '[\\\"abc\\\", \\\"def\\\"]', [6,7,8], 45.5); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //TODO Test the dup model by modify a value type from STRUCT  to DECIMAL
    errorMessage = "errCode = 2, detailMessage = Can not change STRUCT to DECIMAL128"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column STRUCT DECIMAL(38,0)  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 1.23, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, '[\\\"abc\\\", \\\"def\\\"]', [6,7,8], 677.908); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)


    //TODO Test the dup model by modify a value type from STRUCT  to DATE
    errorMessage = "errCode = 2, detailMessage = Can not change STRUCT to DATEV2"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column STRUCT DATE  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 5.6, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, '[\\\"abc\\\", \\\"def\\\"]', [6,7,8], '2023-10-23'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)

    //TODO Test the dup model by modify a value type from STRUCT  to DATEV2
    errorMessage = "errCode = 2, detailMessage = Can not change STRUCT to DATEV2"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column STRUCT DATEV2  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 6.3, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, '[\\\"abc\\\", \\\"def\\\"]', [6,7,8], '2023-10-23'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)


    //TODO Test the dup model by modify a value type from STRUCT  to DATETIME
    errorMessage = "errCode = 2, detailMessage = Can not change STRUCT to DATETIMEV2"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column STRUCT DATETIME  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 9.63, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, '[\\\"abc\\\", \\\"def\\\"]', [6,7,8], '2023-10-23 15:00:26'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)

    //TODO Test the dup model by modify a value type from STRUCT  to DATETIME
    errorMessage = "errCode = 2, detailMessage = Can not change STRUCT to DATETIMEV2"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column STRUCT DATETIMEV2  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 5.69, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, '[\\\"abc\\\", \\\"def\\\"]', [6,7,8], '2023-10-26 15:54:21'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")

    }, errorMessage)


    //Test the dup model by modify a value type from STRUCT  to VARCHAR
    errorMessage = "errCode = 2, detailMessage = Can not change STRUCT to VARCHAR"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column STRUCT VARCHAR(100)  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 5.69, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, '[\\\"abc\\\", \\\"def\\\"]', [6,7,8], "ertet"); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, false, "${tbName1}")
    }, errorMessage)

    //Test the dup model by modify a value type from STRUCT  to STRING
    errorMessage = "errCode = 2, detailMessage = Can not change STRUCT to STRING"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column STRUCT STRING  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 6.59, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, '[\\\"abc\\\", \\\"def\\\"]', [6,7,8], "wrwerew"); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, false, "${tbName1}")
    }, errorMessage)

    //Test the dup model by modify a value type from STRUCT  to JSON
    errorMessage = "errCode = 2, detailMessage = Can not change STRUCT to JSON"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column STRUCT JSON  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 8.47, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, '[\\\"abc\\\", \\\"def\\\"]', [6,7,8],  {'a': 700, 'b': 200}); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)

    //Test the dup model by modify a value type from STRUCT  to MAP
    errorMessage = "errCode = 2, detailMessage = Can not change STRUCT to MAP"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column STRUCT Map<STRING, INT>  """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 8.47, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, '[\"abc\", \"def\"]', [6,7,8], '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


    //Test the dup model by modify a value type from STRUCT  to ARRAY
    errorMessage = "errCode = 2, detailMessage = Can not change STRUCT to ARRAY"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName1} MODIFY  column STRUCT ARRAY<int(11)> """
        insertSql = "insert into ${tbName1} values(923456689, 'Alice', 8.47, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, '[\"abc\", \"def\"]', [6,7,8], {1, 'sn1', 'sa1'}); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName1}")
    }, errorMessage)


}
