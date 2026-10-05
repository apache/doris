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

suite("test_unique_schema_value_modify3", "p0") {
    def tbName = "test_unique_model_value_change3"
    def tbName2 = "test_unique_model_value_change_3"
    def on_write = getRandomBoolean()
    println String.format("current enable_unique_key_merge_on_write is : %s ",on_write)
    //Test the unique model by adding a value column
    sql """ DROP TABLE IF EXISTS ${tbName} """
    def initTable = " CREATE TABLE IF NOT EXISTS ${tbName}\n" +
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
            "          UNIQUE KEY(`user_id`, `username`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\",\n" +
            "          \"enable_unique_key_merge_on_write\" = \"${on_write}\"\n"  +
            "          );"

    def initTableData = "insert into ${tbName} values(123456789, 'Alice', 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
            "               (234567890, 'Bob', 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
            "               (345678901, 'Carol', 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
            "               (456789012, 'Dave', 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
            "               (567890123, 'Eve', 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
            "               (678901234, 'Frank', 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
            "               (789012345, 'Grace', 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"

    //Test the unique model by adding a value column with VARCHAR
    sql initTable
    sql initTableData
    def getTableStatusSql = " SHOW ALTER TABLE COLUMN WHERE IndexName='${tbName}' ORDER BY createtime DESC LIMIT 1  "
    def errorMessage = ""
    def insertSql = "insert into ${tbName} values(923456689, 'Alice', '四川省', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00');"


    /**
     *  Test the unique model by modify a value type from MAP to other type
     */
    sql """ DROP TABLE IF EXISTS ${tbName} """
    initTable = " CREATE TABLE IF NOT EXISTS ${tbName}\n" +
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
            "          UNIQUE KEY(`user_id`, `username`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\",\n" +
            "          \"enable_unique_key_merge_on_write\" = \"${on_write}\"\n"  +
            "          );"

    initTableData = "insert into ${tbName} values(123456789, 'Alice', 1.83, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (234567890, 'Bob', 1.89, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (345678901, 'Carol', 2.6689, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (456789012, 'Dave', 3.9456, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (567890123, 'Eve', 4.223, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (678901234, 'Frank', 2.5454, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00', {'a': 600, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (789012345, 'Grace', 2.19656, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00', {'a': 700, 'b': 200}, '[\"abc\", \"def\"]');"

    //TODO Test the unique model by modify a value type from MAP  to BOOLEAN
    errorMessage = "errCode = 2, detailMessage = Can not change"
    expectExceptionLike({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName} MODIFY  column m BOOLEAN  """
        insertSql = "insert into ${tbName} values(923456689, 'Alice', 1.2, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', false, '{\"k1\":\"v1\", \"k2\": 200}'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName}")
    }, errorMessage)


    // TODO Test the unique model by modify a value type from MAP  to TINYINT
    errorMessage = "errCode = 2, detailMessage = Can not change"
    expectExceptionLike({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName} MODIFY  column m TINYINT  """
        insertSql = "insert into ${tbName} values(923456689, 'Alice', 2.2, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', 1, '{\"k1\":\"v1\", \"k2\": 200}'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 120
        }, insertSql, true, "${tbName}")
    }, errorMessage)


    //Test the unique model by modify a value type from MAP  to SMALLINT
    errorMessage = "errCode = 2, detailMessage = Can not change"
    expectExceptionLike({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName} MODIFY  column m SMALLINT   """
        insertSql = "insert into ${tbName} values(923456689, 'Alice', 3, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', 3, [\"abc\", \"def\"]); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName}")

    }, errorMessage)

    //Test the unique model by modify a value type from MAP  to INT
    errorMessage = "errCode = 2, detailMessage = Can not change"
    expectExceptionLike({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName} MODIFY  column m INT  """
        insertSql = "insert into ${tbName} values(923456689, 'Alice', 4.1, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', 23, [\"abc\", \"def\"]); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName}")

    }, errorMessage)


    //Test the unique model by modify a value type from MAP  to BIGINT
    errorMessage = "errCode = 2, detailMessage = Can not change"
    expectExceptionLike({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName} MODIFY  column m BIGINT  """
        insertSql = "insert into ${tbName} values(923456689, 'Alice', 5.6, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', 4564, [\"abc\", \"def\"]); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName}")

    }, errorMessage)

    //Test the unique model by modify a value type from  MAP to LARGEINT
    errorMessage = "errCode = 2, detailMessage = Can not change"
    expectExceptionLike({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName} MODIFY  column m LARGEINT """
        insertSql = "insert into ${tbName} values(923456689, 'Alice', 2.36, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', 43643734, [\"abc\", \"def\"]); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName}")
    }, errorMessage)


    //Test the unique model by modify a value type from MAP  to FLOAT
    errorMessage = "errCode = 2, detailMessage = Can not change"
    expectExceptionLike({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName} MODIFY  column m FLOAT  """
        insertSql = "insert into ${tbName} values(923456689, 'Alice', 1.23, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', 5.6, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName}")
    }, errorMessage)


    //TODO Test the unique model by modify a value type from MAP  to DECIMAL
    errorMessage = "errCode = 2, detailMessage = Can not change"
    expectExceptionLike({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName} MODIFY  column m DECIMAL(38,0)  """
        insertSql = "insert into ${tbName} values(923456689, 'Alice', 1.23, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', 895.666, '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName}")

    }, errorMessage)


    //TODO Test the unique model by modify a value type from MAP  to DATE
    errorMessage = "errCode = 2, detailMessage = Can not change"
    expectExceptionLike({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName} MODIFY  column m DATE  """
        insertSql = "insert into ${tbName} values(923456689, 'Alice', 5.6, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', '2003-12-31', '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName}")

    }, errorMessage)

    //TODO Test the unique model by modify a value type from MAP  to DATEV2
    errorMessage = "errCode = 2, detailMessage = Can not change"
    expectExceptionLike({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName} MODIFY  column m DATEV2  """
        insertSql = "insert into ${tbName} values(923456689, 'Alice', 6.3, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', '2003-12-31', '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName}")

    }, errorMessage)


    //TODO Test the unique model by modify a value type from MAP  to DATETIME
    errorMessage = "errCode = 2, detailMessage = Can not change"
    expectExceptionLike({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName} MODIFY  column m DATETIME  """
        insertSql = "insert into ${tbName} values(923456689, 'Alice', 9.63, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', '2003-12-31 20:12:12', '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName}")

    }, errorMessage)

    //TODO Test the unique model by modify a value type from MAP  to DATETIME
    errorMessage = "errCode = 2, detailMessage = Can not change"
    expectExceptionLike({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName} MODIFY  column m DATETIMEV2  """
        insertSql = "insert into ${tbName} values(923456689, 'Alice', 5.69, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', '2003-12-31 20:12:12', '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName}")

    }, errorMessage)


    //Test the unique model by modify a value type from MAP  to VARCHAR
    errorMessage = "errCode = 2, detailMessage = Can not change"
    expectExceptionLike({
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName} MODIFY  column m VARCHAR(100)  """
    insertSql = "insert into ${tbName} values(923456689, 'Alice', 5.69, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', 'sdfghjk', '[\"abc\", \"def\"]'); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName}")
    }, errorMessage)

    //Test the unique model by modify a value type from MAP  to STRING
    errorMessage = "errCode = 2, detailMessage = Can not change"
    expectExceptionLike({
    sql initTable
    sql initTableData
    sql """ alter  table ${tbName} MODIFY  column m STRING  """
    insertSql = "insert into ${tbName} values(923456689, 'Alice', 6.59, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', 'wertyu', '[\"abc\", \"def\"]'); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName}")
    }, errorMessage)

    //Test the unique model by modify a value type from MAP  to JSON
    errorMessage = "errCode = 2, detailMessage = Can not change"
    expectExceptionLike({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName} MODIFY  column m JSON  """
        insertSql = "insert into ${tbName} values(923456689, 'Alice', 8.47, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', '{'a': 100, 'b': 200}', '[\"abc\", \"def\"]'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName}")
    }, errorMessage)


    /**
     *  Test the unique model by modify a value type from JSON to other type
     */
    sql """ DROP TABLE IF EXISTS ${tbName} """
    initTable = " CREATE TABLE IF NOT EXISTS ${tbName}\n" +
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
            "          UNIQUE KEY(`user_id`, `username`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\",\n" +
            "          \"enable_unique_key_merge_on_write\" = \"${on_write}\"\n"  +
            "          );"

    initTableData = "insert into ${tbName} values(123456789, 'Alice', 1.83, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 100, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (234567890, 'Bob', 1.89, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00', {'a': 200, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (345678901, 'Carol', 2.6689, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00', {'a': 300, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (456789012, 'Dave', 3.9456, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00', {'a': 400, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (567890123, 'Eve', 4.223, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00', {'a': 500, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (678901234, 'Frank', 2.5454, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00', {'a': 600, 'b': 200}, '[\"abc\", \"def\"]')," +
            "               (789012345, 'Grace', 2.19656, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00', {'a': 700, 'b': 200}, '[\"abc\", \"def\"]');"

    //TODO Test the unique model by modify a value type from JSON  to BOOLEAN
    errorMessage = "errCode = 2, detailMessage = Can not change JSON to BOOLEAN"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName} MODIFY  column j BOOLEAN  """
        insertSql = "insert into ${tbName} values(923456689, 'Alice', 1.2, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', , false); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName}")
    }, errorMessage)


    // TODO Test the unique model by modify a value type from JSON  to TINYINT
    errorMessage = "errCode = 2, detailMessage = Can not change JSON to TINYINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName} MODIFY  column j TINYINT  """
        insertSql = "insert into ${tbName} values(923456689, 'Alice', 2.2, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, 1); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 120
        }, insertSql, true, "${tbName}")
    }, errorMessage)


    //Test the unique model by modify a value type from JSON  to SMALLINT
    errorMessage = "errCode = 2, detailMessage = Can not change JSON to SMALLINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName} MODIFY  column j SMALLINT   """
        insertSql = "insert into ${tbName} values(923456689, 'Alice', 3, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, 21); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName}")

    }, errorMessage)

    //Test the unique model by modify a value type from JSON  to INT
    errorMessage = "errCode = 2, detailMessage = Can not change JSON to INT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName} MODIFY  column j INT  """
        insertSql = "insert into ${tbName} values(923456689, 'Alice', 4.1, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, 25); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName}")

    }, errorMessage)


    //Test the unique model by modify a value type from JSON  to BIGINT
    errorMessage = "errCode = 2, detailMessage = Can not change JSON to BIGINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName} MODIFY  column j BIGINT  """
        insertSql = "insert into ${tbName} values(923456689, 'Alice', 5.6, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, 32523); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName}")

    }, errorMessage)

    //Test the unique model by modify a value type from  JSON to LARGEINT
    errorMessage = "errCode = 2, detailMessage = Can not change JSON to LARGEINT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName} MODIFY  column j LARGEINT """
        insertSql = "insert into ${tbName} values(923456689, 'Alice', 2.36, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, 356436); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName}")
    }, errorMessage)


    //Test the unique model by modify a value type from JSON  to FLOAT
    errorMessage = "errCode = 2, detailMessage = Can not change JSON to FLOAT"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName} MODIFY  column j FLOAT  """
        insertSql = "insert into ${tbName} values(923456689, 'Alice', 1.23, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, 86.5); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName}")
    }, errorMessage)


    //TODO Test the unique model by modify a value type from JSON  to DECIMAL
    errorMessage = "errCode = 2, detailMessage = Can not change JSON to DECIMAL128"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName} MODIFY  column j DECIMAL(38,0)  """
        insertSql = "insert into ${tbName} values(923456689, 'Alice', 1.23, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, 896.2356); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName}")

    }, errorMessage)


    //TODO Test the unique model by modify a value type from JSON  to DATE
    errorMessage = "errCode = 2, detailMessage = Can not change JSON to DATEV2"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName} MODIFY  column j DATE  """
        insertSql = "insert into ${tbName} values(923456689, 'Alice', 5.6, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, '2003-12-31'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName}")

    }, errorMessage)

    //TODO Test the unique model by modify a value type from JSON  to DATEV2
    errorMessage = "errCode = 2, detailMessage = Can not change JSON to DATEV2"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName} MODIFY  column j DATEV2  """
        insertSql = "insert into ${tbName} values(923456689, 'Alice', 6.3, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, '2003-12-31'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName}")

    }, errorMessage)


    //TODO Test the unique model by modify a value type from JSON  to DATETIME
    errorMessage = "errCode = 2, detailMessage = Can not change JSON to DATETIMEV2"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName} MODIFY  column j DATETIME  """
        insertSql = "insert into ${tbName} values(923456689, 'Alice', 9.63, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, '2003-12-31 20:12:12'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName}")

    }, errorMessage)

    //TODO Test the unique model by modify a value type from JSON  to DATETIME
    errorMessage = "errCode = 2, detailMessage = Can not change JSON to DATETIMEV2"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName} MODIFY  column j DATETIMEV2  """
        insertSql = "insert into ${tbName} values(923456689, 'Alice', 5.69, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, '2003-12-31 20:12:12'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName}")

    }, errorMessage)


    //Test the unique model by modify a value type from JSON  to VARCHAR
    errorMessage = "errCode = 2, detailMessage = Can not change JSON to VARCHAR"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName} MODIFY  column j VARCHAR(100)  """
        insertSql = "insert into ${tbName} values(923456689, 'Alice', 5.69, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, 'erwtewxa'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, false, "${tbName}")
    }, errorMessage)

    //Test the unique model by modify a value type from JSON  to STRING
    errorMessage = "errCode = 2, detailMessage = Can not change JSON to STRING"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName} MODIFY  column j STRING  """
        insertSql = "insert into ${tbName} values(923456689, 'Alice', 6.59, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, '36tgeryda'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, false, "${tbName}")
    }, errorMessage)

    //Test the unique model by modify a value type from JSON  to MAP
    errorMessage = "expected:<[FINISH]ED> but was:<[CANCELL]ED>"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName} MODIFY  column j JSON  """
        insertSql = "insert into ${tbName} values(923456689, 'Alice', 8.47, 'yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00', {'a': 700, 'b': 200}, {'a': 700, 'b': 200}); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName}")
    }, errorMessage)


}
