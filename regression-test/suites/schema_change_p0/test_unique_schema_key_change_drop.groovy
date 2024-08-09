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

suite("test_unique_schema_key_change_drop", "p0") {
    def tbName = "test_unique_schema_key_change_drop"
    def tbName2 = "test_unique_model_schema_key_drop_1"
    sql """ DROP TABLE IF EXISTS ${tbName} """
    def getTableStatusSql = " SHOW ALTER TABLE COLUMN WHERE IndexName='${tbName}' ORDER BY createtime DESC LIMIT 1  "
    def errorMessage = ""
    def insertSql = "insert into ${tbName} values(923456689, 'Alice', '四川省', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00');"
    def on_write = getRandomBoolean()
    println String.format("current enable_unique_key_merge_on_write is : %s ",on_write)


    /**
     *  Test the unique model by dorp a key type
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
            "              `t_datetime` DATETIME COMMENT \"用户注册时间\"\n" +
            "          )\n" +
            "          UNIQUE KEY(`user_id`, `username`,  `score`, `city`, `age`, `sex`, `phone`,`is_ok`, `t_int`, `t_bigint`, `t_date`, `t_datev2`, `t_datetimev2`,  `t_datetime`)\n" +
            "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
            "          PROPERTIES (\n" +
            "          \"replication_allocation\" = \"tag.location.default: 1\",\n" +
            "              \"enable_unique_key_merge_on_write\" = \"${on_write}\"\n" +
            "          );"

    def initTableData = "insert into ${tbName} values(1, 'John Doe', 95.5, 'New York', 25, 1, 1234567890, true, 10, 1000000000, '2024-06-11', '2024-06-11', '2024-06-11 08:30:00', '2024-06-11 08:30:00')," +
            "               (2, 'Jane Smith', 85.2, 'Los Angeles', 30, 2, 9876543210, false, 20, 2000000000, '2024-06-12', '2024-06-12', '2024-06-12 09:45:00', '2024-06-12 09:45:00')," +
            "               (3, 'Mike Johnson', 77.8, 'Chicago', 35, 1, 1112223334, true, 30, 3000000000, '2024-06-13', '2024-06-13', '2024-06-13 11:15:00', '2024-06-13 11:15:00')," +
            "               (4, 'Emily Brown', 92.0, 'San Francisco', 28, 2, 5556667778, true, 40, 4000000000, '2024-06-14', '2024-06-14', '2024-06-14 13:30:00', '2024-06-14 13:30:00')," +
            "               (5, 'David Wilson', 88.9, 'Seattle', 32, 1, 9998887776, false, 50, 5000000000, '2024-06-15', '2024-06-15', '2024-06-15 15:45:00', '2024-06-15 15:45:00');"

    //TODO Test the unique model by drop a key type from BOOLEAN
    errorMessage = "errCode = 2, detailMessage = Can not drop key column in Unique data model table"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName} DROP  column is_ok  """
        insertSql = "insert into ${tbName} values(6, 'Sophia Lee', 91.3, 'Boston', 29, 2, 7778889990,  60, 6000000000, '2024-06-16', '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00', 'Test String 6', {'a': 500, 'b': 200}, '{\"k1\":\"v1\", \"k2\": 200}'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName}")
    }, errorMessage)


    //TODO Test the unique model by drop a key type from TINYINT
    errorMessage = "errCode = 2, detailMessage = Can not drop key column in Unique data model table"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName} DROP  column sex  """
        insertSql = "insert into ${tbName} values(6, 'Sophia Lee', 91.3, 'Boston', 29,  7778889990, true, 60, 6000000000, '2024-06-16', '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00', 'Test String 6', {'a': 500, 'b': 200}, '{\"k1\":\"v1\", \"k2\": 200}'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName}")
    }, errorMessage)


    //TODO Test the unique model by drop a key type from SMALLINT
    errorMessage = "errCode = 2, detailMessage = Can not drop key column in Unique data model table"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName} DROP  column age  """
        insertSql = "insert into ${tbName} values(6, 'Sophia Lee', 91.3, 'Boston', 2, 7778889990, true, 60, 6000000000, '2024-06-16', '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00', 'Test String 6', {'a': 500, 'b': 200}, '{\"k1\":\"v1\", \"k2\": 200}'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName}")
    }, errorMessage)

    //TODO Test the unique model by drop a key type from INT
    errorMessage = "errCode = 2, detailMessage = Can not drop key column in Unique data model table"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName} DROP  column t_int  """
        insertSql = "insert into ${tbName} values(6, 'Sophia Lee', 91.3, 'Boston', 29, 2, 7778889990, true,  6000000000, '2024-06-16', '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00', 'Test String 6', {'a': 500, 'b': 200}, '{\"k1\":\"v1\", \"k2\": 200}'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName}")
    }, errorMessage)


    //TODO Test the unique model by drop a key type from BIGINT
    errorMessage = "errCode = 2, detailMessage = Can not drop key column in Unique data model table"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName} DROP  column t_bigint  """
        insertSql = "insert into ${tbName} values(6, 'Sophia Lee', 91.3, 'Boston', 29, 2, 7778889990, true, 60,  '2024-06-16', '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00', 'Test String 6', {'a': 500, 'b': 200}, '{\"k1\":\"v1\", \"k2\": 200}'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName}")
    }, errorMessage)


    //TODO  Test the unique model by drop a key type from LARGEINT
    errorMessage = "errCode = 2, detailMessage = Can not drop key column in Unique data model table"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName} DROP  column phone  """
        insertSql = "insert into ${tbName} values(6, 'Sophia Lee', 91.3, 'Boston', 29, 2, true, 60, 6000000000, '2024-06-16', '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00', 'Test String 6', {'a': 500, 'b': 200}, '{\"k1\":\"v1\", \"k2\": 200}'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName}")
    }, errorMessage)

    //TODO  Test the unique model by drop a key type from DATE
    errorMessage = "errCode = 2, detailMessage = Can not drop key column in Unique data model table"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName} DROP  column t_date  """
        insertSql = "insert into ${tbName} values(6, 'Sophia Lee', 91.3, 'Boston', 29, 2, 7778889990, true, 60, 6000000000,  '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00', 'Test String 6', {'a': 500, 'b': 200}, '{\"k1\":\"v1\", \"k2\": 200}'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName}")
    }, errorMessage)


    //TODO  Test the unique model by drop a key type from DATEV2
    errorMessage = "errCode = 2, detailMessage = Can not drop key column in Unique data model table"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName} DROP  column t_datev2  """
        insertSql = "insert into ${tbName} values(6, 'Sophia Lee', 91.3, 'Boston', 29, 2, 7778889990, true, 60, 6000000000,  '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00', 'Test String 6', {'a': 500, 'b': 200}, '{\"k1\":\"v1\", \"k2\": 200}'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName}")
    }, errorMessage)


    //TODO  Test the unique model by drop a key type from t_datetimev2
    errorMessage = "errCode = 2, detailMessage = Can not drop key column in Unique data model table"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName} DROP  column t_datetimev2  """
        insertSql = "insert into ${tbName} values(6, 'Sophia Lee', 91.3, 'Boston', 29, 2, 7778889990, true, 60, 6000000000,  '2024-06-16', '2024-06-16',  '2024-06-16 17:00:00', 'Test String 6', {'a': 500, 'b': 200}, '{\"k1\":\"v1\", \"k2\": 200}'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName}")
    }, errorMessage)


    //TODO  Test the unique model by drop a key type from t_datetimev2
    errorMessage = "errCode = 2, detailMessage = Can not drop key column in Unique data model table"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName} DROP  column t_datetimev2  """
        insertSql = "insert into ${tbName} values(6, 'Sophia Lee', 91.3, 'Boston', 29, 2, 7778889990, true, 60, 6000000000,  '2024-06-16', '2024-06-16',  '2024-06-16 17:00:00', 'Test String 6', {'a': 500, 'b': 200}, '{\"k1\":\"v1\", \"k2\": 200}'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName}")
    }, errorMessage)


    //TODO  Test the unique model by drop a key type from t_datetime
    errorMessage = "errCode = 2, detailMessage = Can not drop key column in Unique data model table"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName} DROP  column t_datetime  """
        insertSql = "insert into ${tbName} values(6, 'Sophia Lee', 91.3, 'Boston', 29, 2, 7778889990, true, 60, 6000000000,  '2024-06-16', '2024-06-16',  '2024-06-16 17:00:00', 'Test String 6', {'a': 500, 'b': 200}, '{\"k1\":\"v1\", \"k2\": 200}'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName}")
    }, errorMessage)


    //TODO  Test the unique model by drop a key type from CHAR
    errorMessage = "errCode = 2, detailMessage = Can not drop key column in Unique data model table"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName} DROP  column city  """
        insertSql = "insert into ${tbName} values(6, 'Sophia Lee', 91.3, 29, 2, 7778889990, true, 60, 6000000000, '2024-06-16', '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00', 'Test String 6', {'a': 500, 'b': 200}, '{\"k1\":\"v1\", \"k2\": 200}'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName}")
    }, errorMessage)


    //TODO  Test the unique model by drop a key type from VARCHAR
    errorMessage = "errCode = 2, detailMessage = Can not drop key column in Unique data model table"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter  table ${tbName} DROP  column username  """
        insertSql = "insert into ${tbName} values(6, 91.3, 'Boston', 29, 2, 7778889990, true, 60, 6000000000, '2024-06-16', '2024-06-16', '2024-06-16 17:00:00', '2024-06-16 17:00:00', 'Test String 6', {'a': 500, 'b': 200}, '{\"k1\":\"v1\", \"k2\": 200}'); "
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true, "${tbName}")
    }, errorMessage)


}
