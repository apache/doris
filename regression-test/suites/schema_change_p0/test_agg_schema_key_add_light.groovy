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

suite("test_agg_schema_key_add_light", "p0") {
    def tbName1 = "test_agg_schema_key_add_light1"
    def getTableStatusSql = " SHOW ALTER TABLE COLUMN WHERE IndexName='${tbName1}' ORDER BY createtime DESC LIMIT 1  "

    def lightAddKeyConfig = sql_return_maparray " SHOW FRONTEND CONFIG like 'enable_light_add_key'"

    def checkAddKey = { mayBeLight ->
        def result = sql_return_maparray getTableStatusSql
        if (lightAddKeyConfig[0].value == "true" && mayBeLight) {
            assert result[0].TransactionId == -1
        } else {
            assert result[0].TransactionId != -1
        }
    }


    //Test the AGGREGATE model by adding a key column
    sql """ DROP TABLE IF EXISTS ${tbName1} """
    def initTable = " CREATE TABLE IF NOT EXISTS ${tbName1}\n" +
            "          (\n" +
            "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
            "              `city` VARCHAR(20) COMMENT \"用户所在城市\",\n" +
            "              `age` SMALLINT SUM  COMMENT \"用户年龄\",\n" +
            "              `sex` TINYINT MAX  COMMENT \"用户性别\",\n" +
            "              `phone` LARGEINT MAX  COMMENT \"用户电话\",\n" +
            "              `address` VARCHAR(500)  REPLACE DEFAULT \"青海省西宁市城东区\"COMMENT \"用户地址\",\n" +
            "              `register_time` DATETIME REPLACE DEFAULT \"1970-01-01 00:00:00\" COMMENT \"用户注册时间\"\n" +
            "          )\n" +
            "          AGGREGATE KEY(`user_id`, `username`, `city`)\n" +
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
            "               (678901234, 'Frank', 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
            "               (789012345, 'Grace', 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"

    //Test the AGGREGATE model by adding a key column with INT
    sql initTable
    sql initTableData

    // Add a new key column with INT type with specified pos. Expected is a light schema change.
    sql """ alter  table ${tbName1} add column house_price INT KEY DEFAULT "999" AFTER `city`; """
    def insertSql = " insert into ${tbName1} values(923456689, 'Alice','Yaan',  22536, 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName1}")
    checkAddKey(true)

    qt_sql """ select * from ${tbName1} order by user_id; """

    sql "insert into ${tbName1} values(923456689, 'Alice','Beijing', 999, 25, 0, 13812345679, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
    qt_sql """ select * from ${tbName1} order by user_id; """

    // Add a new key column with INT type without pos. Expected is a light schema change.
    sql """ alter  table ${tbName1} add column house_price1 INT KEY DEFAULT "1000"; """
    insertSql = " insert into ${tbName1} values(923456689, 'Alice', 'Yaan', 22536, 1, 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName1}")
    checkAddKey(true)


    qt_sql """ select * from ${tbName1} order by user_id; """

    // Add a new key column with VARCHAR type before shortkey. Expected is a heavy schema change.
    sql """ alter  table ${tbName1} add column heavy VARCHAR(20) KEY DEFAULT "heavy_sc" AFTER `user_id`; """
    insertSql = " insert into ${tbName1} values(923456689, 'heavy_sc_insert', 'Alice', 'Yaan', 22536, 1, 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "

    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false, "${tbName1}")
    checkAddKey(false)

    qt_sql """ select * from ${tbName1} order by user_id; """

    // Add a new key column with VARCHAR type after shortkey. Expected is a light schema change.
    sql """ alter  table ${tbName1} add column light_mid VARCHAR(16) KEY DEFAULT "light_mid" AFTER `username`; """

    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, "", false, "${tbName1}")
    checkAddKey(true)

    // a heavy schema change after light schema change without writing.
    sql """ alter  table ${tbName1} add column heavy_mid VARCHAR(16) KEY DEFAULT "heavy_mid" AFTER `user_id`; """
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, "", false, "${tbName1}")
    checkAddKey(false)

    qt_sql """ select * from ${tbName1} order by user_id; """
}
