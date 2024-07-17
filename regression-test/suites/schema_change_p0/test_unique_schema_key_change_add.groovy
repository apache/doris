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

suite("test_unique_schema_key_change_add","p0") {
     def tbName = "test_unique_schema_key_change_add"
     def on_write = getRandomBoolean()
     println String.format("current enable_unique_key_merge_on_write is : %s ",on_write)
     //Test the unique model by adding a key column
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
             "          \"enable_unique_key_merge_on_write\" = \"${on_write}\"\n" +
             "          );"

     def initTableData = "insert into ${tbName} values(123456789, 'Alice', 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (234567890, 'Bob', 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
             "               (345678901, 'Carol', 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
             "               (456789012, 'Dave', 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
             "               (567890123, 'Eve', 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
             "               (678901234, 'Frank', 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (789012345, 'Grace', 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"

     //Test the unique model by adding a key column with VARCHAR
     sql initTable
     sql initTableData
     def insertSql = "insert into ${tbName} values(923456689, 'Alice', '四川省', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00');"
     def getTableStatusSql = " SHOW ALTER TABLE COLUMN WHERE IndexName='${tbName}' ORDER BY createtime DESC LIMIT 1  "
     def errorMessage
     sql """ alter  table ${tbName} add  column province VARCHAR(20) KEY DEFAULT "广东省" AFTER username """
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 600
     }, insertSql, true,"${tbName}")


     //Test the unique model by adding a key column with BOOLEAN
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName} add  column special_area BOOLEAN KEY DEFAULT "0" AFTER username """
     insertSql = "insert into ${tbName} values(923456689, 'Alice', 0, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 600
     }, insertSql, true,"${tbName}")



     //Test the unique model by adding a key column with TINYINT
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName} add  column special_area TINYINT KEY DEFAULT "0" AFTER username """
     insertSql = " insert into ${tbName} values(923456689, 'Alice', 0, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 600
     }, insertSql, true,"${tbName}")



     //Test the unique model by adding a key column with SMALLINT
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName} add  column area_num SMALLINT KEY DEFAULT "999" AFTER username """
     insertSql = " insert into ${tbName} values(923456689, 'Alice', 567, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 600
     }, insertSql, true,"${tbName}")


     //Test the unique model by adding a key column with INT
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName} add  column house_price INT KEY DEFAULT "999" AFTER username """
     insertSql = " insert into ${tbName} values(923456689, 'Alice', 2, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 600
     }, insertSql, true,"${tbName}")


     //Test the unique model by adding a key column with BIGINT
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName} add  column house_price1 BIGINT KEY DEFAULT "99999991" AFTER username """
     insertSql = " insert into ${tbName} values(923456689, 'Alice', 88889494646, 'Yaan',  25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 600
     }, insertSql, true,"${tbName}")



     //Test the unique model by adding a key column with LARGEINT
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName} add  column car_price LARGEINT KEY DEFAULT "9999" AFTER username """
     insertSql = " insert into ${tbName} values(923456689, 'Alice', 555888555, 'Yaan',  25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00');"
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 600
     }, insertSql, true,"${tbName}")



     //TODO Test the unique model by adding a key column with FLOAT
     errorMessage="errCode = 2, detailMessage = Float or double can not used as a key, use decimal instead."
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} add  column phone FLOAT KEY DEFAULT "166.6" AFTER username """
          insertSql = " insert into ${tbName} values(923456689, 'Alice', 189.9, 'Yaan',  25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00');"
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 600
          }, insertSql, true,"${tbName}")
     },errorMessage)




     //TODO Test the unique model by adding a key column with DOUBLE
     errorMessage="errCode = 2, detailMessage = Float or double can not used as a key, use decimal instead."
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} add  column watch DOUBLE KEY DEFAULT "166.689" AFTER username """
          insertSql = " insert into ${tbName} values(923456689, 'Alice', 189.479, 'Yaan',  25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 600
          }, insertSql, true,"${tbName}")

     },errorMessage)




     //Test the unique model by adding a key column with DECIMAL
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName} add  column watch DECIMAL(38,10) KEY DEFAULT "16899.6464689" AFTER username """
     insertSql = " insert into ${tbName} values(923456689, 'Alice', 16499.6464689, 'Yaan',  25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00');"
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 600
     }, insertSql, true,"${tbName}")



     //Test the unique model by adding a key column with DATE
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName} add  column watch DATE KEY DEFAULT "1997-01-01" AFTER username """
     insertSql = " insert into ${tbName} values(923456689, 'Alice', \"2024-01-01\", 'Yaan',  25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 600
     }, insertSql, true,"${tbName}")




     //Test the unique model by adding a key column with DATETIME
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName} add  column anniversary DATETIME KEY DEFAULT "1997-01-01 00:00:00" AFTER username """
     insertSql = " insert into ${tbName} values(923456689, 'Alice', \"2024-01-04 09:00:00\", 'Yaan',  25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 600
     }, insertSql, true,"${tbName}")



     //Test the unique model by adding a key column with CHAR
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName} add  column teacher CHAR KEY DEFAULT "F" AFTER username """
     insertSql = " insert into ${tbName} values(923456689, 'Alice', 'T', 'Yaan',  25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00');  "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 600
     }, insertSql, true,"${tbName}")



     //TODO Test the unique model by adding a key column with STRING
     errorMessage="errCode = 2, detailMessage = String Type should not be used in key column[comment]."
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} add  column comment STRING KEY DEFAULT "我是小说家" AFTER username """
          insertSql = " insert into ${tbName} values(923456689, 'Alice', '我是侦探家', 'Yaan',  25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00');  "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 600
          }, insertSql, true,"${tbName}")},errorMessage)



     //TODO Test the unique model by adding a key column with bitmap
     errorMessage="errCode = 2, detailMessage = Key column can not set complex type:device_id"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} add  column device_id   bitmap    KEY  DEFAULT "to_bitmap(243)" AFTER username """
          insertSql = " insert into ${tbName} values(923456689, 'Alice', to_bitmap(243), 'Yaan',  25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00');  "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 600
          }, insertSql, true,"${tbName}")

     },errorMessage)



     //TODO Test the unique model by adding a key column with Map
     errorMessage="errCode = 2, detailMessage = Map can only be used in the non-key column of the duplicate table at present."
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} add  column m   Map<STRING, INT>    KEY  DEFAULT "{'a': 100, 'b': 200}" AFTER username """
          insertSql = " insert into ${tbName} values(923456689, 'Alice', '{'a': 100, 'b': 200}', 'Yaan',  25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00');  "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 600
          }, insertSql, true,"${tbName}")
     },errorMessage)


     //TODO Test the unique model by adding a column with JSON type none default value 
     errorMessage="errCode = 2, detailMessage = JSONB or VARIANT type column default value just support null"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} add  column   j    JSON   DEFAULT '{\"a\": 300}' AFTER username """
          insertSql = " insert into ${tbName} values(923456689, 'Alice', '{\"k1\":\"v31\", \"k2\": 300}', 'Yaan',  25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 600
          }, insertSql, true,"${tbName}")
     },errorMessage)


     //TODO Test the unique model by adding a key column with JSON
     errorMessage="errCode = 2, detailMessage = JSONB or VARIANT type should not be used in key column[j]."
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} add  column   j    JSON    KEY  DEFAULT "{'a': 100, 'b': 200}" AFTER username """
          insertSql = " insert into ${tbName} values(923456689, 'Alice', '{\"k1\":\"v31\", \"k2\": 300}', 'Yaan',  25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 600
          }, insertSql, true,"${tbName}")
     },errorMessage)

}
