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

suite("test_agg_schema_value_modify","p0") {
     def tbName1 = "agg_model_value_change0"
     def tbName2 = "agg_model_value_change_0"
     //Test the agg model by adding a value column
     sql """ DROP TABLE IF EXISTS ${tbName1} """
     def getTableStatusSql = " SHOW ALTER TABLE COLUMN WHERE IndexName='${tbName1}' ORDER BY createtime DESC LIMIT 1  "
     def errorMessagge=""
     /**
      *  Test the agg model by modify a value type
      */
     def initTable2 = ""
     def initTableData2 = ""
     sql """ DROP TABLE IF EXISTS ${tbName1} """
     def initTable = " CREATE TABLE IF NOT EXISTS ${tbName1}\n" +
             "          (\n" +
             "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
             "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
             "              `is_teacher` BOOLEAN REPLACE_IF_NOT_NULL COMMENT \"是否是老师\",\n" +
             "              `city` VARCHAR(20) REPLACE_IF_NOT_NULL COMMENT \"用户所在城市\",\n" +
             "              `agge` SMALLINT REPLACE_IF_NOT_NULL COMMENT \"用户年龄\",\n" +
             "              `sex` TINYINT REPLACE_IF_NOT_NULL COMMENT \"用户性别\",\n" +
             "              `phone` LARGEINT REPLACE_IF_NOT_NULL COMMENT \"用户电话\",\n" +
             "              `address` VARCHAR(500) REPLACE_IF_NOT_NULL COMMENT \"用户地址\",\n" +
             "              `register_time` DATETIME REPLACE_IF_NOT_NULL COMMENT \"用户注册时间\"\n" +
             "          )\n" +
             "          AGGREGATE KEY(`user_id`, `username`)\n" +
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

     //TODO Test the agg model by modify a value type from BOOLEAN to TINYINT
     errorMessagge="errCode = 2, detailMessage = Can not change BOOLEAN to TINYINT"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName1} MODIFY  column is_teacher TINYINT  REPLACE_IF_NOT_NULL """
          insertSql = "insert into ${tbName1} values(993456689, 'Alice', 1, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 600
          }, insertSql, true,"${tbName1}")
     },errorMessagge)


     //TODO Test the agg model by modify a value type from BOOLEAN to SMALLINT
     errorMessagge="errCode = 2, detailMessage = Can not change BOOLEAN to SMALLINT"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName1} MODIFY  column is_teacher SMALLINT  REPLACE_IF_NOT_NULL  """
          insertSql = "insert into ${tbName1} values(993456689, 'Alice', 1, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 600
          }, insertSql, true,"${tbName1}")
     },errorMessagge)


     //TODO Test the agg model by modify a value type from BOOLEAN to INT
     errorMessagge="errCode = 2, detailMessage = Can not change BOOLEAN to INT"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName1} MODIFY  column is_teacher INT  REPLACE_IF_NOT_NULL  """
          insertSql = "insert into ${tbName1} values(993456689, 'Alice', 1, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 600
          }, insertSql, true,"${tbName1}")
     },errorMessagge)



     //TODO Test the agg model by modify a value type from BOOLEAN to BIGINT
     errorMessagge="errCode = 2, detailMessage = Can not change BOOLEAN to BIGINT"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName1} MODIFY  column is_teacher BIGINT  REPLACE_IF_NOT_NULL  """
          insertSql = "insert into ${tbName1} values(993456689, 'Alice', 1, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 600
          }, insertSql, true,"${tbName1}")
     },errorMessagge)



     //TODO Test the agg model by modify a value type from BOOLEAN to FLOAT
     errorMessagge="errCode = 2, detailMessage = Can not change BOOLEAN to FLOAT"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName1} MODIFY  column is_teacher FLOAT  REPLACE_IF_NOT_NULL  """
          insertSql = "insert into ${tbName1} values(993456689, 'Alice', 1.0, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 600
          }, insertSql, true,"${tbName1}")
     },errorMessagge)


     //TODO Test the agg model by modify a value type from BOOLEAN to DOUBLE
     errorMessagge="errCode = 2, detailMessage = Can not change BOOLEAN to DOUBLE"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName1} MODIFY  column is_teacher DOUBLE REPLACE_IF_NOT_NULL """
          insertSql = "insert into ${tbName1} values(993456689, 'Alice', 1.0, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 600
          }, insertSql, true,"${tbName1}")
     },errorMessagge)


     //TODO Test the agg model by modify a value type from BOOLEAN to DECIMAL
     errorMessagge="errCode = 2, detailMessage = Can not change BOOLEAN to DECIMAL32"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName1} MODIFY  column is_teacher DECIMAL  REPLACE_IF_NOT_NULL """
          insertSql = "insert into ${tbName1} values(993456689, 'Alice', 1.0, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 600
          }, insertSql, true,"${tbName1}")
     },errorMessagge)


     //TODO Test the agg model by modify a value type from BOOLEAN to CHAR
     errorMessagge="errCode = 2, detailMessage = Can not change BOOLEAN to CHAR"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName1} MODIFY  column is_teacher CHAR  REPLACE_IF_NOT_NULL  """
          insertSql = "insert into ${tbName1} values(993456689, 'Alice', '1', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 600
          }, insertSql, true,"${tbName1}")
     },errorMessagge)


     //TODO Test the agg model by modify a value type from BOOLEAN to STRING
     errorMessagge="errCode = 2, detailMessage = Can not change BOOLEAN to STRING"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName1} MODIFY  column is_teacher STRING  REPLACE_IF_NOT_NULL  """
          insertSql = "insert into ${tbName1} values(993456689, 'Alice', '1', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 600
          }, insertSql, true,"${tbName1}")
     },errorMessagge)


     //TODO Test the agg model by modify a value type from BOOLEAN to VARCHAR
     errorMessagge="errCode = 2, detailMessage = Can not change BOOLEAN to VARCHAR"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName1} MODIFY  column is_teacher VARCHAR(32)  REPLACE_IF_NOT_NULL """
          insertSql = "insert into ${tbName1} values(993456689, 'Alice', '1', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 600
          }, insertSql, true,"${tbName1}")
     },errorMessagge)




     /**
      *  Test the agg model by modify a value type from TINYINT to other type
      */
     sql """ DROP TABLE IF EXISTS ${tbName1} """
     initTable = " CREATE TABLE IF NOT EXISTS ${tbName1}\n" +
             "          (\n" +
             "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
             "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
             "              `is_student` TINYINT REPLACE_IF_NOT_NULL COMMENT \"是否是学生\",\n" +
             "              `city` VARCHAR(20) REPLACE_IF_NOT_NULL COMMENT \"用户所在城市\",\n" +
             "              `agge` SMALLINT REPLACE_IF_NOT_NULL COMMENT \"用户年龄\",\n" +
             "              `sex` TINYINT REPLACE_IF_NOT_NULL COMMENT \"用户性别\",\n" +
             "              `phone` LARGEINT REPLACE_IF_NOT_NULL COMMENT \"用户电话\",\n" +
             "              `address` VARCHAR(500) REPLACE_IF_NOT_NULL COMMENT \"用户地址\",\n" +
             "              `register_time` DATETIME REPLACE_IF_NOT_NULL COMMENT \"用户注册时间\"\n" +
             "          )\n" +
             "          AGGREGATE KEY(`user_id`, `username`)\n" +
             "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
             "          PROPERTIES (\n" +
             "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
             "          );"

     initTableData = "insert into ${tbName1} values(123456789, 'Alice', 1, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (234567890, 'Bob', 1, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
             "               (345678901, 'Carol', 1, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
             "               (456789012, 'Dave', 1, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
             "               (567890123, 'Eve', 0, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
             "               (678901234, 'Frank', 0, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (789012345, 'Grace', 1, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"

     //TODO Test the agg model by modify a value type from TINYINT  to BOOLEAN
     errorMessagge="errCode = 2, detailMessage = Can not change TINYINT to BOOLEAN"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName1} MODIFY  column is_student BOOLEAN  REPLACE_IF_NOT_NULL """
          insertSql = "insert into ${tbName1} values(993456689, 'Alice', false, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 600
          }, insertSql, true,"${tbName1}")
     },errorMessagge)




     //Test the agg model by modify a value type from TINYINT  to SMALLINT
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName1} MODIFY  column is_student SMALLINT  REPLACE_IF_NOT_NULL """
     insertSql = "insert into ${tbName1} values(993456689, 'Alice', 1, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 600
     }, insertSql, false,"${tbName1}")

     sql """ DROP TABLE IF EXISTS ${tbName2} """
     initTable2 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
             "          (\n" +
             "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
             "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
             "              `is_student` SMALLINT REPLACE_IF_NOT_NULL COMMENT \"是否是学生\",\n" +
             "              `city` VARCHAR(20) REPLACE_IF_NOT_NULL COMMENT \"用户所在城市\",\n" +
             "              `agge` SMALLINT REPLACE_IF_NOT_NULL COMMENT \"用户年龄\",\n" +
             "              `sex` TINYINT REPLACE_IF_NOT_NULL COMMENT \"用户性别\",\n" +
             "              `phone` LARGEINT REPLACE_IF_NOT_NULL COMMENT \"用户电话\",\n" +
             "              `address` VARCHAR(500) REPLACE_IF_NOT_NULL COMMENT \"用户地址\",\n" +
             "              `register_time` DATETIME REPLACE_IF_NOT_NULL COMMENT \"用户注册时间\"\n" +
             "          )\n" +
             "          AGGREGATE KEY(`user_id`, `username`)\n" +
             "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
             "          PROPERTIES (\n" +
             "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
             "          );"

     initTableData2 = "insert into ${tbName2} values(123456789, 'Alice', 1, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (234567890, 'Bob', 1, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
             "               (345678901, 'Carol', 1, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
             "               (456789012, 'Dave', 1, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
             "               (567890123, 'Eve', 0, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
             "               (678901234, 'Frank', 0, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (789012345, 'Grace', 1, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00')," +
             "               (993456689, 'Alice', 1, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00');"
     sql initTable2
     sql initTableData2
     checkTableData("${tbName1}","${tbName2}","is_student")
     sql """ DROP TABLE IF EXISTS ${tbName1} """



     // Test the agg model by modify a value type from TINYINT  to INT
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName1} MODIFY  column is_student INT  REPLACE_IF_NOT_NULL """
     insertSql = "insert into ${tbName1} values(993456689, 'Alice', 1, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 600
     }, insertSql, false,"${tbName1}")


     sql """ DROP TABLE IF EXISTS ${tbName2} """
     initTable2 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
             "          (\n" +
             "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
             "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
             "              `is_student` INT REPLACE_IF_NOT_NULL COMMENT \"是否是学生\",\n" +
             "              `city` VARCHAR(20) REPLACE_IF_NOT_NULL COMMENT \"用户所在城市\",\n" +
             "              `agge` SMALLINT REPLACE_IF_NOT_NULL COMMENT \"用户年龄\",\n" +
             "              `sex` TINYINT REPLACE_IF_NOT_NULL COMMENT \"用户性别\",\n" +
             "              `phone` LARGEINT REPLACE_IF_NOT_NULL COMMENT \"用户电话\",\n" +
             "              `address` VARCHAR(500) REPLACE_IF_NOT_NULL COMMENT \"用户地址\",\n" +
             "              `register_time` DATETIME REPLACE_IF_NOT_NULL COMMENT \"用户注册时间\"\n" +
             "          )\n" +
             "          AGGREGATE KEY(`user_id`, `username`)\n" +
             "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
             "          PROPERTIES (\n" +
             "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
             "          );"

     initTableData2 = "insert into ${tbName2} values(123456789, 'Alice', 1, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (234567890, 'Bob', 1, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
             "               (345678901, 'Carol', 1, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
             "               (456789012, 'Dave', 1, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
             "               (567890123, 'Eve', 0, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
             "               (678901234, 'Frank', 0, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (789012345, 'Grace', 1, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00')," +
             "               (993456689, 'Alice', 1, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00');"
     sql initTable2
     sql initTableData2
     checkTableData("${tbName1}","${tbName2}","is_student")
     sql """ DROP TABLE IF EXISTS ${tbName1} """



     //Test the agg model by modify a value type from TINYINT  to BIGINT
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName1} MODIFY  column is_student BIGINT REPLACE_IF_NOT_NULL """
     insertSql = "insert into ${tbName1} values(993456689, 'Alice', 1, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 600
     }, insertSql, false,"${tbName1}")

     sql """ DROP TABLE IF EXISTS ${tbName2} """
     initTable2 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
             "          (\n" +
             "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
             "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
             "              `is_student` BIGINT REPLACE_IF_NOT_NULL COMMENT \"是否是学生\",\n" +
             "              `city` VARCHAR(20) REPLACE_IF_NOT_NULL COMMENT \"用户所在城市\",\n" +
             "              `agge` SMALLINT REPLACE_IF_NOT_NULL COMMENT \"用户年龄\",\n" +
             "              `sex` TINYINT REPLACE_IF_NOT_NULL COMMENT \"用户性别\",\n" +
             "              `phone` LARGEINT REPLACE_IF_NOT_NULL COMMENT \"用户电话\",\n" +
             "              `address` VARCHAR(500) REPLACE_IF_NOT_NULL COMMENT \"用户地址\",\n" +
             "              `register_time` DATETIME REPLACE_IF_NOT_NULL COMMENT \"用户注册时间\"\n" +
             "          )\n" +
             "          AGGREGATE KEY(`user_id`, `username`)\n" +
             "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
             "          PROPERTIES (\n" +
             "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
             "          );"

     initTableData2 = "insert into ${tbName2} values(123456789, 'Alice', 1, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (234567890, 'Bob', 1, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
             "               (345678901, 'Carol', 1, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
             "               (456789012, 'Dave', 1, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
             "               (567890123, 'Eve', 0, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
             "               (678901234, 'Frank', 0, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (789012345, 'Grace', 1, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00')," +
             "               (993456689, 'Alice', 1, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00');"
     sql initTable2
     sql initTableData2
     checkTableData("${tbName1}","${tbName2}","is_student")
     sql """ DROP TABLE IF EXISTS ${tbName1} """




     //Test the agg model by modify a value type from TINYINT  to LARGEINT
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName1} MODIFY  column is_student LARGEINT  REPLACE_IF_NOT_NULL """
     insertSql = "insert into ${tbName1} values(993456689, 'Alice', 1, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 600
     }, insertSql, false,"${tbName1}")

     sql """ DROP TABLE IF EXISTS ${tbName2} """
     initTable2 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
             "          (\n" +
             "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
             "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
             "              `is_student` LARGEINT REPLACE_IF_NOT_NULL COMMENT \"是否是学生\",\n" +
             "              `city` VARCHAR(20) REPLACE_IF_NOT_NULL COMMENT \"用户所在城市\",\n" +
             "              `agge` SMALLINT REPLACE_IF_NOT_NULL COMMENT \"用户年龄\",\n" +
             "              `sex` TINYINT REPLACE_IF_NOT_NULL COMMENT \"用户性别\",\n" +
             "              `phone` LARGEINT REPLACE_IF_NOT_NULL COMMENT \"用户电话\",\n" +
             "              `address` VARCHAR(500) REPLACE_IF_NOT_NULL COMMENT \"用户地址\",\n" +
             "              `register_time` DATETIME REPLACE_IF_NOT_NULL COMMENT \"用户注册时间\"\n" +
             "          )\n" +
             "          AGGREGATE KEY(`user_id`, `username`)\n" +
             "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
             "          PROPERTIES (\n" +
             "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
             "          );"

     initTableData2 = "insert into ${tbName2} values(123456789, 'Alice', 1, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (234567890, 'Bob', 1, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
             "               (345678901, 'Carol', 1, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
             "               (456789012, 'Dave', 1, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
             "               (567890123, 'Eve', 0, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
             "               (678901234, 'Frank', 0, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (789012345, 'Grace', 1, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00')," +
             "               (993456689, 'Alice', 1, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00');"
     sql initTable2
     sql initTableData2
     checkTableData("${tbName1}","${tbName2}","is_student")
     sql """ DROP TABLE IF EXISTS ${tbName1} """

     //Test the agg model by modify a value type from TINYINT  to FLOAT
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName1} MODIFY  column is_student FLOAT REPLACE_IF_NOT_NULL """
     insertSql = "insert into ${tbName1} values(993456689, 'Alice', 1.2, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 600
     }, insertSql, false,"${tbName1}")

     sql """ DROP TABLE IF EXISTS ${tbName2} """
     initTable2 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
             "          (\n" +
             "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
             "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
             "              `is_student` FLOAT REPLACE_IF_NOT_NULL COMMENT \"是否是学生\",\n" +
             "              `city` VARCHAR(20) REPLACE_IF_NOT_NULL COMMENT \"用户所在城市\",\n" +
             "              `agge` SMALLINT REPLACE_IF_NOT_NULL COMMENT \"用户年龄\",\n" +
             "              `sex` TINYINT REPLACE_IF_NOT_NULL COMMENT \"用户性别\",\n" +
             "              `phone` LARGEINT REPLACE_IF_NOT_NULL COMMENT \"用户电话\",\n" +
             "              `address` VARCHAR(500) REPLACE_IF_NOT_NULL COMMENT \"用户地址\",\n" +
             "              `register_time` DATETIME REPLACE_IF_NOT_NULL COMMENT \"用户注册时间\"\n" +
             "          )\n" +
             "          AGGREGATE KEY(`user_id`, `username`)\n" +
             "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
             "          PROPERTIES (\n" +
             "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
             "          );"

     initTableData2 = "insert into ${tbName2} values(123456789, 'Alice', 1, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (234567890, 'Bob', 1, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
             "               (345678901, 'Carol', 1, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
             "               (456789012, 'Dave', 1, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
             "               (567890123, 'Eve', 0, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
             "               (678901234, 'Frank', 0, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (789012345, 'Grace', 1, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00')," +
             "               (993456689, 'Alice', 1.2, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00');"
     sql initTable2
     sql initTableData2
     checkTableData("${tbName1}","${tbName2}","is_student")
     sql """ DROP TABLE IF EXISTS ${tbName1} """

     //Test the agg model by modify a value type from TINYINT  to DOUBLE
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName1} MODIFY  column is_student DOUBLE REPLACE_IF_NOT_NULL """
     insertSql = "insert into ${tbName1} values(993456689, 'Alice', 1.23, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 600
     }, insertSql, false,"${tbName1}")

     sql """ DROP TABLE IF EXISTS ${tbName2} """
     initTable2 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
             "          (\n" +
             "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
             "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
             "              `is_student` DOUBLE REPLACE_IF_NOT_NULL COMMENT \"是否是学生\",\n" +
             "              `city` VARCHAR(20) REPLACE_IF_NOT_NULL COMMENT \"用户所在城市\",\n" +
             "              `agge` SMALLINT REPLACE_IF_NOT_NULL COMMENT \"用户年龄\",\n" +
             "              `sex` TINYINT REPLACE_IF_NOT_NULL COMMENT \"用户性别\",\n" +
             "              `phone` LARGEINT REPLACE_IF_NOT_NULL COMMENT \"用户电话\",\n" +
             "              `address` VARCHAR(500) REPLACE_IF_NOT_NULL COMMENT \"用户地址\",\n" +
             "              `register_time` DATETIME REPLACE_IF_NOT_NULL COMMENT \"用户注册时间\"\n" +
             "          )\n" +
             "          AGGREGATE KEY(`user_id`, `username`)\n" +
             "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
             "          PROPERTIES (\n" +
             "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
             "          );"

     initTableData2 = "insert into ${tbName2} values(123456789, 'Alice', 1, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (234567890, 'Bob', 1, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
             "               (345678901, 'Carol', 1, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
             "               (456789012, 'Dave', 1, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
             "               (567890123, 'Eve', 0, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
             "               (678901234, 'Frank', 0, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (789012345, 'Grace', 1, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00')," +
             "               (993456689, 'Alice', 1.23, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00');"
     sql initTable2
     sql initTableData2
     checkTableData("${tbName1}","${tbName2}","is_student")
     sql """ DROP TABLE IF EXISTS ${tbName1} """

     //TODO Test the agg model by modify a value type from TINYINT  to DECIMAL32
     errorMessagge="errCode = 2, detailMessage = Can not change TINYINT to DECIMAL32"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName1} MODIFY  column is_student DECIMAL REPLACE_IF_NOT_NULL """
          insertSql = "insert into ${tbName1} values(993456689, 'Alice', 1.23, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 600
          }, insertSql, true,"${tbName1}")

     },errorMessagge)

     //TODO Test the agg model by modify a value type from TINYINT  to CHAR
     errorMessagge="errCode = 2, detailMessage = Can not change TINYINT to CHAR"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName1} MODIFY  column is_student CHAR(15) REPLACE_IF_NOT_NULL """
          insertSql = "insert into ${tbName1} values(993456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 600
          }, insertSql, true,"${tbName1}")
     },errorMessagge)




     //Test Test the agg model by modify a value type from TINYINT  to VARCHAR
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName1} MODIFY  column is_student VARCHAR(100) REPLACE_IF_NOT_NULL """
     insertSql = "insert into ${tbName1} values(993456689, 'Alice', 'asdv', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 600
     }, insertSql, false,"${tbName1}")

     sql """ DROP TABLE IF EXISTS ${tbName2} """
     initTable2 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
             "          (\n" +
             "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
             "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
             "              `is_student` VARCHAR(100) REPLACE_IF_NOT_NULL COMMENT \"是否是学生\",\n" +
             "              `city` VARCHAR(20) REPLACE_IF_NOT_NULL COMMENT \"用户所在城市\",\n" +
             "              `agge` SMALLINT REPLACE_IF_NOT_NULL COMMENT \"用户年龄\",\n" +
             "              `sex` TINYINT REPLACE_IF_NOT_NULL COMMENT \"用户性别\",\n" +
             "              `phone` LARGEINT REPLACE_IF_NOT_NULL COMMENT \"用户电话\",\n" +
             "              `address` VARCHAR(500) REPLACE_IF_NOT_NULL COMMENT \"用户地址\",\n" +
             "              `register_time` DATETIME REPLACE_IF_NOT_NULL COMMENT \"用户注册时间\"\n" +
             "          )\n" +
             "          AGGREGATE KEY(`user_id`, `username`)\n" +
             "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
             "          PROPERTIES (\n" +
             "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
             "          );"

     initTableData2 = "insert into ${tbName2} values(123456789, 'Alice', 1, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (234567890, 'Bob', 1, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
             "               (345678901, 'Carol', 1, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
             "               (456789012, 'Dave', 1, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
             "               (567890123, 'Eve', 0, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
             "               (678901234, 'Frank', 0, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (789012345, 'Grace', 1, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00')," +
             "               (993456689, 'Alice', 'asdv', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00');"
     sql initTable2
     sql initTableData2
     checkTableData("${tbName1}","${tbName2}","is_student")
     sql """ DROP TABLE IF EXISTS ${tbName1} """



     //Test the agg model by modify a value type from TINYINT  to STRING
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName1} MODIFY  column is_student STRING REPLACE_IF_NOT_NULL """
     insertSql = "insert into ${tbName1} values(993456689, 'Alice', 'asds', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 600
     }, insertSql, false,"${tbName1}")

     sql """ DROP TABLE IF EXISTS ${tbName2} """
     initTable2 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
             "          (\n" +
             "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
             "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
             "              `is_student` STRING REPLACE_IF_NOT_NULL COMMENT \"是否是学生\",\n" +
             "              `city` VARCHAR(20) REPLACE_IF_NOT_NULL COMMENT \"用户所在城市\",\n" +
             "              `agge` SMALLINT REPLACE_IF_NOT_NULL COMMENT \"用户年龄\",\n" +
             "              `sex` TINYINT REPLACE_IF_NOT_NULL COMMENT \"用户性别\",\n" +
             "              `phone` LARGEINT REPLACE_IF_NOT_NULL COMMENT \"用户电话\",\n" +
             "              `address` VARCHAR(500) REPLACE_IF_NOT_NULL COMMENT \"用户地址\",\n" +
             "              `register_time` DATETIME REPLACE_IF_NOT_NULL COMMENT \"用户注册时间\"\n" +
             "          )\n" +
             "          AGGREGATE KEY(`user_id`, `username`)\n" +
             "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
             "          PROPERTIES (\n" +
             "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
             "          );"

     initTableData2 = "insert into ${tbName2} values(123456789, 'Alice', 1, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (234567890, 'Bob', 1, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
             "               (345678901, 'Carol', 1, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
             "               (456789012, 'Dave', 1, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
             "               (567890123, 'Eve', 0, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
             "               (678901234, 'Frank', 0, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (789012345, 'Grace', 1, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00')," +
             "               (993456689, 'Alice', 'asds', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00');"
     sql initTable2
     sql initTableData2
     checkTableData("${tbName1}","${tbName2}","is_student")
     sql """ DROP TABLE IF EXISTS ${tbName1} """


     /**
      *  Test the AGGREGATE model by modify a value type from SMALLINT to other type
      */
     sql """ DROP TABLE IF EXISTS ${tbName1} """
     initTable = " CREATE TABLE IF NOT EXISTS ${tbName1}\n" +
             "          (\n" +
             "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
             "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
             "              `car_number` SMALLINT REPLACE_IF_NOT_NULL COMMENT \"市民卡\",\n" +
             "              `city` VARCHAR(20) REPLACE_IF_NOT_NULL COMMENT \"用户所在城市\",\n" +
             "              `agge` SMALLINT REPLACE_IF_NOT_NULL COMMENT \"用户年龄\",\n" +
             "              `sex` TINYINT REPLACE_IF_NOT_NULL COMMENT \"用户性别\",\n" +
             "              `phone` LARGEINT REPLACE_IF_NOT_NULL COMMENT \"用户电话\",\n" +
             "              `address` VARCHAR(500) REPLACE_IF_NOT_NULL COMMENT \"用户地址\",\n" +
             "              `register_time` DATETIME REPLACE_IF_NOT_NULL COMMENT \"用户注册时间\"\n" +
             "          )\n" +
             "          AGGREGATE KEY(`user_id`, `username`)\n" +
             "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
             "          PROPERTIES (\n" +
             "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
             "          );"

     initTableData = "insert into ${tbName1} values(123456789, 'Alice', 13243, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (234567890, 'Bob', 13445, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
             "               (345678901, 'Carol', 15768, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
             "               (456789012, 'Dave', 14243, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
             "               (567890123, 'Eve', 10768, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
             "               (678901234, 'Frank', 14325, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (789012345, 'Grace', 15686, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"

     //TODO Test the AGGREGATE model by modify a value type from SMALLINT  to BOOLEAN
     errorMessagge = "errCode = 2, detailMessage = Can not change SMALLINT to BOOLEAN"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName1} MODIFY  column car_number BOOLEAN REPLACE_IF_NOT_NULL  """
          insertSql = "insert into ${tbName1} values(993456689, 'Alice', false, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 600
          }, insertSql, true, "${tbName1}")
     }, errorMessagge)


     // TODO Test the AGGREGATE model by modify a value type from SMALLINT  to TINYINT
     errorMessagge = "errCode = 2, detailMessage = Can not change SMALLINT to TINYINT"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName1} MODIFY  column car_number TINYINT REPLACE_IF_NOT_NULL """
          insertSql = "insert into ${tbName1} values(993456689, 'Alice', 2, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 600
          }, insertSql, true, "${tbName1}")
     }, errorMessagge)


     //Test the AGGREGATE model by modify a value type from SMALLINT  to INT
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName1} MODIFY  column car_number INT REPLACE_IF_NOT_NULL """
     insertSql = "insert into ${tbName1} values(993456689, 'Alice', 3, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 600
     }, insertSql, false, "${tbName1}")


     sql """ DROP TABLE IF EXISTS ${tbName2} """
     initTable2 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
             "          (\n" +
             "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
             "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
             "              `car_number` INT REPLACE_IF_NOT_NULL COMMENT \"是否是学生\",\n" +
             "              `city` VARCHAR(20) REPLACE_IF_NOT_NULL COMMENT \"用户所在城市\",\n" +
             "              `agge` SMALLINT REPLACE_IF_NOT_NULL COMMENT \"用户年龄\",\n" +
             "              `sex` TINYINT REPLACE_IF_NOT_NULL COMMENT \"用户性别\",\n" +
             "              `phone` LARGEINT REPLACE_IF_NOT_NULL COMMENT \"用户电话\",\n" +
             "              `address` VARCHAR(500) REPLACE_IF_NOT_NULL COMMENT \"用户地址\",\n" +
             "              `register_time` DATETIME REPLACE_IF_NOT_NULL COMMENT \"用户注册时间\"\n" +
             "          )\n" +
             "          AGGREGATE KEY(`user_id`, `username`)\n" +
             "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
             "          PROPERTIES (\n" +
             "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
             "          );"

     initTableData2 = "insert into ${tbName2} values(123456789, 'Alice', 13243, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (234567890, 'Bob', 13445, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
             "               (345678901, 'Carol', 15768, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
             "               (456789012, 'Dave', 14243, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
             "               (567890123, 'Eve', 10768, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
             "               (678901234, 'Frank', 14325, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (993456689, 'Alice', 3, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (789012345, 'Grace', 15686, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"
     sql initTable2
     sql initTableData2
     checkTableData("${tbName1}", "${tbName2}", "car_number")
     sql """ DROP TABLE IF EXISTS ${tbName1} """


     //Test the AGGREGATE model by modify a value type from SMALLINT  to BIGINT
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName1} MODIFY  column car_number BIGINT REPLACE_IF_NOT_NULL """
     insertSql = "insert into ${tbName1} values(993456689, 'Alice', 4, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 600
     }, insertSql, false, "${tbName1}")

     sql """ DROP TABLE IF EXISTS ${tbName2} """
     initTable2 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
             "          (\n" +
             "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
             "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
             "              `car_number` BIGINT REPLACE_IF_NOT_NULL COMMENT \"是否是学生\",\n" +
             "              `city` VARCHAR(20) REPLACE_IF_NOT_NULL COMMENT \"用户所在城市\",\n" +
             "              `agge` SMALLINT REPLACE_IF_NOT_NULL COMMENT \"用户年龄\",\n" +
             "              `sex` TINYINT REPLACE_IF_NOT_NULL COMMENT \"用户性别\",\n" +
             "              `phone` LARGEINT REPLACE_IF_NOT_NULL COMMENT \"用户电话\",\n" +
             "              `address` VARCHAR(500) REPLACE_IF_NOT_NULL COMMENT \"用户地址\",\n" +
             "              `register_time` DATETIME REPLACE_IF_NOT_NULL COMMENT \"用户注册时间\"\n" +
             "          )\n" +
             "          AGGREGATE KEY(`user_id`, `username`)\n" +
             "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
             "          PROPERTIES (\n" +
             "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
             "          );"

     initTableData2 = "insert into ${tbName2} values(123456789, 'Alice', 13243, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (234567890, 'Bob', 13445, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
             "               (345678901, 'Carol', 15768, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
             "               (456789012, 'Dave', 14243, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
             "               (567890123, 'Eve', 10768, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
             "               (678901234, 'Frank', 14325, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (993456689, 'Alice', 4, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (789012345, 'Grace', 15686, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"
     sql initTable2
     sql initTableData2
     checkTableData("${tbName1}", "${tbName2}", "car_number")
     sql """ DROP TABLE IF EXISTS ${tbName1} """


     //Test the AGGREGATE model by modify a value type from SMALLINT  to LARGEINT
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName1} MODIFY  column car_number LARGEINT REPLACE_IF_NOT_NULL """
     insertSql = "insert into ${tbName1} values(993456689, 'Alice', 5, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 600
     }, insertSql, false, "${tbName1}")


     sql """ DROP TABLE IF EXISTS ${tbName2} """
     initTable2 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
             "          (\n" +
             "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
             "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
             "              `car_number` LARGEINT REPLACE_IF_NOT_NULL COMMENT \"是否是学生\",\n" +
             "              `city` VARCHAR(20) REPLACE_IF_NOT_NULL COMMENT \"用户所在城市\",\n" +
             "              `agge` SMALLINT REPLACE_IF_NOT_NULL COMMENT \"用户年龄\",\n" +
             "              `sex` TINYINT REPLACE_IF_NOT_NULL COMMENT \"用户性别\",\n" +
             "              `phone` LARGEINT REPLACE_IF_NOT_NULL COMMENT \"用户电话\",\n" +
             "              `address` VARCHAR(500) REPLACE_IF_NOT_NULL COMMENT \"用户地址\",\n" +
             "              `register_time` DATETIME REPLACE_IF_NOT_NULL COMMENT \"用户注册时间\"\n" +
             "          )\n" +
             "          AGGREGATE KEY(`user_id`, `username`)\n" +
             "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
             "          PROPERTIES (\n" +
             "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
             "          );"

     initTableData2 = "insert into ${tbName2} values(123456789, 'Alice', 13243, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (234567890, 'Bob', 13445, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
             "               (345678901, 'Carol', 15768, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
             "               (456789012, 'Dave', 14243, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
             "               (567890123, 'Eve', 10768, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
             "               (678901234, 'Frank', 14325, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (993456689, 'Alice', 5, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (789012345, 'Grace', 15686, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"
     sql initTable2
     sql initTableData2
     checkTableData("${tbName1}", "${tbName2}", "car_number")
     sql """ DROP TABLE IF EXISTS ${tbName1} """


     sql initTable
     sql initTableData
     sql """ alter  table ${tbName1} MODIFY  column car_number FLOAT REPLACE_IF_NOT_NULL """
     insertSql = "insert into ${tbName1} values(993456689, 'Alice', 1.2, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 600
     }, insertSql, false, "${tbName1}")


     sql """ DROP TABLE IF EXISTS ${tbName2} """
     initTable2 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
             "          (\n" +
             "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
             "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
             "              `car_number` FLOAT REPLACE_IF_NOT_NULL COMMENT \"是否是学生\",\n" +
             "              `city` VARCHAR(20) REPLACE_IF_NOT_NULL COMMENT \"用户所在城市\",\n" +
             "              `agge` SMALLINT REPLACE_IF_NOT_NULL COMMENT \"用户年龄\",\n" +
             "              `sex` TINYINT REPLACE_IF_NOT_NULL COMMENT \"用户性别\",\n" +
             "              `phone` LARGEINT REPLACE_IF_NOT_NULL COMMENT \"用户电话\",\n" +
             "              `address` VARCHAR(500) REPLACE_IF_NOT_NULL COMMENT \"用户地址\",\n" +
             "              `register_time` DATETIME REPLACE_IF_NOT_NULL COMMENT \"用户注册时间\"\n" +
             "          )\n" +
             "          AGGREGATE KEY(`user_id`, `username`)\n" +
             "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
             "          PROPERTIES (\n" +
             "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
             "          );"

     initTableData2 = "insert into ${tbName2} values(123456789, 'Alice', 13243, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (234567890, 'Bob', 13445, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
             "               (345678901, 'Carol', 15768, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
             "               (456789012, 'Dave', 14243, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
             "               (567890123, 'Eve', 10768, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
             "               (678901234, 'Frank', 14325, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (993456689, 'Alice', 1.2, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (789012345, 'Grace', 15686, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"
     sql initTable2
     sql initTableData2
     checkTableData("${tbName1}", "${tbName2}", "car_number")
     sql """ DROP TABLE IF EXISTS ${tbName1} """


     sql initTable
     sql initTableData
     sql """ alter  table ${tbName1} MODIFY  column car_number DOUBLE REPLACE_IF_NOT_NULL """
     insertSql = "insert into ${tbName1} values(993456689, 'Alice', 1.23, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 600
     }, insertSql, false, "${tbName1}")


     sql """ DROP TABLE IF EXISTS ${tbName2} """
     initTable2 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
             "          (\n" +
             "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
             "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
             "              `car_number` DOUBLE REPLACE_IF_NOT_NULL COMMENT \"是否是学生\",\n" +
             "              `city` VARCHAR(20) REPLACE_IF_NOT_NULL COMMENT \"用户所在城市\",\n" +
             "              `agge` SMALLINT REPLACE_IF_NOT_NULL COMMENT \"用户年龄\",\n" +
             "              `sex` TINYINT REPLACE_IF_NOT_NULL COMMENT \"用户性别\",\n" +
             "              `phone` LARGEINT REPLACE_IF_NOT_NULL COMMENT \"用户电话\",\n" +
             "              `address` VARCHAR(500) REPLACE_IF_NOT_NULL COMMENT \"用户地址\",\n" +
             "              `register_time` DATETIME REPLACE_IF_NOT_NULL COMMENT \"用户注册时间\"\n" +
             "          )\n" +
             "          AGGREGATE KEY(`user_id`, `username`)\n" +
             "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
             "          PROPERTIES (\n" +
             "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
             "          );"

     initTableData2 = "insert into ${tbName2} values(123456789, 'Alice', 13243, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (234567890, 'Bob', 13445, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
             "               (345678901, 'Carol', 15768, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
             "               (456789012, 'Dave', 14243, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
             "               (567890123, 'Eve', 10768, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
             "               (678901234, 'Frank', 14325, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (993456689, 'Alice', 1.23, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (789012345, 'Grace', 15686, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"
     sql initTable2
     sql initTableData2
     checkTableData("${tbName1}", "${tbName2}", "car_number")
     sql """ DROP TABLE IF EXISTS ${tbName1} """


     //TODO Test the AGGREGATE model by modify a value type from SMALLINT  to DECIMAL
     errorMessagge = "errCode = 2, detailMessage = Can not change SMALLINT to DECIMAL32"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName1} MODIFY  column car_number DECIMAL REPLACE_IF_NOT_NULL """
          insertSql = "insert into ${tbName1} values(993456689, 'Alice', 1.23, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 600
          }, insertSql, true, "${tbName1}")

     }, errorMessagge)

     //TODO Test the AGGREGATE model by modify a  value type from SMALLINT  to CHAR
     errorMessagge = "errCode = 2, detailMessage = Can not change SMALLINT to CHAR"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName1} MODIFY  column car_number CHAR(15) REPLACE_IF_NOT_NULL """
          insertSql = "insert into ${tbName1} values(993456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 600
          }, insertSql, true, "${tbName1}")
     }, errorMessagge)


     //Test the AGGREGATE model by modify a value type from SMALLINT  to VARCHAR
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName1} MODIFY  column car_number VARCHAR(100) REPLACE_IF_NOT_NULL """
     insertSql = "insert into ${tbName1} values(993456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 600
     }, insertSql, false, "${tbName1}")


     sql """ DROP TABLE IF EXISTS ${tbName2} """
     initTable2 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
             "          (\n" +
             "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
             "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
             "              `car_number` VARCHAR(100) REPLACE_IF_NOT_NULL COMMENT \"是否是学生\",\n" +
             "              `city` VARCHAR(20) REPLACE_IF_NOT_NULL COMMENT \"用户所在城市\",\n" +
             "              `agge` SMALLINT REPLACE_IF_NOT_NULL COMMENT \"用户年龄\",\n" +
             "              `sex` TINYINT REPLACE_IF_NOT_NULL COMMENT \"用户性别\",\n" +
             "              `phone` LARGEINT REPLACE_IF_NOT_NULL COMMENT \"用户电话\",\n" +
             "              `address` VARCHAR(500) REPLACE_IF_NOT_NULL COMMENT \"用户地址\",\n" +
             "              `register_time` DATETIME REPLACE_IF_NOT_NULL COMMENT \"用户注册时间\"\n" +
             "          )\n" +
             "          AGGREGATE KEY(`user_id`, `username`)\n" +
             "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
             "          PROPERTIES (\n" +
             "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
             "          );"

     initTableData2 = "insert into ${tbName2} values(123456789, 'Alice', 13243, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (234567890, 'Bob', 13445, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
             "               (345678901, 'Carol', 15768, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
             "               (456789012, 'Dave', 14243, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
             "               (567890123, 'Eve', 10768, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
             "               (678901234, 'Frank', 14325, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (993456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (789012345, 'Grace', 15686, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"
     sql initTable2
     sql initTableData2
     checkTableData("${tbName1}", "${tbName2}", "car_number")
     sql """ DROP TABLE IF EXISTS ${tbName1} """


     //Test the AGGREGATE model by modify a value type from SMALLINT  to STRING
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName1} MODIFY  column car_number STRING REPLACE_IF_NOT_NULL """
     insertSql = "insert into ${tbName1} values(993456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 600
     }, insertSql, false, "${tbName1}")


     sql """ DROP TABLE IF EXISTS ${tbName2} """
     initTable2 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
             "          (\n" +
             "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
             "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
             "              `car_number` STRING REPLACE_IF_NOT_NULL COMMENT \"是否是学生\",\n" +
             "              `city` VARCHAR(20) REPLACE_IF_NOT_NULL COMMENT \"用户所在城市\",\n" +
             "              `agge` SMALLINT REPLACE_IF_NOT_NULL COMMENT \"用户年龄\",\n" +
             "              `sex` TINYINT REPLACE_IF_NOT_NULL COMMENT \"用户性别\",\n" +
             "              `phone` LARGEINT REPLACE_IF_NOT_NULL COMMENT \"用户电话\",\n" +
             "              `address` VARCHAR(500) REPLACE_IF_NOT_NULL COMMENT \"用户地址\",\n" +
             "              `register_time` DATETIME REPLACE_IF_NOT_NULL COMMENT \"用户注册时间\"\n" +
             "          )\n" +
             "          AGGREGATE KEY(`user_id`, `username`)\n" +
             "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
             "          PROPERTIES (\n" +
             "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
             "          );"

     initTableData2 = "insert into ${tbName2} values(123456789, 'Alice', 13243, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (234567890, 'Bob', 13445, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
             "               (345678901, 'Carol', 15768, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
             "               (456789012, 'Dave', 14243, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
             "               (567890123, 'Eve', 10768, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
             "               (678901234, 'Frank', 14325, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (993456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (789012345, 'Grace', 15686, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"
     sql initTable2
     sql initTableData2
     checkTableData("${tbName1}", "${tbName2}", "car_number")
     sql """ DROP TABLE IF EXISTS ${tbName1} """


     /**
      *  Test the AGGREGATE model by modify a value type from INT to other type
      */
     sql """ DROP TABLE IF EXISTS ${tbName1} """
     initTable = " CREATE TABLE IF NOT EXISTS ${tbName1}\n" +
             "          (\n" +
             "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
             "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
             "              `sn_number` INT REPLACE_IF_NOT_NULL COMMENT \"sn卡\",\n" +
             "              `city` VARCHAR(20) REPLACE_IF_NOT_NULL COMMENT \"用户所在城市\",\n" +
             "              `agge` SMALLINT REPLACE_IF_NOT_NULL COMMENT \"用户年龄\",\n" +
             "              `sex` TINYINT REPLACE_IF_NOT_NULL COMMENT \"用户性别\",\n" +
             "              `phone` LARGEINT REPLACE_IF_NOT_NULL COMMENT \"用户电话\",\n" +
             "              `address` VARCHAR(500) REPLACE_IF_NOT_NULL COMMENT \"用户地址\",\n" +
             "              `register_time` DATETIME REPLACE_IF_NOT_NULL COMMENT \"用户注册时间\"\n" +
             "          )\n" +
             "          AGGREGATE KEY(`user_id`, `username`)\n" +
             "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
             "          PROPERTIES (\n" +
             "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
             "          );"

     initTableData = "insert into ${tbName1} values(123456789, 'Alice', 2147483641, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (234567890, 'Bob', 214748364, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
             "               (345678901, 'Carol', 2147483441, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
             "               (456789012, 'Dave', 2147483141, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
             "               (567890123, 'Eve', 2127483141, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
             "               (678901234, 'Frank', 2124483141, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (789012345, 'Grace', 2123483141, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"

     //TODO Test the AGGREGATE model by modify a value type from INT  to BOOLEAN
     errorMessagge = "errCode = 2, detailMessage = Can not change INT to BOOLEAN"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName1} MODIFY  column sn_number BOOLEAN REPLACE_IF_NOT_NULL  """
          insertSql = "insert into ${tbName1} values(993456689, 'Alice', false, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 600
          }, insertSql, true, "${tbName1}")
     }, errorMessagge)


     // TODO Test the AGGREGATE model by modify a value type from INT  to TINYINT
     errorMessagge = "errCode = 2, detailMessage = Can not change INT to TINYINT"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName1} MODIFY  column sn_number TINYINT REPLACE_IF_NOT_NULL """
          insertSql = "insert into ${tbName1} values(993456689, 'Alice', 2, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 600
          }, insertSql, true, "${tbName1}")
     }, errorMessagge)


     //Test the AGGREGATE model by modify a value type from INT  to SMALLINT
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName1} MODIFY  column sn_number INT REPLACE_IF_NOT_NULL """
     insertSql = "insert into ${tbName1} values(993456689, 'Alice', 3, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 600
     }, insertSql, false, "${tbName1}")

     sql """ DROP TABLE IF EXISTS ${tbName2} """
     initTable2 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
             "          (\n" +
             "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
             "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
             "              `sn_number` INT REPLACE_IF_NOT_NULL COMMENT \"是否是学生\",\n" +
             "              `city` VARCHAR(20) REPLACE_IF_NOT_NULL COMMENT \"用户所在城市\",\n" +
             "              `agge` SMALLINT REPLACE_IF_NOT_NULL COMMENT \"用户年龄\",\n" +
             "              `sex` TINYINT REPLACE_IF_NOT_NULL COMMENT \"用户性别\",\n" +
             "              `phone` LARGEINT REPLACE_IF_NOT_NULL COMMENT \"用户电话\",\n" +
             "              `address` VARCHAR(500) REPLACE_IF_NOT_NULL COMMENT \"用户地址\",\n" +
             "              `register_time` DATETIME REPLACE_IF_NOT_NULL COMMENT \"用户注册时间\"\n" +
             "          )\n" +
             "          AGGREGATE KEY(`user_id`, `username`)\n" +
             "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
             "          PROPERTIES (\n" +
             "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
             "          );"

     initTableData2 = "insert into ${tbName2} values(123456789, 'Alice', 2147483641, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (234567890, 'Bob', 214748364, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
             "               (345678901, 'Carol', 2147483441, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
             "               (456789012, 'Dave', 2147483141, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
             "               (567890123, 'Eve', 2127483141, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
             "               (678901234, 'Frank', 2124483141, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (993456689, 'Alice', 3, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (789012345, 'Grace', 2123483141, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"
     sql initTable2
     sql initTableData2
     checkTableData("${tbName1}", "${tbName2}", "sn_number")
     sql """ DROP TABLE IF EXISTS ${tbName1} """


     //Test the AGGREGATE model by modify a value type from INT  to BIGINT
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName1} MODIFY  column sn_number BIGINT REPLACE_IF_NOT_NULL """
     insertSql = "insert into ${tbName1} values(993456689, 'Alice', 4, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 600
     }, insertSql, false, "${tbName1}")

     sql """ DROP TABLE IF EXISTS ${tbName2} """
     initTable2 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
             "          (\n" +
             "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
             "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
             "              `sn_number` BIGINT REPLACE_IF_NOT_NULL COMMENT \"是否是学生\",\n" +
             "              `city` VARCHAR(20) REPLACE_IF_NOT_NULL COMMENT \"用户所在城市\",\n" +
             "              `agge` SMALLINT REPLACE_IF_NOT_NULL COMMENT \"用户年龄\",\n" +
             "              `sex` TINYINT REPLACE_IF_NOT_NULL COMMENT \"用户性别\",\n" +
             "              `phone` LARGEINT REPLACE_IF_NOT_NULL COMMENT \"用户电话\",\n" +
             "              `address` VARCHAR(500) REPLACE_IF_NOT_NULL COMMENT \"用户地址\",\n" +
             "              `register_time` DATETIME REPLACE_IF_NOT_NULL COMMENT \"用户注册时间\"\n" +
             "          )\n" +
             "          AGGREGATE KEY(`user_id`, `username`)\n" +
             "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
             "          PROPERTIES (\n" +
             "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
             "          );"

     initTableData2 = "insert into ${tbName2} values(123456789, 'Alice', 2147483641, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (234567890, 'Bob', 214748364, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
             "               (345678901, 'Carol', 2147483441, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
             "               (456789012, 'Dave', 2147483141, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
             "               (567890123, 'Eve', 2127483141, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
             "               (678901234, 'Frank', 2124483141, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (993456689, 'Alice', 4, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (789012345, 'Grace', 2123483141, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"
     sql initTable2
     sql initTableData2
     checkTableData("${tbName1}", "${tbName2}", "sn_number")
     sql """ DROP TABLE IF EXISTS ${tbName1} """


     //Test the AGGREGATE model by modify a value type from INT  to LARGEINT
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName1} MODIFY  column sn_number LARGEINT REPLACE_IF_NOT_NULL """
     insertSql = "insert into ${tbName1} values(993456689, 'Alice', 5, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 600
     }, insertSql, false, "${tbName1}")

     sql """ DROP TABLE IF EXISTS ${tbName2} """
     initTable2 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
             "          (\n" +
             "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
             "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
             "              `sn_number` LARGEINT REPLACE_IF_NOT_NULL COMMENT \"是否是学生\",\n" +
             "              `city` VARCHAR(20) REPLACE_IF_NOT_NULL COMMENT \"用户所在城市\",\n" +
             "              `agge` SMALLINT REPLACE_IF_NOT_NULL COMMENT \"用户年龄\",\n" +
             "              `sex` TINYINT REPLACE_IF_NOT_NULL COMMENT \"用户性别\",\n" +
             "              `phone` LARGEINT REPLACE_IF_NOT_NULL COMMENT \"用户电话\",\n" +
             "              `address` VARCHAR(500) REPLACE_IF_NOT_NULL COMMENT \"用户地址\",\n" +
             "              `register_time` DATETIME REPLACE_IF_NOT_NULL COMMENT \"用户注册时间\"\n" +
             "          )\n" +
             "          AGGREGATE KEY(`user_id`, `username`)\n" +
             "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
             "          PROPERTIES (\n" +
             "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
             "          );"

     initTableData2 = "insert into ${tbName2} values(123456789, 'Alice', 2147483641, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (234567890, 'Bob', 214748364, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
             "               (345678901, 'Carol', 2147483441, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
             "               (456789012, 'Dave', 2147483141, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
             "               (567890123, 'Eve', 2127483141, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
             "               (678901234, 'Frank', 2124483141, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (993456689, 'Alice', 5, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (789012345, 'Grace', 2123483141, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"
     sql initTable2
     sql initTableData2
     checkTableData("${tbName1}", "${tbName2}", "sn_number")
     sql """ DROP TABLE IF EXISTS ${tbName1} """


     //Test the AGGREGATE model by modify a value type from INT  to FLOAT
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName1} MODIFY  column sn_number FLOAT REPLACE_IF_NOT_NULL """
     insertSql = "insert into ${tbName1} values(993456689, 'Alice', 1.2, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 600
     }, insertSql, false, "${tbName1}")


     sql """ DROP TABLE IF EXISTS ${tbName2} """
     initTable2 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
             "          (\n" +
             "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
             "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
             "              `sn_number` FLOAT REPLACE_IF_NOT_NULL COMMENT \"是否是学生\",\n" +
             "              `city` VARCHAR(20) REPLACE_IF_NOT_NULL COMMENT \"用户所在城市\",\n" +
             "              `agge` SMALLINT REPLACE_IF_NOT_NULL COMMENT \"用户年龄\",\n" +
             "              `sex` TINYINT REPLACE_IF_NOT_NULL COMMENT \"用户性别\",\n" +
             "              `phone` LARGEINT REPLACE_IF_NOT_NULL COMMENT \"用户电话\",\n" +
             "              `address` VARCHAR(500) REPLACE_IF_NOT_NULL COMMENT \"用户地址\",\n" +
             "              `register_time` DATETIME REPLACE_IF_NOT_NULL COMMENT \"用户注册时间\"\n" +
             "          )\n" +
             "          AGGREGATE KEY(`user_id`, `username`)\n" +
             "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
             "          PROPERTIES (\n" +
             "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
             "          );"

     initTableData2 = "insert into ${tbName2} values(123456789, 'Alice', 2147483641, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (234567890, 'Bob', 214748364, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
             "               (345678901, 'Carol', 2147483441, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
             "               (456789012, 'Dave', 2147483141, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
             "               (567890123, 'Eve', 2127483141, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
             "               (678901234, 'Frank', 2124483141, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (993456689, 'Alice', 1.2, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (789012345, 'Grace', 2123483141, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"
     sql initTable2
     sql initTableData2
     checkTableData("${tbName1}", "${tbName2}", "sn_number")
     sql """ DROP TABLE IF EXISTS ${tbName1} """


     //Test the AGGREGATE model by modify a value type from INT  to DOUBLE
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName1} MODIFY  column sn_number DOUBLE REPLACE_IF_NOT_NULL """
     insertSql = "insert into ${tbName1} values(993456689, 'Alice', 1.23, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 600
     }, insertSql, false, "${tbName1}")


     sql """ DROP TABLE IF EXISTS ${tbName2} """
     initTable2 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
             "          (\n" +
             "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
             "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
             "              `sn_number` DOUBLE REPLACE_IF_NOT_NULL COMMENT \"是否是学生\",\n" +
             "              `city` VARCHAR(20) REPLACE_IF_NOT_NULL COMMENT \"用户所在城市\",\n" +
             "              `agge` SMALLINT REPLACE_IF_NOT_NULL COMMENT \"用户年龄\",\n" +
             "              `sex` TINYINT REPLACE_IF_NOT_NULL COMMENT \"用户性别\",\n" +
             "              `phone` LARGEINT REPLACE_IF_NOT_NULL COMMENT \"用户电话\",\n" +
             "              `address` VARCHAR(500) REPLACE_IF_NOT_NULL COMMENT \"用户地址\",\n" +
             "              `register_time` DATETIME REPLACE_IF_NOT_NULL COMMENT \"用户注册时间\"\n" +
             "          )\n" +
             "          AGGREGATE KEY(`user_id`, `username`)\n" +
             "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
             "          PROPERTIES (\n" +
             "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
             "          );"

     initTableData2 = "insert into ${tbName2} values(123456789, 'Alice', 2147483641, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (234567890, 'Bob', 214748364, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
             "               (345678901, 'Carol', 2147483441, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
             "               (456789012, 'Dave', 2147483141, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
             "               (567890123, 'Eve', 2127483141, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
             "               (678901234, 'Frank', 2124483141, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (993456689, 'Alice', 1.23, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (789012345, 'Grace', 2123483141, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"
     sql initTable2
     sql initTableData2
     checkTableData("${tbName1}", "${tbName2}", "sn_number")
     sql """ DROP TABLE IF EXISTS ${tbName1} """


     //TODO Test the AGGREGATE model by modify a value type from INT  to DECIMAL
     errorMessagge = "errCode = 2, detailMessage = Can not change INT to DECIMAL128"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName1} MODIFY  column sn_number DECIMAL(38,0) REPLACE_IF_NOT_NULL """
          insertSql = "insert into ${tbName1} values(993456689, 'Alice', 1.23, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 600
          }, insertSql, true, "${tbName1}")

     }, errorMessagge)

     //TODO Test the AGGREGATE model by modify a  value type from INT  to CHAR
     errorMessagge = "errCode = 2, detailMessage = Can not change INT to CHAR"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName1} MODIFY  column sn_number CHAR(15) REPLACE_IF_NOT_NULL """
          insertSql = "insert into ${tbName1} values(993456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 600
          }, insertSql, true, "${tbName1}")
     }, errorMessagge)


     //Test the AGGREGATE model by modify a value type from INT  to VARCHAR
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName1} MODIFY  column sn_number VARCHAR(100) REPLACE_IF_NOT_NULL """
     insertSql = "insert into ${tbName1} values(993456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 600
     }, insertSql, false, "${tbName1}")

     sql """ DROP TABLE IF EXISTS ${tbName2} """
     initTable2 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
             "          (\n" +
             "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
             "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
             "              `sn_number` VARCHAR(100) REPLACE_IF_NOT_NULL COMMENT \"是否是学生\",\n" +
             "              `city` VARCHAR(20) REPLACE_IF_NOT_NULL COMMENT \"用户所在城市\",\n" +
             "              `agge` SMALLINT REPLACE_IF_NOT_NULL COMMENT \"用户年龄\",\n" +
             "              `sex` TINYINT REPLACE_IF_NOT_NULL COMMENT \"用户性别\",\n" +
             "              `phone` LARGEINT REPLACE_IF_NOT_NULL COMMENT \"用户电话\",\n" +
             "              `address` VARCHAR(500) REPLACE_IF_NOT_NULL COMMENT \"用户地址\",\n" +
             "              `register_time` DATETIME REPLACE_IF_NOT_NULL COMMENT \"用户注册时间\"\n" +
             "          )\n" +
             "          AGGREGATE KEY(`user_id`, `username`)\n" +
             "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
             "          PROPERTIES (\n" +
             "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
             "          );"

     initTableData2 = "insert into ${tbName2} values(123456789, 'Alice', 2147483641, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (234567890, 'Bob', 214748364, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
             "               (345678901, 'Carol', 2147483441, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
             "               (456789012, 'Dave', 2147483141, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
             "               (567890123, 'Eve', 2127483141, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
             "               (678901234, 'Frank', 2124483141, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (993456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (789012345, 'Grace', 2123483141, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"
     sql initTable2
     sql initTableData2
     checkTableData("${tbName1}", "${tbName2}", "sn_number")
     sql """ DROP TABLE IF EXISTS ${tbName1} """


     //Test the AGGREGATE model by modify a value type from INT  to VARCHAR
     errorMessagge = "errCode = 2, detailMessage = Can not change from wider type int to narrower type varchar(2)"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName1} MODIFY  column sn_number VARCHAR(2) REPLACE_IF_NOT_NULL """
          insertSql = "insert into ${tbName1} values(993456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 600
          }, insertSql, true, "${tbName1}")
     }, errorMessagge)

     //Test the AGGREGATE model by modify a value type from INT  to STRING
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName1} MODIFY  column sn_number STRING REPLACE_IF_NOT_NULL """
     insertSql = "insert into ${tbName1} values(993456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 600
     }, insertSql, false, "${tbName1}")


     sql """ DROP TABLE IF EXISTS ${tbName2} """
     initTable2 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
             "          (\n" +
             "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
             "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
             "              `sn_number` STRING REPLACE_IF_NOT_NULL COMMENT \"是否是学生\",\n" +
             "              `city` VARCHAR(20) REPLACE_IF_NOT_NULL COMMENT \"用户所在城市\",\n" +
             "              `agge` SMALLINT REPLACE_IF_NOT_NULL COMMENT \"用户年龄\",\n" +
             "              `sex` TINYINT REPLACE_IF_NOT_NULL COMMENT \"用户性别\",\n" +
             "              `phone` LARGEINT REPLACE_IF_NOT_NULL COMMENT \"用户电话\",\n" +
             "              `address` VARCHAR(500) REPLACE_IF_NOT_NULL COMMENT \"用户地址\",\n" +
             "              `register_time` DATETIME REPLACE_IF_NOT_NULL COMMENT \"用户注册时间\"\n" +
             "          )\n" +
             "          AGGREGATE KEY(`user_id`, `username`)\n" +
             "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
             "          PROPERTIES (\n" +
             "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
             "          );"

     initTableData2 = "insert into ${tbName2} values(123456789, 'Alice', 2147483641, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (234567890, 'Bob', 214748364, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
             "               (345678901, 'Carol', 2147483441, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
             "               (456789012, 'Dave', 2147483141, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
             "               (567890123, 'Eve', 2127483141, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
             "               (678901234, 'Frank', 2124483141, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (993456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (789012345, 'Grace', 2123483141, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"
     sql initTable2
     sql initTableData2
     checkTableData("${tbName1}", "${tbName2}", "sn_number")
     sql """ DROP TABLE IF EXISTS ${tbName1} """


     /**
      *  Test the AGGREGATE model by modify a value type from BIGINT to other type
      */
     sql """ DROP TABLE IF EXISTS ${tbName1} """
     initTable = " CREATE TABLE IF NOT EXISTS ${tbName1}\n" +
             "          (\n" +
             "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
             "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
             "              `fan_number` BIGINT REPLACE_IF_NOT_NULL COMMENT \"fan序列号\",\n" +
             "              `city` VARCHAR(20) REPLACE_IF_NOT_NULL COMMENT \"用户所在城市\",\n" +
             "              `agge` SMALLINT REPLACE_IF_NOT_NULL COMMENT \"用户年龄\",\n" +
             "              `sex` TINYINT REPLACE_IF_NOT_NULL COMMENT \"用户性别\",\n" +
             "              `phone` LARGEINT REPLACE_IF_NOT_NULL COMMENT \"用户电话\",\n" +
             "              `address` VARCHAR(500) REPLACE_IF_NOT_NULL COMMENT \"用户地址\",\n" +
             "              `register_time` DATETIME REPLACE_IF_NOT_NULL COMMENT \"用户注册时间\"\n" +
             "          )\n" +
             "          AGGREGATE KEY(`user_id`, `username`)\n" +
             "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
             "          PROPERTIES (\n" +
             "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
             "          );"

     initTableData = "insert into ${tbName1} values(123456789, 'Alice', 21474832641, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (234567890, 'Bob', 21474348364, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
             "               (345678901, 'Carol', 214742383441, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
             "               (456789012, 'Dave', 21474283141, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
             "               (567890123, 'Eve', 21274863141, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
             "               (678901234, 'Frank', 21244883141, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (789012345, 'Grace', 21234683141, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"

     //TODO Test the AGGREGATE model by modify a value type from BIGINT  to BOOLEAN
     errorMessagge = "errCode = 2, detailMessage = Can not change BIGINT to BOOLEAN"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName1} MODIFY  column fan_number BOOLEAN   """
          insertSql = "insert into ${tbName1} values(993456689, 'Alice', false, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 600
          }, insertSql, true, "${tbName1}")
     }, errorMessagge)


     // TODO Test the AGGREGATE model by modify a value type from BIGINT  to TINYINT
     errorMessagge = "errCode = 2, detailMessage = Can not change BIGINT to TINYINT"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName1} MODIFY  column fan_number TINYINT  """
          insertSql = "insert into ${tbName1} values(993456689, 'Alice', 2, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 600
          }, insertSql, true, "${tbName1}")
     }, errorMessagge)


     //Test the AGGREGATE model by modify a value type from BIGINT  to SMALLINT
     errorMessagge = "errCode = 2, detailMessage = Can not change BIGINT to SMALLINT"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName1} MODIFY  column fan_number SMALLINT  """
          insertSql = "insert into ${tbName1} values(993456689, 'Alice', 3, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 600
          }, insertSql, false, "${tbName1}")
     }, errorMessagge)


     //Test the AGGREGATE model by modify a value type from BIGINT  to INT
     errorMessagge = "errCode = 2, detailMessage = Can not change BIGINT to INT"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName1} MODIFY  column fan_number INT  """
          insertSql = "insert into ${tbName1} values(993456689, 'Alice', 4, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 600
          }, insertSql, true, "${tbName1}")
     }, errorMessagge)

     //Test the AGGREGATE model by modify a value type from BIGINT  to LARGEINT
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName1} MODIFY  column fan_number LARGEINT REPLACE_IF_NOT_NULL """
     insertSql = "insert into ${tbName1} values(993456689, 'Alice', 5, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 600
     }, insertSql, false, "${tbName1}")

     sql """ DROP TABLE IF EXISTS ${tbName2} """
     initTable2 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
             "          (\n" +
             "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
             "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
             "              `fan_number` LARGEINT REPLACE_IF_NOT_NULL COMMENT \"是否是学生\",\n" +
             "              `city` VARCHAR(20) REPLACE_IF_NOT_NULL COMMENT \"用户所在城市\",\n" +
             "              `agge` SMALLINT REPLACE_IF_NOT_NULL COMMENT \"用户年龄\",\n" +
             "              `sex` TINYINT REPLACE_IF_NOT_NULL COMMENT \"用户性别\",\n" +
             "              `phone` LARGEINT REPLACE_IF_NOT_NULL COMMENT \"用户电话\",\n" +
             "              `address` VARCHAR(500) REPLACE_IF_NOT_NULL COMMENT \"用户地址\",\n" +
             "              `register_time` DATETIME REPLACE_IF_NOT_NULL COMMENT \"用户注册时间\"\n" +
             "          )\n" +
             "          AGGREGATE KEY(`user_id`, `username`)\n" +
             "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
             "          PROPERTIES (\n" +
             "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
             "          );"

     initTableData2 = "insert into ${tbName2} values(123456789, 'Alice', 21474832641, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (234567890, 'Bob', 21474348364, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
             "               (345678901, 'Carol', 214742383441, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
             "               (456789012, 'Dave', 21474283141, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
             "               (567890123, 'Eve', 21274863141, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
             "               (678901234, 'Frank', 21244883141, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (993456689, 'Frank', 5, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (789012345, 'Grace', 21234683141, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"
     sql initTable2
     sql initTableData2
     checkTableData("${tbName1}", "${tbName2}", "fan_number")
     sql """ DROP TABLE IF EXISTS ${tbName1} """


     //Test the AGGREGATE model by modify a value type from BIGINT  to FLOAT
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName1} MODIFY  column fan_number FLOAT REPLACE_IF_NOT_NULL """
     insertSql = "insert into ${tbName1} values(993456689, 'Alice', 1.2, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 600
     }, insertSql, false, "${tbName1}")


     sql """ DROP TABLE IF EXISTS ${tbName2} """
     initTable2 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
             "          (\n" +
             "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
             "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
             "              `fan_number` FLOAT REPLACE_IF_NOT_NULL COMMENT \"是否是学生\",\n" +
             "              `city` VARCHAR(20) REPLACE_IF_NOT_NULL COMMENT \"用户所在城市\",\n" +
             "              `agge` SMALLINT REPLACE_IF_NOT_NULL COMMENT \"用户年龄\",\n" +
             "              `sex` TINYINT REPLACE_IF_NOT_NULL COMMENT \"用户性别\",\n" +
             "              `phone` LARGEINT REPLACE_IF_NOT_NULL COMMENT \"用户电话\",\n" +
             "              `address` VARCHAR(500) REPLACE_IF_NOT_NULL COMMENT \"用户地址\",\n" +
             "              `register_time` DATETIME REPLACE_IF_NOT_NULL COMMENT \"用户注册时间\"\n" +
             "          )\n" +
             "          AGGREGATE KEY(`user_id`, `username`)\n" +
             "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
             "          PROPERTIES (\n" +
             "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
             "          );"

     initTableData2 = "insert into ${tbName2} values(123456789, 'Alice', 21474832641, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (234567890, 'Bob', 21474348364, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
             "               (345678901, 'Carol', 214742383441, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
             "               (456789012, 'Dave', 21474283141, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
             "               (567890123, 'Eve', 21274863141, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
             "               (678901234, 'Frank', 21244883141, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (993456689, 'Frank', 1.2, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (789012345, 'Grace', 21234683141, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"
     sql initTable2
     sql initTableData2
     checkTableData("${tbName1}", "${tbName2}", "fan_number")
     sql """ DROP TABLE IF EXISTS ${tbName1} """


     //Test the AGGREGATE model by modify a value type from BIGINT  to DOUBLE
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName1} MODIFY  column fan_number DOUBLE REPLACE_IF_NOT_NULL """
     insertSql = "insert into ${tbName1} values(993456689, 'Alice', 1.23, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 600
     }, insertSql, false, "${tbName1}")


     sql """ DROP TABLE IF EXISTS ${tbName2} """
     initTable2 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
             "          (\n" +
             "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
             "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
             "              `fan_number` DOUBLE REPLACE_IF_NOT_NULL COMMENT \"是否是学生\",\n" +
             "              `city` VARCHAR(20) REPLACE_IF_NOT_NULL COMMENT \"用户所在城市\",\n" +
             "              `agge` SMALLINT REPLACE_IF_NOT_NULL COMMENT \"用户年龄\",\n" +
             "              `sex` TINYINT REPLACE_IF_NOT_NULL COMMENT \"用户性别\",\n" +
             "              `phone` LARGEINT REPLACE_IF_NOT_NULL COMMENT \"用户电话\",\n" +
             "              `address` VARCHAR(500) REPLACE_IF_NOT_NULL COMMENT \"用户地址\",\n" +
             "              `register_time` DATETIME REPLACE_IF_NOT_NULL COMMENT \"用户注册时间\"\n" +
             "          )\n" +
             "          AGGREGATE KEY(`user_id`, `username`)\n" +
             "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
             "          PROPERTIES (\n" +
             "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
             "          );"

     initTableData2 = "insert into ${tbName2} values(123456789, 'Alice', 21474832641, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (234567890, 'Bob', 21474348364, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
             "               (345678901, 'Carol', 214742383441, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
             "               (456789012, 'Dave', 21474283141, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
             "               (567890123, 'Eve', 21274863141, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
             "               (678901234, 'Frank', 21244883141, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (993456689, 'Frank', 1.23, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (789012345, 'Grace', 21234683141, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"
     sql initTable2
     sql initTableData2
     checkTableData("${tbName1}", "${tbName2}", "fan_number")
     sql """ DROP TABLE IF EXISTS ${tbName1} """


     //TODO Test the AGGREGATE model by modify a value type from BIGINT  to DECIMAL
     errorMessagge = "errCode = 2, detailMessage = Can not change BIGINT to DECIMAL128"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName1} MODIFY  column fan_number DECIMAL(38,0) REPLACE_IF_NOT_NULL """
          insertSql = "insert into ${tbName1} values(993456689, 'Alice', 1.23, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 600
          }, insertSql, true, "${tbName1}")

     }, errorMessagge)

     //TODO Test the AGGREGATE model by modify a  value type from BIGINT  to CHAR
     errorMessagge = "errCode = 2, detailMessage = Can not change BIGINT to CHAR"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName1} MODIFY  column fan_number CHAR(15) REPLACE_IF_NOT_NULL """
          insertSql = "insert into ${tbName1} values(993456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 600
          }, insertSql, true, "${tbName1}")
     }, errorMessagge)


     //Test the AGGREGATE model by modify a value type from BIGINT  to VARCHAR
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName1} MODIFY  column fan_number VARCHAR(100) REPLACE_IF_NOT_NULL """
     insertSql = "insert into ${tbName1} values(993456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 600
     }, insertSql, false, "${tbName1}")


     sql """ DROP TABLE IF EXISTS ${tbName2} """
     initTable2 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
             "          (\n" +
             "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
             "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
             "              `fan_number` VARCHAR(100) REPLACE_IF_NOT_NULL COMMENT \"是否是学生\",\n" +
             "              `city` VARCHAR(20) REPLACE_IF_NOT_NULL COMMENT \"用户所在城市\",\n" +
             "              `agge` SMALLINT REPLACE_IF_NOT_NULL COMMENT \"用户年龄\",\n" +
             "              `sex` TINYINT REPLACE_IF_NOT_NULL COMMENT \"用户性别\",\n" +
             "              `phone` LARGEINT REPLACE_IF_NOT_NULL COMMENT \"用户电话\",\n" +
             "              `address` VARCHAR(500) REPLACE_IF_NOT_NULL COMMENT \"用户地址\",\n" +
             "              `register_time` DATETIME REPLACE_IF_NOT_NULL COMMENT \"用户注册时间\"\n" +
             "          )\n" +
             "          AGGREGATE KEY(`user_id`, `username`)\n" +
             "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
             "          PROPERTIES (\n" +
             "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
             "          );"

     initTableData2 = "insert into ${tbName2} values(123456789, 'Alice', 21474832641, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (234567890, 'Bob', 21474348364, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
             "               (345678901, 'Carol', 214742383441, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
             "               (456789012, 'Dave', 21474283141, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
             "               (567890123, 'Eve', 21274863141, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
             "               (678901234, 'Frank', 21244883141, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (993456689, 'Frank', 'asd', 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (789012345, 'Grace', 21234683141, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"
     sql initTable2
     sql initTableData2
     checkTableData("${tbName1}", "${tbName2}", "fan_number")
     sql """ DROP TABLE IF EXISTS ${tbName1} """


     //Test the AGGREGATE model by modify a value type from BIGINT  to VARCHAR(2)
     errorMessagge = "errCode = 2, detailMessage = Can not change from wider type bigint to narrower type varchar(2)"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName1} MODIFY  column fan_number VARCHAR(2) REPLACE_IF_NOT_NULL """
          insertSql = "insert into ${tbName1} values(993456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 600
          }, insertSql, true, "${tbName1}")
     }, errorMessagge)


     //Test the AGGREGATE model by modify a value type from BIGINT  to STRING
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName1} MODIFY  column fan_number STRING REPLACE_IF_NOT_NULL """
     insertSql = "insert into ${tbName1} values(993456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 600
     }, insertSql, false, "${tbName1}")

     sql """ DROP TABLE IF EXISTS ${tbName2} """
     initTable2 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
             "          (\n" +
             "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
             "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
             "              `fan_number` STRING REPLACE_IF_NOT_NULL COMMENT \"是否是学生\",\n" +
             "              `city` VARCHAR(20) REPLACE_IF_NOT_NULL COMMENT \"用户所在城市\",\n" +
             "              `agge` SMALLINT REPLACE_IF_NOT_NULL COMMENT \"用户年龄\",\n" +
             "              `sex` TINYINT REPLACE_IF_NOT_NULL COMMENT \"用户性别\",\n" +
             "              `phone` LARGEINT REPLACE_IF_NOT_NULL COMMENT \"用户电话\",\n" +
             "              `address` VARCHAR(500) REPLACE_IF_NOT_NULL COMMENT \"用户地址\",\n" +
             "              `register_time` DATETIME REPLACE_IF_NOT_NULL COMMENT \"用户注册时间\"\n" +
             "          )\n" +
             "          AGGREGATE KEY(`user_id`, `username`)\n" +
             "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
             "          PROPERTIES (\n" +
             "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
             "          );"

     initTableData2 = "insert into ${tbName2} values(123456789, 'Alice', 21474832641, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (234567890, 'Bob', 21474348364, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
             "               (345678901, 'Carol', 214742383441, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
             "               (456789012, 'Dave', 21474283141, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
             "               (567890123, 'Eve', 21274863141, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
             "               (678901234, 'Frank', 21244883141, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (993456689, 'Frank', 'asd', 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (789012345, 'Grace', 21234683141, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"
     sql initTable2
     sql initTableData2
     checkTableData("${tbName1}", "${tbName2}", "fan_number")
     sql """ DROP TABLE IF EXISTS ${tbName1} """


     /**
      *  Test the AGGREGATE model by modify a value type from LARGEINT to other type
      */
     sql """ DROP TABLE IF EXISTS ${tbName1} """
     initTable = " CREATE TABLE IF NOT EXISTS ${tbName1}\n" +
             "          (\n" +
             "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
             "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
             "              `item_number` LARGEINT REPLACE_IF_NOT_NULL COMMENT \"item序列号\",\n" +
             "              `city` VARCHAR(20) REPLACE_IF_NOT_NULL COMMENT \"用户所在城市\",\n" +
             "              `agge` SMALLINT REPLACE_IF_NOT_NULL COMMENT \"用户年龄\",\n" +
             "              `sex` TINYINT REPLACE_IF_NOT_NULL COMMENT \"用户性别\",\n" +
             "              `phone` LARGEINT REPLACE_IF_NOT_NULL COMMENT \"用户电话\",\n" +
             "              `address` VARCHAR(500) REPLACE_IF_NOT_NULL COMMENT \"用户地址\",\n" +
             "              `register_time` DATETIME REPLACE_IF_NOT_NULL COMMENT \"用户注册时间\"\n" +
             "          )\n" +
             "          AGGREGATE KEY(`user_id`, `username`)\n" +
             "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
             "          PROPERTIES (\n" +
             "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
             "          );"

     initTableData = "insert into ${tbName1} values(123456789, 'Alice', 21474832641, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (234567890, 'Bob', 21474348364, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
             "               (345678901, 'Carol', 214742383441, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
             "               (456789012, 'Dave', 21474283141, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
             "               (567890123, 'Eve', 21274863141, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
             "               (678901234, 'Frank', 21244883141, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (789012345, 'Grace', 21234683141, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"

     //TODO Test the AGGREGATE model by modify a value type from LARGEINT  to BOOLEAN
     errorMessagge = "errCode = 2, detailMessage = Can not change LARGEINT to BOOLEAN"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName1} MODIFY  column item_number BOOLEAN  REPLACE_IF_NOT_NULL """
          insertSql = "insert into ${tbName1} values(993456689, 'Alice', false, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 600
          }, insertSql, true, "${tbName1}")
     }, errorMessagge)


     // TODO Test the AGGREGATE model by modify a value type from LARGEINT  to TINYINT
     errorMessagge = "errCode = 2, detailMessage = Can not change LARGEINT to TINYINT"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName1} MODIFY  column item_number TINYINT REPLACE_IF_NOT_NULL """
          insertSql = "insert into ${tbName1} values(993456689, 'Alice', 2, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 600
          }, insertSql, true, "${tbName1}")
     }, errorMessagge)


     //TODO Test the AGGREGATE model by modify a value type from LARGEINT  to SMALLINT
     errorMessagge = "errCode = 2, detailMessage = Can not change LARGEINT to SMALLINT"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName1} MODIFY  column item_number SMALLINT REPLACE_IF_NOT_NULL """
          insertSql = "insert into ${tbName1} values(993456689, 'Alice', 3, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 600
          }, insertSql, true, "${tbName1}")
     }, errorMessagge)

     //TODO Test the AGGREGATE model by modify a value type from LARGEINT  to INT
     errorMessagge = "errCode = 2, detailMessage = Can not change LARGEINT to INT"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName1} MODIFY  column item_number INT REPLACE_IF_NOT_NULL """
          insertSql = "insert into ${tbName1} values(993456689, 'Alice', 4, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 600
          }, insertSql, true, "${tbName1}")
     }, errorMessagge)

     //TODO Test the AGGREGATE model by modify a value type from  LARGEINT to BIGINT
     errorMessagge="errCode = 2, detailMessage = Can not change LARGEINT to BIGINT"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName1} MODIFY  column item_number BIGINT REPLACE_IF_NOT_NULL """
          insertSql = "insert into ${tbName1} values(993456689, 'Alice', 5, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 600
          }, insertSql, true, "${tbName1}")
     },errorMessagge)


     //Test the AGGREGATE model by modify a value type from LARGEINT  to FLOAT
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName1} MODIFY  column item_number FLOAT REPLACE_IF_NOT_NULL """
     insertSql = "insert into ${tbName1} values(993456689, 'Alice', 1.2, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 600
     }, insertSql, false, "${tbName1}")


     sql """ DROP TABLE IF EXISTS ${tbName2} """
     initTable2 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
             "          (\n" +
             "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
             "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
             "              `item_number` FLOAT REPLACE_IF_NOT_NULL COMMENT \"是否是学生\",\n" +
             "              `city` VARCHAR(20) REPLACE_IF_NOT_NULL COMMENT \"用户所在城市\",\n" +
             "              `agge` SMALLINT REPLACE_IF_NOT_NULL COMMENT \"用户年龄\",\n" +
             "              `sex` TINYINT REPLACE_IF_NOT_NULL COMMENT \"用户性别\",\n" +
             "              `phone` LARGEINT REPLACE_IF_NOT_NULL COMMENT \"用户电话\",\n" +
             "              `address` VARCHAR(500) REPLACE_IF_NOT_NULL COMMENT \"用户地址\",\n" +
             "              `register_time` DATETIME REPLACE_IF_NOT_NULL COMMENT \"用户注册时间\"\n" +
             "          )\n" +
             "          AGGREGATE KEY(`user_id`, `username`)\n" +
             "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
             "          PROPERTIES (\n" +
             "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
             "          );"

     initTableData2 = "insert into ${tbName2} values(123456789, 'Alice', 21474832641, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (234567890, 'Bob', 21474348364, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
             "               (345678901, 'Carol', 214742383441, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
             "               (456789012, 'Dave', 21474283141, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
             "               (567890123, 'Eve', 21274863141, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
             "               (678901234, 'Frank', 21244883141, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (993456689, 'Alice', 1.2, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (789012345, 'Grace', 21234683141, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"
     sql initTable2
     sql initTableData2
     checkTableData("${tbName1}","${tbName2}","item_number")
     sql """ DROP TABLE IF EXISTS ${tbName1} """


     //Test the AGGREGATE model by modify a value type from LARGEINT  to DOUBLE
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName1} MODIFY  column item_number DOUBLE REPLACE_IF_NOT_NULL """
     insertSql = "insert into ${tbName1} values(993456689, 'Alice', 1.23, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 600
     }, insertSql, false, "${tbName1}")

     sql """ DROP TABLE IF EXISTS ${tbName2} """
     initTable2 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
             "          (\n" +
             "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
             "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
             "              `item_number` DOUBLE REPLACE_IF_NOT_NULL COMMENT \"是否是学生\",\n" +
             "              `city` VARCHAR(20) REPLACE_IF_NOT_NULL COMMENT \"用户所在城市\",\n" +
             "              `agge` SMALLINT REPLACE_IF_NOT_NULL COMMENT \"用户年龄\",\n" +
             "              `sex` TINYINT REPLACE_IF_NOT_NULL COMMENT \"用户性别\",\n" +
             "              `phone` LARGEINT REPLACE_IF_NOT_NULL COMMENT \"用户电话\",\n" +
             "              `address` VARCHAR(500) REPLACE_IF_NOT_NULL COMMENT \"用户地址\",\n" +
             "              `register_time` DATETIME REPLACE_IF_NOT_NULL COMMENT \"用户注册时间\"\n" +
             "          )\n" +
             "          AGGREGATE KEY(`user_id`, `username`)\n" +
             "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
             "          PROPERTIES (\n" +
             "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
             "          );"

     initTableData2 = "insert into ${tbName2} values(123456789, 'Alice', 21474832641, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (234567890, 'Bob', 21474348364, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
             "               (345678901, 'Carol', 214742383441, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
             "               (456789012, 'Dave', 21474283141, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
             "               (567890123, 'Eve', 21274863141, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
             "               (678901234, 'Frank', 21244883141, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (993456689, 'Alice', 1.23, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (789012345, 'Grace', 21234683141, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"
     sql initTable2
     sql initTableData2
     checkTableData("${tbName1}","${tbName2}","item_number")
     sql """ DROP TABLE IF EXISTS ${tbName1} """


     //TODO Test the AGGREGATE model by modify a value type from LARGEINT  to DECIMAL
     errorMessagge = "errCode = 2, detailMessage = Can not change LARGEINT to DECIMAL128"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName1} MODIFY  column item_number DECIMAL(38,0) REPLACE_IF_NOT_NULL """
          insertSql = "insert into ${tbName1} values(993456689, 'Alice', 1.23, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 600
          }, insertSql, true, "${tbName1}")
     }, errorMessagge)

     //TODO Test the AGGREGATE model by modify a  value type from LARGEINT  to CHAR
     errorMessagge = "errCode = 2, detailMessage = Can not change LARGEINT to CHAR"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName1} MODIFY  column item_number CHAR(15) REPLACE_IF_NOT_NULL """
          insertSql = "insert into ${tbName1} values(993456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 600
          }, insertSql, true, "${tbName1}")
     }, errorMessagge)


     //Test the AGGREGATE model by modify a value type from LARGEINT  to VARCHAR
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName1} MODIFY  column item_number VARCHAR(100) REPLACE_IF_NOT_NULL """
     insertSql = "insert into ${tbName1} values(993456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 600
     }, insertSql, false, "${tbName1}")


     sql """ DROP TABLE IF EXISTS ${tbName2} """
     initTable2 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
             "          (\n" +
             "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
             "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
             "              `item_number` VARCHAR(100) REPLACE_IF_NOT_NULL COMMENT \"是否是学生\",\n" +
             "              `city` VARCHAR(20) REPLACE_IF_NOT_NULL COMMENT \"用户所在城市\",\n" +
             "              `agge` SMALLINT REPLACE_IF_NOT_NULL COMMENT \"用户年龄\",\n" +
             "              `sex` TINYINT REPLACE_IF_NOT_NULL COMMENT \"用户性别\",\n" +
             "              `phone` LARGEINT REPLACE_IF_NOT_NULL COMMENT \"用户电话\",\n" +
             "              `address` VARCHAR(500) REPLACE_IF_NOT_NULL COMMENT \"用户地址\",\n" +
             "              `register_time` DATETIME REPLACE_IF_NOT_NULL COMMENT \"用户注册时间\"\n" +
             "          )\n" +
             "          AGGREGATE KEY(`user_id`, `username`)\n" +
             "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
             "          PROPERTIES (\n" +
             "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
             "          );"

     initTableData2 = "insert into ${tbName2} values(123456789, 'Alice', 21474832641, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (234567890, 'Bob', 21474348364, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
             "               (345678901, 'Carol', 214742383441, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
             "               (456789012, 'Dave', 21474283141, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
             "               (567890123, 'Eve', 21274863141, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
             "               (678901234, 'Frank', 21244883141, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (993456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (789012345, 'Grace', 21234683141, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"
     sql initTable2
     sql initTableData2
     checkTableData("${tbName1}","${tbName2}","item_number")
     sql """ DROP TABLE IF EXISTS ${tbName1} """



     //Test the AGGREGATE model by modify a value type from LARGEINT  to VARCHAR(2)
     errorMessagge="errCode = 2, detailMessage = Can not change from wider type largeint to narrower type varchar(2)"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName1} MODIFY  column item_number VARCHAR(2) REPLACE_IF_NOT_NULL """
          insertSql = "insert into ${tbName1} values(993456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 600
          }, insertSql, true, "${tbName1}")
     },errorMessagge)


     //Test the AGGREGATE model by modify a value type from LARGEINT  to STRING
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName1} MODIFY  column item_number STRING REPLACE_IF_NOT_NULL """
     insertSql = "insert into ${tbName1} values(993456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 600
     }, insertSql, false, "${tbName1}")

     sql """ DROP TABLE IF EXISTS ${tbName2} """
     initTable2 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
             "          (\n" +
             "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
             "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
             "              `item_number` STRING REPLACE_IF_NOT_NULL COMMENT \"是否是学生\",\n" +
             "              `city` VARCHAR(20) REPLACE_IF_NOT_NULL COMMENT \"用户所在城市\",\n" +
             "              `agge` SMALLINT REPLACE_IF_NOT_NULL COMMENT \"用户年龄\",\n" +
             "              `sex` TINYINT REPLACE_IF_NOT_NULL COMMENT \"用户性别\",\n" +
             "              `phone` LARGEINT REPLACE_IF_NOT_NULL COMMENT \"用户电话\",\n" +
             "              `address` VARCHAR(500) REPLACE_IF_NOT_NULL COMMENT \"用户地址\",\n" +
             "              `register_time` DATETIME REPLACE_IF_NOT_NULL COMMENT \"用户注册时间\"\n" +
             "          )\n" +
             "          AGGREGATE KEY(`user_id`, `username`)\n" +
             "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
             "          PROPERTIES (\n" +
             "          \"replication_allocation\" = \"tag.location.default: 1\"\n" +
             "          );"

     initTableData2 = "insert into ${tbName2} values(123456789, 'Alice', 21474832641, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (234567890, 'Bob', 21474348364, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
             "               (345678901, 'Carol', 214742383441, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
             "               (456789012, 'Dave', 21474283141, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
             "               (567890123, 'Eve', 21274863141, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
             "               (678901234, 'Frank', 21244883141, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (993456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (789012345, 'Grace', 21234683141, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"
     sql initTable2
     sql initTableData2
     checkTableData("${tbName1}","${tbName2}","item_number")
     sql """ DROP TABLE IF EXISTS ${tbName1} """



}
