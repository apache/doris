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

suite("test_unique_schema_key_change_modify1","p0") {
     def tbName = "test_unique_schema_key_change_modify_3"
     def tbName2 = "test_unique_schema_key_change_modify_4"
     /**
      *  Test the unique model by modify a value type
      */

     /**
      *  Test the unique model by modify a key type from DATE to other type
      */

     sql """ DROP TABLE IF EXISTS ${tbName} """
     def getTableStatusSql = " SHOW ALTER TABLE COLUMN WHERE IndexName='${tbName}' ORDER BY createtime DESC LIMIT 1  "
     def initTable = " CREATE TABLE IF NOT EXISTS ${tbName}\n" +
             "          (\n" +
             "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
             "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
             "              `login_time` DATE COMMENT \"用户登陆时间\",\n" +
             "              `is_teacher` BOOLEAN COMMENT \"是否是老师\",\n" +
             "              `city` VARCHAR(20) COMMENT \"用户所在城市\",\n" +
             "              `age` SMALLINT COMMENT \"用户年龄\",\n" +
             "              `sex` TINYINT COMMENT \"用户性别\",\n" +
             "              `phone` LARGEINT COMMENT \"用户电话\",\n" +
             "              `address` VARCHAR(500) COMMENT \"用户地址\",\n" +
             "              `register_time` DATETIME COMMENT \"用户注册时间\"\n" +
             "          )\n" +
             "          UNIQUE KEY(`user_id`, `username`, `login_time`)\n" +
             "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
             "          PROPERTIES (\n" +
             "          \"replication_allocation\" = \"tag.location.default: 1\",\n" +
             "          \"enable_unique_key_merge_on_write\" = \"true\"\n" +
             "          );"

     def initTableData = "insert into ${tbName} values(123456789, 'Alice', '2022-01-01', 0, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing',  '2022-01-01 10:00:00')," +
             "               (234567890, 'Bob', '2022-01-01 12:00:00', 0, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai',  '2022-02-02 12:00:00')," +
             "               (345678901, 'Carol', '2022-01-01 12:00:00', 1, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
             "               (456789012, 'Dave', '2022-01-01 12:00:00', 0, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen',  '2022-04-04 16:00:00')," +
             "               (567890123, 'Eve', '2022-01-01 12:00:00', 0, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu',  '2022-05-05 18:00:00')," +
             "               (678901234, 'Frank', '2022-01-01 12:00:00', 1, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou',  '2022-06-06 20:00:00')," +
             "               (789012345, 'Grace', '2022-01-01 12:00:00', 0, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian',  '2022-07-07 22:00:00');"

     //TODO Test the unique model by modify a key type from DATE to BOOLEAN
     def errorMessage="errCode = 2, detailMessage = Can not change DATEV2 to BOOLEAN"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column login_time BOOLEAN KEY DEFAULT "1"  """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 0, 1, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")
     },errorMessage)

     //TODO Test the unique model by modify a key type from DATE to TINYINT
     errorMessage="errCode = 2, detailMessage = Can not change DATEV2 to TINYINT"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column login_time TINYINT KEY DEFAULT "1"  """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 1, 1, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")
     },errorMessage)

     //TODO Test the unique model by modify a key type from DATE to SMALLINT
     errorMessage="errCode = 2, detailMessage = Can not change DATEV2 to SMALLINT"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column login_time SMALLINT KEY DEFAULT "1"  """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 1, 1, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")
     },errorMessage)


     //TODO Test the unique model by modify a key type from DATE to INT
     errorMessage="errCode = 2, detailMessage = Can not change DATEV2 to INT"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column login_time INT KEY DEFAULT "1"  """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 1, 1, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")
     },errorMessage)



     //TODO Test the unique model by modify a key type from DATE to BIGINT
     errorMessage="errCode = 2, detailMessage = Can not change DATEV2 to BIGINT"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column login_time BIGINT KEY DEFAULT "0"  """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 1, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")

     },errorMessage)


     //TODO Test the unique model by modify a key type from DATE to FLOAT
     errorMessage="errCode = 2, detailMessage = Float or double can not used as a key, use decimal instead."
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column login_time FLOAT KEY DEFAULT "0"  """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 1.0, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")
     },errorMessage)



     //TODO Test the unique model by modify a key type from DATE to DECIMAL
     errorMessage="errCode = 2, detailMessage = Can not change DATEV2 to DECIMAL32"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column login_time DECIMAL KEY DEFAULT "0"  """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 1.0, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")
     },errorMessage)


     //TODO Test the unique model by modify a key type from DATE to CHAR
     errorMessage="errCode = 2, detailMessage = Can not change DATEV2 to CHAR"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column login_time CHAR KEY DEFAULT "0"  """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', '1', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")

     },errorMessage)

     //TODO Test the unique model by modify a key type from DATE to STRING
     errorMessage="errCode = 2, detailMessage = String Type should not be used in key column[login_time]."
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column login_time STRING KEY DEFAULT "0"  """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', '1', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")
     },errorMessage)


     //TODO Test the unique model by modify a key type from DATE to VARCHAR
     errorMessage="errCode = 2, detailMessage = Can not change DATEV2 to VARCHAR"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column login_time VARCHAR(32) KEY DEFAULT "0"  """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', '1', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")
     },errorMessage)



     /**
      *  Test the unique model by modify a key type from DATETIME to other type
      */


     initTable = " CREATE TABLE IF NOT EXISTS ${tbName}\n" +
             "          (\n" +
             "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
             "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
             "              `login_time` DATETIME COMMENT \"用户登陆时间\",\n" +
             "              `is_teacher` BOOLEAN COMMENT \"是否是老师\",\n" +
             "              `city` VARCHAR(20) COMMENT \"用户所在城市\",\n" +
             "              `age` SMALLINT COMMENT \"用户年龄\",\n" +
             "              `sex` TINYINT COMMENT \"用户性别\",\n" +
             "              `phone` LARGEINT COMMENT \"用户电话\",\n" +
             "              `address` VARCHAR(500) COMMENT \"用户地址\",\n" +
             "              `register_time` DATETIME COMMENT \"用户注册时间\"\n" +
             "          )\n" +
             "          UNIQUE KEY(`user_id`, `username`, `login_time`)\n" +
             "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
             "          PROPERTIES (\n" +
             "          \"replication_allocation\" = \"tag.location.default: 1\",\n" +
             "          \"enable_unique_key_merge_on_write\" = \"true\"\n" +
             "          );"

     initTableData = "insert into ${tbName} values(123456789, 'Alice', 0, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01', '2022-01-01 10:00:00')," +
             "               (234567890, 'Bob', 0, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-01-01', '2022-02-02 12:00:00')," +
             "               (345678901, 'Carol', 1, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou','2022-01-01', '2022-03-03 14:00:00')," +
             "               (456789012, 'Dave', 0, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-01-01', '2022-04-04 16:00:00')," +
             "               (567890123, 'Eve', 0, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-01-01', '2022-05-05 18:00:00')," +
             "               (678901234, 'Frank', 1, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-01-01', '2022-06-06 20:00:00')," +
             "               (789012345, 'Grace', 0, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-01-01', '2022-07-07 22:00:00');"

     //TODO Test the unique model by modify a key type from DATETIME to BOOLEAN
     errorMessage="errCode = 2, detailMessage = Can not change DATEV2 to BOOLEAN"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column login_time BOOLEAN KEY DEFAULT "1"  """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 0, 1, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")
     },errorMessage)

     //TODO Test the unique model by modify a key type from DATETIME to TINYINT
     errorMessage="errCode = 2, detailMessage = Can not change DATEV2 to TINYINT"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column login_time TINYINT KEY DEFAULT "1"  """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 1, 1, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")
     },errorMessage)

     //TODO Test the unique model by modify a key type from DATETIME to SMALLINT
     errorMessage="errCode = 2, detailMessage = Can not change DATEV2 to SMALLINT"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column login_time SMALLINT KEY DEFAULT "1"  """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 1, 1, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")
     },errorMessage)


     //TODO Test the unique model by modify a key type from DATETIME to INT
     errorMessage="errCode = 2, detailMessage = Can not change DATEV2 to INT"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column login_time INT KEY DEFAULT "1"  """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 1, 1, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")
     },errorMessage)



     //TODO Test the unique model by modify a key type from DATETIME to BIGINT
     errorMessage="errCode = 2, detailMessage = Can not change DATEV2 to BIGINT"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column login_time BIGINT KEY DEFAULT "0"  """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 1, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")

     },errorMessage)


     //TODO Test the unique model by modify a key type from DATETIME to FLOAT
     errorMessage="errCode = 2, detailMessage = Float or double can not used as a key, use decimal instead."
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column login_time FLOAT KEY DEFAULT "0"  """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 1.0, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")
     },errorMessage)



     //TODO Test the unique model by modify a key type from DATETIME to DECIMAL
     errorMessage="errCode = 2, detailMessage = Can not change DATEV2 to DECIMAL32"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column login_time DECIMAL KEY DEFAULT "0"  """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 1.0, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")
     },errorMessage)


     //TODO Test the unique model by modify a key type from DATETIME to CHAR
     errorMessage="errCode = 2, detailMessage = Can not change DATEV2 to CHAR"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column login_time CHAR KEY DEFAULT "0"  """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', '1', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")

     },errorMessage)

     //TODO Test the unique model by modify a key type from DATETIME to STRING
     errorMessage="errCode = 2, detailMessage = String Type should not be used in key column[login_time]."
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column login_time STRING KEY DEFAULT "0"  """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', '1', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")
     },errorMessage)


     //TODO Test the unique model by modify a key type from DATETIME to VARCHAR
     errorMessage="errCode = 2, detailMessage = Can not change DATEV2 to VARCHAR"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column login_time VARCHAR(32) KEY DEFAULT "0"  """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', '1', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")
     },errorMessage)

     /**
      *  Test the unique model by modify a key type from CHAR to other type
      */


     initTable = " CREATE TABLE IF NOT EXISTS ${tbName}\n" +
             "          (\n" +
             "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
             "              `username` CHAR(255) NOT NULL COMMENT \"用户昵称\",\n" +
             "              `login_time` DATETIME COMMENT \"用户登陆时间\",\n" +
             "              `is_teacher` BOOLEAN COMMENT \"是否是老师\",\n" +
             "              `city` VARCHAR(20) COMMENT \"用户所在城市\",\n" +
             "              `age` SMALLINT COMMENT \"用户年龄\",\n" +
             "              `sex` TINYINT COMMENT \"用户性别\",\n" +
             "              `phone` LARGEINT COMMENT \"用户电话\",\n" +
             "              `address` VARCHAR(500) COMMENT \"用户地址\",\n" +
             "              `register_time` DATETIME COMMENT \"用户注册时间\"\n" +
             "          )\n" +
             "          UNIQUE KEY(`user_id`, `username`, `login_time`)\n" +
             "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
             "          PROPERTIES (\n" +
             "          \"replication_allocation\" = \"tag.location.default: 1\",\n" +
             "          \"enable_unique_key_merge_on_write\" = \"true\"\n" +
             "          );"

     initTableData = "insert into ${tbName} values(123456789, 'Alice', 0, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01', '2022-01-01 10:00:00')," +
             "               (234567890, 'Bob', 0, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-01-01', '2022-02-02 12:00:00')," +
             "               (345678901, 'Carol', 1, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou','2022-01-01', '2022-03-03 14:00:00')," +
             "               (456789012, 'Dave', 0, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-01-01', '2022-04-04 16:00:00')," +
             "               (567890123, 'Eve', 0, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-01-01', '2022-05-05 18:00:00')," +
             "               (678901234, 'Frank', 1, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-01-01', '2022-06-06 20:00:00')," +
             "               (789012345, 'Grace', 0, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-01-01', '2022-07-07 22:00:00');"

     //TODO Test the unique model by modify a key type from CHAR to BOOLEAN
     errorMessage="errCode = 2, detailMessage = Can not change VARCHAR to BOOLEAN"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column username BOOLEAN KEY DEFAULT "1"  """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 0, 1, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")
     },errorMessage)

     //TODO Test the unique model by modify a key type from CHAR to TINYINT
     errorMessage="errCode = 2, detailMessage = Can not change default value"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column username TINYINT KEY DEFAULT "1"  """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 1, 1, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")
     },errorMessage)

     //TODO Test the unique model by modify a key type from CHAR to SMALLINT
     errorMessage="errCode = 2, detailMessage = Can not change default value"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column username SMALLINT KEY DEFAULT "1"  """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 1, 1, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")
     },errorMessage)


     //TODO Test the unique model by modify a key type from CHAR to INT
     errorMessage="errCode = 2, detailMessage = Can not change default value"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column username INT KEY DEFAULT "1"  """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 1, 1, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")
     },errorMessage)



     //TODO Test the unique model by modify a key type from CHAR to BIGINT
     errorMessage="errCode = 2, detailMessage = Can not change default value"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column username BIGINT KEY DEFAULT "0"  """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 1, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")

     },errorMessage)


     //TODO Test the unique model by modify a key type from CHAR to FLOAT
     errorMessage="errCode = 2, detailMessage = Float or double can not used as a key, use decimal instead."
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column username FLOAT KEY DEFAULT "0"  """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 1.0, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")
     },errorMessage)



     //TODO Test the unique model by modify a key type from CHAR to DECIMAL
     errorMessage="errCode = 2, detailMessage = Can not change VARCHAR to DECIMAL32"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column username DECIMAL KEY DEFAULT "0"  """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 1.0, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")
     },errorMessage)


     //TODO Test the unique model by modify a key type from CHAR to  DATETIME
     errorMessage="errCode = 2, detailMessage = date literal [0] is invalid: null"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column username DATETIME KEY DEFAULT "0"  """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', '1', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")

     },errorMessage)

     //TODO Test the unique model by modify a key type from CHAR to STRING
     errorMessage="errCode = 2, detailMessage = String Type should not be used in key column[username]."
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column username STRING KEY DEFAULT "0"  """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', '1', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")
     },errorMessage)


     //TODO Test the unique model by modify a key type from CHAR to VARCHAR
     errorMessage="errCode = 2, detailMessage = Can not change default value"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column username VARCHAR(32) KEY DEFAULT "0"  """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', '1', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")
     },errorMessage)

}
