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

suite("test_unique_schema_key_change_modify","p0") {
     def tbName = "test_unique_schema_key_change_modify_1"
     def tbName2 = "test_unique_schema_key_change_modify_2"
     /**
      *  Test the unique model by modify a value type
      */

     sql """ DROP TABLE IF EXISTS ${tbName} """
     def getTableStatusSql = " SHOW ALTER TABLE COLUMN WHERE IndexName='${tbName}' ORDER BY createtime DESC LIMIT 1  "
     def initTable = " CREATE TABLE IF NOT EXISTS ${tbName}\n" +
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
             "          UNIQUE KEY(`user_id`, `username`, `is_teacher`)\n" +
             "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
             "          PROPERTIES (\n" +
             "          \"replication_allocation\" = \"tag.location.default: 1\",\n" +
             "          \"enable_unique_key_merge_on_write\" = \"true\"\n" +
             "          );"

     def initTableData = "insert into ${tbName} values(123456789, 'Alice', 0, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (234567890, 'Bob', 0, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
             "               (345678901, 'Carol', 1, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
             "               (456789012, 'Dave', 0, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
             "               (567890123, 'Eve', 0, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
             "               (678901234, 'Frank', 1, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (789012345, 'Grace', 0, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"

     //TODO Test the unique model by modify a key type from BOOLEAN to TINYINT
     def errorMessage="errCode = 2, detailMessage = Can not change BOOLEAN to TINYINT"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column is_teacher TINYINT KEY DEFAULT "0"  """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 1, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")
     },errorMessage)


     //TODO Test the unique model by modify a key type from BOOLEAN to SMALLINT
     errorMessage="errCode = 2, detailMessage = Can not change BOOLEAN to SMALLINT"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column is_teacher SMALLINT KEY DEFAULT "0"  """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 1, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")
     },errorMessage)


     //TODO Test the unique model by modify a key type from BOOLEAN to INT
     errorMessage="errCode = 2, detailMessage = Can not change BOOLEAN to INT"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column is_teacher INT KEY DEFAULT "0"  """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 1, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")
     },errorMessage)



     //TODO Test the unique model by modify a key type from BOOLEAN to BIGINT
     errorMessage="errCode = 2, detailMessage = Can not change BOOLEAN to BIGINT"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column is_teacher BIGINT KEY DEFAULT "0"  """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 1, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")

     },errorMessage)


     //TODO Test the unique model by modify a key type from BOOLEAN to FLOAT
     errorMessage="errCode = 2, detailMessage = Float or double can not used as a key, use decimal instead."
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column is_teacher FLOAT KEY DEFAULT "0"  """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 1.0, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")
     },errorMessage)



     //TODO Test the unique model by modify a key type from BOOLEAN to DECIMAL
     errorMessage="errCode = 2, detailMessage = Can not change BOOLEAN to DECIMAL32"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column is_teacher DECIMAL KEY DEFAULT "0"  """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 1.0, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")
     },errorMessage)


     //TODO Test the unique model by modify a key type from BOOLEAN to CHAR
     errorMessage="errCode = 2, detailMessage = Can not change BOOLEAN to CHAR"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column is_teacher CHAR KEY DEFAULT "0"  """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', '1', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")

     },errorMessage)

     //TODO Test the unique model by modify a key type from BOOLEAN to STRING
     errorMessage="errCode = 2, detailMessage = String Type should not be used in key column[is_teacher]."
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column is_teacher STRING KEY DEFAULT "0"  """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', '1', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")
     },errorMessage)


     //TODO Test the unique model by modify a key type from BOOLEAN to VARCHAR
     errorMessage="errCode = 2, detailMessage = Can not change BOOLEAN to VARCHAR"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column is_teacher VARCHAR(32) KEY DEFAULT "0"  """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', '1', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")
     },errorMessage)


     /**
      *  Test the unique model by modify a key type from TINYINT to other type
      */
     sql """ DROP TABLE IF EXISTS ${tbName} """
     initTable = " CREATE TABLE IF NOT EXISTS ${tbName}\n" +
             "          (\n" +
             "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
             "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
             "              `is_student` TINYINT COMMENT \"是否是学生\",\n" +
             "              `city` VARCHAR(20) COMMENT \"用户所在城市\",\n" +
             "              `age` SMALLINT COMMENT \"用户年龄\",\n" +
             "              `sex` TINYINT COMMENT \"用户性别\",\n" +
             "              `phone` LARGEINT COMMENT \"用户电话\",\n" +
             "              `address` VARCHAR(500) COMMENT \"用户地址\",\n" +
             "              `register_time` DATETIME COMMENT \"用户注册时间\"\n" +
             "          )\n" +
             "          UNIQUE KEY(`user_id`, `username`, `is_student`)\n" +
             "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
             "          PROPERTIES (\n" +
             "          \"replication_allocation\" = \"tag.location.default: 1\",\n" +
             "          \"enable_unique_key_merge_on_write\" = \"true\"\n" +
             "          );"

     initTableData = "insert into ${tbName} values(123456789, 'Alice', 1, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (234567890, 'Bob', 1, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
             "               (345678901, 'Carol', 1, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
             "               (456789012, 'Dave', 1, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
             "               (567890123, 'Eve', 0, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
             "               (678901234, 'Frank', 0, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (789012345, 'Grace', 1, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"

     //TODO Test the unique model by modify a key type from TINYINT  to BOOLEAN
     errorMessage="errCode = 2, detailMessage = Can not change TINYINT to BOOLEAN"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column is_student BOOLEAN  key  """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', false, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")
     },errorMessage)




     // Test the unique model by modify a key type from TINYINT  to SMALLINT
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName} MODIFY  column is_student SMALLINT key """
     insertSql = "insert into ${tbName} values(123456689, 'Alice', 2, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 60
     }, insertSql, true,"${tbName}")


     //Test the unique model by modify a key type from TINYINT  to INT
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName} MODIFY  column is_student INT key """
     insertSql = "insert into ${tbName} values(123456689, 'Alice', 2, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 60
     }, insertSql, true,"${tbName}")


     //Test the unique model by modify a key type from TINYINT  to BIGINT
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName} MODIFY  column is_student BIGINT key """
     insertSql = "insert into ${tbName} values(123456689, 'Alice', 3, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 60
     }, insertSql, true,"${tbName}")

     //Test the unique model by modify a key type from TINYINT  to LARGEINT
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName} MODIFY  column is_student LARGEINT key """
     insertSql = "insert into ${tbName} values(123456689, 'Alice', 1, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 60
     }, insertSql, true,"${tbName}")


     //TODO Test the unique model by modify a key type from TINYINT  to FLOAT
     errorMessage="errCode = 2, detailMessage = Float or double can not used as a key, use decimal instead."
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column is_student FLOAT key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 1.2, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")
     },errorMessage)


     //TODO Test the unique model by modify a key type from TINYINT  to DOUBLE
     errorMessage="errCode = 2, detailMessage = Float or double can not used as a key, use decimal instead."
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column is_student DOUBLE key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 1.23, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")
     },errorMessage)



     //TODO Test the unique model by modify a key type from TINYINT  to DECIMAL
     errorMessage="errCode = 2, detailMessage = Can not change TINYINT to DECIMAL32"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column is_student DECIMAL key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 1.23, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")

     },errorMessage)

     //TODO Test the unique model by modify a key type from TINYINT  to CHAR
     errorMessage="errCode = 2, detailMessage = Can not change TINYINT to CHAR"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column is_student CHAR(15) key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")
     },errorMessage)




     //Test the unique model by modify a key type from TINYINT  to VARCHAR
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName} MODIFY  column is_student VARCHAR(100) key """
     insertSql = "insert into ${tbName} values(123456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 60
     }, insertSql, true,"${tbName}")


     //Test the unique model by modify a key type from TINYINT  to STRING
     errorMessage="errCode = 2, detailMessage = String Type should not be used in key column[is_student]."
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column is_student STRING key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")
     },errorMessage)


     /**
      *  Test the unique model by modify a key type from SMALLINT to other type
      */
     sql """ DROP TABLE IF EXISTS ${tbName} """
     initTable = " CREATE TABLE IF NOT EXISTS ${tbName}\n" +
             "          (\n" +
             "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
             "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
             "              `car_number` SMALLINT COMMENT \"市民卡\",\n" +
             "              `city` VARCHAR(20) COMMENT \"用户所在城市\",\n" +
             "              `age` SMALLINT COMMENT \"用户年龄\",\n" +
             "              `sex` TINYINT COMMENT \"用户性别\",\n" +
             "              `phone` LARGEINT COMMENT \"用户电话\",\n" +
             "              `address` VARCHAR(500) COMMENT \"用户地址\",\n" +
             "              `register_time` DATETIME COMMENT \"用户注册时间\"\n" +
             "          )\n" +
             "          UNIQUE KEY(`user_id`, `username`, `car_number`)\n" +
             "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
             "          PROPERTIES (\n" +
             "          \"replication_allocation\" = \"tag.location.default: 1\",\n" +
             "          \"enable_unique_key_merge_on_write\" = \"true\"\n" +
             "          );"

     initTableData = "insert into ${tbName} values(123456789, 'Alice', 13243, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (234567890, 'Bob', 13445, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
             "               (345678901, 'Carol', 15768, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
             "               (456789012, 'Dave', 14243, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
             "               (567890123, 'Eve', 10768, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
             "               (678901234, 'Frank', 14325, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (789012345, 'Grace', 15686, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"

     //TODO Test the unique model by modify a key type from SMALLINT  to BOOLEAN
     errorMessage="errCode = 2, detailMessage = Can not change SMALLINT to BOOLEAN"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column car_number BOOLEAN  key  """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', false, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")
     },errorMessage)




     // TODO Test the unique model by modify a key type from SMALLINT  to TINYINT
     errorMessage="errCode = 2, detailMessage = Can not change SMALLINT to TINYINT"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column car_number TINYINT key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 2, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")
     },errorMessage)



     //Test the unique model by modify a key type from SMALLINT  to INT
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName} MODIFY  column car_number INT key """
     insertSql = "insert into ${tbName} values(123456689, 'Alice', 3, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 60
     }, insertSql, true,"${tbName}")


     //Test the unique model by modify a key type from SMALLINT  to BIGINT
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName} MODIFY  column car_number BIGINT key """
     insertSql = "insert into ${tbName} values(123456689, 'Alice', 4, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 60
     }, insertSql, true,"${tbName}")

     //Test the unique model by modify a key type from SMALLINT  to LARGEINT
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName} MODIFY  column car_number LARGEINT key """
     insertSql = "insert into ${tbName} values(123456689, 'Alice', 5, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 60
     }, insertSql, true,"${tbName}")


     //TODO Test the unique model by modify a key type from SMALLINT  to FLOAT
     errorMessage="errCode = 2, detailMessage = Float or double can not used as a key, use decimal instead."
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column car_number FLOAT key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 1.2, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")
     },errorMessage)


     //TODO Test the unique model by modify a key type from SMALLINT  to DOUBLE
     errorMessage="errCode = 2, detailMessage = Float or double can not used as a key, use decimal instead."
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column car_number DOUBLE key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 1.23, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")
     },errorMessage)



     //TODO Test the unique model by modify a key type from SMALLINT  to DECIMAL
     errorMessage="errCode = 2, detailMessage = Can not change SMALLINT to DECIMAL32"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column car_number DECIMAL key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 1.23, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")

     },errorMessage)

     //TODO Test the unique model by modify a key type from SMALLINT  to CHAR
     errorMessage="errCode = 2, detailMessage = Can not change SMALLINT to CHAR"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column car_number CHAR(15) key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")
     },errorMessage)




     //Test the unique model by modify a key type from SMALLINT  to VARCHAR
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName} MODIFY  column car_number VARCHAR(100) key """
     insertSql = "insert into ${tbName} values(123456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 60
     }, insertSql, true,"${tbName}")


     //Test the unique model by modify a key type from SMALLINT  to STRING
     errorMessage="errCode = 2, detailMessage = String Type should not be used in key column[car_number]."
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column car_number STRING key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true,"${tbName}")
     },errorMessage)




     /**
      *  Test the unique model by modify a key type from BIGINT to other type
      */
     sql """ DROP TABLE IF EXISTS ${tbName} """
     initTable = " CREATE TABLE IF NOT EXISTS ${tbName}\n" +
             "          (\n" +
             "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
             "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
             "              `sn_number` BIGINT COMMENT \"sn卡\",\n" +
             "              `city` VARCHAR(20) COMMENT \"用户所在城市\",\n" +
             "              `age` SMALLINT COMMENT \"用户年龄\",\n" +
             "              `sex` TINYINT COMMENT \"用户性别\",\n" +
             "              `phone` LARGEINT COMMENT \"用户电话\",\n" +
             "              `address` VARCHAR(500) COMMENT \"用户地址\",\n" +
             "              `register_time` DATETIME COMMENT \"用户注册时间\"\n" +
             "          )\n" +
             "          UNIQUE KEY(`user_id`, `username`,`sn_number`)\n" +
             "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
             "          PROPERTIES (\n" +
             "          \"replication_allocation\" = \"tag.location.default: 1\",\n" +
             "          \"enable_unique_key_merge_on_write\" = \"true\"\n" +
             "          );"

     initTableData = "insert into ${tbName} values(123456789, 'Alice', 2147483641, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (234567890, 'Bob', 214748364, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
             "               (345678901, 'Carol', 2147483441, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
             "               (456789012, 'Dave', 2147483141, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
             "               (567890123, 'Eve', 2127483141, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
             "               (678901234, 'Frank', 2124483141, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (789012345, 'Grace', 2123483141, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"

     //TODO Test the unique model by modify a key type from BIGINT  to BOOLEAN
     errorMessage = "errCode = 2, detailMessage = Can not change BIGINT to BOOLEAN"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column sn_number BOOLEAN  key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', false, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")
     }, errorMessage)


     // TODO Test the unique model by modify a key type from BIGINT  to TINYINT
     errorMessage = "errCode = 2, detailMessage = Can not change BIGINT to TINYINT"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column sn_number TINYINT key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 2, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")
     }, errorMessage)


     //Test the unique model by modify a key type from BIGINT  to SMALLINT
     errorMessage = "errCode = 2, detailMessage = Can not change BIGINT to SMALLINT"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column sn_number SMALLINT key  """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 3, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")
     }, errorMessage)

     //Test the unique model by modify a key type from BIGINT  to INT
     errorMessage = "errCode = 2, detailMessage = Can not change BIGINT to INT"
     expectException({
          sql initTable
          sql initTableData

          sql """ alter  table ${tbName} MODIFY  column sn_number INT key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 4, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")
     }, errorMessage)


     //Test the unique model by modify a key type from BIGINT  to LARGEINT
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName} MODIFY  column sn_number LARGEINT key """
     insertSql = "insert into ${tbName} values(123456689, 'Alice', 5, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 60
     }, insertSql, true, "${tbName}")

     //Test the unique model by modify a key type from INT  to FLOAT
     errorMessage = "errCode = 2, detailMessage = Float or double can not used as a key, use decimal instead."
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column sn_number FLOAT key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 1.2, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")
     }, errorMessage)


     //Test the unique model by modify a key type from INT  to DOUBLE
     errorMessage = "errCode = 2, detailMessage = Float or double can not used as a key, use decimal instead."
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column sn_number DOUBLE key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 1.23, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")
     }, errorMessage)


     //TODO Test the unique model by modify a key type from BIGINT  to DECIMAL
     errorMessage = "errCode = 2, detailMessage = Can not change BIGINT to DECIMAL128"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column sn_number DECIMAL(38,0) key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 1.23, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")

     }, errorMessage)

     //TODO Test the unique model by modify a  key type from BIGINT  to CHAR
     errorMessage = "errCode = 2, detailMessage = Can not change BIGINT to CHAR"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column sn_number CHAR(15) key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")
     }, errorMessage)


     //Test the unique model by modify a key type from BIGINT  to VARCHAR
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName} MODIFY  column sn_number VARCHAR(100) key """
     insertSql = "insert into ${tbName} values(123456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 60
     }, insertSql, true, "${tbName}")

     //Test the unique model by modify a key type from BIGINT  to VARCHAR
     errorMessage = "errCode = 2, detailMessage = Can not change from wider type BIGINT to narrower type VARCHAR(2)"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column sn_number VARCHAR(2) key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")
     }, errorMessage)

     //Test the unique model by modify a key type from BIGINT  to STRING
     errorMessage = "errCode = 2, detailMessage = String Type should not be used in key column[sn_number]."
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column sn_number STRING key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")
     }, errorMessage)


     /**
      *  Test the unique model by modify a key type from LARGEINT to other type
      */
     sql """ DROP TABLE IF EXISTS ${tbName} """
     initTable = " CREATE TABLE IF NOT EXISTS ${tbName}\n" +
             "          (\n" +
             "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
             "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
             "              `sn_number` LARGEINT COMMENT \"sn卡\",\n" +
             "              `city` VARCHAR(20) COMMENT \"用户所在城市\",\n" +
             "              `age` SMALLINT COMMENT \"用户年龄\",\n" +
             "              `sex` TINYINT COMMENT \"用户性别\",\n" +
             "              `phone` LARGEINT COMMENT \"用户电话\",\n" +
             "              `address` VARCHAR(500) COMMENT \"用户地址\",\n" +
             "              `register_time` DATETIME COMMENT \"用户注册时间\"\n" +
             "          )\n" +
             "          UNIQUE KEY(`user_id`, `username`,`sn_number`)\n" +
             "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
             "          PROPERTIES (\n" +
             "          \"replication_allocation\" = \"tag.location.default: 1\",\n" +
             "          \"enable_unique_key_merge_on_write\" = \"true\"\n" +
             "          );"

     initTableData = "insert into ${tbName} values(123456789, 'Alice', 2147483641, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (234567890, 'Bob', 214748364, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
             "               (345678901, 'Carol', 2147483441, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
             "               (456789012, 'Dave', 2147483141, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
             "               (567890123, 'Eve', 2127483141, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
             "               (678901234, 'Frank', 2124483141, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (789012345, 'Grace', 2123483141, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"

     //TODO Test the unique model by modify a key type from LARGEINT  to BOOLEAN
     errorMessage = "errCode = 2, detailMessage = Can not change LARGEINT to BOOLEAN"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column sn_number BOOLEAN  key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', false, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")
     }, errorMessage)


     // TODO Test the unique model by modify a key type from LARGEINT  to TINYINT
     errorMessage = "errCode = 2, detailMessage = Can not change LARGEINT to TINYINT"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column sn_number TINYINT key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 2, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")
     }, errorMessage)


     //Test the unique model by modify a key type from LARGEINT  to SMALLINT
     errorMessage = "errCode = 2, detailMessage = Can not change LARGEINT to SMALLINT"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column sn_number SMALLINT key  """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 3, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")
     }, errorMessage)

     //Test the unique model by modify a key type from LARGEINT  to INT
     errorMessage = "errCode = 2, detailMessage = Can not change LARGEINT to INT"
     expectException({
          sql initTable
          sql initTableData

          sql """ alter  table ${tbName} MODIFY  column sn_number INT key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 4, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")
     }, errorMessage)


     //Test the unique model by modify a key type from LARGEINT  to BIGINT
     errorMessage = "errCode = 2, detailMessage = Can not change LARGEINT to BIGINT"
     expectException({
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName} MODIFY  column sn_number BIGINT key """
     insertSql = "insert into ${tbName} values(123456689, 'Alice', 5, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 60
     }, insertSql, true, "${tbName}")
     }, errorMessage)

     //Test the unique model by modify a key type from LARGEINT  to FLOAT
     errorMessage = "errCode = 2, detailMessage = Float or double can not used as a key, use decimal instead."
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column sn_number FLOAT key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 1.2, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")
     }, errorMessage)


     //Test the unique model by modify a key type from LARGEINT  to DOUBLE
     errorMessage = "errCode = 2, detailMessage = Float or double can not used as a key, use decimal instead."
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column sn_number DOUBLE key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 1.23, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")
     }, errorMessage)


     //TODO Test the unique model by modify a key type from LARGEINT  to DECIMAL
     errorMessage = "errCode = 2, detailMessage = Can not change LARGEINT to DECIMAL128"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column sn_number DECIMAL(38,0) key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 1.23, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")

     }, errorMessage)

     //TODO Test the unique model by modify a  key type from LARGEINT  to CHAR
     errorMessage = "errCode = 2, detailMessage = Can not change LARGEINT to CHAR"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column sn_number CHAR(15) key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")
     }, errorMessage)


     //Test the unique model by modify a key type from LARGEINT  to VARCHAR
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName} MODIFY  column sn_number VARCHAR(100) key """
     insertSql = "insert into ${tbName} values(123456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 60
     }, insertSql, false, "${tbName}")

     sql """ DROP TABLE IF EXISTS ${tbName2} """
     initTable2 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
             "          (\n" +
             "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
             "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
             "              `sn_number` VARCHAR(100) COMMENT \"sn卡\",\n" +
             "              `city` VARCHAR(20) COMMENT \"用户所在城市\",\n" +
             "              `age` SMALLINT COMMENT \"用户年龄\",\n" +
             "              `sex` TINYINT COMMENT \"用户性别\",\n" +
             "              `phone` LARGEINT COMMENT \"用户电话\",\n" +
             "              `address` VARCHAR(500) COMMENT \"用户地址\",\n" +
             "              `register_time` DATETIME COMMENT \"用户注册时间\"\n" +
             "          )\n" +
             "          UNIQUE KEY(`user_id`, `username`,`sn_number`)\n" +
             "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
             "          PROPERTIES (\n" +
             "          \"replication_allocation\" = \"tag.location.default: 1\",\n" +
             "          \"enable_unique_key_merge_on_write\" = \"true\"\n" +
             "          );"

     initTableData2 = "insert into ${tbName2} values(123456789, 'Alice', 2147483641, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (234567890, 'Bob', 214748364, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
             "               (345678901, 'Carol', 2147483441, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
             "               (456789012, 'Dave', 2147483141, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
             "               (567890123, 'Eve', 2127483141, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
             "               (678901234, 'Frank', 2124483141, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (123456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (789012345, 'Grace', 2123483141, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"

     sql initTable2
     sql initTableData2
     checkTableData("${tbName}","${tbName2}","sn_number")
     sql """ DROP TABLE IF EXISTS ${tbName} """


     //TODO Test the unique model by modify a  key type from LARGEINT  to VARCHAR
     //Test the unique model by modify a key type from LARGEINT  to VARCHAR
     errorMessage = "errCode = 2, detailMessage = Can not change from wider type LARGEINT to narrower type VARCHAR(2)"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column sn_number VARCHAR(2) key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")
     }, errorMessage)

     //TODO Test the unique model by modify a  key type from LARGEINT  to STRING
     //Test the unique model by modify a key type from LARGEINT  to STRING
     errorMessage = "errCode = 2, detailMessage = String Type should not be used in key column[sn_number]."
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column sn_number STRING key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")
     }, errorMessage)

     /**
      *  Test the unique model by modify a key type from FLOAT to other type
      */
     sql """ DROP TABLE IF EXISTS ${tbName} """
     initTable = " CREATE TABLE IF NOT EXISTS ${tbName}\n" +
             "          (\n" +
             "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
             "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
             "              `score` FLOAT COMMENT \"分数\",\n" +
             "              `city` VARCHAR(20) COMMENT \"用户所在城市\",\n" +
             "              `age` SMALLINT COMMENT \"用户年龄\",\n" +
             "              `sex` TINYINT COMMENT \"用户性别\",\n" +
             "              `phone` LARGEINT COMMENT \"用户电话\",\n" +
             "              `address` VARCHAR(500) COMMENT \"用户地址\",\n" +
             "              `register_time` DATETIME COMMENT \"用户注册时间\"\n" +
             "          )\n" +
             "          UNIQUE KEY(`user_id`, `username`,`score`)\n" +
             "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
             "          PROPERTIES (\n" +
             "          \"replication_allocation\" = \"tag.location.default: 1\",\n" +
             "          \"enable_unique_key_merge_on_write\" = \"true\"\n" +
             "          );"

     initTableData = "insert into ${tbName} values(123456789, 'Alice', 1.8, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (234567890, 'Bob', 1.89, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
             "               (345678901, 'Carol', 2.6, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
             "               (456789012, 'Dave', 3.9, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
             "               (567890123, 'Eve', 4.2, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
             "               (678901234, 'Frank', 2.5, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (789012345, 'Grace', 2.1, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"

     //TODO Test the unique model by modify a key type from FLOAT  to BOOLEAN
     errorMessage = "errCode = 2, detailMessage = Float or double can not used as a key, use decimal instead."
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column score BOOLEAN  key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', false, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")
     }, errorMessage)


     // TODO Test the unique model by modify a key type from FLOAT  to TINYINT
     errorMessage = "errCode = 2, detailMessage = Float or double can not used as a key, use decimal instead."
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column score TINYINT key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 2, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")
     }, errorMessage)


     //Test the unique model by modify a key type from FLOAT  to SMALLINT
     errorMessage = "errCode = 2, detailMessage = Float or double can not used as a key, use decimal instead."
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column score SMALLINT key  """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 3, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")
     }, errorMessage)

     //Test the unique model by modify a key type from FLOAT  to INT
     errorMessage = "errCode = 2, detailMessage = Float or double can not used as a key, use decimal instead."
     expectException({
          sql initTable
          sql initTableData

          sql """ alter  table ${tbName} MODIFY  column score INT key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 4, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")
     }, errorMessage)


     //Test the unique model by modify a key type from FLOAT  to BIGINT
     errorMessage = "errCode = 2, detailMessage = Float or double can not used as a key, use decimal instead."
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column score BIGINT key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 545645, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")
     }, errorMessage)

     //Test the unique model by modify a key type from  FLOAT to LARGEINT
     errorMessage = "errCode = 2, detailMessage = Float or double can not used as a key, use decimal instead."
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column score FLOAT key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 156546, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")
     }, errorMessage)


     //Test the unique model by modify a key type from FLOAT  to DOUBLE
     errorMessage = "errCode = 2, detailMessage = Float or double can not used as a key, use decimal instead."
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column score DOUBLE key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 1.23, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")
     }, errorMessage)


     //TODO Test the unique model by modify a key type from FLOAT  to DECIMAL
     errorMessage = "errCode = 2, detailMessage = Float or double can not used as a key, use decimal instead."
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column score DECIMAL(38,0) key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 1.23, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")

     }, errorMessage)

     //TODO Test the unique model by modify a  key type from FLOAT  to CHAR
     errorMessage = "errCode = 2, detailMessage = Float or double can not used as a key, use decimal instead."
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column score CHAR(15) key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")
     }, errorMessage)

     //TODO Test the unique model by modify a  key type from FLOAT  to VARCHAR
     //Test the unique model by modify a key type from FLOAT  to VARCHAR
     errorMessage = "errCode = 2, detailMessage = Float or double can not used as a key, use decimal instead."
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column score VARCHAR(100) key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")
     }, errorMessage)

     //TODO Test the unique model by modify a  key type from FLOAT  to STRING
     //Test the unique model by modify a key type from FLOAT  to STRING
     errorMessage = "errCode = 2, detailMessage = Float or double can not used as a key, use decimal instead."
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column score STRING key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")
     }, errorMessage)


     /**
      *  Test the unique model by modify a key type from DOUBLE to other type
      */
     sql """ DROP TABLE IF EXISTS ${tbName} """
     initTable = " CREATE TABLE IF NOT EXISTS ${tbName}\n" +
             "          (\n" +
             "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
             "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
             "              `score` DOUBLE COMMENT \"分数\",\n" +
             "              `city` VARCHAR(20) COMMENT \"用户所在城市\",\n" +
             "              `age` SMALLINT COMMENT \"用户年龄\",\n" +
             "              `sex` TINYINT COMMENT \"用户性别\",\n" +
             "              `phone` LARGEINT COMMENT \"用户电话\",\n" +
             "              `address` VARCHAR(500) COMMENT \"用户地址\",\n" +
             "              `register_time` DATETIME COMMENT \"用户注册时间\"\n" +
             "          )\n" +
             "          UNIQUE KEY(`user_id`, `username`,`score`)\n" +
             "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
             "          PROPERTIES (\n" +
             "          \"replication_allocation\" = \"tag.location.default: 1\",\n" +
             "          \"enable_unique_key_merge_on_write\" = \"true\"\n" +
             "          );"

     initTableData = "insert into ${tbName} values(123456789, 'Alice', 1.8, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (234567890, 'Bob', 1.89, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
             "               (345678901, 'Carol', 2.6, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
             "               (456789012, 'Dave', 3.9, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
             "               (567890123, 'Eve', 4.2, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
             "               (678901234, 'Frank', 2.5, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (789012345, 'Grace', 2.1, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"

     //Test the unique model by modify a key type from DOUBLE  to BOOLEAN
     errorMessage = "errCode = 2, detailMessage = Float or double can not used as a key, use decimal instead."
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column score BOOLEAN  key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', false, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")
     }, errorMessage)


     // Test the unique model by modify a key type from DOUBLE  to TINYINT
     errorMessage = "errCode = 2, detailMessage = Float or double can not used as a key, use decimal instead."
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column score TINYINT key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 2, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")
     }, errorMessage)


     //Test the unique model by modify a key type from DOUBLE  to SMALLINT
     errorMessage = "errCode = 2, detailMessage = Float or double can not used as a key, use decimal instead."
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column score SMALLINT key  """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 3, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")
     }, errorMessage)

     //Test the unique model by modify a key type from DOUBLE  to INT
     errorMessage = "errCode = 2, detailMessage = Float or double can not used as a key, use decimal instead."
     expectException({
          sql initTable
          sql initTableData

          sql """ alter  table ${tbName} MODIFY  column score INT key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 4, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")
     }, errorMessage)


     //Test the unique model by modify a key type from DOUBLE  to BIGINT
     errorMessage = "errCode = 2, detailMessage = Float or double can not used as a key, use decimal instead."
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column score BIGINT key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 545645, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")
     }, errorMessage)

     //Test the unique model by modify a key type from  DOUBLE to LARGEINT
     errorMessage = "errCode = 2, detailMessage = Float or double can not used as a key, use decimal instead."
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column score FLOAT key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 156546, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")
     }, errorMessage)


     //Test the unique model by modify a key type from DOUBLE to  FLOAT
     errorMessage = "errCode = 2, detailMessage = Float or double can not used as a key, use decimal instead."
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column score FLOAT key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 1.23, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")
     }, errorMessage)


     // Test the unique model by modify a key type from DOUBLE  to DECIMAL
     errorMessage = "errCode = 2, detailMessage = Float or double can not used as a key, use decimal instead."
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column score DECIMAL(38,0) key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 1.23, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")

     }, errorMessage)

     //TODO Test the unique model by modify a  key type from DOUBLE  to CHAR
     errorMessage = "errCode = 2, detailMessage = Float or double can not used as a key, use decimal instead."
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column score CHAR(15) key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")
     }, errorMessage)

     //TODO Test the unique model by modify a  key type from DOUBLE  to VARCHAR
     //Test the unique model by modify a key type from FLOAT  to VARCHAR
     errorMessage = "errCode = 2, detailMessage = Float or double can not used as a key, use decimal instead."
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column score VARCHAR(100) key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")
     }, errorMessage)

     //Test the unique model by modify a key type from DOUBLE  to STRING
     errorMessage = "errCode = 2, detailMessage = Float or double can not used as a key, use decimal instead."
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column score STRING key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")
     }, errorMessage)

     /**
      *  Test the unique model by modify a key type from DECIMAL to other type
      */
     sql """ DROP TABLE IF EXISTS ${tbName} """
     initTable = " CREATE TABLE IF NOT EXISTS ${tbName}\n" +
             "          (\n" +
             "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
             "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
             "              `rice` DECIMAL(38,10) COMMENT \"米粒\",\n" +
             "              `city` VARCHAR(20) COMMENT \"用户所在城市\",\n" +
             "              `age` SMALLINT COMMENT \"用户年龄\",\n" +
             "              `sex` TINYINT COMMENT \"用户性别\",\n" +
             "              `phone` LARGEINT COMMENT \"用户电话\",\n" +
             "              `address` VARCHAR(500) COMMENT \"用户地址\",\n" +
             "              `register_time` DATETIME COMMENT \"用户注册时间\"\n" +
             "          )\n" +
             "          UNIQUE KEY(`user_id`, `username`,`rice`)\n" +
             "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
             "          PROPERTIES (\n" +
             "          \"replication_allocation\" = \"tag.location.default: 1\",\n" +
             "          \"enable_unique_key_merge_on_write\" = \"true\"\n" +
             "          );"

     initTableData = "insert into ${tbName} values(123456789, 'Alice', 1.8, 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (234567890, 'Bob', 1.89, 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
             "               (345678901, 'Carol', 2.6, 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
             "               (456789012, 'Dave', 3.9, 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
             "               (567890123, 'Eve', 4.2, 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
             "               (678901234, 'Frank', 2.5, 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (789012345, 'Grace', 2.1, 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"

     //Test the unique model by modify a key type from DECIMAL  to BOOLEAN
     errorMessage = "errCode = 2, detailMessage = Can not change DECIMAL128 to BOOLEAN"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column rice BOOLEAN  key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', false, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")
     }, errorMessage)


     // Test the unique model by modify a key type from DECIMAL  to TINYINT
     errorMessage = "errCode = 2, detailMessage = Can not change DECIMAL128 to TINYINT"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column rice TINYINT key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 2, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")
     }, errorMessage)


     //Test the unique model by modify a key type from DECIMAL  to SMALLINT
     errorMessage = "errCode = 2, detailMessage = Can not change DECIMAL128 to SMALLINT"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column rice SMALLINT key  """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 3, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")
     }, errorMessage)

     //Test the unique model by modify a key type from DECIMAL  to INT
     errorMessage = "errCode = 2, detailMessage = Can not change DECIMAL128 to INT"
     expectException({
          sql initTable
          sql initTableData

          sql """ alter  table ${tbName} MODIFY  column rice INT key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 4, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")
     }, errorMessage)


     //Test the unique model by modify a key type from DECIMAL  to BIGINT
     errorMessage = "errCode = 2, detailMessage = Can not change DECIMAL128 to BIGINT"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column rice BIGINT key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 545645, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")
     }, errorMessage)

     //Test the unique model by modify a key type from  DECIMAL to LARGEINT
     errorMessage = "errCode = 2, detailMessage = Can not change DECIMAL128 to LARGEINT"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column rice LARGEINT key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 156546, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")
     }, errorMessage)


     //Test the unique model by modify a key type from DECIMAL to  FLOAT
     errorMessage = "errCode = 2, detailMessage = Float or double can not used as a key, use decimal instead."
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column rice FLOAT key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 1.23, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")
     }, errorMessage)


     // Test the unique model by modify a key type from DECIMAL  to DOUBLE
     errorMessage = "errCode = 2, detailMessage = Float or double can not used as a key, use decimal instead."
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column rice DOUBLE key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 1.23, 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")

     }, errorMessage)

     //Test the unique model by modify a  key type from DECIMAL  to CHAR
     errorMessage = "errCode = 2, detailMessage = Can not change DECIMAL128 to CHAR"
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column rice CHAR(15) key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")
     }, errorMessage)

     //Test the unique model by modify a key type from DECIMAL  to VARCHAR
     sql initTable
     sql initTableData
     sql """ alter  table ${tbName} MODIFY  column rice VARCHAR(100) key """
     insertSql = "insert into ${tbName} values(123456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
     waitForSchemaChangeDone({
          sql getTableStatusSql
          time 60
     }, insertSql, false, "${tbName}")

     sql """ DROP TABLE IF EXISTS ${tbName2} """
     initTable2 = " CREATE TABLE IF NOT EXISTS ${tbName2}\n" +
             "          (\n" +
             "              `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
             "              `username` VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n" +
             "              `rice` VARCHAR(100) COMMENT \"米粒\",\n" +
             "              `city` VARCHAR(20) COMMENT \"用户所在城市\",\n" +
             "              `age` SMALLINT COMMENT \"用户年龄\",\n" +
             "              `sex` TINYINT COMMENT \"用户性别\",\n" +
             "              `phone` LARGEINT COMMENT \"用户电话\",\n" +
             "              `address` VARCHAR(500) COMMENT \"用户地址\",\n" +
             "              `register_time` DATETIME COMMENT \"用户注册时间\"\n" +
             "          )\n" +
             "          UNIQUE KEY(`user_id`, `username`,`rice`)\n" +
             "          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1\n" +
             "          PROPERTIES (\n" +
             "          \"replication_allocation\" = \"tag.location.default: 1\",\n" +
             "          \"enable_unique_key_merge_on_write\" = \"true\"\n" +
             "          );"

     initTableData2 = "insert into ${tbName2} values(123456789, 'Alice', '1.8000000000', 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (234567890, 'Bob', '1.8900000000', 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00')," +
             "               (345678901, 'Carol', '2.6000000000', 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00')," +
             "               (456789012, 'Dave', '3.9000000000', 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00')," +
             "               (567890123, 'Eve', '4.2000000000', 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00')," +
             "               (678901234, 'Frank', '2.5000000000', 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00')," +
             "               (123456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00')," +
             "               (789012345, 'Grace', '2.1000000000', 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00');"

     sql initTable2
     sql initTableData2
     checkTableData("${tbName}","${tbName2}","rice")
     sql """ DROP TABLE IF EXISTS ${tbName} """

     //Test the unique model by modify a key type from DECIMAL  to STRING
     errorMessage = "errCode = 2, detailMessage = String Type should not be used in key column[rice]."
     expectException({
          sql initTable
          sql initTableData
          sql """ alter  table ${tbName} MODIFY  column rice STRING key """
          insertSql = "insert into ${tbName} values(123456689, 'Alice', 'asd', 'Yaan', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'); "
          waitForSchemaChangeDone({
               sql getTableStatusSql
               time 60
          }, insertSql, true, "${tbName}")
     }, errorMessage)

}
