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

suite("test_unique_model_schema_change","p0") {
     def tbName = "test_unique_model_schema_change"

     def getJobState = { tableName ->
          def jobStateResult = sql """  SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
          return jobStateResult[0][9]
     }

     //Test the unique model by adding a key column
     sql """ DROP TABLE IF EXISTS ${tbName} """
     sql """
        CREATE TABLE IF NOT EXISTS ${tbName}
          (
              `user_id` LARGEINT NOT NULL COMMENT "用户id",
              `username` VARCHAR(50) NOT NULL COMMENT "用户昵称",
              `city` VARCHAR(20) COMMENT "用户所在城市",
              `age` SMALLINT COMMENT "用户年龄",
              `sex` TINYINT COMMENT "用户性别",
              `phone` LARGEINT COMMENT "用户电话",
              `address` VARCHAR(500) COMMENT "用户地址",
              `register_time` DATETIME COMMENT "用户注册时间"
          )
          UNIQUE KEY(`user_id`, `username`)
          DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
          PROPERTIES (
          "replication_allocation" = "tag.location.default: 1"
          );
         """
     sql """ insert into ${tbName} values(123456789, 'Alice', 'Beijing', 25, 0, 13812345678, 'No. 123 Street, Beijing', '2022-01-01 10:00:00'),
               (234567890, 'Bob', 'Shanghai', 30, 1, 13998765432, 'No. 456 Street, Shanghai', '2022-02-02 12:00:00'),
               (345678901, 'Carol', 'Guangzhou', 28, 0, 13724681357, 'No. 789 Street, Guangzhou', '2022-03-03 14:00:00'),
               (456789012, 'Dave', 'Shenzhen', 35, 1, 13680864279, 'No. 987 Street, Shenzhen', '2022-04-04 16:00:00'),
               (567890123, 'Eve', 'Chengdu', 27, 0, 13572468091, 'No. 654 Street, Chengdu', '2022-05-05 18:00:00'),
               (678901234, 'Frank', 'Hangzhou', 32, 1, 13467985213, 'No. 321 Street, Hangzhou', '2022-06-06 20:00:00'),
               (789012345, 'Grace', 'Xian', 29, 0, 13333333333, 'No. 222 Street, Xian', '2022-07-07 22:00:00'); """
     sql """ alter  table ${tbName} add  column province VARCHAR(20) KEY DEFAULT "广东省" AFTER username """

     int max_try_time = 100
     while (max_try_time--){
          String result = getJobState(tbName)
          if (result == "FINISHED") {
               sleep(3000)
               break
          } else {
               sleep(2000)
               if (max_try_time < 1){
                    assertEquals(1,2)
               }
          }
     }
}
