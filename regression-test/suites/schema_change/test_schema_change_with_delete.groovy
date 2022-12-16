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

// Test schema change for a table, the table has a delete predicate on string column
suite("test_schema_change_with_delete") {

    def tbName = "test_schema_change_with_delete"
    def getJobState = { tableName ->
          def jobStateResult = sql """  SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
          return jobStateResult[0][9]
     }

     sql """ DROP TABLE IF EXISTS ${tbName} FORCE"""
     // Create table and disable light weight schema change
     sql """
            CREATE TABLE IF NOT EXISTS ${tbName}
            (
                event_day int,
                siteid INT ,
                citycode int,
                username VARCHAR(32) DEFAULT ''
            )
            DUPLICATE  KEY(event_day,siteid)
            DISTRIBUTED BY HASH(event_day) BUCKETS 1
            PROPERTIES("replication_num" = "1", "light_schema_change" = "true"); 
         """
     sql """ insert into ${tbName} values(1, 1, 1, 'aaa');"""
     sql """ insert into ${tbName} values(2, 2, 2, 'bbb');"""
     sql """ delete from ${tbName} where username='aaa';"""
     sql """ insert into ${tbName} values(3, 3, 3, 'ccc');"""

    qt_sql """select * from ${tbName};"""

    // Change column type to string
    sql """ alter  table ${tbName} modify column citycode string """

    int max_try_time = 1000
    while(max_try_time--){
          String result = getJobState(tbName)
          if (result == "FINISHED") {
               break
          } else {
               sleep(100)
               if (max_try_time < 1){
                    assertEquals(1,2)
               }
          }
     }
    sql """ insert into ${tbName} values(4, 4, 'efg', 'ddd');"""
    qt_sql """select * from ${tbName};"""
    sql """ DROP TABLE  ${tbName} force"""
}
