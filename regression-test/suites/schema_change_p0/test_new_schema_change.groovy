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

suite("test_new_schema_change") {
     def tbName = "test_new_schema_change"

     def getJobState = { tableName ->
          def jobStateResult = sql """  SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
          return jobStateResult[0][9]
     }

     sql """ DROP TABLE IF EXISTS ${tbName} """
     sql """
            CREATE TABLE IF NOT EXISTS ${tbName}
            (
                event_day DATE,
                siteid INT DEFAULT '10',
                citycode bigint,
                username VARCHAR(32) DEFAULT '',
                pv BIGINT DEFAULT '0'
            )
            UNIQUE  KEY(event_day,siteid)
            DISTRIBUTED BY HASH(siteid) BUCKETS 5
            PROPERTIES("replication_num" = "1", "light_schema_change" = "true"); 
         """
          sql "begin"
          sql """ insert into ${tbName} values('2021-11-01',1,1,'用户A',1),('2021-11-01',1,1,'用户B',1),('2021-11-01',1,1,'用户A',3),('2021-11-02',1,1,'用户A',1),('2021-11-02',1,1,'用户B',1),('2021-11-02',101,112332121,'用户B',112312),('2021-11-02',103,112332211,'用户B',112312); """
          test {
               sql """ alter  table ${tbName} add column vv int after pv"""
               exception "This is in a transaction, only insert, update, delete, commit, rollback is acceptable."
          }
          sql "commit"
          sql """ DROP TABLE  ${tbName} """
}
