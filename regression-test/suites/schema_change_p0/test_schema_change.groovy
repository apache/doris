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

suite("test_schema_change") {
     // todo: test alter table schema change, such as add/drop/modify/order column
     def tbName = "alter_table_column_type"

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
            PARTITION BY RANGE(event_day)
            (
                PARTITION p201706 VALUES LESS THAN ('2021-11-01'),
                PARTITION p201707 VALUES LESS THAN ('2021-12-01')
            )
            DISTRIBUTED BY HASH(siteid) BUCKETS 5
            PROPERTIES("replication_num" = "1", "light_schema_change" = "true"); 
         """
     sql """ insert into ${tbName} values('2021-11-01',1,1,'用户A',1),('2021-11-01',1,1,'用户B',1),('2021-11-01',1,1,'用户A',3),('2021-11-02',1,1,'用户A',1),('2021-11-02',1,1,'用户B',1),('2021-11-02',101,112332121,'用户B',112312),('2021-11-02',103,112332211,'用户B',112312); """
     sql """ alter  table ${tbName} modify column citycode string """

     int max_try_time = 100
     while (max_try_time--){
          String result = getJobState(tbName)
          if (result == "FINISHED") {
               sleep(3000)
               qt_desc_uniq_table """ desc ${tbName} """
               qt_sql """ SELECT * FROM ${tbName} order by event_day,citycode  """
               sql """ DROP TABLE  ${tbName} """
               break
          } else {
               sleep(2000)
               if (max_try_time < 1){
                    assertEquals(1,2)
               }
          }
     }
}
