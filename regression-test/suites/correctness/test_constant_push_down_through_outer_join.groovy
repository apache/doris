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

 suite("constant_push_down_through_outer_join") {
     sql """ DROP TABLE IF EXISTS wftest1 """
     sql """ DROP TABLE IF EXISTS wftest2 """
     sql """
         CREATE TABLE IF NOT EXISTS `wftest1` (
             `aa` varchar(200) NULL COMMENT "",
             `bb` int NULL COMMENT ""
         ) ENGINE=OLAP
         UNIQUE KEY (`aa`) COMMENT "aa" 
         DISTRIBUTED BY HASH(`aa`) BUCKETS 3
         PROPERTIES (
             "replication_allocation" = "tag.location.default: 1",
             "in_memory" = "false",
             "storage_format" = "V2"
         )
     """
     sql """
         CREATE TABLE IF NOT EXISTS `wftest2` (
             `cc` varchar(200) NULL COMMENT "",
             `dd` int NULL COMMENT ""
         ) ENGINE=OLAP
         UNIQUE KEY (`cc`)
         COMMENT "OLAP"
         DISTRIBUTED BY HASH(`cc`) BUCKETS 3
         PROPERTIES (
             "replication_allocation" = "tag.location.default: 1",
             "in_memory" = "false",
             "storage_format" = "V2"
         )
     """

     sql """
         INSERT INTO  wftest1 VALUES('a', 1), ('b', 1), ('c', 1);
     """

     sql """
         INSERT INTO  wftest2 VALUES('a', 1), ('b', 1), ('d', 1);
     """

     order_qt_select """
         select t.* from (select * from wftest1 t1 left join wftest2 t2 on t1.aa=t2.cc) t where dayofweek(current_date())=8; 
     """
 }

