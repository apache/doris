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

 suite("test_bucket_join_with_colocate_table") {
     def colocateTableName = "colocate_table"
     def rightTable = "right_table"


     sql """ DROP TABLE IF EXISTS ${colocateTableName} """
     sql """ DROP TABLE IF EXISTS ${rightTable} """
     sql """
         CREATE TABLE `${colocateTableName}` (
           `c1` int(11) NULL COMMENT "",
           `c2` int(11) NULL COMMENT "",
           `c3` int(11) NULL COMMENT ""
         ) ENGINE=OLAP
         DUPLICATE KEY(`c1`, `c2`, `c3`)
         COMMENT "OLAP"
         PARTITION BY RANGE(`c2`)
         (PARTITION p1 VALUES [("-2147483648"), ("2")),
         PARTITION p2 VALUES [("2"), (MAXVALUE)))
         DISTRIBUTED BY HASH(`c1`) BUCKETS 8
         PROPERTIES (
           "replication_allocation" = "tag.location.default: 1",
           "colocate_with" = "group1",
           "in_memory" = "false",
           "storage_format" = "V2"
         )
     """
     sql """
         CREATE TABLE `${rightTable}` (
           `k1` int(11) NOT NULL COMMENT "",
           `v1` int(11) NOT NULL COMMENT ""
         ) ENGINE=OLAP
         DUPLICATE KEY(`k1`, `v1`)
         COMMENT "OLAP"
         DISTRIBUTED BY HASH(`k1`) BUCKETS 10
         PROPERTIES (
           "replication_allocation" = "tag.location.default: 1",
           "in_memory" = "false",
           "storage_format" = "V2"
         )
     """

     sql """ INSERT INTO ${colocateTableName} VALUES
         (0, 0, 0),
         (1, 1, 1),
         (2, 2, 2),
         (3, 3, 3)
         ;
     """

     sql """ INSERT INTO ${rightTable} VALUES
         (1, 1),
         (2, 2),
         (3, 3),
         (4, 4)
         ;
     """

     // test_vectorized
     sql """ set enable_vectorized_engine = true; """

     qt_select """  select * from ${colocateTableName} right outer join ${rightTable} on ${colocateTableName}.c1 = ${rightTable}.k1; """
 }

