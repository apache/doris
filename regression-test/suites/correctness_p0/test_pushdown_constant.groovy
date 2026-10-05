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

suite("test_pushdown_constant") {
 def tblName = "test_pushdown_constant"
 sql """ DROP TABLE IF EXISTS `${tblName}` """
 sql """
     CREATE TABLE IF NOT EXISTS `${tblName}` (
         `id` int
     ) ENGINE=OLAP
     AGGREGATE KEY(`id`)
     COMMENT "OLAP"
     DISTRIBUTED BY HASH(`id`) BUCKETS 1
     PROPERTIES (
         "replication_allocation" = "tag.location.default: 1",
         "in_memory" = "false",
         "storage_format" = "V2"
     );
 """
 sql """
     insert into ${tblName} values(1);
 """

 qt_sql """
     select 1 from ${tblName} where BITMAP_MAX( BITMAP_AND(BITMAP_EMPTY(), coalesce(NULL, bitmap_empty()))) is NULL;
 """
 sql """ DROP TABLE IF EXISTS `${tblName}` """

 sql """
      CREATE TABLE IF NOT EXISTS `${tblName}` (
          `c1` date,
          `c2` datetime
      ) ENGINE=OLAP
      COMMENT "OLAP"
      DISTRIBUTED BY HASH(`c1`) BUCKETS 1
      PROPERTIES (
          "replication_allocation" = "tag.location.default: 1",
          "in_memory" = "false",
          "storage_format" = "V2"
      );
  """
  sql """
      insert into ${tblName} values('20220101', '20220101111111');
  """

  qt_select_all """ select * from ${tblName} """
  qt_predicate """ select * from ${tblName}  where cast(c2 as date) = date '2022-01-01'"""
  sql """ DROP TABLE IF EXISTS `${tblName}` """
}

