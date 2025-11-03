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

 suite("test_lateral_view_in_subquery") {
     sql """ DROP TABLE IF EXISTS bm """
     sql """ DROP TABLE IF EXISTS gp """
     sql """
         CREATE TABLE IF NOT EXISTS `bm` (
             `id` bigint(20) NULL,
             `hid` smallint(6) NULL,
             `bitmap` bitmap BITMAP_UNION 
         ) ENGINE=OLAP
         AGGREGATE KEY(`id`, `hid`)
         COMMENT "OLAP"
         DISTRIBUTED BY HASH(`hid`) BUCKETS 3
         PROPERTIES (
             "replication_allocation" = "tag.location.default: 1",
             "in_memory" = "false",
             "storage_format" = "V2"
         )
     """
     sql """
         CREATE TABLE IF NOT EXISTS `gp` (
             `id` varchar(128) NOT NULL,
             `uh` bigint(20) NOT NULL,
             `uid` varchar(128) MAX NULL,
             `group` varchar(32) MAX NULL
         ) ENGINE=OLAP
         AGGREGATE KEY(`id`, `uh`)
         COMMENT "OLAP"
         DISTRIBUTED BY HASH(`id`) BUCKETS 3
         PROPERTIES (
             "replication_allocation" = "tag.location.default: 1",
             "in_memory" = "false",
             "storage_format" = "V2"
         )
     """

     sql """
         insert into bm values(20001, 3, to_bitmap(1000)), (20001, 3, to_bitmap(2000));
     """

     sql """
         insert into gp values("1", 1000, "1000", "1000"), ("1", 2000, "1000", "1000"), ("1", 3000, "1000", "1000");
     """

     order_qt_select """
         select * 
         from gp 
         where uh in 
         (
            select uh 
            from 
            (
                select bitmap_intersect(bitmap) as ubm 
                from bm 
                where id in (20001, 30001) and hid=3
            ) t 
            lateral view explode_bitmap(ubm) tmp as uh
        )
     """
 }

