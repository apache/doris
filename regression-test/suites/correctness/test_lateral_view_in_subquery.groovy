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
     sql """ DROP TABLE IF EXISTS user_tag_bitmap """
     sql """ DROP TABLE IF EXISTS tag_group_width """
     sql """
         CREATE TABLE `user_tag_bitmap` (
             `tag_id` bigint(20) NULL COMMENT "标签id",
             `hid` smallint(6) NULL COMMENT "分桶id",
             `user_hash_bitmap` bitmap BITMAP_UNION NULL COMMENT ""
         ) ENGINE=OLAP
         AGGREGATE KEY(`tag_id`, `hid`)
         COMMENT "OLAP"
         DISTRIBUTED BY HASH(`hid`) BUCKETS 3
         PROPERTIES (
             "replication_allocation" = "tag.location.default: 1",
             "in_memory" = "false",
             "storage_format" = "V2"
         )
     """
     sql """
         CREATE TABLE `tag_group_width` (
             `device_id` varchar(128) NOT NULL COMMENT "设备id",
             `user_hash` bigint(20) NOT NULL COMMENT "用户hash，通过对hash算法计算device_id生成",
             `uuid` varchar(128) MAX NULL COMMENT "用户uuid",
             `group_1` varchar(32) MAX NULL COMMENT "标签值"
         ) ENGINE=OLAP
         AGGREGATE KEY(`device_id`, `user_hash`)
         COMMENT "OLAP"
         DISTRIBUTED BY HASH(`device_id`) BUCKETS 3
         PROPERTIES (
             "replication_allocation" = "tag.location.default: 1",
             "in_memory" = "false",
             "storage_format" = "V2"
         )
     """

     sql """
         insert into user_tag_bitmap values(20001, 3, to_bitmap(1000)), (20001, 3, to_bitmap(2000));
     """

     sql """
         insert into tag_group_width values("1", 1000, "1000", "1000"), ("1", 2000, "1000", "1000"), ("1", 3000, "1000", "1000");
     """

     order_qt_select """
         select * 
         from tag_group_width 
         where user_hash in 
         (
            select user_hash 
            from 
            (
                select bitmap_intersect(user_hash_bitmap) as user_bm 
                from user_tag_bitmap 
                where tag_id in (20001, 30001) and hid=3
            ) t 
            lateral view explode_bitmap(user_bm) tmp as user_hash
        )
     """
 }

