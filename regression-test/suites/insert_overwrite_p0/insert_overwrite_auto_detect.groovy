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

suite("test_iot_auto_detect") {
   sql """set enable_nereids_planner = true"""
   sql """set enable_fallback_to_original_planner = false"""
   sql """set enable_nereids_dml = true"""

   sql " drop table if exists range1; "
   sql """
         create table range1(
            k0 int null
         )
         partition by range (k0)
         (
            PARTITION p10 values less than (10),
            PARTITION p100 values less than (100),
            PARTITION pMAX values less than (maxvalue)
         )
         DISTRIBUTED BY HASH(`k0`) BUCKETS 1
         properties("replication_num" = "1");
      """
   sql " insert into range1 values (1), (2), (15), (100), (200); "
   qt_sql " select * from range1 order by k0; "
   sql " insert overwrite table range1 partition(*) values (3), (1234); "
   qt_sql " select * from range1 order by k0; "
   sql " insert overwrite table range1 partition(*) values (1), (2), (333), (444), (555); "
   qt_sql " select * from range1 order by k0; "
   sql " insert overwrite table range1 partition(*) values (-100), (-100), (333), (444), (555); "
   qt_sql " select * from range1 order by k0; "
   sql " insert into range1 values (-12345), (12345); "
   sql " insert overwrite table range1 partition(*) values (-100), (-100), (333), (444), (555); "
   qt_sql " select * from range1 order by k0; "

   sql " drop table if exists list1; "
   sql """
         create table list1(
            k0 varchar null
         )
         partition by list (k0)
         (
            PARTITION p1 values in (("Beijing"), ("BEIJING")),
            PARTITION p2 values in (("Shanghai"), ("SHANGHAI")),
            PARTITION p3 values in (("xxx"), ("XXX")),
            PARTITION p4 values in (("list"), ("LIST")),
            PARTITION p5 values in (("1234567"), ("7654321"))
         )
         DISTRIBUTED BY HASH(`k0`) BUCKETS 1
         properties("replication_num" = "1");
      """
   sql """ insert into list1 values ("Beijing"),("Shanghai"),("xxx"),("list"),("1234567"); """
   qt_sql " select * from list1 order by k0; "
   sql """ insert overwrite table list1 partition(*) values ("BEIJING"); """
   qt_sql " select * from list1 order by k0; "
   sql """ insert overwrite table list1 partition(*) values ("7654321"), ("7654321"), ("7654321"); """
   qt_sql " select * from list1 order by k0; "
   sql """ insert overwrite table list1 partition(*) values ("7654321"), ("list"), ("list"), ("LIST"), ("LIST"); """
   qt_sql " select * from list1 order by k0; "
   sql """ insert overwrite table list1 partition(*) values ("BEIJING"), ("SHANGHAI"), ("XXX"), ("LIST"), ("7654321"); """
   qt_sql " select * from list1 order by k0; "
}
