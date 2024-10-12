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
   // only nereids now
   sql """set enable_nereids_planner = true"""
   sql """set enable_fallback_to_original_planner = false"""
   sql """set enable_nereids_dml = true"""

   // range
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

   // list
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

   // with label - transactions
   def uniqueID1 = Math.abs(UUID.randomUUID().hashCode()).toString()
   def uniqueID2 = Math.abs(UUID.randomUUID().hashCode()).toString()
   def uniqueID3 = Math.abs(UUID.randomUUID().hashCode()).toString()
   sql """ insert overwrite table list1 partition(*) with label `iot_auto_txn${uniqueID1}` values ("BEIJING"), ("7654321"); """
   sql """ insert overwrite table list1 partition(*) with label `iot_auto_txn${uniqueID2}` values ("SHANGHAI"), ("LIST"); """
   sql """ insert overwrite table list1 partition(*) with label `iot_auto_txn${uniqueID3}` values  ("XXX"); """

   def max_try_milli_secs = 10000
   while(max_try_milli_secs) {
      def result = sql " show load where label like 'iot_auto_txn%' order by LoadStartTime desc "
      // the last three loads are loads upper
      if(result[0][2] == "FINISHED" && result[1][2] == "FINISHED" && result[2][2] == "FINISHED" ) {
         break
      } else {
         sleep(1000) // wait 1 second every time
         max_try_milli_secs -= 1000
         if(max_try_milli_secs <= 0) {
            log.info("result: ${result[0][2]}, ${result[1][2]}, ${result[2][2]}")
            fail()
         }
      }
   }

   qt_sql " select * from list1 order by k0; "

   // long partition value
   sql " drop table if exists list_long; "
   sql """
         create table list_long(
            k0 varchar null
         )
         partition by list (k0)
         (
            PARTITION p1 values in (("Beijing"), ("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
            PARTITION p2 values in (("nonono"))
         )
         DISTRIBUTED BY HASH(`k0`) BUCKETS 1
         properties("replication_num" = "1");
      """
   sql """ insert into list_long values ("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"); """
   sql """ insert overwrite table list_long partition(*) values ("Beijing"); """
   qt_sql " select * from list_long order by k0; "
   sql """ insert overwrite table list_long partition(*) values ("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"); """
   qt_sql " select * from list_long order by k0; "

   // miss partitions
   try {
      sql """ insert overwrite table list1 partition(*) values ("BEIJING"), ("invalid"); """
   } catch (Exception e) {
        log.info(e.getMessage())
        assertTrue(e.getMessage().contains('Insert has filtered data in strict mode') || 
            e.getMessage().contains('Cannot found origin partitions in auto detect overwriting'))
    }

   sql " drop table if exists dt; "
   sql """
         create table dt(
            k0 date null
         )
         partition by range (k0)
         (
            PARTITION p10 values less than ("2010-01-01"),
            PARTITION p100 values less than ("2020-01-01"),
            PARTITION pMAX values less than ("2030-01-01")
         )
         DISTRIBUTED BY HASH(`k0`) BUCKETS 1
         properties("replication_num" = "1");
      """
   sql """ insert into dt values ("2005-01-01"), ("2013-02-02"), ("2022-03-03"); """
   sql """ insert overwrite table dt partition(*) values ("2008-01-01"), ("2008-02-02"); """
   qt_sql " select * from dt order by k0; "
   test {
      sql """ insert overwrite table dt partition(*) values ("2023-02-02"), ("3000-12-12"); """
      check { result, exception, startTime, endTime ->
         assertTrue(exception.getMessage().contains('Insert has filtered data in strict mode') || 
               exception.getMessage().contains('Cannot found origin partitions in auto detect overwriting'))
      }
   } 
   // test no rows(no partition hits) overwrite
   sql " drop table if exists dt2"
   sql " create table dt2 like dt"
   sql " insert overwrite table dt2 partition(*) select * from dt2"
   sql " insert overwrite table dt partition(*) select * from dt2"
   sql " insert overwrite table dt partition(p10, pMAX) select * from dt2"
   sql " insert overwrite table dt select * from dt2"
}
