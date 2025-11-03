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

suite("test_bitmap_coalesce") {
    sql """DROP TABLE IF EXISTS `test_bitmap_t0`"""
    sql """
        CREATE TABLE `test_bitmap_t0` (
          `org_sid` int(11) NOT NULL DEFAULT "0",
          `ct_sid` int(11) NOT NULL DEFAULT "0",
          `userid_bitmap` bitmap BITMAP_UNION NOT NULL
        ) ENGINE=OLAP
        AGGREGATE KEY(`org_sid`, `ct_sid`)
        DISTRIBUTED BY HASH(`org_sid`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """DROP TABLE IF EXISTS `test_bitmap_t1`"""
    sql """
        CREATE TABLE `test_bitmap_t1` (
          `org_sid` int(11) NOT NULL DEFAULT "0",
          `ct_sid` int(11) NOT NULL DEFAULT "0",
          `userid_bitmap` bitmap BITMAP_UNION NOT NULL
        ) ENGINE=OLAP
        AGGREGATE KEY(`org_sid`, `ct_sid`)
        DISTRIBUTED BY HASH(`org_sid`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        INSERT INTO test_bitmap_t0 
        (org_sid,ct_sid,userid_bitmap) 
        VALUES
             (70018,70018,bitmap_from_string('1')),
             (70019,70019,bitmap_from_string('')),  
             (70020,70020,bitmap_from_string('1,2')),
             (70021,70021,bitmap_from_string('2'));
    """


    sql """
        INSERT INTO test_bitmap_t1
        (org_sid,ct_sid,userid_bitmap) 
        VALUES
             (70018,70018,bitmap_from_string('')),
             (70019,70019,bitmap_from_string('1')),
             (70020,70020,bitmap_from_string('1')),
             (70022,70022,bitmap_from_string('2'));
    """

    qt_sql_bitmap_coalesce """
        with tmp as(
        select
           COALESCE (t0.org_sid ,t1.org_sid),
           COALESCE (t0.ct_sid,t1.ct_sid) ct_sid,
           t0.userid_bitmap userid_bitmap0, 
           t1.userid_bitmap userid_bitmap1,
           bitmap_or( COALESCE(t0.userid_bitmap,bitmap_empty()) ,COALESCE(t1.userid_bitmap,bitmap_empty()) ) bitmap_or1 ,
           bitmap_or( t0.userid_bitmap ,t1.userid_bitmap ) bitmap_or2 
        from
           (select
                org_sid ,ct_sid , userid_bitmap
            from test_bitmap_t0
           ) t0
           full  join
           (select
                org_sid, ct_sid,
                bitmap_union( userid_bitmap) userid_bitmap
            from test_bitmap_t1
            group by 1,2
           ) t1 on 
           t0.org_sid = t1.org_sid
           and t0.ct_sid = t1.ct_sid
        )
        select ct_sid,
        	concat("A", bitmap_to_string(userid_bitmap0)) bitmap0_str,
        	concat("A", bitmap_to_string(userid_bitmap1)) bitmap1_str,
        	concat("A", bitmap_to_string(bitmap_or1)) bitmap_or1_str,
        	concat("A", bitmap_to_string(bitmap_or2)) bitmap_or2_str,
        	bitmap_count(bitmap_or1) bitmap_or1_count,
        	bitmap_count(bitmap_or2) bitmap_or2_count
        from tmp
            order by ct_sid; 
    """
}
