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

 suite("test_bitmap_count") {
     def tableName = "table_bitmap_count"

     sql """ DROP TABLE IF EXISTS ${tableName} """
     sql """
        CREATE TABLE IF NOT EXISTS ${tableName}  (
        `a` datetime NOT NULL,
        `b` date NOT NULL,
        `c` int(11) NOT NULL,
        `d` varchar(50) NOT NULL ,
        `e` varchar(50) NOT NULL,
        `code1` varchar(50) NOT NULL ,
        `code2` varchar(50) NOT NULL ,
        `code3` int(11) NOT NULL DEFAULT "0",
        `code4` int(11) NOT NULL DEFAULT "0" ,
        `code5` varchar(255) NOT NULL DEFAULT "-",
        `code6` varchar(255) NOT NULL DEFAULT "-" ,
        `bitmap7` bitmap BITMAP_UNION NOT NULL
        ) ENGINE=OLAP
        AGGREGATE KEY(`a`, `b`, `c`, `d`, `e`, `code1`, `code2`, `code3`, `code4`, `code5`, `code6`)
        DISTRIBUTED BY HASH(`code1`, `code2`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
     """

     sql "insert into ${tableName} values ('2019-04-26 00:00:00', '2019-04-26', 1, 'Feb', 'time_zone1','aa','bb',2,3,'gid1','pid1',to_bitmap(10));"
     sql "insert into ${tableName} values ('2019-04-26 00:00:00', '2019-04-26', 1, 'Feb', 'time_zone1','aa','bb',2,3,'gid1','pid1',to_bitmap(20));"
     sql "insert into ${tableName} values ('2019-04-26 00:00:00', '2019-04-26', 2, 'Feb', 'time_zone1','aa','bb',2,3,'gid1','pid1',to_bitmap(20));"
     sql "insert into ${tableName} values ('2019-04-26 00:00:00', '2019-04-26', 1, 'Feb', 'time_zone1','aa','bb',2,3,'gid2','pid1',to_bitmap(10));" 
     sql "insert into ${tableName} values ('2019-04-26 00:00:00', '2019-04-26', 1, 'Feb', 'time_zone1','aa','bb',2,3,'gid2','pid1',to_bitmap(20));"
     sql "insert into ${tableName} values ('2019-04-26 00:00:00', '2019-04-26', 1, 'Feb', 'time_zone1','aa','bb',2,3,'gid2','pid1',to_bitmap(30));"
     sql "insert into ${tableName} values ('2019-04-26 00:00:00', '2019-04-26', 2, 'Feb', 'time_zone1','aa','bb',2,3,'gid2','pid1',to_bitmap(20));"

     qt_select_default """ 
      select  code1 ,code2 ,`b`,c, e,code6,code5,BITMAP_UNION_COUNT(bitmap_intersect(bitmap7)) over(PARTITION by code5 order by c) bitmap7
        from ${tableName} WHERE code1 ='aa' and b='2019-04-26' 
        group by 1,2,3,4,5,6,7
        order by 1,2,3,4,5,6,7;
    """
    qt_select_default """ 
      select  code1 ,code2 ,`b`,c, e,code6,code5,BITMAP_UNION_COUNT(bitmap_union(bitmap7)) over(PARTITION by code5 order by c) bitmap7
        from ${tableName} WHERE code1 ='aa' and b='2019-04-26' 
        group by 1,2,3,4,5,6,7
        order by 1,2,3,4,5,6,7;
    """
 }