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

suite("test_scan_keys_with_bool_type") {
    sql """ DROP TABLE IF EXISTS test_scan_keys_with_bool_type """

    sql """ 
        CREATE TABLE `test_scan_keys_with_bool_type` (
            `col1` tinyint NOT NULL,
            `col2` boolean NOT NULL,
            `col3` tinyint NOT NULL,
            `col5` boolean REPLACE NOT NULL,
            `col4` datetime(2) REPLACE NOT NULL,
            `col6` double REPLACE_IF_NOT_NULL NULL,
            `col7` datetime(3) REPLACE_IF_NOT_NULL NULL
        ) ENGINE=OLAP
        AGGREGATE KEY(`col1`, `col2`, `col3`)
        DISTRIBUTED BY HASH(`col1`, `col2`, `col3`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "true"
        ); 
    """

    sql """ insert into test_scan_keys_with_bool_type values
        ( -100 ,    0 ,  -82 ,    0 , '2023-11-11 10:49:43.00' ,   840968969.872149 , NULL ),
        ( -100 ,    1 ,  -82 ,    1 , '2024-02-16 04:37:37.00' , -1299962421.904282 , NULL ),
        ( -100 ,    1 ,   92 ,    1 , '2024-02-16 04:37:37.00' ,   23423423.0324234 , NULL );
    """

    qt_select1 " select * from test_scan_keys_with_bool_type order by 1, 2, 3, 4, 5, 6, 7; "
    qt_select2 " select * from test_scan_keys_with_bool_type where col1 <= -100 and col2 in (true, false) and col3 = -82 order by 1, 2, 3, 4, 5, 6, 7; "
    qt_select3 " select * from test_scan_keys_with_bool_type where col1 <= -100 and col3 = -82 order by 1, 2, 3, 4, 5, 6, 7; "
    sql """ DROP TABLE IF EXISTS test_scan_keys_with_bool_type2 """

    sql """ 
        CREATE TABLE `test_scan_keys_with_bool_type2` (
            `col1` tinyint NOT NULL,
            `col2` int NOT NULL,
            `col3` tinyint NOT NULL,
            `col5` boolean REPLACE NOT NULL,
            `col4` datetime(2) REPLACE NOT NULL,
            `col6` double REPLACE_IF_NOT_NULL NULL,
            `col7` datetime(3) REPLACE_IF_NOT_NULL NULL
        ) ENGINE=OLAP
        AGGREGATE KEY(`col1`, `col2`, `col3`)
        DISTRIBUTED BY HASH(`col1`, `col2`, `col3`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "true"
        ); 
    """

    sql """ insert into test_scan_keys_with_bool_type2 values
        ( -100 ,    0 ,  -82 ,    0 , '2023-11-11 10:49:43.00' ,   840968969.872149 , NULL ),
        ( -100 ,    1 ,  -82 ,    1 , '2024-02-16 04:37:37.00' , -1299962421.904282 , NULL ),
        ( -100 ,    2 ,  -82 ,    1 , '2024-02-16 04:37:37.00' , -1299962421.904282 , NULL ),
        ( -100 ,    1 ,   92 ,    1 , '2024-02-16 04:37:37.00' ,   23423423.0324234 , NULL );
    """

    qt_select3 " select * from test_scan_keys_with_bool_type2 order by 1, 2, 3, 4, 5, 6, 7; "
    qt_select4 " select * from test_scan_keys_with_bool_type2 where col1 <= -100 and col2 in (1, 2) and col3 = -82 order by 1, 2, 3, 4, 5, 6, 7; "
    qt_select5 " select * from test_scan_keys_with_bool_type2 where col1 <= -100 and col3 = -82 order by 1, 2, 3, 4, 5, 6, 7; "


    sql """ DROP TABLE IF EXISTS test_scan_keys_with_bool_type3 """

    sql """ 
        CREATE TABLE `test_scan_keys_with_bool_type3` (
            `col1` tinyint NOT NULL,
            `col2` char NOT NULL,
            `col3` tinyint NOT NULL,
            `col5` boolean REPLACE NOT NULL,
            `col4` datetime(2) REPLACE NOT NULL,
            `col6` double REPLACE_IF_NOT_NULL NULL,
            `col7` datetime(3) REPLACE_IF_NOT_NULL NULL
        ) ENGINE=OLAP
        AGGREGATE KEY(`col1`, `col2`, `col3`)
        DISTRIBUTED BY HASH(`col1`, `col2`, `col3`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "true"
        ); 
    """

    sql """ insert into test_scan_keys_with_bool_type3 values
        ( -100 ,    'a' ,  -82 ,    0 , '2023-11-11 10:49:43.00' ,   840968969.872149 , NULL ),
        ( -100 ,    'b',  -82 ,    1 , '2024-02-16 04:37:37.00' , -1299962421.904282 , NULL ),
        ( -100 ,    'b' ,   92 ,    1 , '2024-02-16 04:37:37.00' ,   23423423.0324234 , NULL ),
        ( -100 ,    'c' ,   92 ,    1 , '2024-02-16 04:37:37.00' ,   23423423.0324234 , NULL );
    """

    qt_select6 " select * from test_scan_keys_with_bool_type3 order by 1, 2, 3, 4, 5, 6, 7; "
    qt_select7 " select * from test_scan_keys_with_bool_type3 where col1 <= -100 and col2 in ('a', 'b') and col3 = -82 order by 1, 2, 3, 4, 5, 6, 7; "
    qt_select8 " select * from test_scan_keys_with_bool_type3 where col1 <= -100 and col3 = -82 order by 1, 2, 3, 4, 5, 6, 7; "
}
